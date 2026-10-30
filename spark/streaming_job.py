"""Job de Spark Structured Streaming (Variante 10: Inventario y fulfillment).

Kafka -> parseo -> transformaciones -> alertas / agregación por ventana -> Parquet.

Persistencia en el Data Lake (Issue #17), cada tabla con su checkpoint (ver spark/lake.py):

    Raw        datalake/raw/eventos
    Processed  datalake/processed/detalle
    Curated    datalake/curated/picking_por_tienda   (KPI por tienda y ventana)
               datalake/curated/alertas               (Issue #16)

`--consola` imprime además el detalle y el agregado en consola (útil para evidencias).

Ejecución (desde la raíz del repo, con Docker):

    docker compose up -d kafka kafka-init
    docker compose up spark                   # arranca el job
    python -m producer.producer --count 200 --rate 20   # en otra terminal

    # procesa lo que haya en el topic y termina (cierra también las últimas ventanas):
    docker compose run --rm spark ... --una-vez   (ver docker-compose.yml)

Variables de entorno: KAFKA_BOOTSTRAP, KAFKA_TOPIC, STARTING_OFFSETS,
VENTANA, WATERMARK, TRIGGER, DATALAKE_PATH, UMBRAL_STOCK_CRITICO, UMBRAL_PICKING_SEC.
"""
from __future__ import annotations

import argparse
import os

from pyspark.sql import DataFrame, SparkSession

from spark.alertas import detectar_alertas
from spark.lake import (CURATED_ALERTAS, CURATED_PICKING, PROCESSED_DETALLE, RAW_EVENTOS,
                        RUTA_DATALAKE_POR_DEFECTO, escribir_parquet, preparar_processed,
                        preparar_raw)
from spark.parseo import parsear_eventos
from spark.transformaciones import (VENTANA_POR_DEFECTO, WATERMARK_POR_DEFECTO,
                                    agregar_metricas_operativas,
                                    agregar_metricas_pedido,
                                    agregar_picking_por_tienda,
                                    limpiar_y_deduplicar)

COLUMNAS_DETALLE = [
    "kafka_key", "kafka_partition", "kafka_offset", "store_id", "order_id", "sku",
    "stock", "picking_time_sec", "items_faltantes", "pedido_incompleto",
    "fill_rate", "es_stockout", "event_time",
]


def construir_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Streaming de inventario y picking")
    p.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"))
    p.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "inventario-fulfillment"))
    p.add_argument("--offsets", default=os.getenv("STARTING_OFFSETS", "earliest"),
                   choices=["earliest", "latest"],
                   help="desde dónde leer la primera vez (sin checkpoint)")
    p.add_argument("--ventana", default=os.getenv("VENTANA", VENTANA_POR_DEFECTO))
    p.add_argument("--watermark", default=os.getenv("WATERMARK", WATERMARK_POR_DEFECTO))
    p.add_argument("--trigger", default=os.getenv("TRIGGER", "10 seconds"))
    p.add_argument("--datalake", default=RUTA_DATALAKE_POR_DEFECTO,
                   help="carpeta base del Data Lake (por defecto 'datalake' o $DATALAKE_PATH)")
    p.add_argument("--una-vez", action="store_true",
                   help="procesa lo disponible y termina (en lugar de correr indefinidamente)")
    p.add_argument("--consola", action="store_true",
                   help="además de Parquet, imprime detalle y agregado en consola")
    return p


def crear_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("rapidgo-v10-streaming")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "3")  # igual a las particiones del topic
        .getOrCreate()
    )


def leer_kafka(spark: SparkSession, bootstrap: str, topic: str, offsets: str) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", offsets)
        .option("failOnDataLoss", "false")
        .load()
    )


def construir_pipeline(df_kafka: DataFrame, ventana: str, watermark: str):
    """Devuelve (detalle, agregado, alertas): eventos limpios, KPIs por tienda y ventana,
    y alertas disparadas por las reglas de spark/reglas.py."""
    eventos = parsear_eventos(df_kafka)
    limpios = limpiar_y_deduplicar(eventos, watermark)
    detalle = agregar_metricas_operativas(agregar_metricas_pedido(limpios))
    agregado = agregar_picking_por_tienda(detalle, ventana)
    alertas = detectar_alertas(detalle)
    return detalle, agregado, alertas


def iniciar_queries(df_kafka: DataFrame, ventana: str, watermark: str, base: str,
                    trigger: str, una_vez: bool = False, consola: bool = False) -> list:
    """Arranca las queries Raw, Processed y Curated (agregado + alertas)."""
    detalle, agregado, alertas = construir_pipeline(df_kafka, ventana, watermark)
    queries = [
        escribir_parquet(preparar_raw(df_kafka), RAW_EVENTOS, base, trigger, una_vez,
                         particion="fecha_ingesta"),
        escribir_parquet(preparar_processed(detalle), PROCESSED_DETALLE, base, trigger,
                         una_vez, particion="fecha"),
        escribir_parquet(agregado, CURATED_PICKING, base, trigger, una_vez),
        escribir_parquet(alertas, CURATED_ALERTAS, base, trigger, una_vez),
    ]
    if consola:
        queries.append(escribir_consola(detalle.select(*COLUMNAS_DETALLE), "detalle",
                                        "append", trigger))
        queries.append(escribir_consola(agregado, "picking_por_tienda", "update", trigger))
    return queries


def escribir_consola(df: DataFrame, nombre: str, modo: str, trigger: str):
    return (
        df.writeStream
        .queryName(nombre)
        .format("console")
        .outputMode(modo)
        .option("truncate", "false")
        .option("numRows", 20)
        .trigger(processingTime=trigger)
        .start()
    )


def main() -> None:
    args = construir_parser().parse_args()
    spark = crear_spark()
    spark.sparkContext.setLogLevel("WARN")

    crudo = leer_kafka(spark, args.bootstrap, args.topic, args.offsets)
    queries = iniciar_queries(crudo, args.ventana, args.watermark, args.datalake,
                              args.trigger, una_vez=args.una_vez, consola=args.consola)
    if args.una_vez:
        for q in queries:          # --una-vez: esperar a que terminen TODAS las queries
            q.awaitTermination()
    else:
        spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()

"""Job de Spark Structured Streaming (Variante 10: Inventario y fulfillment).

Kafka -> parseo -> transformaciones -> agregación por ventana -> consola.

La salida a consola es temporal: la persistencia en Parquet con checkpoint
(Raw -> Processed -> Curated) es el Issue #17 y reemplaza `escribir_consola`.

Ejecución (desde la raíz del repo, con Docker):

    docker compose up -d kafka kafka-init
    docker compose up spark                   # arranca el job
    python -m producer.producer --count 200 --rate 20   # en otra terminal

Variables de entorno: KAFKA_BOOTSTRAP, KAFKA_TOPIC, STARTING_OFFSETS,
VENTANA, WATERMARK, TRIGGER.
"""
from __future__ import annotations

import argparse
import os

from pyspark.sql import DataFrame, SparkSession

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
    """Devuelve (detalle, agregado): eventos limpios y KPIs por tienda y ventana."""
    eventos = parsear_eventos(df_kafka)
    limpios = limpiar_y_deduplicar(eventos, watermark)
    detalle = agregar_metricas_operativas(agregar_metricas_pedido(limpios))
    agregado = agregar_picking_por_tienda(detalle, ventana)
    return detalle, agregado


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
    detalle, agregado = construir_pipeline(crudo, args.ventana, args.watermark)

    escribir_consola(detalle.select(*COLUMNAS_DETALLE), "detalle", "append", args.trigger)
    escribir_consola(agregado, "picking_por_tienda", "update", args.trigger)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()

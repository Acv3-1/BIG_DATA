"""Data Lake en Parquet con checkpoint: Raw -> Processed -> Curated (Issue #17).

    datalake/
      raw/eventos/                  mensaje de Kafka tal cual llegó (value en texto) + metadatos
      processed/detalle/            eventos limpios, sin duplicados y con métricas por pedido
      curated/picking_por_tienda/   KPIs por tienda y ventana de tiempo (listos para consumir)
      curated/alertas/              una fila por alerta disparada (stock crítico / picking lento)
      _checkpoints/<capa>/<tabla>/  estado de cada query de streaming (offsets + estado)

Cada tabla tiene su propio checkpoint: si el job se reinicia, retoma desde el último
offset confirmado de Kafka en lugar de releer todo o perder datos. Para reprocesar desde
cero hay que borrar los dos: la carpeta de la tabla y su checkpoint.

Nota sobre Curated: el sink Parquet solo admite modo `append`, así que una ventana se
escribe cuando el watermark la cierra (ventana_fin + watermark), no antes. Con las
ventanas de 5 min y watermark de 10 min, un KPI aparece ~15 min después de iniciar la
ventana, o al terminar una ejecución --una-vez (que fuerza un último micro-lote).
"""
from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

RUTA_DATALAKE_POR_DEFECTO = os.getenv("DATALAKE_PATH", "datalake")

RAW_EVENTOS = "raw/eventos"
PROCESSED_DETALLE = "processed/detalle"
CURATED_PICKING = "curated/picking_por_tienda"
CURATED_ALERTAS = "curated/alertas"

COLUMNAS_PROCESSED = [
    "kafka_key", "kafka_partition", "kafka_offset", "event_id", "team", "store_id",
    "order_id", "sku", "stock", "items_requested", "items_picked", "picking_time_sec",
    "picking_time_min", "items_faltantes", "pedido_incompleto", "fill_rate",
    "es_stockout", "event_time",
]


def rutas(base: str = RUTA_DATALAKE_POR_DEFECTO, tabla: str = RAW_EVENTOS) -> tuple[str, str]:
    """Devuelve (ruta de datos, ruta de checkpoint) de una tabla del lake."""
    base_path = Path(base)
    return str(base_path / tabla), str(base_path / "_checkpoints" / tabla)


def preparar_raw(df_kafka: DataFrame) -> DataFrame:
    """Capa Raw: el mensaje sin interpretar (value como texto) más sus metadatos de Kafka.

    No se descarta nada, ni siquiera el JSON malformado: Raw es la fuente de verdad
    para poder reprocesar. `fecha_ingesta` (UTC) particiona la carpeta.
    """
    return df_kafka.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("value"),
        F.col("topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.to_date(F.col("timestamp")).alias("fecha_ingesta"),
    )


def preparar_processed(detalle: DataFrame) -> DataFrame:
    """Capa Processed: eventos limpios con métricas; particionado por día del evento."""
    return (
        detalle.select(*COLUMNAS_PROCESSED)
        .withColumn("fecha", F.to_date("event_time"))
    )


def escribir_parquet(df: DataFrame, tabla: str, base: str = RUTA_DATALAKE_POR_DEFECTO,
                     trigger: str = "10 seconds", una_vez: bool = False,
                     particion: str | None = None) -> StreamingQuery:
    """Escribe un stream en Parquet (modo append) con su checkpoint.

    - `trigger`: intervalo de los micro-lotes (p. ej. "10 seconds").
    - `una_vez`: procesa todo lo disponible y termina (Trigger.AvailableNow); útil para
      demos, pruebas y para forzar el cierre de las últimas ventanas.
    - `particion`: columna por la que se particiona la carpeta (opcional).
    """
    ruta_datos, ruta_checkpoint = rutas(base, tabla)
    escritor = (
        df.writeStream
        .queryName(tabla.replace("/", "_"))
        .format("parquet")
        .outputMode("append")
        .option("path", ruta_datos)
        .option("checkpointLocation", ruta_checkpoint)
    )
    if una_vez:
        escritor = escritor.trigger(availableNow=True)
    else:
        escritor = escritor.trigger(processingTime=trigger)
    if particion:
        escritor = escritor.partitionBy(particion)
    return escritor.start()

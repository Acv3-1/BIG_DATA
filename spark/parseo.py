"""Parseo de los mensajes de Kafka a columnas tipadas (Issue #14)."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark.esquema import ESQUEMA_EVENTO

# event_ts llega como "2026-10-29T15:55:00.123Z" (UTC, milisegundos).
FORMATO_TS = "yyyy-MM-dd'T'HH:mm:ss.SSSX"


def parsear_eventos(df_kafka: DataFrame) -> DataFrame:
    """Convierte el DataFrame crudo de Kafka (key, value, partition, offset...) en eventos.

    - Conserva la key, la partición y el offset de Kafka para poder observarlos
      (sirven como evidencia en los checkpoints y para calibrar el particionado).
    - Parsea el JSON de `value` con el esquema del contrato.
    - Agrega `event_time` (timestamp real) a partir de `event_ts`; es la columna que
      usan el watermark y las ventanas.

    Un mensaje con JSON malformado produce columnas nulas (from_json no falla);
    la limpieza posterior (transformaciones.limpiar_y_deduplicar) lo descarta.
    """
    return (
        df_kafka
        .select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.from_json(F.col("value").cast("string"), ESQUEMA_EVENTO).alias("e"),
        )
        .select("kafka_key", "kafka_partition", "kafka_offset", "e.*")
        .withColumn("event_time", F.to_timestamp(F.col("event_ts"), FORMATO_TS))
    )

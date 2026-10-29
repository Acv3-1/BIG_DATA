"""Esquema del evento de inventario y picking (Variante 10).

Debe coincidir con el contrato definido en producer/eventos.py (CAMPOS_REQUERIDOS).
tests/test_spark_transformaciones.py verifica que ambos se mantengan sincronizados.
"""
from __future__ import annotations

from pyspark.sql.types import IntegerType, StringType, StructField, StructType

ESQUEMA_EVENTO = StructType([
    StructField("event_id", StringType()),
    StructField("team", StringType()),
    StructField("variant", IntegerType()),
    StructField("store_id", StringType()),
    StructField("order_id", StringType()),
    StructField("sku", StringType()),
    StructField("stock", IntegerType()),
    StructField("items_requested", IntegerType()),
    StructField("items_picked", IntegerType()),
    StructField("picking_time_sec", IntegerType()),
    StructField("event_ts", StringType()),
])

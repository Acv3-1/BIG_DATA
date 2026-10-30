"""Transformaciones y agregación por ventana (Issue #15).

Tres transformaciones encadenables + una agregación por ventana:

    1. limpiar_y_deduplicar       -> descarta eventos inválidos y duplicados
    2. agregar_metricas_pedido    -> items_faltantes, pedido_incompleto, fill_rate
    3. agregar_metricas_operativas-> picking_time_min, es_stockout
    4. agregar_picking_por_tienda -> picking promedio (y más KPIs) por tienda y ventana

Todas reciben y devuelven DataFrames, y funcionan igual en streaming y en batch
(por eso se pueden probar con datos pequeños en tests/).

Nota: aquí NO se definen umbrales de alerta (stock crítico, picking fuera de tiempo);
eso corresponde al Issue #16. Estas columnas son la base sobre la que se calculan.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

WATERMARK_POR_DEFECTO = "10 minutes"
VENTANA_POR_DEFECTO = "5 minutes"


def limpiar_y_deduplicar(df: DataFrame, watermark: str = WATERMARK_POR_DEFECTO) -> DataFrame:
    """Transformación 1: valida el evento y elimina duplicados.

    Se descartan los eventos que incumplen el contrato (campos nulos, stock negativo,
    tiempo de picking <= 0, items_picked fuera de [0, items_requested]) y los mensajes
    con JSON malformado (todos sus campos quedan nulos).

    El watermark acota el estado que Spark guarda para detectar duplicados: Kafka
    entrega "al menos una vez", así que un reintento del producer puede repetir un
    event_id. Se deduplica por (event_id, event_time) para que el estado pueda limpiarse.
    """
    validos = df.filter(
        F.col("event_id").isNotNull()
        & F.col("event_time").isNotNull()
        & F.col("store_id").isNotNull()
        & F.col("sku").isNotNull()
        & (F.col("stock") >= 0)
        & (F.col("picking_time_sec") > 0)
        & (F.col("items_requested") > 0)
        & (F.col("items_picked") >= 0)
        & (F.col("items_picked") <= F.col("items_requested"))
    )
    return (
        validos
        .withWatermark("event_time", watermark)
        .dropDuplicates(["event_id", "event_time"])
    )


def agregar_metricas_pedido(df: DataFrame) -> DataFrame:
    """Transformación 2: métricas de cumplimiento del pedido.

    - items_faltantes:   unidades solicitadas que no se pudieron pickear.
    - pedido_incompleto: True si faltó al menos una unidad (KPI "pedidos incompletos").
    - fill_rate:         fracción de lo solicitado que sí se pickeó (0.0 a 1.0).
    """
    return (
        df
        .withColumn("items_faltantes", F.col("items_requested") - F.col("items_picked"))
        .withColumn("pedido_incompleto", F.col("items_picked") < F.col("items_requested"))
        .withColumn("fill_rate",
                    F.round(F.col("items_picked") / F.col("items_requested"), 4))
    )


def agregar_metricas_operativas(df: DataFrame) -> DataFrame:
    """Transformación 3: métricas de tiempo y de inventario.

    - picking_time_min: tiempo de picking en minutos (más legible que segundos).
    - es_stockout:      True si el SKU está agotado (stock == 0) (KPI "stockouts").
    """
    return (
        df
        .withColumn("picking_time_min", F.round(F.col("picking_time_sec") / 60.0, 2))
        .withColumn("es_stockout", F.col("stock") == 0)
    )


def agregar_picking_por_tienda(df: DataFrame, ventana: str = VENTANA_POR_DEFECTO) -> DataFrame:
    """Agregación por ventana: picking promedio (y KPIs asociados) por tienda.

    Ventana tumbling sobre `event_time` (usa el watermark definido en
    limpiar_y_deduplicar). Una fila por (ventana, tienda).
    """
    return (
        df
        .groupBy(F.window("event_time", ventana).alias("ventana"), "store_id")
        .agg(
            F.count("*").alias("pedidos"),
            F.round(F.avg("picking_time_sec"), 2).alias("picking_promedio_sec"),
            F.max("picking_time_sec").alias("picking_max_sec"),
            F.sum(F.col("pedido_incompleto").cast("int")).alias("pedidos_incompletos"),
            F.sum(F.col("es_stockout").cast("int")).alias("stockouts"),
        )
        .select(
            F.col("ventana.start").alias("ventana_inicio"),
            F.col("ventana.end").alias("ventana_fin"),
            "store_id",
            "pedidos",
            "picking_promedio_sec",
            "picking_max_sec",
            "pedidos_incompletos",
            "stockouts",
        )
    )

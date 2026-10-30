"""Alertas en Spark (Issue #16): las reglas de spark/reglas.py aplicadas a un DataFrame.

`detectar_alertas` recibe el detalle limpio (salida de la transformación 1) y devuelve
una fila por alerta disparada. Un evento que cumple las dos reglas genera dos filas.
Funciona igual en batch y en streaming (solo usa filter/select/unionByName).
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from spark.reglas import (PICKING_FUERA_DE_TIEMPO, SEVERIDAD_ALTA, SEVERIDAD_MEDIA,
                          STOCK_CRITICO, UMBRAL_PICKING_SEC, UMBRAL_STOCK_CRITICO)

COLUMNAS_ALERTA = [
    "event_id", "event_time", "store_id", "order_id", "sku",
    "tipo_alerta", "severidad", "valor", "umbral", "mensaje",
]


def _alerta(df: DataFrame, condicion: Column, tipo: str, severidad: Column,
            valor: Column, umbral: int, mensaje: Column) -> DataFrame:
    return (
        df.filter(condicion)
        .select(
            "event_id", "event_time", "store_id", "order_id", "sku",
            F.lit(tipo).alias("tipo_alerta"),
            severidad.alias("severidad"),
            valor.cast("int").alias("valor"),
            F.lit(umbral).alias("umbral"),
            mensaje.alias("mensaje"),
        )
    )


def detectar_alertas(df: DataFrame,
                     umbral_stock: int = UMBRAL_STOCK_CRITICO,
                     umbral_picking: int = UMBRAL_PICKING_SEC) -> DataFrame:
    """Devuelve las alertas de stock crítico y de picking fuera de tiempo."""
    stock_critico = _alerta(
        df,
        F.col("stock") <= umbral_stock,
        STOCK_CRITICO,
        F.when(F.col("stock") == 0, SEVERIDAD_ALTA).otherwise(SEVERIDAD_MEDIA),
        F.col("stock"),
        umbral_stock,
        F.concat(F.lit("Stock crítico del "), F.col("sku"), F.lit(" en "), F.col("store_id"),
                 F.lit(": "), F.col("stock"), F.lit(" unidades (umbral "),
                 F.lit(umbral_stock), F.lit(")")),
    )
    picking_lento = _alerta(
        df,
        F.col("picking_time_sec") > umbral_picking,
        PICKING_FUERA_DE_TIEMPO,
        F.when(F.col("picking_time_sec") > 2 * umbral_picking, SEVERIDAD_ALTA)
         .otherwise(SEVERIDAD_MEDIA),
        F.col("picking_time_sec"),
        umbral_picking,
        F.concat(F.lit("Picking fuera de tiempo en "), F.col("store_id"), F.lit(", pedido "),
                 F.col("order_id"), F.lit(": "), F.col("picking_time_sec"),
                 F.lit(" s (umbral "), F.lit(umbral_picking), F.lit(" s)")),
    )
    return stock_critico.unionByName(picking_lento).select(*COLUMNAS_ALERTA)

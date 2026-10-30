"""Reglas de alerta y umbrales (Issue #16). Python puro: NO depende de Spark.

Dos alertas definidas en la Semana 1:

    1. STOCK_CRITICO         -> el SKU tiene poco stock (stock <= UMBRAL_STOCK_CRITICO)
    2. PICKING_FUERA_DE_TIEMPO -> el picking tardó demasiado (> UMBRAL_PICKING_SEC)

Los umbrales viven aquí, en un solo lugar, y se pueden ajustar por variable de
entorno sin tocar el código (Issue #20: calibración con valores reales):

    UMBRAL_STOCK_CRITICO   (unidades, por defecto 5)
    UMBRAL_PICKING_SEC     (segundos, por defecto 600 = 10 min)

Este módulo se importa sin pyspark, así que sus pruebas corren también en el CI básico.
La versión en columnas de Spark está en spark/alertas.py y usa estas mismas constantes.
"""
from __future__ import annotations

import os

STOCK_CRITICO = "STOCK_CRITICO"
PICKING_FUERA_DE_TIEMPO = "PICKING_FUERA_DE_TIEMPO"
TIPOS_ALERTA = (STOCK_CRITICO, PICKING_FUERA_DE_TIEMPO)

SEVERIDAD_ALTA = "alta"
SEVERIDAD_MEDIA = "media"


def _entero_de_entorno(nombre: str, defecto: int) -> int:
    valor = os.getenv(nombre)
    return int(valor) if valor not in (None, "") else defecto


# Umbrales iniciales. El producer genera stock crítico en 0-5 y picking lento en 600-1200 s.
UMBRAL_STOCK_CRITICO = _entero_de_entorno("UMBRAL_STOCK_CRITICO", 5)
UMBRAL_PICKING_SEC = _entero_de_entorno("UMBRAL_PICKING_SEC", 600)


def es_stock_critico(stock: int, umbral: int = UMBRAL_STOCK_CRITICO) -> bool:
    """True si el stock es menor o igual al umbral (incluye el agotado, stock == 0)."""
    return stock <= umbral


def es_picking_fuera_de_tiempo(picking_time_sec: int, umbral: int = UMBRAL_PICKING_SEC) -> bool:
    """True si el picking tardó estrictamente más que el umbral."""
    return picking_time_sec > umbral


def severidad_stock(stock: int) -> str:
    """Un SKU agotado (stock 0) es más grave que uno con poco stock."""
    return SEVERIDAD_ALTA if stock == 0 else SEVERIDAD_MEDIA


def severidad_picking(picking_time_sec: int, umbral: int = UMBRAL_PICKING_SEC) -> str:
    """Más del doble del umbral es grave; entre 1x y 2x es moderado."""
    return SEVERIDAD_ALTA if picking_time_sec > 2 * umbral else SEVERIDAD_MEDIA


def evaluar_alertas(evento: dict, umbral_stock: int = UMBRAL_STOCK_CRITICO,
                    umbral_picking: int = UMBRAL_PICKING_SEC) -> list[dict]:
    """Devuelve las alertas que dispara un evento (lista vacía si no dispara ninguna).

    Un mismo evento puede disparar las dos alertas a la vez.
    """
    alertas = []
    if es_stock_critico(evento["stock"], umbral_stock):
        alertas.append({
            "tipo_alerta": STOCK_CRITICO,
            "severidad": severidad_stock(evento["stock"]),
            "valor": evento["stock"],
            "umbral": umbral_stock,
        })
    if es_picking_fuera_de_tiempo(evento["picking_time_sec"], umbral_picking):
        alertas.append({
            "tipo_alerta": PICKING_FUERA_DE_TIEMPO,
            "severidad": severidad_picking(evento["picking_time_sec"], umbral_picking),
            "valor": evento["picking_time_sec"],
            "umbral": umbral_picking,
        })
    return alertas

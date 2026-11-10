"""Reglas de calidad de datos del pipeline (Issue #25). Python puro (solo biblioteca estándar).

Se aplican a los mensajes de la capa Raw (el JSON tal cual llegó de Kafka), porque es ahí
donde aparecen los datos malos: Spark limpia al pasar a Processed, así que ya no se verían.

Un evento es INVÁLIDO si incumple al menos una regla:

    json_malformado       el mensaje no es un JSON de objeto
    campos_nulos          falta o es null alguno de los 11 campos del contrato
    tipo_incorrecto       un número llegó como texto (p. ej. stock = "mucho") o viceversa
    stock_negativo        stock < 0
    sku_invalido          el sku no cumple el formato SKU-0000
    tienda_invalida       el store_id no cumple el formato S000
    picking_no_positivo   picking_time_sec <= 0
    items_incoherentes    items_requested <= 0, items_picked < 0 o items_picked > items_requested

La tarea falla si la proporción de eventos inválidos supera el umbral (5 % por defecto).
Se evalúan solo los eventos más recientes (500 por defecto), para que un problema ya
corregido en el origen no bloquee el pipeline para siempre.

Variables de entorno (se leen al ejecutar, así se pueden cambiar sin tocar el código):

    UMBRAL_INVALIDOS   proporción máxima aceptada de eventos inválidos (0.05 = 5 %)
    EVENTOS_CALIDAD    cuántos eventos recientes de Raw se evalúan (500)
"""
from __future__ import annotations

import json
import os
import re
from datetime import datetime

CAMPOS_REQUERIDOS = (
    "event_id", "team", "variant", "store_id", "order_id", "sku", "stock",
    "items_requested", "items_picked", "picking_time_sec", "event_ts",
)
CAMPOS_ENTEROS = ("variant", "stock", "items_requested", "items_picked", "picking_time_sec")
CAMPOS_TEXTO = ("event_id", "team", "store_id", "order_id", "sku", "event_ts")

PATRON_SKU = re.compile(r"^SKU-\d{4}$")
PATRON_TIENDA = re.compile(r"^S\d{3}$")

MOTIVOS = (
    "json_malformado", "campos_nulos", "tipo_incorrecto", "stock_negativo",
    "sku_invalido", "tienda_invalida", "picking_no_positivo", "items_incoherentes",
)

UMBRAL_INVALIDOS_POR_DEFECTO = 0.05
EVENTOS_CALIDAD_POR_DEFECTO = 500


def umbral_invalidos() -> float:
    valor = os.getenv("UMBRAL_INVALIDOS")
    return float(valor) if valor not in (None, "") else UMBRAL_INVALIDOS_POR_DEFECTO


def eventos_calidad() -> int:
    valor = os.getenv("EVENTOS_CALIDAD")
    return int(valor) if valor not in (None, "") else EVENTOS_CALIDAD_POR_DEFECTO


def _es_entero(x) -> bool:
    return isinstance(x, int) and not isinstance(x, bool)


def motivos_evento(valor) -> list[str]:
    """Devuelve los motivos por los que un mensaje es inválido (lista vacía si es válido).

    `valor` es el texto JSON del mensaje (o ya un dict).
    """
    if isinstance(valor, dict):
        evento = valor
    else:
        try:
            evento = json.loads(valor)
        except (TypeError, ValueError):
            return ["json_malformado"]
        if not isinstance(evento, dict):
            return ["json_malformado"]

    motivos: list[str] = []
    if any(evento.get(c) is None for c in CAMPOS_REQUERIDOS):
        motivos.append("campos_nulos")

    # Los chequeos siguientes solo miran los campos presentes (los nulos ya se contaron).
    if any(evento.get(c) is not None and not _es_entero(evento[c]) for c in CAMPOS_ENTEROS) \
            or any(evento.get(c) is not None and not isinstance(evento[c], str)
                   for c in CAMPOS_TEXTO):
        motivos.append("tipo_incorrecto")

    stock, picking = evento.get("stock"), evento.get("picking_time_sec")
    pedidos, pickeados = evento.get("items_requested"), evento.get("items_picked")
    sku, tienda = evento.get("sku"), evento.get("store_id")

    if _es_entero(stock) and stock < 0:
        motivos.append("stock_negativo")
    if isinstance(sku, str) and not PATRON_SKU.match(sku):
        motivos.append("sku_invalido")
    if isinstance(tienda, str) and not PATRON_TIENDA.match(tienda):
        motivos.append("tienda_invalida")
    if _es_entero(picking) and picking <= 0:
        motivos.append("picking_no_positivo")
    if _es_entero(pedidos) and _es_entero(pickeados) and (
            pedidos <= 0 or pickeados < 0 or pickeados > pedidos):
        motivos.append("items_incoherentes")
    return motivos


def seleccionar_recientes(filas: list[dict], n: int) -> list:
    """Devuelve el `value` de los `n` mensajes más recientes (por kafka_timestamp).

    `filas` son dicts con las claves `value` y `kafka_timestamp` (puede ser None).
    """
    minimo = datetime.min
    ordenadas = sorted(
        filas,
        key=lambda f: (f.get("kafka_timestamp") is not None,
                       f.get("kafka_timestamp").replace(tzinfo=None)
                       if f.get("kafka_timestamp") is not None else minimo),
    )
    return [f.get("value") for f in ordenadas[-n:]] if n > 0 else []


def evaluar_calidad(valores: list, umbral: float | None = None) -> dict:
    """Evalúa una lista de mensajes y devuelve un resumen serializable a JSON (va por XCom).

    Claves: ok, total, validos, invalidos, tasa_invalidos, umbral_invalidos,
    por_motivo (conteo por cada motivo; un evento puede tener varios) y ejemplos
    (hasta 3 mensajes inválidos, recortados, para el log).
    """
    umbral = umbral_invalidos() if umbral is None else umbral
    por_motivo = {m: 0 for m in MOTIVOS}
    invalidos = 0
    ejemplos: list[dict] = []
    for valor in valores:
        motivos = motivos_evento(valor)
        if not motivos:
            continue
        invalidos += 1
        for m in motivos:
            por_motivo[m] += 1
        if len(ejemplos) < 3:
            ejemplos.append({"motivos": motivos, "mensaje": str(valor)[:160]})

    total = len(valores)
    tasa = round(invalidos / total, 4) if total else 0.0
    return {
        "ok": total > 0 and tasa <= umbral,
        "total": total,
        "validos": total - invalidos,
        "invalidos": invalidos,
        "tasa_invalidos": tasa,
        "umbral_invalidos": umbral,
        "por_motivo": por_motivo,
        "ejemplos": ejemplos,
    }


def describir(calidad: dict) -> str:
    """Texto corto para el mensaje de error y el log de la task."""
    if calidad["total"] == 0:
        return "Raw no tiene eventos para evaluar"
    motivos = ", ".join(f"{m}={n}" for m, n in calidad["por_motivo"].items() if n) or "ninguno"
    return (f"{calidad['invalidos']} de {calidad['total']} eventos inválidos "
            f"({calidad['tasa_invalidos']:.1%}; umbral {calidad['umbral_invalidos']:.1%}). "
            f"Motivos: {motivos}")

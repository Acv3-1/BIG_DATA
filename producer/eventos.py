"""Generador de eventos de inventario y picking (Variante 10: Inventario y fulfillment).

Este módulo NO depende de Kafka, así que se puede importar en pruebas y en CI.
Cada evento representa el picking de un pedido en una tienda aliada, junto con
el stock observado del SKU. Se inyectan casos anómalos a propósito (stock bajo,
picking lento, pedidos incompletos) para que las alertas y los KPIs tengan qué detectar.

Ajusta los nombres de los campos al contrato JSON definido en la Semana 1 (samples/).
"""
from __future__ import annotations

import random
import uuid
from datetime import datetime, timezone

TEAM_POR_DEFECTO = "equipo-variante10"
VARIANTE = 10
TIENDAS = [f"S{n:03d}" for n in range(1, 7)]   # S001 ... S006
SKUS = [f"SKU-{n:04d}" for n in range(1, 21)]  # SKU-0001 ... SKU-0020

CAMPOS_REQUERIDOS = {
    "event_id": str,
    "team": str,
    "variant": int,
    "store_id": str,
    "order_id": str,
    "sku": str,
    "stock": int,
    "items_requested": int,
    "items_picked": int,
    "picking_time_sec": int,
    "event_ts": str,
}


def generar_evento(rng: random.Random, team: str = TEAM_POR_DEFECTO,
                   tiendas: list[str] = TIENDAS, skus: list[str] = SKUS,
                   ahora: datetime | None = None) -> dict:
    """Devuelve un evento (dict) listo para serializar a JSON."""
    ahora = ahora or datetime.now(timezone.utc)

    # Stock: ~12 % de los eventos llegan con stock crítico (0-5 unidades).
    stock = rng.randint(0, 5) if rng.random() < 0.12 else rng.randint(6, 200)

    # Tiempo de picking en segundos: normal alrededor de 5 min, con ~8 % de casos lentos.
    picking = max(30, int(rng.gauss(300, 80)))
    if rng.random() < 0.08:
        picking = rng.randint(600, 1200)

    # Pedido incompleto: mucho más probable cuando el stock es crítico.
    solicitados = rng.randint(1, 8)
    prob_incompleto = 0.5 if stock <= 5 else 0.05
    if rng.random() < prob_incompleto:
        pickeados = rng.randint(0, solicitados - 1)
    else:
        pickeados = solicitados

    return {
        "event_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        "team": team,
        "variant": VARIANTE,
        "store_id": rng.choice(tiendas),
        "order_id": f"PED-{rng.randint(100000, 999999)}",
        "sku": rng.choice(skus),
        "stock": stock,
        "items_requested": solicitados,
        "items_picked": pickeados,
        "picking_time_sec": picking,
        "event_ts": ahora.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
    }


def clave_particion(evento: dict) -> str:
    """Key de Kafka: todos los eventos de una tienda caen en la misma partición."""
    return evento["store_id"]


def validar_evento(evento: dict) -> list[str]:
    """Devuelve la lista de problemas del evento (vacía si es válido)."""
    problemas = []
    for campo, tipo in CAMPOS_REQUERIDOS.items():
        if campo not in evento:
            problemas.append(f"falta el campo '{campo}'")
        elif not isinstance(evento[campo], tipo):
            problemas.append(f"'{campo}' debe ser {tipo.__name__}")
    return problemas

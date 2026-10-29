"""Pruebas del contrato del evento (Variante 10: Inventario y fulfillment).

Verifican que los eventos generados por el producer cumplan el contrato:
campos obligatorios, tipos correctos y valores coherentes.
"""
import json
import random
from pathlib import Path

from producer.eventos import (CAMPOS_REQUERIDOS, clave_particion,
                              generar_evento, validar_evento)


def _eventos(n=500, semilla=1):
    rng = random.Random(semilla)
    return [generar_evento(rng) for _ in range(n)]


def test_evento_cumple_contrato():
    for evento in _eventos():
        assert validar_evento(evento) == []


def test_evento_tiene_todos_los_campos():
    evento = _eventos(1)[0]
    assert set(CAMPOS_REQUERIDOS) <= set(evento)


def test_evento_identifica_al_equipo():
    evento = _eventos(1)[0]
    assert evento["team"]
    assert evento["variant"] == 10


def test_valores_coherentes():
    for e in _eventos():
        assert e["stock"] >= 0
        assert 0 <= e["items_picked"] <= e["items_requested"]
        assert e["picking_time_sec"] > 0


def test_evento_es_serializable_a_json():
    evento = _eventos(1)[0]
    assert json.loads(json.dumps(evento)) == evento


def test_key_de_kafka_es_la_tienda():
    for e in _eventos(50):
        assert clave_particion(e) == e["store_id"]


def test_validar_detecta_campo_faltante():
    evento = _eventos(1)[0]
    del evento["sku"]
    assert any("sku" in p for p in validar_evento(evento))


def test_validar_detecta_tipo_incorrecto():
    evento = _eventos(1)[0]
    evento["stock"] = "mucho"
    assert any("stock" in p for p in validar_evento(evento))


def test_ejemplos_de_samples_son_json_validos():
    for archivo in Path("samples").glob("*.json"):
        json.loads(archivo.read_text(encoding="utf-8"))

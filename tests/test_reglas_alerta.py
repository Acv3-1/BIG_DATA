"""Pruebas de las 2 reglas de alerta (Issue #16, Python puro).

No requieren Spark ni Java, así que corren también en el CI básico.
"""
import importlib
import random

import pytest

from producer.eventos import generar_evento
from spark import reglas
from spark.reglas import (PICKING_FUERA_DE_TIEMPO, STOCK_CRITICO, es_picking_fuera_de_tiempo,
                          es_stock_critico, evaluar_alertas, severidad_picking,
                          severidad_stock)


def _evento(**cambios):
    base = {"stock": 50, "picking_time_sec": 300}
    base.update(cambios)
    return base


# ------------------------------------------------------------------ regla 1: stock crítico

def test_stock_critico_en_el_limite():
    assert es_stock_critico(5, umbral=5) is True      # igual al umbral: alerta
    assert es_stock_critico(6, umbral=5) is False     # justo encima: sin alerta


def test_stock_agotado_es_critico():
    assert es_stock_critico(0, umbral=5) is True


def test_umbral_por_defecto_de_stock_es_5():
    assert reglas.UMBRAL_STOCK_CRITICO == 5


# ------------------------------------------------------------------ regla 2: picking lento

def test_picking_fuera_de_tiempo_en_el_limite():
    assert es_picking_fuera_de_tiempo(600, umbral=600) is False   # igual al umbral: no alerta
    assert es_picking_fuera_de_tiempo(601, umbral=600) is True


def test_umbral_por_defecto_de_picking_es_600_segundos():
    assert reglas.UMBRAL_PICKING_SEC == 600


# ------------------------------------------------------------------ severidad

def test_severidad_de_stock():
    assert severidad_stock(0) == "alta"      # agotado
    assert severidad_stock(3) == "media"     # poco stock


def test_severidad_de_picking():
    assert severidad_picking(700, umbral=600) == "media"
    assert severidad_picking(1200, umbral=600) == "media"    # justo el doble: aún media
    assert severidad_picking(1201, umbral=600) == "alta"


# ------------------------------------------------------------------ evaluar_alertas

def test_evento_normal_no_dispara_alertas():
    assert evaluar_alertas(_evento(), 5, 600) == []


def test_evento_con_stock_critico_dispara_solo_esa_alerta():
    alertas = evaluar_alertas(_evento(stock=2), 5, 600)
    assert [a["tipo_alerta"] for a in alertas] == [STOCK_CRITICO]
    assert alertas[0]["valor"] == 2 and alertas[0]["umbral"] == 5


def test_evento_con_picking_lento_dispara_solo_esa_alerta():
    alertas = evaluar_alertas(_evento(picking_time_sec=900), 5, 600)
    assert [a["tipo_alerta"] for a in alertas] == [PICKING_FUERA_DE_TIEMPO]
    assert alertas[0]["valor"] == 900 and alertas[0]["umbral"] == 600


def test_un_evento_puede_disparar_las_dos_alertas():
    alertas = evaluar_alertas(_evento(stock=0, picking_time_sec=1300), 5, 600)
    assert {a["tipo_alerta"] for a in alertas} == {STOCK_CRITICO, PICKING_FUERA_DE_TIEMPO}
    assert all(a["severidad"] == "alta" for a in alertas)


def test_los_umbrales_son_parametrizables():
    evento = _evento(stock=8, picking_time_sec=400)
    assert evaluar_alertas(evento, 5, 600) == []
    assert len(evaluar_alertas(evento, umbral_stock=10, umbral_picking=300)) == 2


# ------------------------------------------------------------------ umbrales por entorno

def test_umbrales_se_leen_de_variables_de_entorno(monkeypatch):
    monkeypatch.setenv("UMBRAL_STOCK_CRITICO", "8")
    monkeypatch.setenv("UMBRAL_PICKING_SEC", "450")
    try:
        recargado = importlib.reload(reglas)
        assert recargado.UMBRAL_STOCK_CRITICO == 8
        assert recargado.UMBRAL_PICKING_SEC == 450
    finally:
        monkeypatch.undo()
        importlib.reload(reglas)


def test_entorno_invalido_falla_con_error_claro(monkeypatch):
    monkeypatch.setenv("UMBRAL_STOCK_CRITICO", "mucho")
    try:
        with pytest.raises(ValueError):
            importlib.reload(reglas)
    finally:
        monkeypatch.undo()
        importlib.reload(reglas)


# ------------------------------------------------------------------ contra el producer real

def test_las_reglas_detectan_lo_que_inyecta_el_producer():
    """El producer inyecta stock 0-5 (~12 %) y picking 600-1200 s (~8 %)."""
    rng = random.Random(7)
    eventos = [generar_evento(rng) for _ in range(2000)]
    con_stock = [e for e in eventos if e["stock"] <= 5]
    lentos = [e for e in eventos if e["picking_time_sec"] > 600]

    tipos = [a["tipo_alerta"] for e in eventos for a in evaluar_alertas(e, 5, 600)]
    assert tipos.count(STOCK_CRITICO) == len(con_stock) > 0
    assert tipos.count(PICKING_FUERA_DE_TIEMPO) == len(lentos) > 0
    # Proporciones plausibles (margen amplio para no depender de la semilla).
    assert 0.08 < len(con_stock) / len(eventos) < 0.17
    assert 0.04 < len(lentos) / len(eventos) < 0.13

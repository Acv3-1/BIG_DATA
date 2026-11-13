"""Pruebas finales (Issue #33): los módulos del equipo trabajan de acuerdo entre sí.

Python puro (sin Kafka, Spark, Airflow ni Docker), así que corren en el CI. Recorren el mismo
camino que los datos reales: producer -> mensaje JSON (Raw) -> calidad -> alertas -> KPIs.
Su objetivo es detectar que un umbral o una regla cambió en un sitio y quedó igual en otro.
"""
import json
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from producer.eventos import corromper_evento, generar_evento
from spark import reglas
from spark.reglas import PICKING_FUERA_DE_TIEMPO, STOCK_CRITICO, evaluar_alertas

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "airflow" / "dags"))
sys.path.insert(0, str(RAIZ / "scripts"))

import calibrar_umbrales as cal  # noqa: E402
import kpis_inventario as kpis  # noqa: E402
import rapidgo_calidad as calidad  # noqa: E402


def _eventos(n=3000, semilla=21):
    rng = random.Random(semilla)
    return [generar_evento(rng) for _ in range(n)]


# ---------------------------------------------------------------- umbrales sincronizados

def test_los_umbrales_de_los_kpis_coinciden_con_los_de_las_alertas():
    """kpis_inventario.py (Airflow) y reglas.py (Spark) definen los mismos umbrales."""
    assert kpis.UMBRAL_STOCK_CRITICO == reglas.UMBRAL_STOCK_CRITICO
    assert kpis.UMBRAL_PICKING_SEG == reglas.UMBRAL_PICKING_SEC


def test_umbrales_finales_vigentes():
    """Umbrales definidos en la calibración (ADR-004). Si cambian, se actualiza el ADR."""
    assert reglas.UMBRAL_STOCK_CRITICO == 5
    assert reglas.UMBRAL_PICKING_SEC == 600
    assert reglas.FACTOR_SEVERIDAD_PICKING == 1.5


# ---------------------------------------------------------------- de punta a punta

def test_los_kpis_cuentan_las_mismas_alertas_que_las_reglas():
    eventos = _eventos()
    kp = kpis.calcular_kpis(eventos)
    por_reglas = [a["tipo_alerta"] for e in eventos for a in evaluar_alertas(e)]
    assert kp["alertas"]["stock_critico"] == por_reglas.count(STOCK_CRITICO) > 0
    assert kp["alertas"]["picking_fuera_de_tiempo"] == por_reglas.count(PICKING_FUERA_DE_TIEMPO) > 0


def test_los_stockouts_de_los_kpis_son_las_alertas_de_stock_con_stock_cero():
    eventos = _eventos()
    agotados = sum(1 for e in eventos
                   for a in evaluar_alertas(e)
                   if a["tipo_alerta"] == STOCK_CRITICO and a["valor"] == 0)
    assert kpis.stockouts(eventos) == agotados > 0


def test_recorrido_completo_con_datos_sanos():
    """producer -> JSON (como llega a Raw) -> calidad OK -> alertas -> KPIs."""
    eventos = _eventos()
    raw = [json.dumps(e) for e in eventos]                    # lo que guarda Raw
    estado = calidad.evaluar_calidad(raw, umbral=0.05)
    assert estado["ok"] is True and estado["invalidos"] == 0

    kp = kpis.calcular_kpis([json.loads(v) for v in raw])
    assert kp["total_eventos"] == len(eventos)
    assert set(kp["picking_promedio_por_tienda"]) <= {f"S{n:03d}" for n in range(1, 7)}


def test_recorrido_completo_con_datos_malos_se_detecta_y_se_recupera():
    rng = random.Random(4)
    sanos = _eventos(500, semilla=5)
    mezcla = [corromper_evento(rng, e) if rng.random() < 0.4 else e for e in _eventos(300, 6)]

    malos = calidad.evaluar_calidad([json.dumps(e) for e in mezcla], umbral=0.05)
    assert malos["ok"] is False and malos["tasa_invalidos"] > 0.3

    # Recuperación: llegan 500 eventos sanos y la ventana de los 500 más recientes queda limpia.
    filas = [{"value": json.dumps(e), "kafka_timestamp": None} for e in mezcla + sanos]
    for i, f in enumerate(filas):
        f["kafka_timestamp"] = datetime(2026, 11, 13) + timedelta(seconds=i)
    recientes = calidad.seleccionar_recientes(filas, 500)
    assert calidad.evaluar_calidad(recientes, umbral=0.05)["ok"] is True


# ---------------------------------------------------------------- calibración (ADR-004)

@pytest.fixture(scope="module")
def analisis():
    picking, stock = cal.datos_simulados(30000, semilla=3)
    return cal.analizar(picking, stock)


def test_el_umbral_de_picking_cae_en_el_hueco_entre_normales_y_lentos(analisis):
    h = analisis["histograma_picking"]
    assert h["550-599"] < 0.3                  # casi no hay eventos justo antes del umbral
    assert h["600-699"] > 1.0                  # y el grupo de lentos empieza en 600


def test_alertas_de_picking_y_stock_cerca_de_lo_que_inyecta_el_producer(analisis):
    assert 6.5 <= analisis["pct_alerta_picking"][600] <= 9.0    # producer: ~8 % lentos
    assert 10.5 <= analisis["pct_alerta_stock"][5] <= 13.5      # producer: ~12 % stock crítico


def test_un_umbral_de_picking_mas_bajo_dispararia_falsos_positivos(analisis):
    assert analisis["pct_alerta_picking"][500] > analisis["pct_alerta_picking"][600] + 0.3


def test_la_severidad_alta_de_picking_es_alcanzable(analisis):
    """Con el factor inicial (2.0) la severidad alta no se activaba nunca (máx. 1200 s)."""
    assert analisis["pct_picking_alta_con_factor_2"] == 0.0
    assert analisis["pct_picking_alta"] > 2.0


def test_analizar_con_datos_conocidos():
    r = cal.analizar([100, 200, 650, 1000], [0, 3, 6, 50], umbral_picking=600, umbral_stock=5)
    assert r["eventos"] == 4 and r["picking_max"] == 1000
    assert r["pct_alerta_picking"][600] == 50.0          # 650 y 1000
    assert r["pct_alerta_stock"][5] == 50.0              # 0 y 3
    assert r["pct_picking_alta"] == 25.0                 # solo 1000 > 900


def test_el_script_de_calibracion_corre_y_sale_con_0(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["calibrar_umbrales", "--eventos", "500", "--seed", "1"])
    assert cal.main() == 0
    salida = capsys.readouterr().out
    assert "<- vigente" in salida and "Histograma" in salida

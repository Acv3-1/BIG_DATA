"""Pruebas de los KPIs de la variante 10 (reglas de negocio)."""
import json

from kpis_inventario import (UMBRAL_PICKING_SEG, UMBRAL_STOCK_CRITICO,
                             calcular_kpis, generar_reporte,
                             pedidos_incompletos, picking_promedio,
                             picking_promedio_por_tienda, stockouts)


def _e(store="S001", stock=50, pedidos=3, picked=3, picking=300):
    return {"store_id": store, "stock": stock, "items_requested": pedidos,
            "items_picked": picked, "picking_time_sec": picking}


EVENTOS = [
    _e("S001", stock=0, pedidos=4, picked=2, picking=900),   # agotado, incompleto, lento
    _e("S001", stock=3, picking=300),                        # stock crítico
    _e("S002", stock=100, picking=100),
    _e("S002", stock=20, picking=500),
]


def test_stockouts_cuenta_solo_stock_cero():
    assert stockouts(EVENTOS) == 1


def test_picking_promedio():
    assert picking_promedio(EVENTOS) == (900 + 300 + 100 + 500) / 4


def test_picking_promedio_sin_eventos():
    assert picking_promedio([]) is None


def test_picking_promedio_por_tienda():
    assert picking_promedio_por_tienda(EVENTOS) == {"S001": 600.0, "S002": 300.0}


def test_pedidos_incompletos():
    r = pedidos_incompletos(EVENTOS)
    assert r == {"total": 4, "incompletos": 1, "porcentaje": 25.0}


def test_pedidos_incompletos_sin_eventos():
    assert pedidos_incompletos([])["porcentaje"] == 0.0


def test_alertas_usan_los_umbrales():
    alertas = calcular_kpis(EVENTOS)["alertas"]
    assert alertas["stock_critico"] == 2            # stock 0 y stock 3 (<= 5)
    assert alertas["picking_fuera_de_tiempo"] == 1  # 900 s (> 600)
    assert UMBRAL_STOCK_CRITICO == 5 and UMBRAL_PICKING_SEG == 600


def test_umbral_en_el_limite_no_dispara_alerta():
    eventos = [_e(stock=UMBRAL_STOCK_CRITICO + 1, picking=UMBRAL_PICKING_SEG)]
    alertas = calcular_kpis(eventos)["alertas"]
    assert alertas == {"stock_critico": 0, "picking_fuera_de_tiempo": 0}


def test_kpis_son_serializables_a_json():
    kpis = calcular_kpis(EVENTOS)
    assert json.loads(json.dumps(kpis)) == kpis     # requisito para pasar por XCom


def test_calcular_kpis_sin_eventos():
    kpis = calcular_kpis([])
    assert kpis["total_eventos"] == 0 and kpis["picking_promedio_seg"] is None


def test_generar_reporte_crea_json_y_markdown(tmp_path):
    kpis = calcular_kpis(EVENTOS)
    rutas = generar_reporte(kpis, str(tmp_path), "2026-01-01")
    assert json.loads(open(rutas["json"], encoding="utf-8").read()) == kpis
    texto = open(rutas["markdown"], encoding="utf-8").read()
    assert "Stockouts: 1" in texto and "| S001 |" in texto

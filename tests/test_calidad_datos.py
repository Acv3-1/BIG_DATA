"""Pruebas de la validación de calidad de datos (Issue #25) y del fallo controlado (#27).

Python puro: no requieren Airflow, Spark, pyarrow ni Java, así que corren en el CI básico.
"""
import json
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from producer.eventos import TIPOS_DE_CORRUPCION, corromper_evento, generar_evento

DAGS = Path(__file__).resolve().parents[1] / "airflow" / "dags"
sys.path.insert(0, str(DAGS))

import rapidgo_calidad as c  # noqa: E402
import rapidgo_tareas as t  # noqa: E402


def _evento(**cambios):
    base = generar_evento(random.Random(1))
    base.update(cambios)
    return base


def _json(**cambios):
    return json.dumps(_evento(**cambios))


# ------------------------------------------------------------ reglas por evento

def test_un_evento_valido_no_tiene_motivos():
    assert c.motivos_evento(_json()) == []


def test_acepta_un_dict_ademas_de_texto_json():
    assert c.motivos_evento(_evento()) == []


@pytest.mark.parametrize("valor", ["{roto", "", None, "esto no es json", "[1, 2]", "42", "null"])
def test_json_malformado(valor):
    assert c.motivos_evento(valor) == ["json_malformado"]


@pytest.mark.parametrize("campo", c.CAMPOS_REQUERIDOS)
def test_campo_nulo_o_ausente(campo):
    assert "campos_nulos" in c.motivos_evento(_json(**{campo: None}))
    evento = _evento()
    del evento[campo]
    assert "campos_nulos" in c.motivos_evento(evento)


def test_stock_negativo():
    assert c.motivos_evento(_json(stock=-1)) == ["stock_negativo"]
    assert c.motivos_evento(_json(stock=0)) == []          # agotado es válido


@pytest.mark.parametrize("sku", ["SKU-XYZ", "sku-0001", "SKU-1", "SKU-00001", "", "12345"])
def test_sku_invalido(sku):
    assert c.motivos_evento(_json(sku=sku)) == ["sku_invalido"]


def test_sku_con_formato_correcto_es_valido():
    assert c.motivos_evento(_json(sku="SKU-0020")) == []


@pytest.mark.parametrize("tienda", ["S1", "T001", "S0001", "tienda"])
def test_tienda_invalida(tienda):
    assert c.motivos_evento(_json(store_id=tienda)) == ["tienda_invalida"]


@pytest.mark.parametrize("segundos", [0, -30])
def test_picking_no_positivo(segundos):
    assert c.motivos_evento(_json(picking_time_sec=segundos)) == ["picking_no_positivo"]


@pytest.mark.parametrize("pedidos,pickeados", [(4, 5), (4, -1), (0, 0)])
def test_items_incoherentes(pedidos, pickeados):
    assert c.motivos_evento(
        _json(items_requested=pedidos, items_picked=pickeados)) == ["items_incoherentes"]


def test_tipo_incorrecto():
    assert c.motivos_evento(_json(stock="mucho")) == ["tipo_incorrecto"]
    assert c.motivos_evento(_json(stock=True)) == ["tipo_incorrecto"]   # bool no es un entero
    assert c.motivos_evento(_json(sku=123)) == ["tipo_incorrecto"]


def test_un_evento_puede_tener_varios_motivos():
    motivos = c.motivos_evento(_json(stock=-5, sku="SKU-XYZ"))
    assert set(motivos) == {"stock_negativo", "sku_invalido"}


def test_todos_los_motivos_documentados_son_alcanzables():
    """Cada motivo de MOTIVOS aparece en algún caso de arriba (evita motivos muertos)."""
    casos = ["{roto", _json(sku=None), _json(stock="x"), _json(stock=-1), _json(sku="X"),
             _json(store_id="X"), _json(picking_time_sec=0), _json(items_picked=99)]
    vistos = {m for caso in casos for m in c.motivos_evento(caso)}
    assert vistos == set(c.MOTIVOS)


# ------------------------------------------------------------ contra el producer real

def test_los_eventos_del_producer_son_todos_validos():
    rng = random.Random(5)
    for _ in range(1000):
        assert c.motivos_evento(generar_evento(rng)) == []


@pytest.mark.parametrize("semilla", range(60))
def test_todo_evento_corrompido_por_el_producer_se_detecta(semilla):
    rng = random.Random(semilla)
    malo = corromper_evento(rng, generar_evento(rng))
    assert c.motivos_evento(malo), malo


def test_corromper_cubre_los_tres_tipos_y_no_toca_la_key():
    rng = random.Random(0)
    original = generar_evento(rng)
    motivos = set()
    for _ in range(100):
        malo = corromper_evento(rng, original)
        assert malo["store_id"] == original["store_id"]
        motivos.update(c.motivos_evento(malo))
    assert {"stock_negativo", "sku_invalido", "campos_nulos"} <= motivos
    assert len(TIPOS_DE_CORRUPCION) == 3


def test_corromper_no_modifica_el_evento_original():
    original = _evento()
    copia = dict(original)
    corromper_evento(random.Random(2), original)
    assert original == copia


# ------------------------------------------------------------ evaluación del lote

def _lote(n_validos, n_malos):
    return [_json() for _ in range(n_validos)] + [_json(stock=-1) for _ in range(n_malos)]


def test_lote_limpio_es_ok():
    r = c.evaluar_calidad(_lote(100, 0), umbral=0.05)
    assert r["ok"] is True
    assert (r["total"], r["validos"], r["invalidos"], r["tasa_invalidos"]) == (100, 100, 0, 0.0)
    assert r["ejemplos"] == []


def test_exactamente_en_el_umbral_es_ok_y_por_encima_falla():
    assert c.evaluar_calidad(_lote(95, 5), umbral=0.05)["ok"] is True      # 5 %
    assert c.evaluar_calidad(_lote(94, 6), umbral=0.05)["ok"] is False     # 6 %


def test_cuenta_por_motivo_y_guarda_ejemplos():
    valores = [_json(stock=-1), _json(sku="X"), _json(stock=-9, sku="Y"), "{roto"] + _lote(6, 0)
    r = c.evaluar_calidad(valores, umbral=0.5)
    assert r["invalidos"] == 4 and r["total"] == 10
    assert r["por_motivo"]["stock_negativo"] == 2
    assert r["por_motivo"]["sku_invalido"] == 2
    assert r["por_motivo"]["json_malformado"] == 1
    assert len(r["ejemplos"]) == 3                      # máximo 3
    assert all(len(e["mensaje"]) <= 160 for e in r["ejemplos"])


def test_sin_eventos_no_es_ok():
    r = c.evaluar_calidad([], umbral=0.05)
    assert r["ok"] is False and r["total"] == 0
    assert "sin eventos" in c.describir(r) or "no tiene eventos" in c.describir(r)


def test_el_resultado_es_serializable_a_json():
    r = c.evaluar_calidad(_lote(10, 3), umbral=0.05)
    assert json.loads(json.dumps(r)) == r               # XCom guarda JSON


def test_describir_resume_tasa_y_motivos():
    texto = c.describir(c.evaluar_calidad(_lote(8, 2), umbral=0.05))
    assert "2 de 10" in texto and "20.0%" in texto and "stock_negativo=2" in texto


def test_umbral_y_ventana_por_variables_de_entorno(monkeypatch):
    assert c.umbral_invalidos() == 0.05 and c.eventos_calidad() == 500
    monkeypatch.setenv("UMBRAL_INVALIDOS", "0.2")
    monkeypatch.setenv("EVENTOS_CALIDAD", "50")
    assert c.umbral_invalidos() == 0.2 and c.eventos_calidad() == 50
    # y evaluar_calidad usa el umbral del entorno cuando no se le pasa uno
    assert c.evaluar_calidad(_lote(85, 15))["ok"] is True       # 15 % <= 20 %


# ------------------------------------------------------------ ventana de eventos recientes

T0 = datetime(2026, 11, 9, 10, 0, 0)


def _fila(i, ts="auto"):
    return {"value": f"v{i}", "kafka_timestamp": T0 + timedelta(seconds=i) if ts == "auto" else ts}


def test_seleccionar_recientes_toma_los_n_mas_nuevos_aunque_lleguen_desordenados():
    filas = [_fila(i) for i in (5, 1, 9, 3, 7)]
    assert c.seleccionar_recientes(filas, 2) == ["v7", "v9"]


def test_seleccionar_recientes_con_menos_filas_que_n():
    assert sorted(c.seleccionar_recientes([_fila(1), _fila(2)], 500)) == ["v1", "v2"]


def test_seleccionar_recientes_trata_timestamp_nulo_como_el_mas_antiguo():
    filas = [_fila(1, ts=None), _fila(2), _fila(3)]
    assert c.seleccionar_recientes(filas, 2) == ["v2", "v3"]


def test_seleccionar_recientes_con_zona_horaria():
    from datetime import timezone
    filas = [{"value": "a", "kafka_timestamp": T0.replace(tzinfo=timezone.utc)},
             {"value": "b", "kafka_timestamp": (T0 + timedelta(seconds=5)).replace(tzinfo=timezone.utc)}]
    assert c.seleccionar_recientes(filas, 1) == ["b"]


def test_un_problema_ya_corregido_sale_de_la_ventana():
    """Recuperación (#27): 300 eventos con 40 % malos y luego 500 limpios -> la ventana de 500 queda limpia."""
    viejos = [_fila(i) | {"value": _json(stock=-1) if i % 5 < 2 else _json()} for i in range(300)]
    nuevos = [_fila(1000 + i) | {"value": _json()} for i in range(500)]
    assert c.evaluar_calidad(c.seleccionar_recientes(viejos, 500), 0.05)["ok"] is False
    assert c.evaluar_calidad(c.seleccionar_recientes(viejos + nuevos, 500), 0.05)["ok"] is True


# ------------------------------------------------------------ la task validar_calidad

RESUMEN_OK = {
    t.RAW_EVENTOS: {"existe": True, "archivos_parquet": 3},
    t.PROCESSED_DETALLE: {"existe": True, "archivos_parquet": 3},
    t.CURATED_PICKING: {"existe": True, "archivos_parquet": 1},
    t.CURATED_ALERTAS: {"existe": True, "archivos_parquet": 1},
}


def test_validar_calidad_ok_devuelve_fuentes_mas_datos(monkeypatch):
    monkeypatch.setattr(t, "leer_raw_recientes", lambda base=None, n=None: _lote(50, 0))
    r = t.validar_calidad(RESUMEN_OK)
    assert r["ok"] is True and r["problemas"] == []
    assert r["datos"]["total"] == 50 and r["datos"]["ok"] is True
    assert json.loads(json.dumps(r)) == r


def test_validar_calidad_falla_con_demasiados_invalidos_y_dice_por_que(monkeypatch):
    monkeypatch.setattr(t, "leer_raw_recientes", lambda base=None, n=None: _lote(60, 40))
    with pytest.raises(t.CalidadInsuficiente) as error:
        t.validar_calidad(RESUMEN_OK)
    mensaje = str(error.value)
    assert "40 de 100" in mensaje and "stock_negativo=40" in mensaje and "umbral 5.0%" in mensaje


def test_validar_calidad_falla_si_raw_no_tiene_eventos(monkeypatch):
    monkeypatch.setattr(t, "leer_raw_recientes", lambda base=None, n=None: [])
    with pytest.raises(t.CalidadInsuficiente):
        t.validar_calidad(RESUMEN_OK)


def test_validar_calidad_no_lee_datos_si_faltan_las_fuentes(monkeypatch):
    def no_deberia_llamarse(base=None, n=None):
        raise AssertionError("se leyó Raw aunque faltan fuentes")
    monkeypatch.setattr(t, "leer_raw_recientes", no_deberia_llamarse)
    resumen_vacio = {tabla: {"existe": False, "archivos_parquet": 0} for tabla in t.TABLAS}
    with pytest.raises(t.FuenteNoDisponible):
        t.validar_calidad(resumen_vacio)


def test_calidad_insuficiente_y_fuente_no_disponible_son_errores_distintos():
    assert not issubclass(t.CalidadInsuficiente, t.FuenteNoDisponible)


def test_el_reporte_incluye_las_metricas_de_calidad(monkeypatch, tmp_path):
    monkeypatch.setattr(t, "leer_raw_recientes", lambda base=None, n=None: _lote(50, 1))
    monkeypatch.setenv("REPORTES_PATH", str(tmp_path))
    calidad = t.validar_calidad(RESUMEN_OK)
    ruta = t.generar_reporte("run-calidad", RESUMEN_OK, calidad, {})
    contenido = json.loads(Path(ruta).read_text(encoding="utf-8"))
    assert contenido["calidad"]["datos"]["invalidos"] == 1


# ------------------------------------------------------------ producer --malos

def _correr_producer(monkeypatch, capsys, *argumentos):
    from producer import producer
    monkeypatch.setattr(sys, "argv", ["producer", "--dry-run", "--rate", "100000", *argumentos])
    assert producer.main() == 0
    salida = capsys.readouterr().out.splitlines()
    return [json.loads(linea) for linea in salida if linea.startswith("{")]


def test_producer_por_defecto_no_genera_eventos_malos(monkeypatch, capsys):
    eventos = _correr_producer(monkeypatch, capsys, "--count", "200", "--seed", "3")
    assert len(eventos) == 200
    assert all(c.motivos_evento(e) == [] for e in eventos)


def test_producer_malos_1_corrompe_todos(monkeypatch, capsys):
    eventos = _correr_producer(monkeypatch, capsys, "--count", "100", "--seed", "3", "--malos", "1")
    assert all(c.motivos_evento(e) for e in eventos)


def test_producer_malos_aproxima_la_fraccion_pedida(monkeypatch, capsys):
    eventos = _correr_producer(monkeypatch, capsys, "--count", "1000", "--seed", "3", "--malos", "0.4")
    invalidos = sum(1 for e in eventos if c.motivos_evento(e))
    assert 330 <= invalidos <= 470


def test_producer_rechaza_malos_fuera_de_rango(monkeypatch, capsys):
    from producer import producer
    monkeypatch.setattr(sys, "argv", ["producer", "--dry-run", "--count", "1", "--malos", "1.5"])
    assert producer.main() == 1
    assert "--malos" in capsys.readouterr().err


def test_producer_sin_malos_da_la_misma_secuencia_que_antes(monkeypatch, capsys):
    """--malos 0 no debe consumir números aleatorios: los resultados con --seed no cambian."""
    def sin_hora(eventos):      # event_ts usa la hora real, es lo único que cambia entre corridas
        return [{k: v for k, v in e.items() if k != "event_ts"} for e in eventos]

    a = _correr_producer(monkeypatch, capsys, "--count", "20", "--seed", "1")
    b = _correr_producer(monkeypatch, capsys, "--count", "20", "--seed", "1", "--malos", "0")
    assert sin_hora(a) == sin_hora(b)

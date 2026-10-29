"""Pruebas de la lógica de Spark (parseo, transformaciones y agregación por ventana).

Se ejecutan sobre DataFrames batch pequeños; las mismas funciones se usan en streaming.
Requieren pyspark y Java: si faltan, las pruebas se omiten (no rompen el CI básico).
Para ejecutarlas:  pip install -r requirements-spark.txt && pytest -v
"""
import json
import os
import shutil
import sys
from datetime import datetime, timezone

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import IntegerType, StringType  # noqa: E402

from producer.eventos import CAMPOS_REQUERIDOS  # noqa: E402
from spark.esquema import ESQUEMA_EVENTO  # noqa: E402
from spark.parseo import parsear_eventos  # noqa: E402
from spark.transformaciones import (agregar_metricas_operativas,  # noqa: E402
                                    agregar_metricas_pedido,
                                    agregar_picking_por_tienda,
                                    limpiar_y_deduplicar)

T0 = datetime(2026, 10, 29, 10, 0, 0, tzinfo=timezone.utc)  # inicio de una ventana de 5 min


def _ts(segundos):
    """Instante T0 + segundos, en el formato exacto que genera el producer."""
    ms = int((T0.timestamp() + segundos) * 1000)
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _evento(**cambios):
    base = {
        "event_id": "e1", "team": "equipo-test", "variant": 10, "store_id": "S001",
        "order_id": "PED-100001", "sku": "SKU-0001", "stock": 50,
        "items_requested": 4, "items_picked": 4, "picking_time_sec": 300,
        "event_ts": _ts(30),
    }
    base.update(cambios)
    return base


@pytest.fixture(scope="module")
def spark():
    if shutil.which("java") is None and not os.getenv("JAVA_HOME"):
        pytest.skip("Java no está instalado")
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    sesion = (
        SparkSession.builder.master("local[1]").appName("tests-rapidgo")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield sesion
    sesion.stop()



def _kafka(spark, mensajes):
    """DataFrame con la forma que entrega la fuente Kafka (key, value, partition, offset).

    `mensajes` es una lista de dicts (se serializan a JSON) o de strings (se envían tal cual).
    """
    filas = []
    for i, m in enumerate(mensajes):
        valor = m if isinstance(m, str) else json.dumps(m)
        llave = None if isinstance(m, str) else m["store_id"]
        filas.append((llave, valor, i % 3, i))
    return spark.createDataFrame(filas, "key string, value string, partition int, offset long")


def _limpios(spark, mensajes):
    return limpiar_y_deduplicar(parsear_eventos(_kafka(spark, mensajes)))


# ---------------------------------------------------------------- esquema / parseo (#14)

def test_esquema_coincide_con_el_contrato():
    tipos = {str: StringType, int: IntegerType}
    esquema = {f.name: type(f.dataType) for f in ESQUEMA_EVENTO.fields}
    esperado = {campo: tipos[tipo] for campo, tipo in CAMPOS_REQUERIDOS.items()}
    assert esquema == esperado


def test_parsear_convierte_json_a_columnas(spark):
    df = parsear_eventos(_kafka(spark, [_evento(stock=7)]))
    fila = df.collect()[0]
    assert fila["store_id"] == "S001"
    assert fila["stock"] == 7
    assert fila["kafka_key"] == "S001"
    assert fila["kafka_offset"] == 0


def test_parsear_event_time_es_un_timestamp_correcto(spark):
    df = parsear_eventos(_kafka(spark, [_evento(event_ts=_ts(30))]))
    epoch = df.select(F.col("event_time").cast("long").alias("t")).collect()[0]["t"]
    assert epoch == int(T0.timestamp()) + 30


def test_json_malformado_no_rompe_el_parseo(spark):
    df = parsear_eventos(_kafka(spark, ["esto no es json", _evento()]))
    assert df.count() == 2
    assert df.filter(F.col("event_id").isNull()).count() == 1


# ---------------------------------------------------------------- transformación 1

def test_limpiar_descarta_json_malformado(spark):
    assert _limpios(spark, ["{roto", _evento()]).count() == 1


def test_limpiar_descarta_eventos_incoherentes(spark):
    malos = [
        _evento(event_id="a", stock=-1),
        _evento(event_id="b", picking_time_sec=0),
        _evento(event_id="c", items_picked=5, items_requested=4),
        _evento(event_id="d", items_picked=-1),
        _evento(event_id="e", store_id=None),
    ]
    assert _limpios(spark, malos + [_evento(event_id="ok")]).count() == 1


def test_limpiar_elimina_duplicados_por_event_id(spark):
    df = _limpios(spark, [_evento(), _evento(), _evento(event_id="e2")])
    assert sorted(r["event_id"] for r in df.collect()) == ["e1", "e2"]


# ---------------------------------------------------------------- transformaciones 2 y 3

def test_metricas_pedido(spark):
    eventos = [
        _evento(event_id="a", items_requested=4, items_picked=3),
        _evento(event_id="b", items_requested=4, items_picked=4),
        _evento(event_id="c", items_requested=5, items_picked=0),
    ]
    df = agregar_metricas_pedido(_limpios(spark, eventos))
    filas = {r["event_id"]: r for r in df.collect()}
    assert (filas["a"]["items_faltantes"], filas["a"]["pedido_incompleto"], filas["a"]["fill_rate"]) \
        == (1, True, 0.75)
    assert (filas["b"]["items_faltantes"], filas["b"]["pedido_incompleto"], filas["b"]["fill_rate"]) \
        == (0, False, 1.0)
    assert (filas["c"]["items_faltantes"], filas["c"]["fill_rate"]) == (5, 0.0)


def test_metricas_operativas(spark):
    eventos = [
        _evento(event_id="a", stock=0, picking_time_sec=90),
        _evento(event_id="b", stock=3, picking_time_sec=600),
    ]
    df = agregar_metricas_operativas(_limpios(spark, eventos))
    filas = {r["event_id"]: r for r in df.collect()}
    assert filas["a"]["es_stockout"] is True
    assert filas["a"]["picking_time_min"] == 1.5
    assert filas["b"]["es_stockout"] is False   # stock bajo pero no agotado
    assert filas["b"]["picking_time_min"] == 10.0


# ---------------------------------------------------------------- agregación por ventana

def test_picking_promedio_por_tienda_y_ventana(spark):
    eventos = [
        # Tienda S001, ventana [10:00, 10:05): promedio (300 + 600 + 300) / 3 = 400
        _evento(event_id="a", store_id="S001", picking_time_sec=300, event_ts=_ts(10)),
        _evento(event_id="b", store_id="S001", picking_time_sec=600, event_ts=_ts(100),
                items_picked=2),
        _evento(event_id="c", store_id="S001", picking_time_sec=300, event_ts=_ts(250),
                stock=0),
        # Tienda S002, misma ventana
        _evento(event_id="d", store_id="S002", picking_time_sec=200, event_ts=_ts(60)),
        # Tienda S001, ventana siguiente [10:05, 10:10)
        _evento(event_id="e", store_id="S001", picking_time_sec=500, event_ts=_ts(420)),
    ]
    df = agregar_metricas_operativas(agregar_metricas_pedido(_limpios(spark, eventos)))
    agg = agregar_picking_por_tienda(df, "5 minutes").select(
        F.col("ventana_inicio").cast("long").alias("ini"),
        F.col("ventana_fin").cast("long").alias("fin"),
        "store_id", "pedidos", "picking_promedio_sec", "picking_max_sec",
        "pedidos_incompletos", "stockouts",
    )
    filas = {(r["store_id"], r["ini"]): r for r in agg.collect()}
    t0 = int(T0.timestamp())

    assert len(filas) == 3
    s1 = filas[("S001", t0)]
    assert s1["fin"] == t0 + 300
    assert s1["pedidos"] == 3
    assert s1["picking_promedio_sec"] == 400.0
    assert s1["picking_max_sec"] == 600
    assert s1["pedidos_incompletos"] == 1
    assert s1["stockouts"] == 1
    assert filas[("S002", t0)]["picking_promedio_sec"] == 200.0
    assert filas[("S001", t0 + 300)]["picking_promedio_sec"] == 500.0

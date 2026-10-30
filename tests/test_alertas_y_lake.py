"""Pruebas de las alertas en Spark (#16) y de la persistencia en Parquet con checkpoint (#17).

Requieren pyspark y Java: si faltan, se omiten.
    pip install -r requirements-spark.txt && pytest -v
"""
import json
import os
import shutil
import sys
from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from producer.eventos import generar_evento  # noqa: E402
from spark.alertas import COLUMNAS_ALERTA, detectar_alertas  # noqa: E402
from spark.lake import (CURATED_ALERTAS, CURATED_PICKING, PROCESSED_DETALLE,  # noqa: E402
                        RAW_EVENTOS, rutas)
from spark.parseo import parsear_eventos  # noqa: E402
from spark.reglas import (PICKING_FUERA_DE_TIEMPO, STOCK_CRITICO,  # noqa: E402
                          evaluar_alertas)
from spark.streaming_job import iniciar_queries  # noqa: E402
from spark.transformaciones import (agregar_metricas_operativas,  # noqa: E402
                                    agregar_metricas_pedido, limpiar_y_deduplicar)

T0 = datetime(2026, 10, 29, 10, 0, 0, tzinfo=timezone.utc)  # inicio de una ventana de 5 min
ESQUEMA_ARCHIVO = "key string, value string, topic string, partition int, offset long, timestamp string"


def _ts(segundos):
    dt = T0 + timedelta(seconds=segundos)
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
        SparkSession.builder.master("local[1]").appName("tests-alertas-lake")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield sesion
    sesion.stop()


def _kafka(spark, eventos):
    filas = [(e["store_id"], json.dumps(e), i % 3, i) for i, e in enumerate(eventos)]
    return spark.createDataFrame(filas, "key string, value string, partition int, offset long")


def _detalle(spark, eventos):
    limpios = limpiar_y_deduplicar(parsear_eventos(_kafka(spark, eventos)))
    return agregar_metricas_operativas(agregar_metricas_pedido(limpios))


# ============================================================ #16 alertas en Spark

def test_detectar_alertas_tiene_las_columnas_esperadas(spark):
    alertas = detectar_alertas(_detalle(spark, [_evento(stock=1)]), 5, 600)
    assert alertas.columns == COLUMNAS_ALERTA


def test_evento_normal_no_genera_alertas(spark):
    assert detectar_alertas(_detalle(spark, [_evento()]), 5, 600).count() == 0


def test_alertas_por_tipo_y_severidad(spark):
    eventos = [
        _evento(event_id="a", stock=0),                              # stock agotado -> alta
        _evento(event_id="b", stock=3),                              # stock bajo -> media
        _evento(event_id="c", picking_time_sec=700),                 # lento -> media
        _evento(event_id="d", picking_time_sec=1300),                # muy lento -> alta
        _evento(event_id="e", stock=6, picking_time_sec=600),        # en el límite: sin alerta
    ]
    filas = detectar_alertas(_detalle(spark, eventos), 5, 600).collect()
    obtenido = {(r["event_id"], r["tipo_alerta"]): (r["severidad"], r["valor"], r["umbral"])
                for r in filas}
    assert obtenido == {
        ("a", STOCK_CRITICO): ("alta", 0, 5),
        ("b", STOCK_CRITICO): ("media", 3, 5),
        ("c", PICKING_FUERA_DE_TIEMPO): ("media", 700, 600),
        ("d", PICKING_FUERA_DE_TIEMPO): ("alta", 1300, 600),
    }


def test_un_evento_con_las_dos_condiciones_genera_dos_alertas(spark):
    df = detectar_alertas(_detalle(spark, [_evento(stock=0, picking_time_sec=900)]), 5, 600)
    assert sorted(r["tipo_alerta"] for r in df.collect()) == [PICKING_FUERA_DE_TIEMPO,
                                                              STOCK_CRITICO]


def test_el_mensaje_de_alerta_identifica_tienda_y_sku(spark):
    fila = detectar_alertas(_detalle(spark, [_evento(stock=2, sku="SKU-0009",
                                                     store_id="S004")]), 5, 600).collect()[0]
    assert "SKU-0009" in fila["mensaje"] and "S004" in fila["mensaje"]


def test_spark_y_python_puro_aplican_las_mismas_reglas(spark):
    """Si alguien cambia una regla en un lado y no en el otro, esta prueba falla."""
    import random
    rng = random.Random(3)
    eventos = [generar_evento(rng) for _ in range(300)]
    esperado = sorted((e["event_id"], a["tipo_alerta"], a["severidad"])
                      for e in eventos for a in evaluar_alertas(e, 5, 600))
    df = detectar_alertas(_detalle(spark, eventos), 5, 600)
    obtenido = sorted((r["event_id"], r["tipo_alerta"], r["severidad"]) for r in df.collect())
    assert obtenido == esperado
    assert len(esperado) > 0


def test_los_eventos_invalidos_no_generan_alertas(spark):
    """La limpieza va antes: un stock negativo no debe disparar 'stock crítico'."""
    df = detectar_alertas(_detalle(spark, [_evento(stock=-4)]), 5, 600)
    assert df.count() == 0


# ============================================================ #17 Parquet + checkpoint

def _escribir_lote(carpeta, nombre, eventos, offset_inicial=0):
    """Simula los mensajes de Kafka como un archivo JSON-lines (una fuente de streaming)."""
    carpeta.mkdir(parents=True, exist_ok=True)
    with open(carpeta / nombre, "w", encoding="utf-8") as f:
        for i, e in enumerate(eventos):
            fila = {"key": e["store_id"], "value": json.dumps(e), "topic": "inventario-fulfillment",
                    "partition": 0, "offset": offset_inicial + i,
                    "timestamp": e["event_ts"].replace("T", " ").replace("Z", "")}
            f.write(json.dumps(fila) + "\n")


def _correr_una_vez(spark, entrada, lake, max_archivos=None):
    """Arranca todas las queries del job con Trigger.AvailableNow y espera a que terminen."""
    lector = spark.readStream.schema(ESQUEMA_ARCHIVO)
    if max_archivos:
        lector = lector.option("maxFilesPerTrigger", max_archivos)
    crudo = (lector.json(str(entrada))
             .withColumn("timestamp", F.col("timestamp").cast("timestamp")))
    queries = iniciar_queries(crudo, "5 minutes", "10 minutes", str(lake),
                              trigger="1 second", una_vez=True)
    for q in queries:
        q.awaitTermination(120)
        if q.exception() is not None:
            raise q.exception()


def _leer(spark, lake, tabla):
    return spark.read.parquet(rutas(str(lake), tabla)[0])


def test_el_pipeline_escribe_raw_processed_y_alertas(spark, tmp_path):
    entrada, lake = tmp_path / "entrada", tmp_path / "lake"
    eventos = [
        _evento(event_id="a", stock=2, event_ts=_ts(10)),                  # alerta de stock
        _evento(event_id="b", picking_time_sec=800, event_ts=_ts(20)),     # alerta de picking
        _evento(event_id="c", event_ts=_ts(30)),                           # normal
        _evento(event_id="a", stock=2, event_ts=_ts(10)),                  # duplicado de 'a'
        _evento(event_id="d", stock=-1, event_ts=_ts(40)),                 # inválido
    ]
    _escribir_lote(entrada, "lote1.json", eventos)
    # Un mensaje no-JSON también debe quedar en Raw (y no en Processed).
    with open(entrada / "lote1.json", "a", encoding="utf-8") as f:
        f.write(json.dumps({"key": None, "value": "{roto", "topic": "inventario-fulfillment",
                            "partition": 0, "offset": 99,
                            "timestamp": "2026-10-29 10:00:50"}) + "\n")

    _correr_una_vez(spark, entrada, lake)

    raw = _leer(spark, lake, RAW_EVENTOS)
    assert raw.count() == 6                                   # todo, incluso lo inválido
    assert raw.filter(F.col("value") == "{roto").count() == 1
    assert {"kafka_key", "value", "kafka_partition", "kafka_offset"} <= set(raw.columns)

    processed = _leer(spark, lake, PROCESSED_DETALLE)
    assert sorted(r["event_id"] for r in processed.collect()) == ["a", "b", "c"]
    assert {"fill_rate", "es_stockout", "picking_time_min"} <= set(processed.columns)

    alertas = _leer(spark, lake, CURATED_ALERTAS)
    assert sorted((r["event_id"], r["tipo_alerta"]) for r in alertas.collect()) == [
        ("a", STOCK_CRITICO), ("b", PICKING_FUERA_DE_TIEMPO)]


def test_cada_tabla_tiene_su_checkpoint(spark, tmp_path):
    entrada, lake = tmp_path / "entrada", tmp_path / "lake"
    _escribir_lote(entrada, "lote1.json", [_evento()])
    _correr_una_vez(spark, entrada, lake)
    for tabla in (RAW_EVENTOS, PROCESSED_DETALLE, CURATED_PICKING, CURATED_ALERTAS):
        _, checkpoint = rutas(str(lake), tabla)
        assert os.path.isdir(os.path.join(checkpoint, "offsets")), tabla


def test_reiniciar_con_checkpoint_no_duplica_datos(spark, tmp_path):
    """Segunda ejecución: solo se procesa el archivo nuevo, no se releen los anteriores."""
    entrada, lake = tmp_path / "entrada", tmp_path / "lake"
    _escribir_lote(entrada, "lote1.json",
                   [_evento(event_id="a", event_ts=_ts(10)), _evento(event_id="b", event_ts=_ts(20))])
    _correr_una_vez(spark, entrada, lake)
    assert _leer(spark, lake, RAW_EVENTOS).count() == 2

    _escribir_lote(entrada, "lote2.json", [_evento(event_id="c", event_ts=_ts(30))], offset_inicial=2)
    _correr_una_vez(spark, entrada, lake)

    assert _leer(spark, lake, RAW_EVENTOS).count() == 3
    ids = sorted(r["event_id"] for r in _leer(spark, lake, PROCESSED_DETALLE).collect())
    assert ids == ["a", "b", "c"]


def test_curated_escribe_la_ventana_cuando_el_watermark_la_cierra(spark, tmp_path):
    """El sink Parquet es 'append': la ventana de 10:00 solo sale cuando el watermark la pasa.

    Se envían 3 archivos con eventos separados 1 hora (un archivo por micro-lote): al llegar
    el tercero, el watermark (evento más reciente del 2.º lote - 10 min) ya superó la ventana
    [10:00, 10:05) y esta se escribe en Curated.
    """
    entrada, lake = tmp_path / "entrada", tmp_path / "lake"
    _escribir_lote(entrada, "a.json", [
        _evento(event_id="a", store_id="S001", picking_time_sec=300, event_ts=_ts(10)),
        _evento(event_id="b", store_id="S001", picking_time_sec=500, event_ts=_ts(100)),
        _evento(event_id="c", store_id="S002", picking_time_sec=200, event_ts=_ts(60)),
    ])
    _escribir_lote(entrada, "b.json", [_evento(event_id="d", event_ts=_ts(3600 + 10))], 10)
    _escribir_lote(entrada, "c.json", [_evento(event_id="e", event_ts=_ts(7200 + 10))], 20)

    _correr_una_vez(spark, entrada, lake, max_archivos=1)

    filas = {r["store_id"]: r for r in _leer(spark, lake, CURATED_PICKING)
             .filter(F.col("ventana_inicio") == F.lit(T0.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp"))
             .collect()}
    assert set(filas) == {"S001", "S002"}
    assert filas["S001"]["pedidos"] == 2
    assert filas["S001"]["picking_promedio_sec"] == 400.0
    assert filas["S002"]["picking_promedio_sec"] == 200.0

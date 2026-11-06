"""Pruebas de la lógica de las tareas del DAG (Issue #23 / #28).

Python puro: no requieren Airflow, Spark ni Java, así que corren en el CI básico.
"""
import json
import sys
from pathlib import Path

import pytest

DAGS = Path(__file__).resolve().parents[1] / "airflow" / "dags"
sys.path.insert(0, str(DAGS))

import rapidgo_tareas as t  # noqa: E402


def _crear_parquet(base: Path, tabla: str, nombre: str = "part-0000.parquet", contenido=b"datos"):
    carpeta = base / tabla
    carpeta.mkdir(parents=True, exist_ok=True)
    (carpeta / nombre).write_bytes(contenido)


@pytest.fixture
def lake_completo(tmp_path):
    for tabla in t.TABLAS:
        _crear_parquet(tmp_path, tabla)
    return tmp_path


# ------------------------------------------------------------- task 1: listar fuentes

def test_listar_fuentes_cuenta_archivos_parquet(lake_completo):
    _crear_parquet(lake_completo, t.RAW_EVENTOS, "part-0001.parquet", b"1234567")
    resumen = t.listar_fuentes(str(lake_completo))
    raw = resumen[t.RAW_EVENTOS]
    assert raw["existe"] is True
    assert raw["archivos_parquet"] == 2
    assert raw["bytes"] == len(b"datos") + 7
    assert raw["ultima_modificacion"] is not None


def test_listar_fuentes_cuenta_parquet_en_subcarpetas_de_particion(tmp_path):
    _crear_parquet(tmp_path, f"{t.PROCESSED_DETALLE}/fecha=2026-10-29")
    assert t.listar_fuentes(str(tmp_path))[t.PROCESSED_DETALLE]["archivos_parquet"] == 1


def test_listar_fuentes_ignora_archivos_que_no_son_parquet(tmp_path):
    carpeta = tmp_path / t.RAW_EVENTOS
    carpeta.mkdir(parents=True)
    (carpeta / ".part-0.parquet.crc").write_bytes(b"x")
    (carpeta / "_SUCCESS").write_bytes(b"")
    assert t.listar_fuentes(str(tmp_path))[t.RAW_EVENTOS]["archivos_parquet"] == 0


def test_listar_fuentes_lake_vacio_no_falla(tmp_path):
    resumen = t.listar_fuentes(str(tmp_path))
    assert set(resumen) == set(t.TABLAS)
    assert all(info["existe"] is False for info in resumen.values())


def test_el_resumen_es_serializable_a_json(lake_completo):
    # XCom guarda el valor como JSON: el resumen debe poder serializarse.
    resumen = t.listar_fuentes(str(lake_completo))
    assert json.loads(json.dumps(resumen)) == resumen


# ------------------------------------------------------------- task 2: validar fuentes

def test_validar_fuentes_ok_con_lake_completo(lake_completo):
    resultado = t.validar_fuentes(t.listar_fuentes(str(lake_completo)))
    assert resultado == {"ok": True, "problemas": [], "advertencias": []}


def test_validar_fuentes_falla_si_no_hay_raw(lake_completo):
    for archivo in (lake_completo / t.RAW_EVENTOS).glob("*.parquet"):
        archivo.unlink()
    with pytest.raises(t.FuenteNoDisponible) as error:
        t.validar_fuentes(t.listar_fuentes(str(lake_completo)))
    assert "raw/eventos (0 archivos Parquet)" in str(error.value)


def test_validar_fuentes_falla_si_la_carpeta_no_existe(tmp_path):
    with pytest.raises(t.FuenteNoDisponible) as error:
        t.validar_fuentes(t.listar_fuentes(str(tmp_path)))
    mensaje = str(error.value)
    assert "raw/eventos (la carpeta no existe)" in mensaje
    assert "processed/detalle (la carpeta no existe)" in mensaje


def test_curated_vacio_solo_genera_advertencia(tmp_path):
    # Curated tarda ~15 min en aparecer por el watermark: no debe bloquear el pipeline.
    _crear_parquet(tmp_path, t.RAW_EVENTOS)
    _crear_parquet(tmp_path, t.PROCESSED_DETALLE)
    resultado = t.validar_fuentes(t.listar_fuentes(str(tmp_path)))
    assert resultado["ok"] is True
    assert resultado["problemas"] == []
    assert len(resultado["advertencias"]) == 2
    assert any("curated/picking_por_tienda" in a for a in resultado["advertencias"])


# ------------------------------------------------------------- tasks 3-5

def test_agregar_kpis_placeholder_cuenta_tablas_con_datos(lake_completo):
    resumen = t.listar_fuentes(str(lake_completo))
    kpis = t.agregar_kpis(resumen, {"ok": True})
    assert kpis["tablas_con_datos"] == 4
    assert kpis["calidad_ok"] is True


def test_nombre_de_reporte_es_seguro_para_archivos():
    nombre = t.nombre_reporte("manual__2026-11-05T21:00:00+00:00")
    assert nombre == "reporte_manual__2026-11-05T21_00_00_00_00.json"
    assert "/" not in nombre and ":" not in nombre and "+" not in nombre


def test_generar_reporte_escribe_json_con_las_tres_secciones(lake_completo, monkeypatch):
    monkeypatch.delenv("REPORTES_PATH", raising=False)
    resumen = t.listar_fuentes(str(lake_completo))
    calidad = t.validar_fuentes(resumen)
    kpis = t.agregar_kpis(resumen, calidad)

    ruta = t.generar_reporte("run-1", resumen, calidad, kpis, base=str(lake_completo))

    assert Path(ruta).parent == lake_completo / "curated" / "reportes"
    contenido = json.loads(Path(ruta).read_text(encoding="utf-8"))
    assert contenido["run_id"] == "run-1"
    assert contenido["fuentes"] == resumen
    assert contenido["calidad"]["ok"] is True
    assert contenido["kpis"] == kpis


def test_generar_reporte_es_idempotente_por_run_id(lake_completo, monkeypatch):
    # Un reintento de la task con el mismo run_id sobrescribe, no duplica el reporte.
    monkeypatch.delenv("REPORTES_PATH", raising=False)
    args = ("run-2", {}, {"ok": True}, {})
    t.generar_reporte(*args, base=str(lake_completo))
    t.generar_reporte(*args, base=str(lake_completo))
    assert len(list((lake_completo / "curated" / "reportes").glob("*.json"))) == 1


def test_reportes_path_cambia_la_carpeta_de_salida(tmp_path, monkeypatch):
    destino = tmp_path / "otra_carpeta"
    monkeypatch.setenv("REPORTES_PATH", str(destino))
    ruta = t.generar_reporte("run-3", {}, {"ok": True}, {}, base=str(tmp_path))
    assert Path(ruta).parent == destino


def test_resumen_final_indica_estado_y_ruta():
    mensaje = t.resumen_final("/tmp/r.json", {"ok": True, "advertencias": ["a", "b"]})
    assert mensaje == "Pipeline OK | advertencias: 2 | reporte: /tmp/r.json"
    assert "CON PROBLEMAS" in t.resumen_final("/tmp/r.json", {"ok": False})

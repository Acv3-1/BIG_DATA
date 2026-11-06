"""Lógica de las tareas del DAG `rapidgo_pipeline` (Variante 10: Inventario y fulfillment).

Está escrita en Python puro (solo biblioteca estándar) a propósito: así se puede probar con
pytest sin instalar Airflow (ver tests/test_airflow_tareas.py), y el archivo del DAG
(rapidgo_pipeline.py) se limita a conectar estas funciones como tasks.

Reparto del trabajo en estas funciones (para que el equipo no pise el trabajo de otros):

    listar_fuentes / validar_fuentes  -> base (Issue #23). Calidad de datos: Issue #25 (C)
    agregar_kpis                      -> placeholder; la agregación real es del Issue #26 (B)
    generar_reporte                   -> base (Issue #23); el contenido crece con #26

El Data Lake que se lee lo escribe el job de Spark (ver datalake/README.md):

    raw/eventos  ·  processed/detalle  ·  curated/picking_por_tienda  ·  curated/alertas
"""
from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

RAW_EVENTOS = "raw/eventos"
PROCESSED_DETALLE = "processed/detalle"
CURATED_PICKING = "curated/picking_por_tienda"
CURATED_ALERTAS = "curated/alertas"

TABLAS = [RAW_EVENTOS, PROCESSED_DETALLE, CURATED_PICKING, CURATED_ALERTAS]

# Sin Raw y Processed el stream no está funcionando: eso sí hace fallar la validación.
# Curated puede tardar en aparecer (el sink Parquet escribe una ventana cuando el
# watermark la cierra, ~15 min después de iniciarla), así que solo genera advertencia.
REQUERIDAS = [RAW_EVENTOS, PROCESSED_DETALLE]


class FuenteNoDisponible(Exception):
    """Una fuente requerida del Data Lake no existe o no tiene datos."""


def ruta_datalake() -> str:
    return os.getenv("DATALAKE_PATH", "datalake")


def listar_fuentes(base: str | None = None) -> dict:
    """Task 1: inspecciona las tablas del Data Lake y devuelve un resumen (va por XCom).

    Por tabla: si la carpeta existe, cuántos archivos Parquet tiene, su tamaño en bytes
    y la fecha de última modificación (UTC, ISO 8601).
    """
    base_path = Path(base or ruta_datalake())
    resumen: dict[str, dict] = {}
    for tabla in TABLAS:
        carpeta = base_path / tabla
        archivos = list(carpeta.rglob("*.parquet")) if carpeta.is_dir() else []
        ultima = max((a.stat().st_mtime for a in archivos), default=None)
        resumen[tabla] = {
            "existe": carpeta.is_dir(),
            "archivos_parquet": len(archivos),
            "bytes": sum(a.stat().st_size for a in archivos),
            "ultima_modificacion": (
                datetime.fromtimestamp(ultima, tz=timezone.utc).isoformat() if ultima else None
            ),
        }
    return resumen


def evaluar_fuentes(resumen: dict, requeridas: list[str] | None = None) -> dict:
    """Clasifica el resumen en problemas (bloquean) y advertencias (solo se informan)."""
    requeridas = REQUERIDAS if requeridas is None else requeridas
    problemas: list[str] = []
    advertencias: list[str] = []
    for tabla in TABLAS:
        info = resumen.get(tabla, {})
        if not info.get("existe"):
            motivo = "la carpeta no existe"
        elif info.get("archivos_parquet", 0) == 0:
            motivo = "0 archivos Parquet"
        else:
            continue
        if tabla in requeridas:
            problemas.append(f"{tabla} ({motivo})")
        else:
            advertencias.append(f"{tabla} ({motivo}; Curated puede tardar ~15 min en aparecer)")
    return {"ok": not problemas, "problemas": problemas, "advertencias": advertencias}


def validar_fuentes(resumen: dict) -> dict:
    """Task 2: valida que las fuentes requeridas tengan datos; si no, falla.

    Falla con FuenteNoDisponible y un mensaje que dice QUÉ tabla falta; ese mensaje es el
    que aparece en el log de la task. Un reintento tiene sentido: los datos pueden estar
    llegando. Pendiente (Issue #25, C): nulos, stock negativo y SKU inválido sobre los datos.
    """
    resultado = evaluar_fuentes(resumen)
    if not resultado["ok"]:
        raise FuenteNoDisponible(
            "Fuentes requeridas sin datos: " + "; ".join(resultado["problemas"])
        )
    return resultado


def agregar_kpis(resumen: dict, calidad: dict) -> dict:
    """Task 3 (placeholder): agregación de KPIs. La implementación real es el Issue #26 (B).

    Por ahora solo deja constancia de cuántas tablas con datos hay, para que el reporte y el
    paso de información entre tasks (XCom) ya funcionen de punta a punta.
    """
    return {
        "estado": "pendiente_issue_26",
        "tablas_con_datos": sum(1 for i in resumen.values() if i.get("archivos_parquet", 0) > 0),
        "calidad_ok": calidad.get("ok", False),
    }


def nombre_reporte(run_id: str) -> str:
    """Nombre de archivo seguro y único por ejecución (así un reintento no duplica reportes)."""
    return "reporte_" + re.sub(r"[^A-Za-z0-9_.-]", "_", run_id) + ".json"


def generar_reporte(run_id: str, resumen: dict, calidad: dict, kpis: dict,
                    base: str | None = None) -> str:
    """Task 4: escribe el reporte de la ejecución en JSON y devuelve su ruta."""
    base_path = Path(base or ruta_datalake())
    carpeta = Path(os.getenv("REPORTES_PATH", str(base_path / "curated" / "reportes")))
    carpeta.mkdir(parents=True, exist_ok=True)
    reporte = {
        "run_id": run_id,
        "generado_en": datetime.now(timezone.utc).isoformat(),
        "fuentes": resumen,
        "calidad": calidad,
        "kpis": kpis,
    }
    ruta = carpeta / nombre_reporte(run_id)
    ruta.write_text(json.dumps(reporte, indent=2, ensure_ascii=False), encoding="utf-8")
    return str(ruta)


def resumen_final(ruta_reporte: str, calidad: dict) -> str:
    """Task 5: línea de cierre que queda en el log de la ejecución."""
    estado = "OK" if calidad.get("ok") else "CON PROBLEMAS"
    avisos = len(calidad.get("advertencias", []))
    return f"Pipeline {estado} | advertencias: {avisos} | reporte: {ruta_reporte}"

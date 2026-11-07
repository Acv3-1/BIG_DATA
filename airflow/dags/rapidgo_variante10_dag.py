"""DAG de RapidGo - Variante 10 (Inventario y fulfillment).

validar_fuentes -> chequeo_calidad -> calcular_kpis -> generar_reporte -> notificar_alertas

Reparto sugerido del trabajo en la Semana 3:
  - validar_fuentes y notificar_alertas: Integrante A (#23)
  - chequeo_calidad: Integrante C (#25) -> aquí hay una versión mínima que C amplía
  - calcular_kpis, generar_reporte, retries y XCom: Integrante B (#24 y #26)

Las dependencias salen del paso de datos entre tasks (TaskFlow): cada valor que
una task devuelve viaja por XCom a la siguiente.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

try:                                    # Airflow 3
    from airflow.sdk import DAG, Variable, task
except ImportError:                     # Airflow 2
    from airflow import DAG
    from airflow.decorators import task
    from airflow.models import Variable

from kpis_inventario import calcular_kpis, cargar_eventos, generar_reporte

log = logging.getLogger("airflow.task")

# Rutas dentro del contenedor de Airflow (monta ./datalake en /opt/airflow/datalake)
PROCESSED = os.getenv("RAPIDGO_PROCESSED", "/opt/airflow/datalake/processed")
REPORTES = os.getenv("RAPIDGO_REPORTES", "/opt/airflow/datalake/curated/reportes")

default_args = {
    "owner": "equipo-variante10",
    "retries": 2,                              # reintentos antes de marcar la task como fallida
    "retry_delay": timedelta(seconds=30),      # espera entre reintentos
}


def _variable(nombre: str, defecto: str) -> str:
    try:
        return str(Variable.get(nombre))
    except Exception:                          # la variable no existe
        return defecto


with DAG(
    dag_id="rapidgo_variante10",
    description="Pipeline de control de inventario y picking (variante 10)",
    start_date=datetime(2026, 1, 1),
    schedule=None,                             # se dispara a mano desde la interfaz
    catchup=False,
    default_args=default_args,
    tags=["rapidgo", "variante10"],
) as dag:

    @task
    def validar_fuentes() -> str:
        """Comprueba que existan archivos Parquet en Processed."""
        carpeta = Path(PROCESSED)
        archivos = list(carpeta.rglob("*.parquet")) if carpeta.exists() else []
        if not archivos:
            raise FileNotFoundError(f"No hay archivos Parquet en {carpeta}")
        log.info("Fuente validada: %s archivos en %s", len(archivos), carpeta)
        return str(carpeta)

    @task
    def chequeo_calidad(ruta: str) -> dict:
        """Versión mínima; C la amplía en #25 (nulos, rangos, duplicados...)."""
        eventos = cargar_eventos(ruta)
        invalidos = sum(
            1 for e in eventos
            if e.get("stock") is None or e["stock"] < 0
            or e.get("picking_time_sec") is None or e["picking_time_sec"] <= 0
        )
        resultado = {"eventos": len(eventos), "invalidos": invalidos}
        if not eventos or invalidos / len(eventos) > 0.05:
            raise ValueError(f"Calidad insuficiente: {resultado}")
        log.info("Calidad OK: %s", resultado)
        return resultado

    @task
    def calcular_kpis_task(ruta: str, calidad: dict) -> dict:
        """KPIs de la variante. 'calidad' llega por XCom y ordena la ejecución."""
        # Fallo controlado (#27): crea la Variable rapidgo_forzar_fallo = true en la
        # interfaz de Airflow (Admin -> Variables) para provocarlo; ponla en false
        # y reintenta la task para demostrar la recuperación.
        if _variable("rapidgo_forzar_fallo", "false").lower() == "true":
            raise RuntimeError("Fallo controlado: rapidgo_forzar_fallo=true")
        kpis = calcular_kpis(cargar_eventos(ruta))
        log.info("KPIs calculados sobre %s eventos (calidad: %s)", kpis["total_eventos"], calidad)
        return kpis                            # viaja por XCom (debe ser JSON-serializable)

    @task
    def generar_reporte_task(kpis: dict, **contexto) -> dict:
        etiqueta = str(contexto.get("ds") or datetime.now().strftime("%Y-%m-%d"))
        rutas = generar_reporte(kpis, REPORTES, etiqueta)
        log.info("Reporte generado: %s", rutas)
        return rutas

    @task
    def notificar_alertas(kpis: dict, reporte: dict) -> None:
        """Versión mínima: deja las alertas en el log. A/C pueden ampliarla."""
        alertas = kpis["alertas"]
        if alertas["stock_critico"] or alertas["picking_fuera_de_tiempo"]:
            log.warning("ALERTAS: %s (reporte: %s)", alertas, reporte["markdown"])
        else:
            log.info("Sin alertas")

    ruta = validar_fuentes()
    calidad = chequeo_calidad(ruta)
    kpis = calcular_kpis_task(ruta, calidad)
    reporte = generar_reporte_task(kpis)
    notificar_alertas(kpis, reporte)

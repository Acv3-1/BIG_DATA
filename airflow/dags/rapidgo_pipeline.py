"""DAG base de RapidGo - Variante 10 (Inventario y fulfillment). Issue #23.

Orquesta las tareas de control sobre el Data Lake que escribe Spark. El streaming
(Kafka -> Spark) sigue corriendo de forma continua fuera de Airflow; este DAG es la parte
batch/control: validar las fuentes, validar la calidad, agregar KPIs y generar un reporte.

    verificar_fuentes -> validar_calidad -> agregar_kpis -> generar_reporte -> notificar_resultado

- Dependencias: las define el orden de las llamadas (TaskFlow) y se ven en la pestaña Graph.
- Retries: `default_args` (2 reintentos, 30 s entre intentos) aplican a todas las tasks.
- XCom: cada task devuelve un valor y la siguiente lo recibe como argumento (TaskFlow lo
  guarda y lo pasa por XCom automáticamente).
- La lógica está en rapidgo_tareas.py (Python puro, con pruebas en tests/).

Se lanza a mano (schedule=None) desde la interfaz: http://localhost:8080 (ver airflow/README.md).
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG, task

import rapidgo_tareas as t

default_args = {
    "owner": "equipo-variante10",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="rapidgo_pipeline",
    description="Control sobre el Data Lake: fuentes, calidad, KPIs y reporte (Variante 10)",
    schedule=None,
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=default_args,
    tags=["rapidgo", "variante10"],
) as dag:

    @task
    def verificar_fuentes() -> dict:
        """Lista las tablas del Data Lake (archivos Parquet, tamaño, última modificación)."""
        return t.listar_fuentes()

    @task
    def validar_calidad(resumen: dict) -> dict:
        """Falla si Raw o Processed no tienen datos (calidad detallada: Issue #25)."""
        return t.validar_fuentes(resumen)

    @task
    def agregar_kpis(resumen: dict, calidad: dict) -> dict:
        """Agrega los KPIs de la variante (implementación real: Issue #26)."""
        return t.agregar_kpis(resumen, calidad)

    @task
    def generar_reporte(resumen: dict, calidad: dict, kpis: dict, run_id: str | None = None) -> str:
        """Escribe el reporte JSON de la ejecución y devuelve su ruta.

        `run_id` es una variable de contexto: Airflow la rellena sola (por eso su valor por
        defecto debe ser None; cualquier otro valor hace fallar la importación del DAG).
        """
        return t.generar_reporte(run_id or "sin_run_id", resumen, calidad, kpis)

    
    @task
    def notificar_resultado(ruta_reporte: str, calidad: dict) -> str:
        """Deja en el log una línea de cierre con el estado y la ruta del reporte."""
        mensaje = t.resumen_final(ruta_reporte, calidad)
        print(mensaje)
        return mensaje

    fuentes = verificar_fuentes()
    calidad = validar_calidad(fuentes)
    kpis = agregar_kpis(fuentes, calidad)
    reporte = generar_reporte(fuentes, calidad, kpis)

    notificar_resultado(reporte, calidad)

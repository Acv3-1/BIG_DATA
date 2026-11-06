"""Importación y estructura real del DAG con Airflow instalado (Issue #28).

Se ejecutan en el job `validar-dag` del CI, donde se instala Airflow. Sin Airflow se omiten
(así el CI básico y tu entorno local no necesitan instalarlo).
"""
import sys
from pathlib import Path

import pytest

# Ojo: se importa `airflow.sdk` y no `airflow`, porque la carpeta airflow/ del repo (sin
# __init__.py) es un "namespace package" que `import airflow` encontraría aunque Airflow
# no esté instalado.
pytest.importorskip("airflow.sdk")

DAGS = Path(__file__).resolve().parents[1] / "airflow" / "dags"
sys.path.insert(0, str(DAGS))

import rapidgo_pipeline  # noqa: E402  (si falla la importación, el test falla: eso es lo que se valida)

dag = rapidgo_pipeline.dag

DEPENDENCIAS = [
    ("verificar_fuentes", "validar_calidad"),
    ("validar_calidad", "agregar_kpis"),
    ("agregar_kpis", "generar_reporte"),
    ("generar_reporte", "notificar_resultado"),
]


def test_el_dag_se_importa_con_el_id_esperado():
    assert dag.dag_id == "rapidgo_pipeline"


def test_el_dag_tiene_al_menos_5_tasks():
    assert len(dag.task_dict) >= 5


@pytest.mark.parametrize("origen,destino", DEPENDENCIAS)
def test_dependencias_entre_tasks(origen, destino):
    assert destino in dag.task_dict[origen].downstream_task_ids


def test_todas_las_tasks_tienen_retries():
    for task in dag.tasks:
        assert task.retries >= 1, f"{task.task_id} no tiene retries"


def test_el_dag_no_tiene_ciclos_ni_tasks_aisladas():
    for task in dag.tasks:
        conectada = task.upstream_task_ids or task.downstream_task_ids
        assert conectada, f"{task.task_id} no está conectada a ninguna otra task"

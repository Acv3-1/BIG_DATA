"""Pruebas de estructura del DAG leyendo el código fuente (Issue #28).

Usan `ast`, así que NO necesitan Airflow instalado y corren en el CI básico. Comprueban lo que
la guía exige del DAG de la Semana 3: mínimo 5 tasks, retries y paso de información (XCom).
La importación real del DAG con Airflow se prueba en tests/test_dag_rapidgo.py (job validar-dag).
"""
import ast
from pathlib import Path

ARCHIVO_DAG = Path(__file__).resolve().parents[1] / "airflow" / "dags" / "rapidgo_pipeline.py"


def _arbol():
    return ast.parse(ARCHIVO_DAG.read_text(encoding="utf-8"))


def _funciones_task(arbol):
    """Nombres de las funciones decoradas con @task (cada una es una task del DAG)."""
    nombres = []
    for nodo in ast.walk(arbol):
        if isinstance(nodo, ast.FunctionDef):
            for deco in nodo.decorator_list:
                destino = deco.func if isinstance(deco, ast.Call) else deco
                if (isinstance(destino, ast.Name) and destino.id == "task") or (
                        isinstance(destino, ast.Attribute) and destino.attr == "task"):
                    nombres.append(nodo.name)
    return nombres


def test_el_dag_tiene_al_menos_5_tasks():
    assert len(_funciones_task(_arbol())) >= 5


def test_las_tasks_esperadas_existen():
    esperadas = {"verificar_fuentes", "validar_calidad", "agregar_kpis",
                 "generar_reporte", "notificar_resultado"}
    assert esperadas <= set(_funciones_task(_arbol()))


def test_default_args_define_retries_y_espera_entre_intentos():
    for nodo in ast.walk(_arbol()):
        if isinstance(nodo, ast.Assign) and any(
                isinstance(t, ast.Name) and t.id == "default_args" for t in nodo.targets):
            claves = {k.value for k in nodo.value.keys}
            assert {"retries", "retry_delay"} <= claves
            retries = nodo.value.values[[k.value for k in nodo.value.keys].index("retries")]
            assert retries.value >= 1
            return
    raise AssertionError("el DAG no define default_args")


def test_el_dag_es_de_lanzamiento_manual_y_sin_catchup():
    codigo = ARCHIVO_DAG.read_text(encoding="utf-8")
    assert "schedule=None" in codigo
    assert "catchup=False" in codigo


def test_hay_paso_de_informacion_entre_tasks_por_xcom():
    """Con TaskFlow, pasar el resultado de una task como argumento de otra usa XCom.

    Se busca al menos una llamada a una task cuyo argumento sea el resultado de otra
    (p. ej. `calidad = validar_calidad(fuentes)`).
    """
    arbol = _arbol()
    tasks = set(_funciones_task(arbol))
    variables = {}   # variable -> task que la produjo
    encadenadas = 0
    for nodo in ast.walk(arbol):
        if isinstance(nodo, ast.Assign) and isinstance(nodo.value, ast.Call):
            llamada = nodo.value
            if isinstance(llamada.func, ast.Name) and llamada.func.id in tasks:
                usadas = [a.id for a in llamada.args if isinstance(a, ast.Name)]
                encadenadas += sum(1 for u in usadas if u in variables)
                if isinstance(nodo.targets[0], ast.Name):
                    variables[nodo.targets[0].id] = llamada.func.id
    assert encadenadas >= 3

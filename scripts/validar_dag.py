"""Validación estática de los DAG de Airflow (no requiere instalar Airflow).

Comprueba, leyendo el código con `ast`, que:
  - al menos un archivo de airflow/dags/ define un DAG con dag_id,
  - el DAG tiene como mínimo 5 tasks (funciones @task u Operators),
  - se configuran reintentos (retries).

Uso (desde la raíz del repo):  python scripts/validar_dag.py
Es una validación de forma: no sustituye ejecutar el DAG en Airflow.
"""
from __future__ import annotations

import ast
import sys
from pathlib import Path

CARPETA_DAGS = Path("airflow/dags")
MIN_TASKS = 5


def _nombre(nodo: ast.AST) -> str:
    if isinstance(nodo, ast.Name):
        return nodo.id
    if isinstance(nodo, ast.Attribute):
        return f"{_nombre(nodo.value)}.{nodo.attr}"
    if isinstance(nodo, ast.Call):
        return _nombre(nodo.func)
    return ""


def analizar(ruta: Path) -> dict:
    arbol = ast.parse(ruta.read_text(encoding="utf-8"), filename=str(ruta))
    info = {"es_dag": False, "dag_ids": [], "tasks": 0, "retries": False}
    for nodo in ast.walk(arbol):
        if isinstance(nodo, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for dec in nodo.decorator_list:
                n = _nombre(dec)
                if n == "task" or n.startswith("task."):
                    info["tasks"] += 1
                if n == "dag":
                    info["es_dag"] = True
        elif isinstance(nodo, ast.Call):
            n = _nombre(nodo.func)
            if n.split(".")[-1].endswith("Operator"):
                info["tasks"] += 1
            if n.split(".")[-1] == "DAG":
                info["es_dag"] = True
                for kw in nodo.keywords:
                    if kw.arg == "dag_id" and isinstance(kw.value, ast.Constant):
                        info["dag_ids"].append(kw.value.value)
        elif isinstance(nodo, ast.keyword) and nodo.arg == "retries":
            info["retries"] = True
        elif isinstance(nodo, ast.Dict):
            for clave in nodo.keys:
                if isinstance(clave, ast.Constant) and clave.value == "retries":
                    info["retries"] = True
    return info


def main() -> int:
    errores: list[str] = []
    dags_encontrados = 0
    for ruta in sorted(CARPETA_DAGS.glob("*.py")):
        try:
            info = analizar(ruta)
        except SyntaxError as exc:
            errores.append(f"{ruta}: error de sintaxis: {exc}")
            continue
        if not info["es_dag"]:
            continue                      # módulo auxiliar (por ejemplo, kpis_inventario.py)
        dags_encontrados += 1
        print(f"{ruta}: dag_id={info['dag_ids']} tasks={info['tasks']} retries={info['retries']}")
        if info["tasks"] < MIN_TASKS:
            errores.append(f"{ruta}: tiene {info['tasks']} tasks (mínimo {MIN_TASKS})")
        if not info["retries"]:
            errores.append(f"{ruta}: no configura 'retries'")
    if dags_encontrados == 0:
        errores.append(f"No se encontró ningún DAG en {CARPETA_DAGS}/")
    for e in errores:
        print(f"ERROR: {e}", file=sys.stderr)
    return 1 if errores else 0


if __name__ == "__main__":
    sys.exit(main())

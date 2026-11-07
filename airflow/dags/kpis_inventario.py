"""KPIs de la variante 10 (Inventario y fulfillment).

Funciones puras: reciben una lista de eventos (dicts) y devuelven resultados
serializables a JSON. No dependen de Airflow ni de Spark, así se prueban con
pytest y los resultados viajan por XCom sin problema.

AJUSTA los umbrales y las definiciones a lo que el equipo fijó en la Semana 1
(KPIs) y en la calibración de la Semana 2 (#20 / ADR-002).
"""
from __future__ import annotations

import json
from pathlib import Path

# --- Umbrales (deben coincidir con las reglas de alerta del equipo) ---------
UMBRAL_STOCK_CRITICO = 5       # unidades: stock <= 5 -> alerta de stock crítico
UMBRAL_PICKING_SEG = 600       # segundos: picking > 10 min -> fuera de tiempo


def stockouts(eventos: list[dict]) -> int:
    """Eventos con stock agotado (stock == 0)."""
    return sum(1 for e in eventos if e["stock"] == 0)


def picking_promedio(eventos: list[dict]) -> float | None:
    """Tiempo de picking promedio en segundos (None si no hay eventos)."""
    if not eventos:
        return None
    return sum(e["picking_time_sec"] for e in eventos) / len(eventos)


def picking_promedio_por_tienda(eventos: list[dict]) -> dict[str, float]:
    acumulado: dict[str, list[int]] = {}
    for e in eventos:
        acumulado.setdefault(e["store_id"], []).append(e["picking_time_sec"])
    return {t: round(sum(v) / len(v), 1) for t, v in sorted(acumulado.items())}


def pedidos_incompletos(eventos: list[dict]) -> dict:
    """Pedidos donde se pickearon menos ítems de los solicitados."""
    total = len(eventos)
    incompletos = sum(1 for e in eventos if e["items_picked"] < e["items_requested"])
    return {
        "total": total,
        "incompletos": incompletos,
        "porcentaje": round(100 * incompletos / total, 1) if total else 0.0,
    }


def calcular_kpis(eventos: list[dict],
                  umbral_stock: int = UMBRAL_STOCK_CRITICO,
                  umbral_picking: int = UMBRAL_PICKING_SEG) -> dict:
    """Calcula todos los KPIs y los conteos de alertas. Resultado serializable."""
    promedio = picking_promedio(eventos)
    return {
        "total_eventos": len(eventos),
        "stockouts": stockouts(eventos),
        "picking_promedio_seg": round(promedio, 1) if promedio is not None else None,
        "picking_promedio_por_tienda": picking_promedio_por_tienda(eventos),
        "pedidos_incompletos": pedidos_incompletos(eventos),
        "alertas": {
            "stock_critico": sum(1 for e in eventos if e["stock"] <= umbral_stock),
            "picking_fuera_de_tiempo": sum(
                1 for e in eventos if e["picking_time_sec"] > umbral_picking),
        },
    }


def cargar_eventos(ruta: str) -> list[dict]:
    """Lee un dataset Parquet (carpeta) y devuelve los eventos como lista de dicts.

    Requiere pyarrow instalado en el entorno de Airflow.
    """
    import pyarrow.dataset as ds
    return ds.dataset(ruta, format="parquet", partitioning="hive").to_table().to_pylist()


def generar_reporte(kpis: dict, carpeta: str, etiqueta: str) -> dict:
    """Escribe el reporte de KPIs en JSON y Markdown. Devuelve las rutas."""
    destino = Path(carpeta)
    destino.mkdir(parents=True, exist_ok=True)
    ruta_json = destino / f"reporte_kpis_{etiqueta}.json"
    ruta_md = destino / f"reporte_kpis_{etiqueta}.md"

    ruta_json.write_text(json.dumps(kpis, indent=2, ensure_ascii=False), encoding="utf-8")

    inc = kpis["pedidos_incompletos"]
    lineas = [
        f"# Reporte de KPIs - Variante 10 ({etiqueta})",
        "",
        f"- Eventos analizados: {kpis['total_eventos']}",
        f"- Stockouts: {kpis['stockouts']}",
        f"- Picking promedio: {kpis['picking_promedio_seg']} s",
        f"- Pedidos incompletos: {inc['incompletos']} de {inc['total']} ({inc['porcentaje']} %)",
        "",
        "## Alertas",
        f"- Stock crítico: {kpis['alertas']['stock_critico']}",
        f"- Picking fuera de tiempo: {kpis['alertas']['picking_fuera_de_tiempo']}",
        "",
        "## Picking promedio por tienda (s)",
        "| Tienda | Picking promedio |",
        "|---|---|",
    ]
    lineas += [f"| {t} | {v} |" for t, v in kpis["picking_promedio_por_tienda"].items()]
    ruta_md.write_text("\n".join(lineas) + "\n", encoding="utf-8")
    return {"json": str(ruta_json), "markdown": str(ruta_md)}

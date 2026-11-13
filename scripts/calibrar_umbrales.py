"""Calibración de los umbrales de alerta (Issue #33): ¿dónde conviene poner cada umbral?

Para cada umbral candidato muestra qué porcentaje de eventos dispararía la alerta, y un
histograma del tiempo de picking para ver si hay un "hueco" natural entre los eventos normales
y los lentos (el umbral debe quedar en ese hueco).

Uso (desde la raíz del repo):

    python scripts/calibrar_umbrales.py                       # datos simulados con el producer
    python scripts/calibrar_umbrales.py --eventos 50000 --seed 7
    python scripts/calibrar_umbrales.py --lake datalake       # datos REALES de Processed
                                                              # (requiere pandas y pyarrow)

Los umbrales vigentes salen de spark/reglas.py (se pueden cambiar con UMBRAL_STOCK_CRITICO y
UMBRAL_PICKING_SEC). La decisión final y su justificación están en
docs/decisions/ADR-004-umbrales-finales.md.
"""
from __future__ import annotations

import argparse
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))   # importar producer/ y spark/

from producer.eventos import generar_evento  # noqa: E402
from spark.reglas import (FACTOR_SEVERIDAD_PICKING, UMBRAL_PICKING_SEC,  # noqa: E402
                          UMBRAL_STOCK_CRITICO)

UMBRALES_PICKING = (400, 450, 500, 550, 600, 700)
UMBRALES_STOCK = (0, 3, 5, 6, 8, 10, 20)
CORTES_HISTOGRAMA = (0, 200, 300, 400, 450, 500, 550, 600, 700, 800, 900, 1000, 1100, 1201)


def porcentaje(cantidad: int, total: int) -> float:
    return round(100 * cantidad / total, 2) if total else 0.0


def analizar(picking: list[int], stock: list[int],
             umbral_picking: int = UMBRAL_PICKING_SEC,
             umbral_stock: int = UMBRAL_STOCK_CRITICO,
             factor_alta: float = FACTOR_SEVERIDAD_PICKING) -> dict:
    """Calcula los porcentajes de alerta por umbral candidato y el histograma de picking."""
    n = len(picking)
    histograma = {
        f"{a}-{b - 1}": porcentaje(sum(1 for x in picking if a <= x < b), n)
        for a, b in zip(CORTES_HISTOGRAMA, CORTES_HISTOGRAMA[1:])
    }
    return {
        "eventos": n,
        "picking_promedio": round(sum(picking) / n, 1) if n else None,
        "picking_max": max(picking) if n else None,
        "histograma_picking": histograma,
        "pct_alerta_picking": {u: porcentaje(sum(1 for x in picking if x > u), n)
                               for u in UMBRALES_PICKING},
        "pct_alerta_stock": {u: porcentaje(sum(1 for x in stock if x <= u), n)
                             for u in UMBRALES_STOCK},
        "umbral_picking": umbral_picking,
        "umbral_stock": umbral_stock,
        "umbral_alta_picking": factor_alta * umbral_picking,
        "pct_picking_alta": porcentaje(
            sum(1 for x in picking if x > factor_alta * umbral_picking), n),
        "pct_picking_alta_con_factor_2": porcentaje(
            sum(1 for x in picking if x > 2 * umbral_picking), n),
    }


def datos_simulados(n: int, semilla: int) -> tuple[list[int], list[int]]:
    rng = random.Random(semilla)
    eventos = [generar_evento(rng) for _ in range(n)]
    return [e["picking_time_sec"] for e in eventos], [e["stock"] for e in eventos]


def datos_del_lake(base: str) -> tuple[list[int], list[int]]:
    """Lee stock y picking de la capa Processed (datos reales escritos por Spark)."""
    import pandas as pd
    df = pd.read_parquet(Path(base) / "processed" / "detalle",
                         columns=["stock", "picking_time_sec"])
    return df["picking_time_sec"].tolist(), df["stock"].tolist()


def imprimir(r: dict, origen: str) -> None:
    marca = lambda u, vigente: "  <- vigente" if u == vigente else ""   # noqa: E731
    print(f"Origen: {origen} | eventos: {r['eventos']} | picking promedio: "
          f"{r['picking_promedio']} s | máximo: {r['picking_max']} s\n")
    print("Histograma del tiempo de picking (% de eventos)")
    for rango, pct in r["histograma_picking"].items():
        print(f"  {rango:>9} s  {pct:6.2f} %  {'#' * int(pct)}")
    print("\nUmbral de picking -> % de eventos que disparan la alerta")
    for u, pct in r["pct_alerta_picking"].items():
        print(f"  > {u:<4} s   {pct:6.2f} %{marca(u, r['umbral_picking'])}")
    print("\nUmbral de stock -> % de eventos que disparan la alerta")
    for u, pct in r["pct_alerta_stock"].items():
        print(f"  <= {u:<3}     {pct:6.2f} %{marca(u, r['umbral_stock'])}")
    print(f"\nSeveridad alta del picking (> {r['umbral_alta_picking']:.0f} s): "
          f"{r['pct_picking_alta']} % de los eventos "
          f"(con factor 2 -> > {2 * r['umbral_picking']} s: "
          f"{r['pct_picking_alta_con_factor_2']} %)")


def main() -> int:
    p = argparse.ArgumentParser(description="Calibración de umbrales de alerta")
    p.add_argument("--eventos", type=int, default=20000, help="eventos simulados")
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--lake", default=None,
                   help="carpeta del Data Lake: usa datos reales de processed/detalle")
    args = p.parse_args()
    if args.lake:
        picking, stock = datos_del_lake(args.lake)
        origen = f"datos reales ({args.lake}/processed/detalle)"
    else:
        picking, stock = datos_simulados(args.eventos, args.seed)
        origen = f"simulación con el producer (semilla {args.seed})"
    if not picking:
        print("No hay eventos para analizar.", file=sys.stderr)
        return 1
    imprimir(analizar(picking, stock), origen)
    return 0


if __name__ == "__main__":
    sys.exit(main())

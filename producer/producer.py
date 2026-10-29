"""Producer de eventos para Kafka (Variante 10: Inventario y fulfillment).

Ejecutar desde la RAÍZ del repositorio:

    python -m producer.producer                      # envío continuo a Kafka (2 eventos/s)
    python -m producer.producer --count 50 --rate 5  # 50 eventos a 5 por segundo
    python -m producer.producer --dry-run --count 3  # solo imprime los eventos, sin Kafka
"""
from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time

from producer.eventos import TEAM_POR_DEFECTO, clave_particion, generar_evento


def construir_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Producer de eventos de inventario y picking")
    p.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
                   help="servidor de Kafka (por defecto localhost:9092)")
    p.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "inventario-fulfillment"))
    p.add_argument("--team", default=os.getenv("TEAM_ID", TEAM_POR_DEFECTO),
                   help="identificador del equipo incluido en cada evento")
    p.add_argument("--rate", type=float, default=2.0, help="eventos por segundo")
    p.add_argument("--count", type=int, default=0, help="cantidad de eventos (0 = infinito)")
    p.add_argument("--seed", type=int, default=None, help="semilla para resultados reproducibles")
    p.add_argument("--dry-run", action="store_true", help="imprime los eventos sin enviarlos a Kafka")
    return p


def al_confirmar(err, msg) -> None:
    """Callback de Kafka: muestra en qué partición y offset quedó cada evento."""
    if err is not None:
        print(f"[ERROR] no se pudo entregar: {err}", file=sys.stderr)
    else:
        print(f"[OK] key={msg.key().decode()} -> partición {msg.partition()} offset {msg.offset()}")


def main() -> int:
    args = construir_parser().parse_args()
    if args.rate <= 0:
        print("--rate debe ser mayor que 0", file=sys.stderr)
        return 1

    rng = random.Random(args.seed)
    productor = None
    if not args.dry_run:
        from confluent_kafka import Producer  # import tardío: --dry-run no requiere Kafka
        productor = Producer({
            "bootstrap.servers": args.bootstrap,
            "acks": "all",
            "linger.ms": 20,
            "client.id": f"producer-{args.team}",
        })

    enviados = 0
    try:
        while args.count == 0 or enviados < args.count:
            evento = generar_evento(rng, team=args.team)
            valor = json.dumps(evento, ensure_ascii=False)
            if productor is None:
                print(valor)
            else:
                productor.produce(args.topic, key=clave_particion(evento), value=valor,
                                  callback=al_confirmar)
                productor.poll(0)
            enviados += 1
            time.sleep(1.0 / args.rate)
    except KeyboardInterrupt:
        print("\nInterrumpido por el usuario.")
    finally:
        if productor is not None:
            pendientes = productor.flush(10)
            if pendientes:
                print(f"[AVISO] {pendientes} mensajes sin confirmar", file=sys.stderr)
        print(f"Eventos generados: {enviados}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

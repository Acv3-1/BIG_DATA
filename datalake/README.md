# Data Lake (Parquet con checkpoint)

Lo escribe el job `spark/streaming_job.py` (Issue #17). Los datos generados **no se versionan** (ver `.gitignore`); solo este README.

```
datalake/
├── raw/eventos/                   Raw: el mensaje de Kafka tal cual llegó (value en texto + key, partición, offset)
├── processed/detalle/             Processed: eventos limpios, sin duplicados, con métricas por pedido
├── curated/picking_por_tienda/    Curated: picking promedio, incompletos y stockouts por tienda y ventana de 5 min
├── curated/alertas/               Curated: una fila por alerta (STOCK_CRITICO / PICKING_FUERA_DE_TIEMPO)
└── _checkpoints/<capa>/<tabla>/   estado de cada query (offsets de Kafka + estado del watermark)
```

| Capa | Para qué sirve | Particionado |
|---|---|---|
| Raw | Fuente de verdad: conserva incluso el JSON malformado, para poder reprocesar | `fecha_ingesta` |
| Processed | Datos confiables para analizar evento por evento | `fecha` (día del evento) |
| Curated | Resultados listos para consumir (KPIs y alertas) | — |

## Alertas (Issue #16)

| Regla | Condición | Umbral inicial | Variable de entorno |
|---|---|---|---|
| `STOCK_CRITICO` | `stock <= umbral` | 5 unidades | `UMBRAL_STOCK_CRITICO` |
| `PICKING_FUERA_DE_TIEMPO` | `picking_time_sec > umbral` | 600 s (10 min) | `UMBRAL_PICKING_SEC` |

Severidad: `alta` si el stock es 0 o el picking supera el doble del umbral; `media` en los demás casos.
Las reglas están en `spark/reglas.py` (Python puro) y se aplican a DataFrames en `spark/alertas.py`.
Los umbrales son **iniciales**: se ajustan en la calibración (Issue #20) tras observar valores reales.

## Cómo ejecutarlo

```bash
docker compose up -d kafka kafka-init
docker compose up spark                              # job continuo (escribe en ./datalake)
python -m producer.producer --count 200 --rate 20    # en otra terminal
```

Para ajustar umbrales sin tocar código, añade en `environment:` del servicio `spark`
(en `docker-compose.yml`) `UMBRAL_STOCK_CRITICO` y/o `UMBRAL_PICKING_SEC`.

## Cómo leerlo

```python
spark.read.parquet("datalake/curated/alertas").show(truncate=False)
spark.read.parquet("datalake/curated/picking_por_tienda").orderBy("ventana_inicio", "store_id").show()
```

## Cosas a tener en cuenta

- **Curated tarda en aparecer:** el sink Parquet solo admite `append`, así que una ventana se escribe cuando el
  watermark (10 min) la cierra, no antes. Con ventanas de 5 min, el KPI de una ventana aparece unos 15 min
  después de que empezó. Raw, Processed y alertas se escriben en cada micro-lote (cada 10 s).
- **Reiniciar el job** retoma desde el checkpoint: no relee ni duplica datos.
- **Reprocesar desde cero:** detener el job y borrar `datalake/<tabla>/` **y** `datalake/_checkpoints/<tabla>/`
  (o todo `raw/ processed/ curated/ _checkpoints/`). Borrar solo uno de los dos deja datos inconsistentes.

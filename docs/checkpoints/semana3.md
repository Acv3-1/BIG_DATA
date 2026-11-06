# Checkpoint Semana 3

**Variante 10 – Inventario y fulfillment** · Release objetivo: `v0.3.0-orchestration`

> Lo que ya es cierto está escrito. Lo marcado **[COMPLETAR]** necesita datos reales de la ejecución (la guía no acepta texto genérico). Borra esta nota al terminar.

## 1. Qué quedó funcionando esta semana

- Airflow 3.3.2 en Docker (servicio `airflow`, http://localhost:8080) con el DAG `rapidgo_pipeline` visible y ejecutado con éxito tras una recuperación (Issue #23).
- DAG de 5 tasks: `verificar_fuentes → validar_calidad → agregar_kpis → generar_reporte → notificar_resultado`, con retries (2 × 30 s) y paso de datos por XCom (#23, #24).
- CI ampliado: job `validar-dag` que instala Airflow, importa el DAG y comprueba sus tasks, dependencias y retries, más pruebas de las tareas (#28).
- **[COMPLETAR por B y C]** calidad de datos (#25), KPIs y reporte (#26) y fallo controlado (#27).
- Pruebas: **[COMPLETAR: resultado real de `python -m pytest -v`, p. ej. «N passed»]**.

## 2. Valor real observado en la ejecución

Reporte de la ejecución recuperada (`/opt/airflow/reportes/reporte_manual__2026-11-06T00:59:46…json`): archivos Parquet en `raw/eventos`: **6** (37 141 bytes) · `processed/detalle`: **6** (48 165 bytes) · `curated/picking_por_tienda`: **4** · `curated/alertas`: **13** (42 514 bytes). Calidad: `ok: true`, sin problemas ni advertencias.

## 3. Decisión técnica tomada y por qué

Airflow en modo `standalone` (un solo contenedor) con 2 reintentos por task, validación estricta solo de Raw y Processed (Curated vacío es solo advertencia porque Spark cierra las ventanas con el watermark, ~15 min) y recuperación re-ejecutando la task fallida con sus dependientes. Detalle y alternativas: `docs/decisions/ADR-003-orquestacion-y-recuperacion.md`.

## 4. Problema encontrado y cómo se resolvió

| | |
|---|---|
| Tarea que falló | `generar_reporte`, tras 3 intentos (1 + 2 reintentos); el DAG tardó 1 min 14 s en fallar |
| Mensaje real del log | `PermissionError: [Errno 13] Permission denied: '/opt/airflow/datalake/curated/reportes'` (en `rapidgo_tareas.py`, línea 127, al llamar a `mkdir`) |
| Tareas afectadas | `notificar_resultado` → `upstream_failed` («Fallido en Upstream»). Las 3 anteriores terminaron en verde. |
| Cambio que lo resolvió | Spark corre como `root`, así que sus carpetas en `datalake/` no admiten escritura del usuario de Airflow. El reporte pasó a escribirse en el volumen de Airflow (`REPORTES_PATH=/opt/airflow/reportes`) y `datalake/` se monta `:ro`. PR: **[COMPLETAR]** |

Recuperación: con el cambio aplicado, **Clear** sobre `generar_reporte` y luego sobre `notificar_resultado`; ambas pasaron a verde sin repetir las otras 3 tasks.

![DAG con la tarea fallida](../evidencias/semana3/dag-fallo.png)
![DAG recuperado](../evidencias/semana3/dag-recuperado.png)

## 5. Enlaces

- Issues #23, #24, #25, #26, #27, #28: **[COMPLETAR]**
- Pull Requests: **[COMPLETAR]**
- GitHub Actions (CI en verde): **[COMPLETAR]**

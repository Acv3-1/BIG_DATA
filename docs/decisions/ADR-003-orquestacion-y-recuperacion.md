# ADR-003 - Orquestación con Airflow standalone y recuperación por reintento y re-ejecución parcial

**Estado:** propuesto (el equipo debe revisarlo antes del merge) · **Fecha:** 2026-11-05

## Contexto / problema

El streaming de Spark escribe el Data Lake de forma continua, pero algo tiene que **controlar** ese resultado periódicamente: comprobar que las fuentes tienen datos, validar su calidad, agregar KPIs y generar un reporte. Si una de esas comprobaciones falla, hay que saber **qué tarea falló, qué quedó sin ejecutar** y poder recuperar sin repetir todo.

Dato del propio pipeline que condiciona el diseño: el sink Parquet solo admite `append`, así que Spark escribe cada ventana de Curated cuando el watermark la cierra (~15 min después de iniciarla, ver `datalake/README.md`). Es decir, que Curated esté vacío un rato **es normal**; que Raw o Processed estén vacíos **no lo es** (el stream no está funcionando).

## Opciones consideradas

1. **Docker Compose oficial de Airflow (CeleryExecutor):** scheduler, workers, Redis, PostgreSQL... Es lo más parecido a producción, pero son varios contenedores y bastante memoria para una demo local.
2. **Airflow en modo `standalone` en un solo contenedor (SQLite):** scheduler, API/UI y base de datos juntos. Suficiente para un DAG de 5 tasks; arranca con un solo servicio de `docker-compose.yml`.
3. **Un script o cron que ejecute las comprobaciones:** más simple, pero sin reintentos controlados, sin historial por tarea y sin ver qué dependía de qué.

## Decisión tomada

Opción 2, con estas políticas en el DAG `rapidgo_pipeline` (`airflow/dags/`):

- **Reintentos:** 2 reintentos con 30 s de espera en todas las tasks (`default_args`). Un fallo por datos que aún no llegaron puede resolverse solo.
- **Validación estricta solo donde importa:** `validar_calidad` falla si Raw o Processed no tienen archivos Parquet; si Curated está vacío solo registra una advertencia.
- **Recuperación parcial:** al fallar una task, las dependientes quedan en `upstream_failed` (no se ejecutan con datos malos). Tras corregir la causa se usa **Clear** sobre la task fallida con *Downstream*, y Airflow re-ejecuta solo esa y las siguientes.
- **Tasks idempotentes:** el reporte se escribe en `reporte_<run_id>.json`; un reintento sobrescribe el mismo archivo en lugar de duplicarlo.
- **Historial persistente:** volumen `airflow-home`, para conservar ejecuciones y logs entre reinicios.

## Evidencia utilizada

- **Fallo real (ejecución `manual__2026-11-06T00:59:46`):** `generar_reporte` falló con `PermissionError: [Errno 13] Permission denied: '/opt/airflow/datalake/curated/reportes'`. Hizo 3 intentos (1 + 2 reintentos) y el DAG tardó 1 min 14 s en fallar.
- **Tareas afectadas:** `notificar_resultado` quedó en `upstream_failed`. `verificar_fuentes`, `validar_calidad` y `agregar_kpis` terminaron en verde y no se repitieron.
- **Recuperación:** tras mover el reporte al volumen de Airflow, **Clear** sobre `generar_reporte` y `notificar_resultado`; ambas pasaron a verde y el reporte quedó generado (`calidad.ok: true`; 6 archivos Parquet en Raw y en Processed, 4 en picking por tienda y 13 en alertas).
- **Capturas:** `docs/evidencias/semana3/dag-fallo.png` y `dag-recuperado.png`.
- PR / Action relacionados: **[COMPLETAR: enlaces]**.

## Consecuencias / trade-offs

- (+) Un solo servicio de Docker: fácil de levantar y de explicar en la demo.
- (+) Fallar de forma temprana y clara evita generar reportes con datos incompletos.
- (+) La lógica de las tasks está en Python puro (`rapidgo_tareas.py`) y se prueba en CI sin instalar Airflow.
- (−) SQLite y `standalone` no son para producción: no hay ejecución distribuida ni alta disponibilidad.
- (−) Sin login (`SIMPLE_AUTH_MANAGER_ALL_ADMINS`): aceptable solo en local.
- (−) Los reintentos solo ayudan con fallos pasajeros (datos que aún no llegaron). El `PermissionError` de la ejecución real es determinista: los 2 reintentos no podían resolverlo y solo alargaron el fallo a 1 min 14 s. Para errores así, el camino es corregir la causa y usar Clear.
- (−) Mantener el lake en solo lectura para Airflow (`:ro`) obliga a guardar los reportes en el volumen de Airflow, no en `datalake/`.

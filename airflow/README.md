# Airflow (Semana 3)

Orquesta las tareas de control sobre el Data Lake que escribe Spark. El streaming (Kafka → Spark) sigue corriendo de forma continua; Airflow es la parte batch: validar fuentes y calidad, agregar KPIs y generar un reporte.

```
verificar_fuentes → validar_calidad → agregar_kpis → generar_reporte → notificar_resultado
```

| Task | Qué hace | Issue |
|---|---|---|
| `verificar_fuentes` | Lista las tablas Raw / Processed / Curated (archivos Parquet, tamaño, última modificación) | #23 |
| `validar_calidad` | Falla si Raw o Processed no tienen datos. Calidad detallada (nulos, stock negativo, SKU inválido) | #23 / #25 |
| `agregar_kpis` | Agrega los KPIs de la variante (hoy es un placeholder) | #26 |
| `generar_reporte` | Escribe `/opt/airflow/reportes/reporte_<run_id>.json` (volumen de Airflow) | #23 |
| `notificar_resultado` | Deja una línea de cierre en el log con el estado y la ruta del reporte | #23 |

- **Retries:** 2 reintentos, 30 s entre intentos (`default_args` en `dags/rapidgo_pipeline.py`).
- **XCom:** cada task devuelve un valor y la siguiente lo recibe como argumento (TaskFlow lo pasa por XCom).
- **Lógica:** en `dags/rapidgo_tareas.py` (Python puro, con pruebas en `tests/test_airflow_tareas.py`).
- Curated solo genera *advertencia* si está vacío: Spark escribe una ventana cuando el watermark la cierra (~15 min), ver `datalake/README.md`.

## Cómo ejecutarlo

Prerrequisito: Docker Desktop abierto. Desde la raíz del repo:

```bash
docker compose up -d airflow
```

La primera vez descarga la imagen y tarda 1–2 minutos en arrancar. Cuando esté listo:

1. Abre http://localhost:8080 (no pide login: `SIMPLE_AUTH_MANAGER_ALL_ADMINS`, solo para uso local).
   Si aun así pide usuario, es `admin` y la contraseña sale en `docker compose logs airflow`.
2. Busca el DAG **`rapidgo_pipeline`** y pulsa **Trigger** (▶).
3. Mira la ejecución en las pestañas **Graph** y **Task Instances**; el log de cada task está en *Logs*.

Para que `validar_calidad` pase, el Data Lake debe tener datos. Genéralos antes:

```bash
docker compose up -d kafka kafka-init spark
python -m producer.producer --count 200 --rate 20
```

### Por línea de comandos

```bash
docker compose exec airflow airflow dags list
docker compose exec airflow airflow dags list-import-errors     # debe salir vacío
docker compose exec airflow airflow dags test rapidgo_pipeline  # ejecuta el DAG completo en el terminal
```

### Ver el reporte generado

```bash
docker compose exec airflow ls /opt/airflow/reportes
docker compose exec airflow bash -c "cat /opt/airflow/reportes/*.json"
```

### Detener y reiniciar

```bash
docker compose stop airflow          # conserva historial de ejecuciones y logs (volumen airflow-home)
docker compose down -v               # borra TAMBIÉN el historial de Airflow y los logs
```

## Probar un fallo y la recuperación

Con el Data Lake vacío (o sin Spark corriendo), `validar_calidad` falla. Es el fallo controlado más simple: no hace falta tocar el código.

```
FuenteNoDisponible: Fuentes requeridas sin datos: raw/eventos (la carpeta no existe); processed/detalle (la carpeta no existe)
```

Tras los 2 reintentos la task queda en `failed` y las siguientes (`agregar_kpis`, `generar_reporte`, `notificar_resultado`) en `upstream_failed`. Para recuperar: generar datos (Spark + producer), y en la interfaz usar **Clear** sobre `validar_calidad` con *Downstream*: Airflow re-ejecuta solo esa task y las que dependen de ella, sin repetir `verificar_fuentes`.

## Notas

- La carpeta `airflow/` **no debe tener `__init__.py`**: taparía el paquete real de Airflow en las pruebas.
- **Por qué el reporte no va en `datalake/`:** Spark corre como `root` y sus carpetas quedan sin permiso de escritura para el usuario de Airflow (50000). Probado en Windows con Docker Desktop: `generar_reporte` fallaba con `PermissionError`. Por eso el lake se monta de solo lectura y el reporte se escribe en el volumen de Airflow (`REPORTES_PATH`).
- La versión de Airflow (3.3.2) está en `docker-compose.yml` y en `.github/workflows/ci.yml`: si se cambia, hay que cambiar las dos.

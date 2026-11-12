# Airflow (Semana 3)

Orquesta las tareas de control sobre el Data Lake que escribe Spark. El streaming (Kafka → Spark) sigue corriendo de forma continua; Airflow es la parte batch: validar fuentes y calidad, agregar KPIs y generar un reporte.

```
verificar_fuentes → validar_calidad → agregar_kpis → generar_reporte → notificar_resultado
```

| Task | Qué hace | Issue |
|---|---|---|
| `verificar_fuentes` | Lista las tablas Raw / Processed / Curated (archivos Parquet, tamaño, última modificación) | #23 |
| `validar_calidad` | Falla si Raw o Processed no tienen datos. Además revisa los 500 eventos más recientes de Raw (nulos, stock negativo, SKU inválido, etc.) y falla si más del 5 % son inválidos | #23 / #25 |
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

## Calidad de datos (Issue #25)

`validar_calidad` revisa la capa **Raw** (el mensaje tal cual llegó de Kafka), porque Spark limpia al pasar a Processed y ahí los datos malos ya no se verían. Las reglas están en `dags/rapidgo_calidad.py` (Python puro, con pruebas en `tests/test_calidad_datos.py`):

| Motivo | Qué detecta |
|---|---|
| `json_malformado` | el mensaje no es un JSON de objeto |
| `campos_nulos` | falta o es `null` alguno de los 11 campos del contrato |
| `tipo_incorrecto` | un número llegó como texto (p. ej. `stock = "mucho"`) |
| `stock_negativo` | `stock < 0` (stock 0 es válido: agotado) |
| `sku_invalido` | el sku no cumple `SKU-0000` |
| `tienda_invalida` | el `store_id` no cumple `S000` |
| `picking_no_positivo` | `picking_time_sec <= 0` |
| `items_incoherentes` | `items_picked` fuera de `[0, items_requested]` o `items_requested <= 0` |

- **Umbral:** la task falla (`CalidadInsuficiente`) si más del **5 %** de los eventos evaluados son inválidos. Cambiar con `UMBRAL_INVALIDOS` (p. ej. `0.1`).
- **Ventana:** solo se evalúan los **500 eventos más recientes** (`EVENTOS_CALIDAD`). Así, un problema ya corregido en el origen deja de bloquear el pipeline cuando entran datos sanos.
- **Resultado:** las métricas (total, inválidos, tasa, conteo por motivo y hasta 3 ejemplos) viajan por XCom y quedan en `reporte_<run_id>.json`, dentro de `calidad.datos`.
- **pyarrow:** la task lee Parquet con `pyarrow`, que `docker-compose.yml` instala al arrancar Airflow (`_PIP_ADDITIONAL_REQUIREMENTS`). La primera vez el contenedor tarda un poco más en estar listo.

## Fallo controlado por datos malos y recuperación (Issue #27)

Segundo escenario de fallo, distinto del de «Raw vacío» (arriba): el pipeline tiene datos, pero **de mala calidad**. El producer tiene la opción `--malos` (fracción de eventos con stock negativo, SKU inválido o un campo nulo).

**Provocar el fallo**

```bash
docker compose up -d kafka kafka-init spark airflow
python -m producer.producer --count 300 --rate 20 --malos 0.4      # 40 % de eventos malos
```

Espera ~40 s a que Spark los guarde en Raw y lanza el DAG `rapidgo_pipeline` desde la interfaz. Resultado esperado:

- `verificar_fuentes`: verde.
- `validar_calidad`: falla en los 3 intentos (1 + 2 reintentos de 30 s) con un mensaje como `CalidadInsuficiente: Calidad insuficiente en raw/eventos: 120 de 500 eventos inválidos (24.0%; umbral 5.0%). Motivos: campos_nulos=..., stock_negativo=..., sku_invalido=...`
- `agregar_kpis`, `generar_reporte`, `notificar_resultado`: `upstream_failed` (no se ejecutan con datos malos).

**Recuperar**

1. La causa es el producer defectuoso, no Airflow: los reintentos no la resuelven (el dato malo sigue en Raw).
2. Corregir el origen: enviar datos sanos, suficientes para que los 500 eventos más recientes queden limpios: `python -m producer.producer --count 500 --rate 50` (~10 s) y esperar ~40 s a que Spark los guarde.
3. En la interfaz, **Clear** sobre `validar_calidad` con *Downstream*: se re-ejecutan esa task y las tres siguientes; `verificar_fuentes` no se repite. Pasan a verde.

Otra salida, si el problema es solo que el umbral es muy estricto: subir `UMBRAL_INVALIDOS` (variable de entorno del servicio `airflow` en `docker-compose.yml`) y repetir el **Clear**.

## Notas

- La carpeta `airflow/` **no debe tener `__init__.py`**: taparía el paquete real de Airflow en las pruebas.
- **Por qué el reporte no va en `datalake/`:** Spark corre como `root` y sus carpetas quedan sin permiso de escritura para el usuario de Airflow (50000). Probado en Windows con Docker Desktop: `generar_reporte` fallaba con `PermissionError`. Por eso el lake se monta de solo lectura y el reporte se escribe en el volumen de Airflow (`REPORTES_PATH`).
- La versión de Airflow (3.3.2) está en `docker-compose.yml` y en `.github/workflows/ci.yml`: si se cambia, hay que cambiar las dos.

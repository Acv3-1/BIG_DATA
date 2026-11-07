# ADR-003 - Estrategia de reintentos y recuperación ante fallos en Airflow

**Estado:** [Propuesto / Aceptado]
**Fecha:** [dd/mm/aaaa]
**Relacionado con:** Issues #24, #27 y #29

## Contexto / problema

El DAG `rapidgo_variante10` tiene 5 tasks encadenadas (validar_fuentes ->
chequeo_calidad -> calcular_kpis -> generar_reporte -> notificar_alertas).
Necesitamos decidir cómo reacciona cuando una task falla: cuántos reintentos
hacer, cuánto esperar y qué pasa con las tasks siguientes.

Durante el fallo controlado (#27) observamos: [describir qué provocaron, en
qué task falló, cuántos reintentos hizo Airflow y qué estado quedó en las
tasks siguientes (por ejemplo, `upstream_failed`)].

## Opciones consideradas

1. **Sin reintentos:** la task falla a la primera. Simple, pero un fallo
   transitorio (archivo aún escribiéndose, lectura momentánea) detiene el DAG.
2. **Reintentos fijos** (`retries=[N]`, espera `[X]` s). Absorbe fallos
   transitorios; un fallo real tarda más en mostrarse.
3. **Reintentos con espera creciente** (`retry_exponential_backoff`).
4. [Otra opción que el equipo haya considerado.]

## Decisión tomada

[Escribir la decisión del equipo: valores de `retries` y `retry_delay`, y si el
DAG se detiene o continúa cuando falla una task.]

## Evidencia utilizada

- Log real de la task fallida: [pegar las líneas relevantes del log].
- Capturas de la vista Grid de Airflow con la ejecución fallida y la recuperada.
- Tiempos observados entre reintentos: [valores].
- PR que aplicó el cambio: [enlace].

## Consecuencias / trade-offs

- [Qué se gana con esta decisión.]
- [Qué se pierde o qué riesgo queda.]

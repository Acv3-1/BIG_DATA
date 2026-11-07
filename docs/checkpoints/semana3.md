# Checkpoint Semana 3 - Variante 10: Inventario y fulfillment

**Release:** v0.3.0-orchestration

## 1. Qué quedó funcionando esta semana

- DAG `rapidgo_variante10` con 5 tasks, retries, dependencias y XCom,
  ejecutado correctamente en Airflow: [enlace o captura de la ejecución exitosa].
- Reporte de KPIs generado en [ruta del reporte].
- CI ampliado para importar el DAG y ejecutar las pruebas de KPIs.

## 2. Valor o evidencia real observada en la ejecución

[Un valor real del reporte: por ejemplo, picking promedio de [X] s, [N]
stockouts y [P] % de pedidos incompletos sobre [M] eventos.]

## 3. Decisión técnica tomada y por qué

[Resumen de la decisión del ADR-003 y la razón, con el valor que la sustenta.]

## 4. Problema encontrado y cómo se resolvió

- **Tarea que falló:** [task_id]
- **Mensaje real del log:** [pegar la línea del error]
- **Tareas afectadas:** [por ejemplo, generar_reporte y notificar_alertas
  quedaron en `upstream_failed`]
- **Cambio que resolvió el problema:** [qué se hizo; enlace al PR]

## 5. Enlaces

- Issues: [#24, #26, #27, #29]
- PRs: [enlaces]
- Actions (CI en verde): [enlace]
- ADR-003: `docs/decisions/ADR-003-reintentos-y-recuperacion.md`

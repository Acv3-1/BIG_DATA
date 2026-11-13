# ADR-004 - Umbrales finales de alerta y de calidad (calibración)

**Estado:** propuesto (confirmar con el equipo antes del merge) · **Fecha:** 2026-11-13 · **Relacionado con:** Issues #20 y #33

## Contexto / problema

En la Semana 1 se definieron dos alertas con umbrales iniciales: **stock crítico** (`stock <= 5`) y **picking fuera de tiempo** (`picking_time_sec > 600`). En la Semana 3 se añadió una validación de calidad que falla si más del **5 %** de los eventos recientes de Raw son inválidos. Antes del release final hay que decidir, con datos de las ejecuciones, si esos umbrales se quedan o se ajustan.

## Opciones consideradas

**Umbral de picking** (ver `python scripts/calibrar_umbrales.py`, 20 000 eventos simulados con el producer, semilla 1):

| Umbral | % de eventos que alertan |
|---|---|
| > 450 s | 10.31 % |
| > 500 s | 8.19 % |
| > 550 s | 7.67 % |
| **> 600 s (inicial)** | **7.59 %** |
| > 700 s | 6.44 % |

**Umbral de stock:** `<= 3` → 7.73 %; **`<= 5` (inicial) → 11.63 %**; `<= 6` → 12.18 %; `<= 10` → 14.09 %.

**Severidad `alta` del picking:** (a) más del doble del umbral (> 1200 s, la regla inicial); (b) más de 1.5 veces el umbral (> 900 s).

## Decisión tomada

1. **Se mantienen** los umbrales de picking (**600 s**) y de stock (**5 unidades**).
2. **Se cambia** la severidad `alta` del picking de "más del doble del umbral" a **"más de 1.5 veces el umbral" (> 900 s)** (`FACTOR_SEVERIDAD_PICKING = 1.5` en `spark/reglas.py`; la usan las reglas de Python y las de Spark).
3. **Se mantiene** el umbral de calidad en **5 %** de eventos inválidos sobre los 500 más recientes de Raw.

## Evidencia utilizada

- **Hueco natural en el picking:** el histograma muestra que los eventos normales llegan hasta unos 550 s (solo el 0.06 % cae entre 550 y 599 s) y que el grupo de eventos lentos empieza justo en 600 s (≈1.2–1.3 % por cada tramo de 100 s hasta 1200 s). El umbral de 600 s queda en el hueco: con 500 s ya se alertaría sobre eventos normales (+0.6 puntos porcentuales sobre los lentos reales).
- **Coincide con lo inyectado:** el producer inyecta ~8 % de picking lento y ~12 % de stock crítico; con 600 s y 5 unidades se alerta al 7.6 % y al 11.6 %. Un umbral de stock de 6 ya añadiría eventos normales.
- **Ejecución real (200 eventos, Semana 2):** 16 alertas de picking (en la muestra revisada, entre 618 y 1191 s) y 22 de stock (4 agotados y 18 con poco stock); picking promedio por tienda entre 272 y 387 s. Ninguna de las 16 alertas de picking tuvo severidad `alta`.
- **Severidad inalcanzable:** el producer nunca genera más de 1200 s, así que `> 2 × 600 = 1200 s` no se cumple jamás (0.0 % de los eventos). Con `> 900 s` se activa en ≈3.9 % de los eventos, la mitad de las alertas de picking.
- **Calidad (Semana 3, ejecución real en Airflow):** con datos sanos, 0 % de eventos inválidos; con `--malos 0.4`, 119 de 500 inválidos (23.8 %). Los dos casos quedan muy lejos del 5 %, por lo que el umbral separa bien datos sanos de datos malos.

## Consecuencias / trade-offs

- (+) Los umbrales no cambian: no se invalidan las alertas ya generadas ni la documentación previa.
- (+) La severidad `alta` ahora distingue los casos graves (> 15 min) de los moderados.
- (+) La calibración se puede repetir con `scripts/calibrar_umbrales.py` (datos simulados o reales con `--lake datalake`) y las pruebas de `tests/test_integracion_final.py` avisan si un umbral cambia en un solo lugar.
- (−) Los datos son sintéticos: el producer los genera con una distribución conocida, así que "calibrar" aquí confirma que los umbrales coinciden con lo inyectado. En un caso real habría que recalibrar con el comportamiento de las tiendas.
- (−) `kpis_inventario.py` (Airflow) repite los umbrales como constantes propias; una prueba comprueba que coincidan con `spark/reglas.py`, pero lo ideal sería una sola fuente.

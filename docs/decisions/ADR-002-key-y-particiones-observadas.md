# ADR-002 - Mantener `store_id` como key de Kafka pese al desbalance observado

**Estado:** propuesto (confirmar con el equipo antes del merge) · **Fecha:** 2026-10-29

## Contexto / problema

En el ADR-001 se definió `store_id` como key del topic `inventario-fulfillment`, con 3 particiones, para que todos los eventos de una tienda caigan en la misma partición.

Al ejecutar el pipeline con 200 eventos (`--count 200 --rate 20`) se observó cómo se repartieron entre las particiones (a partir de los offsets que imprime el producer):

| Partición | Eventos | Tiendas |
|---|---|---|
| 0 | 67 | S004, S005 |
| 1 | 33 | S006 |
| 2 | **100** | S001, S002, S003 |

La partición 2 recibió el 50 % del tráfico (lo equilibrado sería ~33 %). Con solo 6 tiendas y 3 particiones, el hash de la key no puede repartir de forma pareja.

## Opciones consideradas

1. **Mantener `store_id` y 3 particiones.** Conserva el orden de los eventos por tienda; el desbalance se queda.
2. **Cambiar la key a `order_id`.** Reparte mejor, pero los eventos de una misma tienda quedan dispersos y se pierde el orden por tienda.
3. **Subir a 6 particiones con `store_id`.** Mejora el reparto en teoría, pero con 6 keys el hash puede seguir juntando varias tiendas en una partición.

## Decisión tomada

Opción 1: **mantener `store_id` y 3 particiones** en `v0.2.0-streaming`.

## Evidencia utilizada

- Offsets del producer: p0 = 67, p1 = 33, p2 = 100 (total 200).
- La agregación de Spark es por ventana de tiempo del evento y por tienda; no depende de cómo estén repartidos los eventos entre particiones. En el Batch 2, la suma de pedidos por tienda fue 41 + 26 + 31 + 38 + 33 + 31 = **200**: no hubo pérdidas ni duplicados.
- A 20 eventos/s, Spark (`local[2]`) procesó los 200 eventos en dos micro-lotes (Batch 1 y Batch 2), sin errores en el log.

## Consecuencias / trade-offs

- (+) Se mantiene el orden por tienda, útil si más adelante se analiza la evolución del stock por tienda.
- (+) No se cambia el producer ni la configuración de Kafka a estas alturas.
- (−) Una partición concentra la mitad del tráfico; con un volumen mucho mayor sería el cuello de botella.
- A revisar en Semana 3–4: si el volumen crece, evaluar la opción 2 o aumentar particiones. Registrar el reparto observado de nuevo tras la calibración de umbrales.

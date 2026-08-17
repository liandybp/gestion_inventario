# Roadmap de evolución — Oportunidades de valor

> **Fecha:** 2026-08-17
> **Versión base analizada:** `0.4.2`
> **Alcance:** documento de análisis. **No implementa nada** — es el insumo para
> generar los PAE (vía `SDD-Agent`) de cada evolución cuando se priorice.
> **Método:** huecos identificados leyendo el schema real (`app/models.py`),
> servicios (`app/services/`), tools del agente (`app/agent/tools.py`) y routers.

---

## 1. Resumen ejecutivo

El producto está maduro en su núcleo (CRUD multi-negocio, FIFO por lotes,
documentos PDF, copiloto IA con 29 tools, Coach financiero-operativo, reportes,
CI/CD verde). Las oportunidades de mayor valor **no están en pulir lo existente**
sino en **huecos del dominio** que hoy dejan procesos de negocio incompletos:
cobrar, controlar caducidad, gestionar proveedores y avisar proactivamente.

Este documento cataloga 7 oportunidades, priorizadas por relación **valor /
esfuerzo**, cada una anclada a un hueco concreto y verificable en el código.

---

## 2. Matriz de priorización

| # | Oportunidad | Valor | Esfuerzo | Toca dinero | Prioridad |
|---|-------------|:-----:|:--------:|:-----------:|:---------:|
| 1 | Cobros / cuentas por cobrar (payment status) | Alto | Medio | ✅ | **P0** |
| 2 | Caducidad de lotes + FEFO | Alto | Medio | ✅ | **P0** |
| 3 | Proveedores como entidad + cuentas por pagar | Alto | Alto | ✅ | **P1** |
| 4 | Notificaciones proactivas (email/Telegram) | Medio-Alto | Bajo | ➖ | **P1** |
| 5 | Escaneo por código de barras (EAN) | Medio | Bajo-Medio | ➖ | **P2** |
| 6 | Exportación CSV/Excel de reportes | Medio | Bajo | ➖ | **P2** |
| 7 | Conteo físico / arqueo de inventario | Medio | Medio | ➖ | **P3** |

**Recomendación de secuencia:** empezar por **#1 + #2** (ambas tocan dinero
directamente y se montan sobre modelos existentes). #4 se vuelve trivial una vez
existen #1 y #2 porque solo empuja datos ya calculados.

---

## 3. Relación con el roadmap ya existente

Estas evoluciones **no duplican** lo ya planificado; se anotan aquí para evitar solapes:

- **Fase 2 del agente** (insights proactivos) — se solapa parcialmente con la
  oportunidad **#4**; convendría diseñarlas juntas (motor de alertas compartido).
- **Estacionalidad (REQ-8)** del Coach — pendiente de decidir, independiente de este roadmap.

---

## 4. Oportunidades en detalle

Cada ficha sigue el mismo formato: **hueco → valor → impacto en el schema →
dependencias → notas de diseño → criterios de aceptación (borrador)**.

---

### #1 — Cobros / cuentas por cobrar `[P0]`

**Hueco (verificado).** `SalesDocument` (`app/models.py`) emite facturas y
presupuestos pero **no guarda estado de pago**: no hay `payment_status`, ni
`due_date`, ni registro de pagos parciales. La app factura pero no sabe quién
debe ni cuánto está vencido.

**Valor.** Es el hueco de mayor impacto operativo: convierte la facturación en un
ciclo de caja real. Habilita panel "Por cobrar", antigüedad de saldos (aging
30/60/90) y foco de cobranza.

**Impacto en el schema (propuesta, no implementada).**
- `SalesDocument`: añadir `payment_status` (`pending`/`partial`/`paid`/`overdue`),
  `due_date` opcional, y `amount_paid` derivable.
- Nueva tabla `Payment` (o `DocumentPayment`): `document_id`, `paid_at`, `amount`,
  `method`, `note` — para soportar pagos parciales y múltiples.
- Reutiliza `Customer` y `SalesDocument` existentes; no altera FIFO ni inventario.

**Dependencias.** Ninguna dura. Se apoya en `SalesDocumentService`.

**Notas de diseño.**
- El estado `overdue` es **derivado** (due_date < hoy y saldo > 0), no un estado
  persistido que haya que recalcular con cron — calcularlo en lectura.
- Solo aplica a `doc_type` de factura, no a presupuestos.
- Sumar tools al copiloto: `accounts_receivable`, `overdue_invoices`.

**Criterios de aceptación (borrador).**
- [ ] Registrar uno o varios pagos parciales sobre una factura.
- [ ] Panel "Por cobrar" con total pendiente y aging por cliente.
- [ ] Estado derivado correcto en 4 casos (pending/partial/paid/overdue).
- [ ] Tests unit del cálculo de saldo/aging + e2e del flujo cobrar.

---

### #2 — Caducidad de lotes + FEFO `[P0]`

**Hueco (verificado).** `InventoryLot` (`app/models.py`) solo tiene `received_at`;
**no hay `expiry_date`**. El consumo FIFO ordena por antigüedad de recepción, no
por vencimiento. Para perecederos (p. ej. **Cervezas Mariela**) esto produce
mermas: se puede vender un lote más nuevo mientras uno más viejo caduca.

**Valor.** Impacto directo en dinero perdido por caducidad. Habilita FEFO
(First-Expired-First-Out), alertas "caduca en X días" y trazabilidad sanitaria.

**Impacto en el schema (propuesta, no implementada).**
- `InventoryLot`: añadir `expiry_date` opcional (nullable — no todos los
  productos caducan).
- Lógica de asignación FIFO en el servicio de inventario: cuando el lote tiene
  `expiry_date`, ordenar por vencimiento (FEFO); si es NULL, mantener FIFO por
  `received_at` como hoy.

**Dependencias.** Ninguna. Aislado en la capa de asignación de lotes.

**Notas de diseño.**
- **Modo mixto:** FEFO solo entre lotes con fecha; los sin fecha conservan FIFO.
  Definir la regla de desempate explícitamente en el design.
- Capturar `expiry_date` en la compra/entrada de stock (opcional).
- Alerta reutilizable por el motor de notificaciones (#4) y el Coach.

**Criterios de aceptación (borrador).**
- [ ] Registrar entrada de stock con fecha de caducidad opcional.
- [ ] Venta consume primero el lote que caduca antes (con fecha).
- [ ] Lotes sin fecha siguen FIFO por recepción (sin regresión).
- [ ] Reporte/alerta de lotes por vencer en ventana configurable.
- [ ] Tests unit de la asignación FEFO/FIFO mixta + e2e.

---

### #3 — Proveedores como entidad + cuentas por pagar `[P1]`

**Hueco (verificado).** Existe `SupplierReturnLotCreate` en `app/schemas.py`
(devolución a proveedor) pero **no hay modelo `Supplier`**: el proveedor es texto
libre en las compras. No hay directorio, ni historial de precios por proveedor,
ni cuentas por pagar.

**Valor.** Cierra el flujo de caja por el lado de las salidas (junto con #1 que lo
cierra por las entradas). Desbloquea comparativa de costos entre proveedores y
control de deuda a pagar.

**Impacto en el schema (propuesta, no implementada).**
- Nueva tabla `Supplier`: `business_id`, `name`, `tax_id`, `contact`, `notes`.
- Vincular compras (`InventoryMovement` type=purchase o entrada de lote) a
  `supplier_id` opcional; migrar el texto libre existente si lo hay.
- Estado de pago de compras análogo a #1 (cuentas por pagar).

**Dependencias.** Se beneficia de compartir el patrón de "estado de pago" con #1
(diseñar el modelo de pagos de forma genérica, reutilizable en cobros y pagos).

**Notas de diseño.**
- Multi-negocio: `Supplier` scoped por `business_id` (como el resto).
- No romper la importación de facturas PDF (`invoice_parsers.py`), que hoy detecta
  emisores por nombre — mapear a `Supplier` cuando exista.

**Criterios de aceptación (borrador).**
- [ ] CRUD de proveedores scoped por negocio.
- [ ] Compra asociada a un proveedor; historial de precios por proveedor.
- [ ] Panel "Por pagar" con deuda a proveedores.
- [ ] Tests unit + e2e del alta de proveedor y compra asociada.

---

### #4 — Notificaciones proactivas (email/Telegram) `[P1]`

**Hueco (verificado).** El Coach (`coach_service.py`), `low_stock`,
`replenishment_table` y — con #1/#2 — vencidas y caducidades ya **calculan** todo,
pero son *pull*: hay que entrar a mirar. No hay push.

**Valor.** Convierte datos ya calculados en acción sin construir análisis nuevo.
Alto valor con esfuerzo bajo porque la lógica de detección ya existe.

**Impacto en el schema (propuesta, no implementada).**
- Configuración de canal por negocio (email/Telegram) — puede vivir en la config
  de negocio existente en BD (`Business`).
- Opcional: tabla `NotificationLog` para deduplicar y no repetir avisos.

**Dependencias.** Se potencia con #1 (vencidas) y #2 (caducidades). Debe
**diseñarse junto con la Fase 2 del agente** para compartir el motor de detección.

**Notas de diseño.**
- Job diario (respetar la regla de despliegue: **nada de cron watchers ni nohup**;
  usar el mecanismo estándar del proyecto / Gitea Actions o scheduler in-app).
- Idempotencia: no reenviar el mismo aviso cada día — de ahí `NotificationLog`.
- Empezar por un solo canal (email o Telegram) y un digest diario, no por evento.

**Criterios de aceptación (borrador).**
- [ ] Digest diario configurable por negocio con: bajo mínimo + vencidas + por caducar.
- [ ] Sin duplicados en envíos consecutivos.
- [ ] Tests del ensamblado del digest (sin enviar de verdad) + e2e de configuración.

---

### #5 — Escaneo por código de barras (EAN) `[P2]`

**Hueco (verificado).** JsBarcode está vendorizado y la app **genera** códigos,
pero `Product` (`app/models.py`) **no guarda EAN/barcode** ni existe flujo
escanear→vender.

**Valor.** Acelera el POS, que es donde el operador tiene más fricción hoy.

**Impacto en el schema (propuesta, no implementada).**
- `Product`: añadir `barcode`/`ean` opcional, indexado por `(business_id, barcode)`.
- Input de escaneo en el POS que resuelve producto por código.

**Dependencias.** Ninguna. Aislado.

**Notas de diseño.**
- El escaneo desde el navegador puede ser vía input de teclado (lectores HID
  emulan teclado) — no requiere APIs de cámara para el MVP.
- Validar unicidad de barcode por negocio, no global.

**Criterios de aceptación (borrador).**
- [ ] Alta/edición de producto con código de barras.
- [ ] Buscar producto por código en el POS.
- [ ] Tests unit de resolución por barcode + e2e del flujo escaneo→venta.

---

### #6 — Exportación CSV/Excel de reportes `[P2]`

**Hueco (verificado).** Solo hay generación PDF de documentos
(`sales_document_pdf.py`). Los reportes (profit, ventas por producto, stock,
dividendos) no se exportan a un formato tabular.

**Valor.** Muy pedido por dueños que llevan los números a su contador. Esfuerzo bajo.

**Impacto en el schema.** Ninguno — solo capa de presentación/export sobre
`ReportingService` y `FinanceService` existentes.

**Dependencias.** Ninguna.

**Notas de diseño.**
- CSV cubre el 90% del valor con esfuerzo mínimo (sin dependencia nueva). Excel
  (`openpyxl`) solo si se pide explícitamente — requiere aprobar dependencia.
- Reutilizar los mismos cálculos que ya alimentan los reportes y el copiloto.

**Criterios de aceptación (borrador).**
- [ ] Botón "Exportar CSV" en profit, ventas por producto y stock.
- [ ] Cifras del CSV coinciden con las de la vista.
- [ ] Tests del contenido exportado + e2e de la descarga.

---

### #7 — Conteo físico / arqueo de inventario `[P3]`

**Hueco (verificado).** No hay reconciliación físico-vs-sistema. Los ajustes
(`type=adjustment`) existen pero no hay un flujo de conteo guiado que los genere.

**Valor.** Cierra el ciclo de confianza en el stock; detecta mermas/robos.

**Impacto en el schema (propuesta, no implementada).**
- Opcional: tabla `StockCount` / `StockCountLine` para una sesión de conteo con
  cantidades esperadas vs contadas, que al confirmar genera `adjustment`.

**Dependencias.** Se apoya en el mecanismo de ajuste existente.

**Notas de diseño.**
- El conteo confirma → genera movimientos `adjustment` con nota que referencia la
  sesión (trazabilidad).
- Considerar conteo por ubicación (multi-ubicación ya existe).

**Criterios de aceptación (borrador).**
- [ ] Iniciar sesión de conteo, capturar contados, ver diferencias.
- [ ] Confirmar genera ajustes correctos y trazables.
- [ ] Tests unit del cálculo de diferencias + e2e del flujo de conteo.

---

## 5. Cómo avanzar cada oportunidad

Cuando se priorice una, **no escribir el PAE a mano**: invocar la skill
`SDD-Agent` (requirements → design → task envelopes → PAE assembly), como marca el
harness del proyecto. El brainstorming previo (si hace falta) produce un design en
`docs/plans/` que alimenta la Fase 1-2 de SDD-Agent.

Toda evolución nueva se cierra **solo con tests e2e** (`tests/e2e/`, Playwright)
además de los unit, y con la documentación viva actualizada (este roadmap, el
`CLAUDE.md` raíz y la nota de Obsidian).

---

## 6. Notas de verificación

Los huecos de este documento se comprobaron contra el código real a 2026-08-17:

- `SalesDocument` sin `payment_status`/`due_date` → **#1**.
- `InventoryLot` con `received_at` y sin `expiry_date` → **#2**.
- Sin modelo `Supplier` (solo `SupplierReturnLotCreate` en schemas) → **#3**.
- Detección ya existe en `coach_service.py` / tools `low_stock`,
  `replenishment_table`; falta canal push → **#4**.
- `Product` sin campo `barcode`/`ean` pese a JsBarcode vendorizado → **#5**.
- Export solo PDF (`sales_document_pdf.py`), sin CSV/Excel → **#6**.
- Ajustes sí, flujo de conteo guiado no → **#7**.

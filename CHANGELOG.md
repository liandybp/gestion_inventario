# Changelog

Este archivo se genera a partir del historial de Git (`git log`).

## 0.4.12 (2026-10-10)

- feat(ui): sugerencias dinámicas de productos al teclear (`GET /ui/product-suggest`, mínimo
  2 caracteres, 30 resultados, solo del negocio activo) en Compras, Documentos, edición de
  compras, Beneficios y Devoluciones: antes solo se sugerían los 200 productos más antiguos.
  Ventas y Traspasos mantienen su lista propia, filtrada por stock en la ubicación. Elegir
  una sugerencia ya no vacía la lista y, al vaciar el campo, vuelve la lista inicial.
- fix(products): el error al guardar en el modal de edición de producto se muestra dentro del
  modal y conserva lo tecleado (antes se cerraba y se perdía). Si falla, la imagen recién
  subida se borra.
- fix: un precio o costo tecleado que no es un número (`nan`, `inf`, texto) da 422 en compras,
  ventas y edición de producto, en vez de usar el valor por defecto sin avisar. `nan`/`inf`
  también se rechazan en `ProductService.create/update`, `InventoryService.sale` y en las
  ediciones de compra y venta y la devolución a proveedor.
- fix(products): mensaje claro cuando el SKU está en uso en otro negocio (el SKU es único
  global); un conflicto al editar sin cambiar el SKU ya no se atribuye al SKU.
- refactor(products): un único bucle de asignación de SKU (`_insert_with_sku_retry`);
  `list_sku_prefixes` pasa a `ProductRepository`; las respuestas de compras dejan de cargar
  la lista de productos que no usan; la búsqueda filtra sobre (id, sku, nombre) y solo carga
  los productos que coinciden.

## 0.4.11 (2026-10-10)

- feat(purchases): una sola vía de creación de producto en la pestaña Compras. Se
  elimina el formulario "Crear producto manualmente" y la ruta `POST /ui/product`;
  "Registrar compra" gana el campo opcional "Precio de venta (si es nuevo)". La
  creación pasa a `ProductService.create_for_purchase` (validaciones únicas, SKU
  automático con reintentos).
- fix(purchases): una compra rechazada (costo vacío, cantidad 0, lote duplicado) dejaba
  el producto nuevo creado (huérfano, y duplicados al reintentar). Producto, auditoría y
  compra se confirman ahora en un único commit, con rollback si la compra falla. La
  importación de facturas PDF usa el mismo servicio, con transacción por línea.
- fix(products): "Importar productos desde factura" creaba los productos sin `business_id`
  (invisibles para todos los negocios y con el SKU global bloqueado) y no hacía rollback
  por línea. Ahora usa `ProductService` con el negocio activo.
- fix(audit): `log_event(..., isolated=True)` usa un SAVEPOINT: un fallo de auditoría ya no
  revierte el producto/compra del llamador (opt-in, solo en compras e importación).
- fix(purchases): cantidad 0 o fecha inválida daban un 500 sin mensaje; ahora 422 con panel.
- fix(inventory): `InventoryService.purchase` rechaza cantidad o costo `nan`/`inf`
  (Pydantic los aceptaba y se guardaba stock NaN).
- fix(purchases): la creación sin SKU no registraba `product_create` en `audit_logs`;
  la lista de sugerencias de "Artículo" y el historial de productos no se refrescaban
  tras crear un producto; `nan`/`inf` se aceptaban como precio o costo; aviso cuando
  el precio se ignora porque el producto ya existe.
- fix(ui): los paneles de error 4xx ("Error en compra", ventas, gastos, extracciones,
  productos) nunca se mostraban porque htmx no hace swap de respuestas >= 400. Handler
  global `htmx:beforeSwap` en `app.js` (solo HTML; el chat del copiloto se excluye). Los
  modales de edición se cierran si el error se pinta en el panel de detrás, y el
  formulario de compra conserva lo tecleado tras un error.

## 0.4.10 (2026-10-09)

- feat(sales): nuevo campo `paid_by_transfer` en el panel de venta rápida
  (`InventoryMovement`) — checkbox "Pagado por transferencia", desmarcado por
  defecto, editable después de creada la venta, visible y filtrable en el
  historial de ventas. Propagado también al outbox de sync (`offline-desktop-sync`)
  para que las ventas de un nodo desktop no pierdan el dato al sincronizar. No
  aplica a Documentos de venta (facturas/presupuestos).
- fix(sales): `_rebuild_product_fifo` podía rechazar la edición de una venta
  válida con "Insufficient stock" si una compra y la venta caían en el mismo
  minuto — el redondeo a minuto del campo "Fecha" en el formulario de edición
  invertía su orden cronológico real. Descubierto por el e2e nuevo de
  `paid_by_transfer`; el campo ahora conserva milisegundos en el panel de venta.

## 0.4.8 (2026-08-29)

- feat(sync): backend P2 del épico offline-desktop-sync — `DeviceToken` revocable
  con rate limit y expiración por inactividad, `SyncService` (replay idempotente
  vía `InventoryService.sale`, dedup de clientes por teléfono/email), endpoints
  `POST /sync/register|push`, `GET /sync/conflicts`. Corregidos 3 hallazgos
  críticos de aislamiento multi-tenant encontrados en auditoría (IDOR en
  `register_device`, `password_hash` sin cifrar en el pull, tablas de detalle
  volcadas sin filtro de negocio) más una regresión propia (dedup de
  `client_id` scopeado por negocio pese a ser único global, misma familia de
  bug que el SKU de P1/ADR-012).
- fix(purchases): `_zara_pdf_diagnostics()` dejaba de filtrar por emisor real
  del PDF y pegaba texto de debug de ZARA en importaciones de facturas AUTODOC.

## 0.4.6 (2026-08-19)

- fix(purchases): `PurchaseRepository.list_sku_prefixes()` filtraba por
  `business_id` (fix `27f9da2`, 2026-08-17) para "no compartir numeración
  entre negocios", pero `products.sku` tiene un `unique=True` **global**
  (`ux_products_sku`, no compuesto por `business_id` pese a lo documentado).
  El candidato de SKU autogenerado de un negocio con pocos productos caía en
  un rango ya ocupado por otro negocio (ej. `SKU000502` de "cerveza_mariela"
  ya usado por "bazar") → `IntegrityError` idéntico en los 5 reintentos (el
  scope por negocio no cambia entre intentos) → 409 "No se pudo generar un
  SKU automáticamente". Bloqueaba el registro de compras de producto nuevo
  sin SKU manual en **todos los negocios excepto el que tenía el SKU global
  más alto**. Reportado en vivo en prod. Revertido a numeración global
  (coincide con el constraint real de la BD). Fix estructural (SKU único
  compuesto `(business_id, sku)`) queda pendiente, ver "Pendiente" en
  `CLAUDE.md`.

## 0.4.5 (2026-08-18)

- fix(locations): `get_business_locations()` dejaba que la inferencia legacy
  (filas `role=NULL` cuyo código termina en `_CENTRAL`) sobreescribiera sin
  condición una central ya resuelta por `role='central'` explícito. Si un
  negocio tenía una ubicación huérfana con ese patrón de código, la app
  resolvía la ubicación por defecto de venta hacia la huérfana (0 stock) en
  vez de la central real, dejando el panel de venta sin sugerencias de
  artículos. Reportado en vivo en prod (negocio Bazar Mariela).
- fix(locations): `LocationRepository.location_id_for_code()` auto-creaba
  ubicaciones nuevas con el prefijo literal `BUSINESS_` en vez del código real
  del negocio cuando no se le pasaba `business_code` explícito (ocurría desde
  `InventoryService`, que siempre lo dejaba en `None`). Esto es lo que generó
  la ubicación huérfana en primer lugar, y puede haber generado ubicaciones
  fantasma similares en otros negocios. Ahora resuelve el código real desde
  `businesses` cuando no se lo pasan.
- data(prod): eliminada la ubicación huérfana `BUSINESS_CENTRAL` (id=147,
  sin movimientos/lotes/documentos asociados) del negocio Bazar Mariela.

## 0.4.4 (2026-08-17)

- fix(agent): el copiloto podía responder con el texto de la reply
  ("Voy a obtener los datos...") sin renderizar ninguna gráfica/tabla. Causa: el
  LLM puede rellenar `tool`/`tool_args` a nivel de plan y dejar el array `widgets`
  vacío; `_execute_node` solo itera `widgets`, así que ese caso no ejecutaba nada.
  Ahora se reintenta con feedback (igual que un JSON inválido) y, si el modelo
  insiste tras agotar los reintentos, se sintetiza un widget con la tool que sí
  eligió en vez de mostrar solo el reply sin nada.

## 0.4.3 (2026-08-17)

- feat(agent): 4 tools nuevas para el copiloto IA — `dead_stock` (productos sin venta
  reciente y valor inmovilizado), `inventory_turnover` (rotación de inventario en días),
  `profit_by_category` y `profit_by_location` (ganancia agrupada por categoría/ubicación,
  hasta ahora solo disponible por producto).
- feat(agent): `sales_by_product`, `profit_by_product` y `daily_sales_series` aceptan un
  argumento opcional `location` (código o nombre de ubicación) para filtrar por POS/central.

## Unreleased

- chore(ui): se elimina del tab de inventario el bloque temporal de traspaso general CENTRAL -> POS.
- fix(security/ui): normalización de rol (`trim + lower`) para reconocer correctamente `owner` en permisos y acciones de traspasos.
- fix(inventory/security): `owner` puede eliminar artículos seleccionados en Inventario (igual que `admin`) y se normaliza rol en UI.

## 0.3.4 (2026-03-07)

- feat(inventory): reposición por SS/ROP (90%) con demanda semanal (solo `sale`), ventana móvil (histórico hasta 12m, luego últimos 12m) y lead time fijo 25 días.
- feat(ui): Home → tabla de artículos muestra columnas SS/ROP/Falta; tabla de reposición se basa en `stock <= ROP` y ordena por faltante (desc).
- feat(ui): inventario agrega acción temporal de traspaso general CENTRAL -> POS seleccionado.
- feat(security): acción temporal de traspaso general visible/ejecutable solo para rol `admin`.
- fix(ui): vista de traspasos elimina columna de lote en listados de salida/entrada.
- fix(transfers): edición/guardado de traspasos robustecida para evitar pantalla negra en errores y diferencias por negocio activo.
- feat(sales): resolución de barcode soporta formatos SPP adicionales (`spp=`, `SPP:`, `SPP|SKU`, etc.) manteniendo compatibilidad con formatos previos.

## 0.3.3 (2026-02-25)

- fix(ui): utilidades por artículo: botones de edición (venta/compra) ahora abren el modal correctamente (override de `hx-select` para evitar herencia desde el contenedor de resultados).
- fix(security): cierre de sesión por inactividad (1h) vuelve a funcionar sin que el polling de HTMX mantenga viva la sesión.

## 0.3.2 (2026-02-01)

- feat(ui): utilidad por artículo: eliminación masiva de ventas + edición/eliminación masiva de compras/traspasos/ajustes.
- feat(security): cierre de sesión por inactividad (1h) con auto-logout.
- feat(ui): filtros por ubicación y rangos de fechas en métricas/tablas (historial, ventas, overview).
- fix(ui): mejoras de UX en transferencias (acciones sticky, tablas responsivas, prevención de doble submit).

## 0.3.1 (2026-01-18)

- feat(security): aislamiento por negocio y permisos (incluye permitir owner en tabs admin donde aplique).
- feat(ui): mejoras de consistencia en Utilidades por artículo e Inventario.
- feat(ui): inventario: filtros mejorados + campos adicionales (categoría, costo compra por defecto).

## 0.3.0 (2026-01-18)

- feat(security): roles ampliados (incluye owner) y controles de acceso.
- feat(security): usuarios: gestión completa (crear/editar/actualizar/eliminar) + flujo de “debe cambiar contraseña”.
- feat(core): configuración y aislamiento por negocio + mejoras de caching.
- feat(db): versionado de esquema y tareas de migración en el arranque.

## 0.2.1 (2026-01-17)

- feat(ui): impresión de reposición (restock) + tabla con detalles de ubicación.
- feat(ui): análisis de utilidad: aumentar top items de 10 a 30.
- feat(ui): métricas de inventario: conteo de productos por ubicación seleccionada.

## 0.2.0 (2026-01-14)

- feat(ui): home charts: selector de ubicación + mejoras en métricas de inventario.

## 0.1.9 (2026-01-13)

- feat(ui): transferencias: selector de ubicación + opción de impresión.
- feat(ui): historial inventario/ventas: rango de fechas + búsqueda.
- feat(ui): persistencia de pestaña activa + mejoras en matching de movimientos.
- feat(ui): inventario: filtro de stock.

## 0.1.8 (2026-01-07)

- feat: inventario multi-ubicación (CENTRAL + POS) con selectores en Inventario/Ventas/Documentos
- feat: transferencias CENTRAL→POS multi-SKU (atómicas, preservando FIFO/costos)
- feat(ui): envíos: edición y eliminación de envíos desde el historial (Edit/Del)
- feat: envíos: vincular entradas a la salida mediante out_id para edición/borrado consistentes
- feat: devoluciones a proveedor **por lote** (selección de lote, preserva costo) + filtros en Historial

- fix(ui): ventas: datalist/selector de artículo filtrado por stock disponible en el POS seleccionado
- fix: ventas: mensaje de stock insuficiente más explícito (incluye ubicación y disponible vs solicitado)

- fix(ui): transfers/envíos: refresco inmediato del tab tras crear envío y limpieza del formulario en éxito
- fix(ui): transfers/envíos: historial ordenado y tablas más compactas (mejor UX en móvil)
- fix(ui): transfers/envíos: prevención de doble submit (deshabilita botón durante la request)
- fix: transfers/envíos: mensajes más claros cuando no hay stock en el almacén CENTRAL (incluye disponible vs solicitado)
- fix(ui): inventario: preservar ubicación seleccionada al editar producto
- fix(ui): inventario: deshabilitar eliminar producto si tiene movimientos/lotes (con tooltip explicativo)

- d957087 (2026-01-05) feat: update inventory handling to set received date for initial inventory adjustments and improve checkbox styling
- 71f270c (2026-01-05) feat: enhance stock list with minimum purchase cost and default sale price
- 0a1d240 (2026-01-05) feat: add initial inventory checkbox and adjust stock handling logic in product edit form

## 0.1.6 (2026-01-05)

- ad9b0e5 (2026-01-05) chore(release): bump version 0.1.5 → 0.1.6
- 156efa7 (2026-01-05) feat: add desired stock input and inventory adjustment logic in product edit form

## 0.1.5 (2026-01-05)

- f18cbd6 (2026-01-05) chore(release): bump version 0.1.4 → 0.1.5
- 57719b8 (2026-01-05) feat: add home charts feature with sales metrics and filtering options
- fbb9b63 (2026-01-05) feat: implement modal dialogs for editing customer, expense, product, and purchase details
- c62e0a0 (2026-01-04) fix: update SKU and name queries to use case-insensitive matching

## 0.1.4 (2026-01-04)

- 5c5c38e (2026-01-04) chore(release): bump version 0.1.3 → 0.1.4
- d67f3bb (2026-01-04) feat: add product import functionality from PDF invoice
- 3979403 (2026-01-04) fix: update asset versioning for app.css and app.js

## 0.1.3 (2026-01-04)

- e65d36a (2026-01-04) chore(release): bump version 0.1.2 → 0.1.3
- fffc647 (2026-01-04) fix: lint
- 3ba7671 (2026-01-04) fix: improve HTML structure and formatting for extraction table
- 0fd2cc0 (2026-01-04) fix: improve HTML structure and formatting for extraction table
- 35b339a (2026-01-04) chore(release): bump version 0.1.1 → 0.1.2
- 07fd93f (2026-01-04) feat: implement mobile menu and sidebar navigation, add month/year filters for purchases and sales

## 0.1.2 (2026-01-04)

- e0b90c5 (2026-01-04) chore(release): bump version 0.1.1 → 0.1.2
- a412f6a (2026-01-04) refactor: update HTML structure and styling for consistency across forms and components

## 0.1.1 (2026-01-04)

- 06cf618 (2025-12-14) fead: implement initial FastAPI application with inventory and product management features
- 2d27bb0 (2025-12-14) refactor: enhance inventory management with lot tracking and allocation features
- 59b7333 (2025-12-14) feat: add UI for product management and stock overview
- 616c084 (2025-12-14) feat: enhance dashboard with product, purchase, and sale panels
- a341bb2 (2025-12-14) feat: add unit of measure and default cost/price fields to product management
- 074778f (2025-12-14) feat: add expense tracking and profit reporting features with UI updates
- cc3064b (2025-12-14) fix: implement expense editing functionality with UI updates
- 681b883 (2025-12-14) fix: enhance monthly overview with JSON data for chart rendering and debug information
- 39820ed (2025-12-14) feat: add dividends tracking and extraction management features with UI updates
- 81028bd (2025-12-15) feat: implement delete functionality for extractions, expenses, purchases, and sales with UI updates
- 226928b (2025-12-15) feat: add image upload functionality for products and enhance product forms with image previews
- d415ec5 (2025-12-15) feat: add label printing functionality for purchases with UI enhancements
- 223fc21 (2025-12-16) feat: implement user authentication and logging with role management and session handling
- 9e4da79 (2025-12-16) feat: add user authentication and session management with UI updates
- dfb6d94 (2025-12-16) feat: add movement history tab with filtering options for inventory tracking
- 45bb759 (2025-12-16) feat: refactor inventory and product services for improved database access and add session secret utility
- bb648fb (2025-12-16) feat: configure PostgreSQL support and enhance database initialization logic
- 6be0001 (2025-12-27) feat: enhance user role management and improve inventory movement logging
- 428ce4a (2025-12-27) feat: enhance invoice processing
- 4a6ca9f (2025-12-27) feat: add sales and inventory value calculations, enhance dashboard with yearly sales data and charts
- bd3c642 (2025-12-27) feat: add activity tab for user session and event logging, enhance dashboard layout and styling
- ab1978e (2025-12-27) feat: implement business configuration management and sales document handling
- 4b48304 (2025-12-27) feat: add customer management features including CRUD operations and UI updates
- ef20b3c (2025-12-31) feat: add README files
- f405d72 (2026-01-01) Adding new docker-compose for homelab deployment
- 551fc67 (2026-01-01) Adding new docker-compose for homelab deployment
- 1558597 (2026-01-01) Changed docekrfile
- 54ef3da (2026-01-01) Changed dockerfile
- 95b6cad (2026-01-01) Changed dockerfile and main
- 65c64e1 (2026-01-04) feat: add opening_pending configuration and update inventory service to handle pending amounts
- 0ab5a7d (2026-01-04) chore: release tooling (gitignore, docs, bump2version)
- 3f5ceb3 (2026-01-04) chore(release): bump version 0.1.0 → 0.1.1

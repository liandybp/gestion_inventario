# Changelog

Este archivo se genera a partir del historial de Git (`git log`).

## 0.5.0 (2026-11-02 — release del frontend modular, fases 0-5)

- fix(release): quick wins de la auditoría de la release (`docs/auditorias/2026-11-02-release-0.5.0.md`):
  `/health` envía la API key al comprobar el LLM (producción marcaba `agent_llm: degraded` en falso),
  `/csp-report` rechaza cuerpos grandes antes de leerlos y `static_version()` solo recalcula el hash cuando
  cambian los estáticos (TTL de 1 s; de 0,66 a 0,02 ms por llamada). `develop` incluye ya el roadmap que se
  commiteó directamente en `main`.
- feat(agent): nueva tool `product_movements` — movimientos de un artículo en un período (compras, ventas,
  traspasos, ajustes y devoluciones a proveedor) con fila de saldo inicial y saldo acumulado, filtro por
  ubicación y nota de descuadre contra el stock por lotes. 35 tools.
- refactor(frontend): fase 5b — las consultas salen de los routers UI hacia repositorios/servicios y
  `UIContext` queda completo (campos perezosos `active_business_id`/`business_code`/`config`, todos a
  partir de `ctx.user`). `get_active_business_id` acepta `user=` opcional (sin cambios de lógica). Los
  routers UI ya no usan el ORM directo (guarda `tests/unit/test_routers_no_orm.py`) ni resuelven el
  negocio por su cuenta; de 4 a 1 SELECT sobre `users` por petición UI (guarda
  `tests/unit/test_ui_query_count.py`). Se borra `_write_business_config` (código muerto desde que la
  configuración vive en BD, con un `configparser` sin importar). HTML sin cambios (arnés y estilos
  idénticos a la baseline de 5a).
- refactor(frontend): fase 5a — las 54 plantillas de `partials/` pasan a `features/<dominio>/` (mismo
  nombre, solo cambia la carpeta), se borran las 2 muertas (`action_result.html`, `products_history.html`)
  y una guarda (`tests/unit/test_templates_layout.py`) impide volver a `partials/` y detecta rutas de
  plantilla rotas. HTML sin cambios (arnés y estilos idénticos a la baseline de bf38a2b).
- refactor(frontend): fase 4 — plantillas OOB en `features/<dominio>/` (salen el HTML en Python de
  `product_defaults_purchase/sale`, `sales_doc_product_defaults`, `sales_doc_customer_defaults`,
  `sale_product_options`, `transfer_product_options` y `ui_supplier_return_lots`), macros compartidas
  `qty_label`/`number_input`/`text_input` en `components/forms.html`, dependencia perezosa `UIContext`
  en los 20 routers UI (una resolución de usuario/negocio por petición) y sidebar data-driven con
  `NAV_ITEMS`. Red ampliada: arnés H-303 (fragmentos, POST con error, roles, estados vacíos) y e2e de
  caracterización. HTML renderizado sin cambios (canónico idéntico a la baseline de b741a46).
- feat(permisos): política por rol decidida el 2026-11-01. El **owner** ve Clientes (antes recibía 403 aunque el
  sidebar le mostraba la pestaña). El **operador** vende, crea y edita clientes (sin borrarlos: 403 y botón oculto)
  y crea, edita y borra documentos de venta; ve en el sidebar Ventas, Documentos y Clientes. Los formularios de
  edición de gastos y de ventas pasan a exigir admin/owner, igual que sus `update` (el operador ya no ve el botón
  "Edit" del historial de ventas). +9 unit (`test_operator_permissions.py`), +2 e2e.
- fix(utilidades): filtrar "Utilidades por artículo" por fechas daba error 500 (`NameError: timedelta`, faltaba el
  import desde el refactor de la fase B, 2026-08-15). Test de regresión.
- fix(documentos): borrar un documento de venta daba error 500 (`IntegrityError`: el documento se borraba antes que
  sus líneas, sin `relationship` que fije el orden). Ahora se borran las líneas, `flush` y después el documento.
- fix(documentos): el autorrelleno de precio con un SKU inexistente devolvía 404; ahora un fragmento vacío, como
  Compras y Ventas.
- refactor(ui): `role_of()` normaliza el rol en un único sitio (guardias de `UIContext` y sidebar); imports sin
  uso fuera; la guarda `test_routers_no_html.py` busca HTML solo en literales de cadena (AST), no en comentarios.

- fix(ui): contraste de los gráficos en tema oscuro. La serie de ventas de Inicio (navy) no se distinguía
  del fondo y los textos y líneas de ejes usaban los valores fijos de Chart.js (`#666`, `rgba(0,0,0,0.1)`).
  Nuevas variables `--chart-text`/`--chart-border` (en claro, los mismos valores de antes) que
  `applyChartDefaults()` aplica a `Chart.defaults` antes de cada render, y versión oscura de
  `--home-chart-sales`, `--home-chart-line-fill`, `--home-pie-1` y `--chart-grid`. +1 e2e.
- fix(ui): Escape cierra el modal de más arriba, salvo los estáticos (cambio de contraseña obligatorio). +1 e2e.
- test(snapshots): el arnés congela el reloj de Inicio; la baseline dejaba de valer al cambiar de mes y tumbó
  el CI del 2026-11-01.

- feat(agent): nueva tool `sales_by_location_monthly` — ventas de cada punto de venta mes a mes
  (una serie por ubicación, últimos N meses calendario incluyendo el actual). El copiloto ya puede
  dibujar barras agrupadas por mes (antes devolvía el total del período, sin meses). 34 tools.
- feat(ui): los gráficos de barras y líneas del Dashboard Dinámico aceptan varias series
  (`payload.datasets`) con leyenda (`components/widget-chart.js`).
- fix(agent): "últimos N meses" se resuelve como N meses calendario (antes el modelo tomaba N+1).
- fix(agent): el reply avisa cuando no puede dar exactamente lo pedido o cuando falla la tool, y el
  widget de rescate se titula con la descripción de la tool en vez de su nombre interno.
- fix(inicio): XSS almacenado en los gráficos de Inicio. Los datos se volcaban con `json.dumps` +
  `|safe` dentro de `<script type="application/json">`; un nombre de producto con `</script>` (p. ej.
  desde una factura importada) cerraba el bloque y ejecutaba HTML/JS. Ahora el router pasa dicts y la
  plantilla usa `tojson`, que escapa `< > & '`. Test de regresión en `test_ui_xss_escapes.py`.
- docs: diagnóstico de arquitectura del frontend (`docs/auditorias/2026-10-23-frontend-arquitectura-diagnostico.md`).
- refactor(frontend): fase 1 — `base.html` + `layouts/app_shell.html` y `layouts/print.html` con `{% extends %}`
  (dashboard, login y las 3 impresiones; login e impresiones siguen sin cargar htmx/Chart.js/JsBarcode/app.css/
  app.js). Macros `components/modal.html` (`modal()` con `caller()`) y `components/forms.html` (`field()`); los 18
  modales migrados y los 4 `profit_items_*_edit_form.html` unificados en `profit_items_edit_form.html`. HTML
  renderizado sin cambios, verificado con el arnés `tests/snapshots/ui_snapshot.py` (40 páginas, baseline en
  `tests/snapshots/baseline/`). +22 unit, +3 e2e.
- fix(frontend): revisión de la fase 1 (`docs/auditorias/2026-10-23-frontend-fase1-layouts-macros.md`).
  `field()` pasa `attrs` por `xmlattr`, que rechaza claves de atributo inválidas. El arnés de snapshots no depende
  del `.env` ni del shell, siembra valores con `< > & " '` (detecta un `|safe` colado) e imprime el traspaso por
  `?ref=`, como la UI. El e2e de Utilidades guarda el modal unificado.
- fix(traspasos): `/ui/transfers/print?movement_id=` mostraba la referencia con `:` al final si el traspaso
  tenía nota; ahora la extrae como `InventoryService` (`ref=([^\s:;]+)`). Test en `test_transfer_print_ref.py`.
- ci: el arnés de snapshots corre en el job `test` (paso `UI snapshots`). Si el HTML cambia y la baseline no se
  regenera en el mismo commit, el CI falla.
- fix(frontend): `modal()` con un `heading` que no sea `h2`/`h3` y `profit_items_edit_form.html` con un `kind`
  desconocido fallan con un error explícito (global `template_error`), en vez de renderizar un modal roto.
- refactor(frontend): fase 2 — todo el JS en módulos ES nativos bajo `app/static/js/` (`main.js` +
  `core/`, `components/`, `features/`, `pages/`; `theme-init.js` clásico en `<head>`), sin build. Fuera
  `app.js`, los 5 `<script>` inline ejecutables y los 15 `onclick=`/`onchange=`/`onkeydown=` (sustituidos por
  atributos `data-js-*`), el listener no-op `debugReturnLots`, los 3 `#*-debug` y el sondeo de `window.Chart`.
  El CSP pasa a `script-src 'self'` (sin `'unsafe-inline'`). `/static/js/` se sirve con `Cache-Control:
  no-cache` y `static_version()` calcula el hash de todos los `.css`/`.js`, también los módulos anidados.
- fix(inicio): el tooltip del gráfico de pastel metía la etiqueta (`SKU - nombre de producto`) en
  `innerHTML` sin escapar (XSS al pasar el ratón); ahora se construye con `textContent`.
- test(frontend): el arnés de snapshots ignora el cableado JS (scripts ejecutables, `on*`, `data-js-*`,
  `#*-debug`); baseline regenerada desde el commit anterior a la fase. +9 unit (CSP, sin JS inline, scripts por
  plantilla, grafo de módulos, sin código de depuración, escape de `data-js-*`) y +23 e2e que recorren las 17
  pestañas, los 18 modales, login e impresiones, y cada comportamiento migrado, y fallan ante cualquier error
  de JS o violación de CSP.
- refactor(frontend): fase 3 — CSS sin estilos inline. Los 106 `style="…"` de 22 plantillas pasan a clases de
  `app.css` (bloque final «Fase 3»), los 4 `<style>` a `login.css`, `print.css` y `label-print.css`, y las macros
  `card`/`subcard`/`modal` pierden sus parámetros de estilo libre (`modal(container_class=)`). Colores por tokens:
  sin hex en las plantillas; en tema oscuro las miniaturas y el badge de reposición siguen el tema. La CSP pasa a
  `style-src 'self'` (sin `'unsafe-inline'`).
- fix(frontend): con la CSP estricta, JsBarcode en modo SVG dejaba el código de barras negro sobre negro (su fondo
  y sus barras van en `style`): `components/barcode-svg.js` dibuja la misma geometría con atributos de
  presentación. htmx ya no inyecta su hoja de `.htmx-indicator` ni copia `style`, `width` ni `height` en el *settle* (meta
  `htmx-config`): esa copia podía dejar deformados los gráficos de Inicio al refrescarlos.
- feat(ui): el botón de tema cambia claro/oscuro sin recargar la página; los gráficos toman sus colores de variables
  CSS (`--chart-*`, `--home-*`) y se redibujan (`gi:themechange`).
- test(frontend): red de estilos calculados `tests/snapshots/style_snapshot.py` (74 capturas, claro y oscuro, contra
  el commit anterior a la fase; fuera de CI); el arnés HTML traduce cada `style` antiguo a sus clases; `js_errors`
  falla ante cualquier violación de CSP. +10 unit, +9 e2e.
- refactor(frontend): colores en `tokens.css` (`:root` y tema oscuro) y utilidades compartidas en `utilities.css`,
  cargadas por el dashboard, el login y las impresiones; `print.css` y `label-print.css` ya no repiten hex ni clases.
- feat(security): `report-uri /csp-report`. Cada violación de CSP queda en el log como `WARNING csp_violation`
  (campos truncados, sin `script-sample`, cuerpo de 16 KB como máximo y tope de 60 por minuto).
- test(frontend): el arnés de estilos calculados ignora el tamaño de los `<canvas>` y de lo que los contiene, porque
  lo fija Chart.js según el momento en que mide. +12 unit, +1 e2e (el navegador envía de verdad el reporte de CSP).

## 0.4.15 (2026-10-19)

- fix(negocios): guardar el formulario de edición de un negocio borraba el saldo inicial de
  dividendos (`opening_pending`). El JSON iba en un atributo `value="..."` y `tojson` no escapa las
  comillas dobles: el navegador lo cortaba en `{` y se guardaba `{}`. En Recambios se perdió el saldo
  el 2026-10-18 y los pendientes de los socios pasaron a negativo. Test de ida y vuelta + e2e.

## 0.4.14 (2026-10-11)

- fix(coach): las extracciones a la etiqueta del negocio ("Negocio", configurable) ya no cuentan
  como retiros ni restan de la caja disponible: no salen de la caja, solo marcan el coste y los
  gastos recuperados para volver a comprar (misma lógica que el informe de dividendos).
- fix(coach): la alerta "drena caja" (`partner_overdrawn`, antes `extraction_ratio_high`) solo salta
  cuando un socio ha retirado más que su parte acumulada de la utilidad (pendiente negativo en
  dividendos), con el socio y el importe. Antes comparaba lo retirado en el mes con la utilidad del
  mes y avisaba cuando un socio cobraba lo acumulado de meses anteriores. El score resta 1 punto por
  socio en negativo (máx. 2) en vez de penalizar el porcentaje mensual, que queda como dato informativo.
- fix(coach): la caja estimada restaba las ventas en vez de sumarlas (se guardan con cantidad
  negativa); ahora usa `abs(quantity)` como el resto de informes.

## 0.4.13 (2026-10-11)

- feat(ui): la pestaña Compras deja de mostrar la sección "Producto" ("Importar productos desde
  factura", historial de productos y sus acciones). Se mantiene "Importar compra desde factura".
  `#product-panel` queda como contenedor invisible del datalist `#product-options` del campo
  "Artículo", que se sigue refrescando vía `hx-swap-oob` al crear un producto con una compra.
  La edición y el borrado de productos siguen en la pestaña Inventario.

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

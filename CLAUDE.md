# CLAUDE.md — Gestión de Inventario

## Resumen del proyecto

App web FastAPI + HTMX para gestión de inventario multi-negocio con soporte multi-ubicación (CENTRAL + POS), control FIFO de lotes, documentos de venta (facturas/presupuestos), clientes, compras, gastos, transferencias y reportes.

- **Versión actual:** `0.5.0` (ver `VERSION`, tag `v0.5.0`) — **en producción desde el 2026-11-02** (`main`,
  `bca1835`): frontend modular (fases 0-5: JS en módulos ES, CSS sin inline, CSP `script-src`/`style-src 'self'`,
  plantillas por dominio, routers sin ORM, `UIContext`), tools del copiloto `sales_by_location_monthly` y
  `product_movements`, rangos de fecha del prompt y los quick wins de la auditoría de la release (4.70 🟢,
  `docs/auditorias/2026-11-02-release-0.5.0.md`). Volcado previo al despliegue en `laptop-server`:
  `~/backups/pre-release-0.5.0-20261102-1945.sql.gz`. 543 unit, 95 e2e, 91 snapshots.
- **Rama principal:** `develop`
- **Repo:** `gitea.legatumbp.com/liandybp/gestion_inventario` (mirror a `github.com/liandybp/gestion_inventario`)
- **Obsidian:** [[05-Proyectos/gestion-inventario]]

## Stack tecnológico

| Componente | Tecnología |
|---|---|
| Framework | FastAPI |
| Base de datos | SQLite (dev) / PostgreSQL (prod vía `DATABASE_URL`) |
| ORM | SQLAlchemy 2.0 |
| Frontend | Jinja2 + HTMX |
| Auth | Sesiones (Starlette SessionMiddleware) + PBKDF2-SHA256 |
| PDF | fpdf2 + pdfplumber |
| CSS/JS | Vanilla CSS + Chart.js + JsBarcode |
| Testing | pytest (465 unit + 84 e2e Playwright) + arnés HTML (40 snapshots, reloj de Inicio congelado) + arnés de estilos calculados (74 capturas, fuera de CI) |

## Estructura del proyecto

```
gestion_inventario/
├── app/
│   ├── main.py              ← FastAPI app + lifespan + auth middleware
│   ├── db.py                ← SQLAlchemy engine + session
│   ├── models.py            ← Modelos ORM (Business, User, Customer, Location,
│   │                           Product, Lot, InventoryMovement, Sale, Purchase,
│   │                           SalesDocument, Expense, Extraction, Transfer)
│   ├── schemas.py           ← Pydantic v2 schemas
│   ├── auth.py              ← PBKDF2-SHA256 password hashing
│   ├── security.py          ← RBAC (admin/operator) + business resolution
│   ├── audit.py             ← Audit event writer
│   ├── deps.py              ← FastAPI dependencies (DB session, current user)
│   ├── migrations.py        ← Startup DB migrations
│   ├── business_config.py   ← .conf file parser (business parameters)
│   ├── invoice_parsers.py   ← PDF invoice import (Autodoc, H&M, ZARA)
│   ├── sales_document_pdf.py← PDF generation for invoices/quotes
│   ├── utils.py             ← Utilities
│   ├── settings.py          ← Config centralizada (env vars, H-008)
│   ├── agent/               ← Copiloto IA (LangGraph + LLM): graph, tools,
│   │                           runner, widgets, schemas, prompts, llm,
│   │                           config, chat_store, circuit_breaker
│   ├── repositories/        ← Data access layer (business, customer, inventory,
│   │                           location, product, purchase, user)
│   ├── services/            ← Business logic (finance, inventory, product,
│   │                           reporting, sales_document)
│   ├── routers/             ← ~25 routers (3 API JSON + ~22 UI HTMX)
│   ├── templates/           ← Jinja2 + HTMX (layouts/, components/, features/<dominio>/)
│   └── static/              ← CSS, JS, uploads
├── tests/
│   ├── conftest.py
│   ├── unit/                ← tests unitarios (SQLite en memoria)
│   └── e2e/                 ← Playwright e2e (flujo completo, live server + SQLite temp)
├── docker-compose.yml       ← PostgreSQL + App + Caddy
├── requirements.txt
└── VERSION
```

## Cómo ejecutar

```bash
cd ~/PyCharmMiscProject/gestion_inventario
source .venv/bin/activate
# Dev local usa PostgreSQL (localhost:5433/inventario) via .env
uvicorn app.main:app --host 0.0.0.0 --port 10000 --reload
# URL: http://localhost:10000 → admin/admin

# Producción (vm-apps): Docker
cd ~/PyCharmMiscProject/gestion_inventario  # en vm-apps
docker compose -f docker-compose-homelab.yml up -d --build

# Tests unitarios (SQLite en memoria)
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -v

# Tests e2e (Playwright; requiere `python3 -m playwright install chromium`)
python3 -m pytest tests/e2e -m e2e --no-cov -v
```

## Entornos (dev vs producción)

| Entorno | URL | Acceso | Rama | Dónde corre | Servidor |
|---|---|---|---|---|---|
| **Desarrollo (staging)** | `http://dev.gestion.legatumbp.com` | **Interno (LAN)** — NO sale a internet | `develop` | host `ai-lab-home` (192.168.1.162) | uvicorn `:10000` (systemd `gestion-inventario-dev`) |
| **Producción** | `https://gestion.legatumbp.com` | **Público** (Cloudflare Tunnel) | `main` | `laptop-server` (192.168.1.25) | Docker `gestion_inventario` `:10000` |

- **Dev es interno**: AdGuard resuelve `dev.gestion.legatumbp.com` a `laptop-server` (192.168.1.25). Su nginx
  (`~/apps/nginx/conf.d/apps.conf`, bloque `server_name dev.gestion.legatumbp.com`) lo pasa a `192.168.1.162:10000`.
  NO usa Cloudflare Tunnel.
  - Ruta: `navegador → AdGuard → laptop-server:80 (nginx) → ai-lab-home:10000 (uvicorn)`.
  - ⚠️ Entre el 2026-09-07 (migración desde `vm-apps`) y el 2026-11-01 ese bloque faltaba y el dominio servía
    **producción** (servidor por defecto de nginx). Comprobar que staging es staging:
    `curl -s -o /dev/null -w '%{http_code}' http://dev.gestion.legatumbp.com/static/js/main.js` → 200.
- **Deploy dev/prod**: **Gitea Actions CD** (`.gitea/workflows/deploy.yml`). En `push` corre `test` (unit + UI snapshots + e2e) y, si pasa, `deploy-dev` (rama `develop`) o `deploy-prod` (rama `main`) en runners self-hosted. Dev: `git reset` en `~/staging/gestion_inventario` + `systemctl restart gestion-inventario-dev.service` (systemd `Restart=always`). Prod: build local en `laptop-server` + `docker compose up -d --no-deps gestion_inventario` en `~/apps`, con rollback si el health falla. Proceso general en `~/obsidian-lab/04-Harness/deploy-gitea-actions.md`.
- **BD dev**: PostgreSQL `localhost:5433/inventario` (contenedor `dev_postgres` en `ai-lab-home`; copia de prod de
  agosto 2026). **BD prod**: contenedor `postgres` de `laptop-server` (`172.18.0.2:5432/inventario`, sin puerto
  publicado; lectura vía túnel SSH con el rol `mcp_readonly`).
- Usuario de prueba en staging: `admin`/`admin`.

## Variables de entorno clave

- `DATABASE_URL` — dev: `postgresql+psycopg://liandy:devpassword@localhost:5433/inventario` (.env); prod: `192.168.1.20:5432`
- `SESSION_SECRET` — requerido en producción
- `ADMIN_USERNAME` / `ADMIN_PASSWORD` — credenciales admin
- `OPERATOR_USERNAME` / `OPERATOR_PASSWORD` — credenciales operador
- `SESSION_HTTPS_ONLY` — `"1"` para cookies solo HTTPS

## Configuración de negocio

Archivo `app/business_config.conf` (INI):
- `[issuer]` — datos del emisor
- `[currency]` — moneda
- `[sales_documents]` — tipos de documento
- `[locations]` — ubicaciones (CENTRAL + POS)

## Notas técnicas

### Base de datos
- SQLite en modo WAL para dev
- PostgreSQL en producción vía Docker Compose
- Migraciones inline en `migrations.py` (se ejecutan en startup)
- ⚠️ `products.sku` y `inventory_lots.lot_code` son `unique=True` **globales** (`ux_products_sku`,
  `ux_inventory_lots_lot_code` en `migrations.py`), **NO** compuestos por `business_id` — pese a
  que este documento lo dijo así hasta v0.4.5. Confirmado como causa raíz del bug de v0.4.6
  (numeración de SKU autogenerado colisionaba entre negocios). Hay índices compuestos
  `(business_id, sku)` para performance (`ix_products_business_sku`, H-053) pero **no** son
  `UNIQUE`. Migrar a unicidad compuesta real queda pendiente (ver Estado actual).

### Multi-negocio
- `business_id` en todas las tablas
- Negocios se sincronizan desde `business_config.conf` en startup

### Auth
- Roles: `admin`, `owner` y `operator`
- Sesiones con inactividad máxima de 1h
- HTMX polling no extiende la sesión (requiere `X-User-Activity` header)

### FIFO
- Control de inventario por lotes
- Los lotes marcados como `is_initial_stock` se consumen primero

## Próximas evoluciones (Roadmap)

Ver `docs/roadmap/2026-08-17-oportunidades-evolucion.md` para análisis completo de
7 oportunidades de valor identificadas (cobros, caducidad FEFO, proveedores,
notificaciones, barcode, export CSV, conteo físico). Prioridad: P0 (cobros +
caducidad FEFO) → P1 → P2 → P3.

---

## Estado actual

- [x] **0.5.2 — PDF de facturas y presupuestos con el diseño de la plantilla "OFERTA" (2026-11-18, sesión 26, rama
      `feature/pdf-documentos-venta-oferta`)** — PAE `specs/pdf-documentos-venta-oferta/` (26 envelopes, todos
      `Apply: script`; Fases 1-2 en Opus, Fases 3-4 en Sonnet-2). `app/sales_document_pdf.py` reescrito (banda
      azul, iconos SVG de teléfono y email, `DESTINATARIO:`, bloque documento, `Subtotal:`/`TOTAL:`, tabla con
      precio unitario y cabecera repetida, pie `FORMA DE PAGO` + `Recibido por`); funciones puras en
      `app/sales_documents_format.py`. Negocio: `issuer_bank_name/branch/account/account_currency` y
      `sales_quote_validity_days` (0-365, defecto 15), editables en *Negocios*. Documento: `sales_documents.valid_until`
      (solo `P`), por defecto emisión + plazo, editable al emitir y al editar, conservada en el borrador del carrito.
      Fecha inválida → 422 HTML "Fecha de vencimiento inválida" sin tocar nada. Columnas de importe con la moneda del
      documento (`GEN` → `CUP`). Migraciones idempotentes (SQLite y PostgreSQL). Baselines de snapshots regeneradas en
      el mismo lote que las plantillas (lección de 0.5.1).
      - Pendiente tras el despliegue: el usuario rellena en *Negocios → Editar* teléfono, email, banco y plazo de
        DyB Autopartes (los `.conf` son legacy y no se releen); el PDF lee teléfono, email y banco del negocio al
        generarse (como el TCP), no del documento emitido.
      - Fuera de alcance: Marca/Matrícula, plazo de ejecución, lema, número `LOT.`, icono de dirección, opciones de
        moneda nuevas en el selector del carrito.

- [x] **0.5.1 — cabecera TCP/NIT en facturas y presupuestos (2026-11-18, sesión 25, directo en `develop`)** — campo
      nuevo `businesses.issuer_holder` (`IssuerConfig.holder`, "Titular (TCP)" en *Negocios → Crear/Editar*; columna
      añadida de forma idempotente por `_BUSINESS_CONFIG_COLS`). El PDF imprime `TCP: …` y `NIT: …` (antes "ID
      fiscal") y ya no lleva "(Precios sin IVA)" (tampoco la vista previa ni el panel). `.conf` default/recambios:
      emisor DyB Autopartes. Referencia de formato: plantilla "OFERTA" del cliente (COMERCIO GEELY). 543 unit, 95 e2e,
      91 snapshots. Probado en staging y desplegado a prod (`main` `974f3f8`, tag `v0.5.1`).
      - Ojo: en *Negocios → Editar* el campo "Nombre" escribe `name` **e** `issuer_name` (el nombre del PDF). En
        prod el negocio se renombra a "DyB Autopartes" desde la web (lo hace el usuario).
      - El TCP no se guarda en el documento emitido: el PDF lo lee del negocio actual al generarse.
      - Diferencias con la plantilla de referencia no implementadas (decisión del usuario, no pedidas): título
        OFERTA, marca/matrícula, fecha de vencimiento, columna unidades, separador de miles, forma de pago/firma.
- [x] **Copiloto: tool `product_movements` + fechas de "este mes" (2026-11-02, sesión 24, rama
      `feature/copiloto-movimientos-articulo`, entra en 0.5.0)** — movimientos de un artículo (SKU, "SKU - nombre"
      o nombre) en un período (por defecto 3 meses), en todas las ubicaciones o en una: fila de saldo inicial, saldo
      acumulado, traspaso = una fila "origen → destino" (agrupa la salida con sus entradas por lote vía `out_id`),
      nota si el saldo no cuadra con los lotes. Lógica en `ReportingService.product_movements`. 35 tools. Corregido
      de paso un fallo previo: a principios de mes el copiloto tomaba "este mes" por el anterior; el prompt lleva ya
      los rangos calculados (eval 96/100/100 %, antes 96/96/76 % en `develop`). Auditoría **4.90 🟢**
      (`docs/auditorias/2026-11-02-copiloto-movimientos-articulo.md`). 539 unit, 95 e2e. Primer PAE con las
      comprobaciones de fidelidad nuevas de `cc_local_pae`.
- [x] **Release 0.5.0, fase 5b del frontend: consultas fuera de los routers UI y `UIContext` completo (2026-11-02,
      sesión 24, rama `feature/frontend-fase5b`)** — PAE `specs/frontend-fase5b-queries-uicontext/` (60 envelopes vía
      SDD-Agent; Opus Fases 1-2, `cc_deepseek` Fases 3-4, `cc_local_pae` lotes A-H). Los routers UI no usan el ORM
      ni resuelven el negocio: `ctx.config`/`ctx.business_code`/`ctx.active_business_id` en las ~50 llamadas,
      auxiliares con `ctx`, `get_active_business_id(user=)` en `security.py`, `ProductService.resolve_sku`/
      `barcode_to_sku`/`extract_sku`, `InventoryRepository.get_movement` (negocio + tipo), escrituras en
      `business_repository`/`SalesDocumentService`/`UserRepository`/`CoachService` (sin `commit`: la transacción la
      cierra el router). Guardas `test_routers_no_orm.py` y `test_ui_query_count.py`: **de 4 a 1 SELECT a `users` por
      petición**. Borrado `_write_business_config` (muerto, `configparser` sin importar). Auditoría **4.75 🟢**
      (`docs/auditorias/2026-11-02-frontend-fase5b-queries-uicontext.md`): `cc_local` dejó lotes a medias o dañados
      (147 líneas borradas en `ui_transfers.py`, un lote E "OK" sin migrar routers, firmas erróneas que los tests no
      veían); todo corregido por Claude Code. 521 unit, 94 e2e, 91 snapshots y estilos idénticos.
      - Pendiente: bump único de 0.5.0, `develop` → `main` y despliegue a producción.
- [x] **Release 0.5.0, fase 5a del frontend: `partials/` → `features/<dominio>/` (2026-11-02, sesión 24, rama
      `feature/frontend-fase5a`)** — PAE `specs/frontend-fase5a-features-templates/` (30 envelopes, `cc_local_pae`
      lotes A-D; el lote E paró y lo cerró Claude Code aplicando T26-T30 tal cual). 54 plantillas movidas con el
      mismo nombre a 21 dominios (16 carpetas nuevas + las 5 de la fase 4), 2 muertas borradas
      (`action_result.html`, `products_history.html`). Guarda `tests/unit/test_templates_layout.py`: cada nombre
      literal de plantilla (routers + `include/extends/from/import`) se carga de verdad, `partials/` no existe y
      ninguna línea de `app/` ni `tests/` lo nombra. HTML idéntico (91 snapshots) y estilos idénticos (0
      diferencias) a la baseline de `bf38a2b`. Revisión + auditoría 4.60 🟢 con el lote E pendiente → corregido
      (`docs/auditorias/2026-11-02-frontend-fase5a-features-templates.md`): `cc_local` había borrado la entrada de
      la fase 4 de este fichero, escrito CHANGELOG/diagnóstico con afirmaciones falsas, debilitado la guarda y
      añadido `profit-items` a un e2e de operador (rompía). 499 unit, 94 e2e.
      - Pendiente: fase 5b (consultas fuera de los routers UI + `UIContext` completo,
        `specs/frontend-fase5b-queries-uicontext/`, requirements + design aprobados), después bump único de 0.5.0
        y `develop` → `main`.
- [x] **Release 0.5.0, fase 4 del frontend: plantillas OOB, `UIContext` y sidebar data-driven — en `develop` y
      probada en staging (2026-11-01, sesión 23)** — merge `990e61d` (`feature/frontend-fase4`). HTML en Python
      movido a `templates/features/<dominio>/` (macros `qty_label`/`number_input`/`text_input` compartidas por
      panel y fragmento). `UIContext` perezoso en los 20 routers UI (`ui_auth.py` fuera): cada handler hace
      `ctx = ui_context(request, db)` en el cuerpo (llamada directa, no `Depends`, para que los tests puedan llamar
      a los handlers); `role_of()` es la única normalización del rol. Consultas de usuario por petición: de 3-4 a 2
      (`require_active_business_id` aún consulta el usuario por dentro; resolverlo es fase 5). Sidebar con
      `NAV_ITEMS`/`visible_nav_items`/`default_tab`. Red: arnés H-303 (91 capturas: fragmentos, POST con error,
      roles, estados vacíos) y e2e de caracterización.
      - **Revisión + auditoría** (`docs/auditorias/2026-11-01-frontend-fase4-oob-uicontext.md`, 4.25 → **4.65 🟢**
        tras corregir) y correcciones: permisos por rol decididos por el usuario (owner ve Clientes; operador
        vende, crea/edita clientes sin borrarlos y gestiona documentos de venta — sidebar con Documentos/Clientes;
        formularios de edición de gastos/ventas solo admin/owner) y **tres bugs ya en producción**, destapados por
        la auditoría: filtro de fechas de Utilidades por artículo (500, `timedelta` sin importar desde la fase B
        del 2026-08-15), borrar documento de venta (500, `IntegrityError` por FK — líneas sin borrar antes que el
        documento) y autorrelleno de Documentos con SKU inexistente (404 en vez de fragmento vacío). 496 unit, 94
        e2e, 91 snapshots, estilos calculados idénticos en los dos temas.
      - **Probada en staging como admin y como operador** (usuario de prueba creado y borrado en la misma sesión):
        las 17 pestañas, el filtro de fechas, crear+borrar un documento de venta, y los permisos del operador
        (Clientes sin botón Eliminar → 403 directo; Documentos con borrar; sin botón "Edit" en Ventas; 403 en
        editar gasto/venta). Log del servicio sin `csp_violation` ni error 500 tras el despliegue.
      - Pendiente de decisión: `.serena/`/`.idea/` versionados en `develop` (arrastrado desde las fases 1-3).
        Fase 5 (reagrupar `partials/` en `features/<dominio>/`, sacar queries de routers) sin empezar.
- [x] **Release 0.5.0 (fases 1-3 + PAE-015) en `develop` y probada en staging (2026-11-01, sesión 23)** — merge
      `77c25bb`. El CI cayó en "UI snapshots" porque la baseline de Inicio dependía de la fecha: el arnés congela el
      reloj de `ui_home` (`b201f64`). Recorrido de staging con Playwright (17 pestañas × 3 negocios, dos temas,
      modales, impresiones, copiloto): 0 errores de JS/CSP. Arreglados dos fallos previos (`5f891ce`, merge
      `b741a46`): contraste de gráficos en tema oscuro (`--chart-text`/`--chart-border` aplicados a
      `Chart.defaults` con `applyChartDefaults()`, variantes oscuras de `--home-chart-sales`, `--home-chart-line-fill`,
      `--home-pie-1` y `--chart-grid`) y Escape cierra el modal de más arriba salvo los estáticos. 465 unit, 40
      snapshots, 84 e2e. `main` espera a las fases 4-5, que entran en 0.5.0.
      - **Incidente:** `dev.gestion.legatumbp.com` servía producción desde la migración (ver "Entornos"). Único resto
        de pruebas en prod: producto `SKU010001` "test" en Recambios (id 889), pendiente de borrar desde la UI.
      - **Fase 4** cerrada en `feature/frontend-fase4` (sin push) — ver entrada arriba.
      - Pendiente de decidir: `.serena/` y `.idea/` entraron versionados en `develop` con el merge.
- [x] **Release 0.5.0, fase 3 del frontend: CSS sin estilos inline, CSP `style-src 'self'` (PAE-018, 2026-10-24, rama
      `feature/frontend-modular`, sin push)** — 106 `style=` y 4 `<style>` fuera de las plantillas. Colores en
      `static/css/tokens.css` y utilidades compartidas en `utilities.css`, que comparten dashboard, login e
      impresiones. Códigos de barras SVG sin `style` (`components/barcode-svg.js`). Tema claro/oscuro sin recarga
      (`gi:themechange`, gráficos con colores de `--chart-*`/`--home-*`). htmx sin hoja inyectada y con
      `attributesToSettle: ["class"]`. Reportes de CSP en el log (`POST /csp-report`, `WARNING csp_violation`).
      Red de estilos calculados `tests/snapshots/style_snapshot.py`: 74 capturas contra `b0f5dd8`, fuera de CI, y
      solo admite las diferencias intencionadas de `style_allowlist.txt`.
      - Auditoría **4.45 → re-auditoría 4.90 🟢**, sin hallazgos abiertos
        (`docs/auditorias/2026-10-24-frontend-fase3-css-csp{,-reaudit}.md`).
      - Probada de punta a punta sobre una copia de la BD de dev: 108 comprobaciones sin errores.
      - Pendiente solo el flujo: push → `develop` → dev → `main`, con confirmación del usuario.
- [x] **PAE-015 — Copiloto: ventas por punto de venta mes a mes (2026-10-24, rama `feature/frontend-modular`,
      sin push, entra en 0.5.0)** — bug de prod del 2026-10-18 (total por ubicación sin meses, 7 meses en vez de 6,
      título interno, reply engañoso). Tool `sales_by_location_monthly(months)` (35 tools), gráfico multi-serie
      en `components/widget-chart.js` (solo bar/line), reglas de prompt ("últimos N meses", honestidad, dividendos
      pendiente vs retirado), título legible del widget de rescate y aviso cuando falla la tool del widget
      mostrado. Un primer intento (Haiku) se descartó (3.10 🔴, rama `backup/pae015-haiku`, borrada el 2026-10-24); PAE regenerado sobre
      la fase 2 y reimplementado aplicando los envelopes tal cual. Re-auditoría **4.70 🟢**
      (`docs/auditorias/2026-10-23-copiloto-ventas-ubicacion-mensual-reaudit.md`). Eval: 96/96/92 % (base
      96/96/79 %). **DoD cerrado (2026-10-24):** tool contra prod en solo lectura = tabla de `requirements.md` (oct. 3199,0 en Playa por ventas del 19-21); prueba manual dada por hecha.
- [x] **Release 0.5.0, fase 2 del frontend: JS a módulos ES, CSP `script-src 'self'` (2026-10-23, rama
      `feature/frontend-modular`, sin push)** — 19 módulos bajo `app/static/js/` (`main.js` + `core/`,
      `components/`, `features/`, `pages/`; `theme-init.js` clásico), sin build; fuera `app.js`, los `<script>`
      inline y los `on*=`; `Cache-Control: no-cache` en `/static/js/`; XSS del tooltip del pastel corregido.
      Auditoría 4.35 🟢 (`docs/auditorias/2026-10-23-frontend-fase2-js-modulos.md`); el fixture `js_errors`
      (errores de JS/CSP) es autouse en todos los e2e. Fase 1 (layouts + macros) cerrada el mismo día (4.80 🟢).
- [x] **Coach: extracciones al negocio y alerta "drena caja" (v0.4.14, 2026-10-11)** — falso positivo reportado
      por el usuario. Las extracciones a la etiqueta del negocio (`FinanceService.business_label()`, "Negocio" por
      defecto) no salen de la caja: marcan el coste y los gastos recuperados para recomprar. El coach las sumaba como
      retiros y las restaba de la caja. Ahora la regla `partner_overdrawn` (sustituye a `extraction_ratio_high`, clave
      nueva para que los descartes de la alerta falsa no oculten la real) solo avisa si un socio tiene pendiente
      acumulado negativo en dividendos. El retiro del mes contra la utilidad del mes daba falsos positivos cuando un
      socio cobraba lo acumulado. Con datos reales de dev (may-sep, Recambios y Ropa) la alerta antigua saltaba en
      varios meses y la nueva en ninguno. Probado de punta a punta sobre una copia de la BD de dev (código viejo y
      nuevo lado a lado): esa prueba destapó que la caja estimada **restaba las ventas** (cantidad negativa en
      `inventory_movements`), corregido con `abs()`. 385 unit (mutation-test de las 6 correcciones) + 43 e2e. Pendiente: la
      evolución del coach con contexto del negocio (perfil estructurado + motivo al descartar + chat LLM que propone
      ajustes; hoy "descartar" oculta una regla para siempre).
- [x] **Compras sin la sección "Producto" (v0.4.13, 2026-10-11)** — a pedido del usuario, solo visual: fuera
      "Importar productos desde factura", el historial de productos y sus acciones (editar/borrar). Se mantiene
      "Importar compra desde factura". `partials/product_panel.html` queda como `#product-panel` invisible con el
      datalist `#product-options` del campo "Artículo" (se sigue refrescando por `hx-swap-oob` al crear un producto
      con una compra). Editar y borrar productos: pestaña Inventario. Las rutas `/ui/product/from-invoice`,
      `/ui/product/{sku}/edit|update|delete` siguen en el backend sin enlace desde la UI. e2e: el precio de venta se
      verifica por `GET /products`; eliminado el e2e del modal de edición (inalcanzable). 377 unit + 43 e2e.
- [x] **Sugerencias dinámicas de productos + deuda de v0.4.11 (v0.4.12, 2026-10-10)** — PAE vía
      SDD-Agent (`specs/audit-debt-product-suggest/`, 27 envelopes, validado antes en un worktree) implementado por
      `cc_local` en 61 min: `GET /ui/product-suggest` + listener `data-product-suggest` en `app.js` (H-012),
      no finitos rechazados (H-104), mensaje de SKU en otro negocio (H-105), `_insert_with_sku_retry` único y
      `list_sku_prefixes` en `ProductRepository` (H-106), error dentro del modal de edición (H-108), sin
      `product_options` sin uso (H-109). 350 unit (cov 86%) + 41 e2e. `/code-review` + auditoría **3.75 🟡**
      (`docs/auditorias/2026-10-10-audit-debt-product-suggest.md`): **error de diseño del PAE** — Ventas y
      Traspasos filtran sus sugerencias por stock en la ubicación (`ui_sales.py:83`, `ui_transfers.py:641`), la
      búsqueda dinámica les quita ese filtro (H-201/202/206); elegir una sugerencia vacía el datalist (H-203);
      `nan` en precio de venta pasa en silencio (H-204); resto H-205..H-210. **Corregidos todos el mismo día por
      Claude Code** dentro de 0.4.12: Ventas/Traspasos vuelven a su lista filtrada por stock (sin búsqueda
      dinámica), `app.js` restaura la lista inicial y no la vacía al elegir, `parse_typed_float` (tecleado
      inválido → 422) en compras/ventas/edición de producto, `isfinite` en ediciones y devoluciones, búsqueda
      sobre columnas, sin imágenes huérfanas. Re-auditoría **4.15 🟡** sin hallazgos abiertos del diff
      (lo que falta para 🟢 es previo: CSP `unsafe-inline`, búsqueda O(N) en Python). 377 unit + 44 e2e
      (e2e validados por mutación). Probada a mano en dev y pasada a `main` el 2026-10-11.
- [x] **Quitar "Crear producto manualmente" de la pestaña Compras (v0.4.11, 2026-10-10)** — PAE vía SDD-Agent
      (`specs/remove-duplicate-product-creation/`) implementado por `cc_local` (Claude Code + Qwen3.6 local,
      piloto, un envelope por sesión). Se elimina el form y la ruta `POST /ui/product`; "Registrar compra" gana
      "Precio de venta (si es nuevo)". `/code-review` + `auditoria-ingenieria` (3.70 🟡,
      `docs/auditorias/2026-10-10-remove-duplicate-product-creation.md`) → corregidos todos los hallazgos:
      creación unificada en `ProductService.create_for_purchase` (flush, sin commit), producto y compra en un
      solo commit (antes una compra rechazada dejaba producto huérfano), `product_create` en ambas ramas,
      `#product-panel` refrescado por `hx-swap-oob`, `nan`/`inf` rechazados; la importación de facturas usa
      el mismo servicio con transacción por línea. Segunda ronda de `/code-review` (10 hallazgos) también
      corregida. Tercera ronda + re-auditoría (`...-reaudit.md`, 3.35 🟡 → **3.95 🟡** tras corregir
      H-101 cantidad 0 = 500 silencioso, regresión propia; H-102 "Importar productos desde factura" creaba
      productos sin `business_id`; H-103 `log_event(isolated=True)` con SAVEPOINT). Resto (H-104..H-109) para
      un PAE aparte. Pendiente de decisión: H-012 (los datalist de toda la app solo
      muestran los 200 productos más antiguos). **Bug previo descubierto por el
      e2e**: htmx no hace swap de respuestas >= 400, así que ningún panel de error 409/422 se mostraba
      (compras, ventas, gastos, extracciones) — handler global `htmx:beforeSwap` en `app.js` (cualquier 4xx HTML; cierra modales). 323 unit + 39 e2e.
- [x] CRUD completo: productos, clientes, ventas, compras, documentos
- [x] Multi-negocio + multi-ubicación
- [x] **Gestión de negocios desde frontend** (tab Negocios, admin-only, auto-crea ubicación CENTRAL)
- [x] FIFO inventory con lotes
- [x] Documentos PDF (facturas/presupuestos)
- [x] Importación de facturas PDF
- [x] Transferencias CENTRAL → POS
- [x] Reposición SS/ROP
- [x] Reportes (profit mensual, dividendos, gráficos)
- [x] Docker Compose (PostgreSQL + App + Caddy)
- [x] 80 tests unitarios (SQLite en memoria, cobertura 86%)
- [x] **CI/CD Gitea Actions** (`.gitea/workflows/test.yml`) + mirror GitHub — **verde, 80 tests**
- [x] **CSRF protection** (vía header `HX-Request` en middleware)
- [x] **Rate limiting** en login (in-memory, 10 intentos / 5 min)
- [x] **Security headers** (CSP, X-Frame-Options, nosniff) — H-006
- [x] FK enforcement SQLite (`PRAGMA foreign_keys=ON`)
- [x] Logging centralizado (`app/logger.py`)
- [x] **Config centralizada** (`app/settings.py`, H-008)
- [x] **PostgreSQL en dev y producción** — migración SQLite→PG completada (BD `inventario`)
- [x] **Deploy producción** — vm-apps (Docker `gestion-inventario` :10000), `gestion.legatumbp.com`
- [x] **Formulario completo de config de negocio** — crear/editar negocio genera `business_config.<code>.conf` con todos los campos (issuer, currency, purchase, sales_documents, dividends, locations, inventory)
- [x] **Acceso multi-negocio para owners** — tabla `user_businesses` (many-to-many), admin asigna múltiples negocios a un owner desde la pestaña Usuarios, y el owner cambia entre sus negocios asignados vía sidebar
- [x] **Fix borrado de usuario** — anula `audit_logs.user_id` antes de borrar (FK sin ON DELETE)
- [x] **Code-review + auditoría** — score 2.35 → 3.03 (informe `docs/auditorias/2026-08-13-general.md`)
- [x] **Copiloto IA (dashboard dinámico)** — `app/agent/` (LangGraph + tools read-only + widget persistente por negocio). Spec en `specs/copiloto-dashboard-ia/`. **Dev usa modelo local Qwen3-8B** (`qwen3_8b_instruct` vía llama-swap, default); **prod usa API LLM externa** (env `AGENT_LLM_*` en docker-compose). Code-review + fixes aplicados. Auditoría 3.45 (`docs/auditorias/2026-08-14-general.md`) → 3.13 (`2026-08-14-copiloto-completo.md`)
- [x] **Refactor config de negocio: `.conf` → BD (fuente única de verdad)** — `Business` +22 columnas tipadas (issuer/currency/purchase/sales_documents/dividends/inventory) y `Location` + `role`/`is_default`/`position`. Nuevo `app/repositories/business_repository.py` reconstruye el mismo `BusinessConfig` pydantic desde la BD; 47 call sites sustituidos (`get_active_business_config` / `get_business_config`). `.conf` queda legacy (solo backfill one-time, `schema_version=4`); los `.conf` se mantienen inertes (renombrar a `.bak` pendiente, F5).
- [x] **Fix 3 bugs de prod (2026-08-15)** — (1) cache-busting automático por hash de contenido (`?v=40` congelado servía CSS viejo → tema oscuro + burbuja copiloto rotos); (2) quitar `strict:true` del `response_format` del copiloto (OpenAI 400 por `anyOf`/`additionalProperties:true`); (3) firma de `_location_name_for_code` (`db`, `request`) → `NameError` en `/ui/restock-table`. Tests de regresión + auditoría 2.70 (`docs/auditorias/2026-08-15-general.md`)
- [x] **Traspaso masivo** — modal con checklist + 6 fixes de code-review + **campo "Fecha" visible** (traspasos atrasados) + **e2e Playwright** (`tests/e2e/`, institucionalizado en el DoD).
- [x] **Copiloto ampliado a 27 tools** — fixes tabla/POS/chat + Tier 1 (profit/dividendos/reposición/historial) + Tier 2 (clientes/documentos/tendencias vía `SalesDocumentService`/`ReportingService`).
- [x] **Despliegue CD con Gitea Actions** — runners self-hosted `dev`/`prod`, test-gate, systemd `Restart=always`; cron watcher y Dokploy eliminados. Doc: `04-Harness/deploy-gitea-actions.md`.
- [x] **Negocio sin POS** — `LocationsConfig.default_pos` opcional (`None`), sin fallback que inyecte `POS1` (business_config/business_repository/ui_businesses), ventas/traspasos usan la central (`default_sale_code()`), UI permite crear/editar sin POS (campo vacío). Limpieza de `CERVEZA_MARIELA_POS1` en prod.
- [x] **Auditoría quick wins (H-029/H-034/H-036/H-053)** — XSS escapado en `customer.client_id`/`address` y `unit_of_measure`; seed sin credenciales triviales (fail-fast si falta `ADMIN_PASSWORD`/`OPERATOR_PASSWORD`); índices compuestos `(business_id, type, movement_date)`, `(business_id, product_id)`, `(business_id, sku)` (models + migraciones SQLite/PG, constante `_COMPOSITE_INDEX_SQL`).
- [x] **Code-review + auditoría del negocio sin POS (2026-08-15)** — code-review (8 ángulos) encontró y corrigió H-067..H-077: dropdown de ubicación vacío en Documentos (nuevo `LocationsConfig.all_locations()` = central + POS), traspasos 422 para sin-POS (form oculto si hay 1 sola ubicación), `"None"` literal en edit form, `Location(code="")` por spec `:Tienda`, default POS1 pre-rellenado en create, seed random solo en log (→ fail-fast). Auditoría **2.95** (`docs/auditorias/2026-08-15-negocio-sin-pos.md`, delta +0.25 vs 2.70). +4 unit +1 e2e (negocio sin POS). 138 unit + 5 e2e verdes.
- [x] **Auditoría Fase A (quick wins estructurales)** — JSON logging + `X-Request-ID` (H-043), health profundo con 503 (H-065), circuit breaker LLM (H-030), logs en excepts de routers (H-039), `flush()` en `_location_id_for_code` (H-040), audit atómico en ventas/traspasos + correlación de historial (H-041), `os.getenv` → `settings.py` (H-045). Score objetivo 2.95 → ~3.45. Spec: `specs/auditoria-fase-a/`. 143 unit + 5 e2e verdes.
- [x] **Refactor Fase B (SoC/capas)** — split de `ui_tabs.py`, extracción de `FinanceService`/`ReportingService` desde `InventoryService`, `LocationRepository` + repos por dominio. Spec: `specs/refactor-fase-b/`. Auditoría **3.85** (`docs/auditorias/2026-08-15-fase-ab.md`).
- [x] **Refactor Fase C (RBAC API + escalabilidad)** — `require_admin_or_owner_api` en purchase/return-supplier/transfer/adjustment (operador solo vende), paginación + cache SS/ROP, ORM de routers → repos/services (183→27 ops), e2e en CI (Playwright en test-gate). Spec: `specs/refactor-fase-c/`. Auditoría **4.00** (`docs/auditorias/2026-08-15-fase-c.md`).
- [x] **Refactor Fase D (empujón al verde)** — API contract tests (RBAC + schemas + error paths), LLM retry con backoff exponencial + jitter (`AGENT_RETRY_*`), rate limiter login keyed por (IP real + username). Spec: `specs/refactor-fase-d/`. Auditoría **4.10** (`docs/auditorias/2026-08-15-fase-d.md`). 149 unit + 16 e2e verdes (4 e2e flaky con selectores rotos eliminados).
- [ ] **Verde definitivo (≥4.2)** — timeout global del agente (`asyncio.wait_for` en `runner.run_agent`, dim5 4→5) + quirk panel ventas operador (T7). CSP `unsafe-inline` como W-1 (dim4 4→5).
- [ ] Sistema agéntico con LLM local multi-tool (20+ tools, insights proactivos) — fase 2 planificada
- [x] **Coach financiero y operativo** — CoachService (indicadores fin/op/clientes), motor de
      reglas (5 reglas + logging), círculo cerrado [A]. Tab admin-only con score, recomendaciones
      accionables (síntoma+causa+acción+impacto), burbuja chat vía agente existente. 24 task
      envelopes + auditoría (3.65) + code-review (6 hallazgos, todos corregidos: guard de "sin
      datos", ventanas de tiempo consistentes, cómputo cacheado, current_cash resiliente). 203
      unit + 18 e2e verdes. v0.4.1. **Pendiente:** estacionalidad [B] (REQ-8) no se implementó
      — ver `specs/coach-financiero-operativo/PAE-coach-financiero-operativo.md` §5.4.
- [x] **Fix regresión Fase C en compras** — `_generate_auto_sku()` desempaquetaba mal lista de
      SKU tras refactor f6e25dd. ValueError cada vez que se registraba compra como producto nuevo
      sin SKU. También scope `list_sku_prefixes()` por `business_id` para evitar numeración
      compartida entre negocios. E2e regression + 205 unit + 20 e2e verdes. v0.4.1.
- [x] **Coach restricción RBAC temporal** — admin-only (era admin/owner) mientras se valida con
      otros roles en dev. Visible en sidebar solo para admin. Commit `27f9da2`.
- [x] **Copiloto: 4 tools nuevas + filtro location (v0.4.3)** — auditoría de cobertura de tools
      (29→33): `dead_stock` (productos sin venta reciente), `inventory_turnover` (rotación en
      días), `profit_by_category`/`profit_by_location` (ganancia agrupada, antes solo por
      producto, con `GROUP BY` SQL en `ReportingService`, no agregación Python per-línea).
      `sales_by_product`/`profit_by_product`/`daily_sales_series` aceptan `location` opcional
      (nuevo `InventoryService.resolve_location_id`, API pública de solo lectura, nunca
      autocrea). Generado vía SDD-Agent (`specs/agent-tools-p0-p1/`), `/code-review` (5
      hallazgos) + `auditoria-ingenieria` (10 dimensiones, 3.20→4.05) — los 7 hallazgos
      corregidos en la misma sesión. 219 unit verdes.
- [x] **Fix: copiloto respondía texto sin gráfica (v0.4.4, bug de prod)** — el LLM puede llenar
      `tool`/`tool_args` a nivel de `PlanModel` y dejar `widgets` vacío; `_execute_node` solo
      itera `widgets`, así que ese caso no ejecutaba ninguna tool ni renderizaba nada (el chat
      mostraba el `reply` sin gráfica/tabla). Reportado en vivo en prod tras el deploy de
      0.4.3, con la pregunta original de esta sesión ("5 productos que más utilidades...").
      Fix en `app/agent/graph.py::_plan_node`: se trata como error de formato retryable (mismo
      mecanismo que un JSON inválido) y, si el modelo insiste tras agotar los reintentos, se
      sintetiza un widget con la tool que sí eligió. 221 unit verdes (2 nuevos).
- [x] **Fix: sugerencias de artículos vacías en venta (v0.4.5, bug de prod)** — `get_business_locations()`
      dejaba que la inferencia legacy (filas `role=NULL` con código terminado en `_CENTRAL`)
      sobreescribiera sin condición una central ya resuelta por `role='central'` explícito; una
      ubicación huérfana (`role=NULL`, 0 stock) generada por otro bug (`LocationRepository`
      auto-creaba ubicaciones con prefijo literal `BUSINESS_` cuando `InventoryService` no le
      pasaba el código real del negocio) terminaba "ganando" como central por defecto, dejando el
      panel de venta sin sugerencias. Reportado en vivo en prod (Bazar Mariela). Fix en
      `business_repository.py::get_business_locations` (no pisar central ya resuelta) +
      `location_repository.py::location_id_for_code` (resuelve `business_code` desde `businesses`
      si no se lo pasan, en vez de caer a `"BUSINESS"`). Dato huérfano borrado en prod
      (`locations.id=147`, sin movimientos/lotes/documentos asociados). 254 unit verdes.
- [x] **Método de pago (transferencia) en venta rápida (v0.4.10, 2026-10-09)** — checkbox
      "Pagado por transferencia" en `InventoryMovement` (endpoints `/ui/sale`,
      `/ui/sale/barcode`, `/ui/movement/sale/{id}/update`), desmarcado por defecto,
      editable, visible y filtrable en el historial de ventas (tab Ventas). **NO** aplica
      a `SalesDocument` (facturas/presupuestos). PAE del 2026-08-18 (`specs/sale-payment-method/`)
      ejecutado 7 semanas después — auditado contra el código real primero (firmas sin
      cambios, solo líneas), implementado **directamente por Claude Code**, no vía
      OpenCode. `InventoryService.update_sale()` usa `paid_by_transfer: Optional[bool] = None`
      (no tocar si `None`) para no resetear el valor en ediciones desde
      `ui_profit_items.py`. Propagado también al outbox de `offline-desktop-sync`
      (`app/sync/service.py::_apply_sale`, no existía cuando se escribió el PAE). **Bug
      real descubierto por el e2e nuevo**: `_rebuild_product_fifo` podía rechazar una
      edición de venta válida con "Insufficient stock" si una compra y la venta caían en
      el mismo minuto (el form de edición solo tenía precisión de minuto en la fecha,
      invirtiendo el orden cronológico real al reconstruir el FIFO) — corregido con
      milisegundos de precisión, acotado a `ui_sales.py`/`sale_panel.html`/
      `sale_edit_form.html` (el mismo bug late sin corregir en compras/traspasos/gastos/
      extracciones/dividendos, que comparten el helper `dt_to_local_input` de
      `ui_common.py` — pendiente). 304 unit + 24 e2e verdes. Commit `a2f6c21` en
      `develop`, mergeado a `main` (`ce3ccf6`), desplegado y verificado en vivo en dev y
      prod. Detalle completo: `.claude/session-notes.md`.
- [x] **Fix bug de prod: compra de producto nuevo bloqueada (v0.4.6)** — regresión del fix
      `27f9da2` (2026-08-17): `list_sku_prefixes()` pasó a filtrar por `business_id` para
      "no compartir numeración entre negocios", pero `products.sku` tiene `unique=True`
      **global** (no compuesto por `business_id` pese a lo que decía este documento). El
      candidato autogenerado de un negocio con pocos productos caía en un rango ya ocupado
      por otro negocio → `IntegrityError` idéntico en los 5 reintentos → 409 "No se pudo
      generar un SKU automáticamente", bloqueando la compra de producto nuevo sin SKU manual
      en todos los negocios excepto el de mayor SKU global (confirmado con datos reales:
      `cerveza_mariela` chocaba con `bazar` en `SKU000502`). Revertido a numeración global en
      `purchase_repository.py::list_sku_prefixes` (coincide con el constraint real de la BD).
      Regression test en `tests/unit/test_repositories.py`. 222 unit verdes. **Pendiente**:
      fix estructural (SKU único compuesto `(business_id, sku)` + migración + auditar 2
      call sites que asumen SKU global en `ui_common.py`/`ui_products.py`) — decisión
      explícita del usuario de aplicar solo el hotfix por ahora.
- [x] **Épico offline-desktop-sync — P1 (Fundamentos)** — 5 PAEs generados vía SDD-Agent en
      `specs/offline-desktop-sync/` (P1 Fundamentos, P2 Backend de sync, P3 Cliente+escritura
      offline, P4 Desktop+cifrado Windows, P5 e2e multi-nodo). P1 (outbox `pending_sync_events`
      + rangos de SKU 10k) implementado por OpenCode, auditado en rojo (2.90/5.00 — boundary
      bug crítico en `next_sku_in_range`), **corregido y re-auditado a 3.80/5.00 🟡**, **commiteado**
      (`2dbad8a`, en `develop`, sin mergear a `main` todavía). 241 unit verdes en ese commit.
- [x] **Épico offline-desktop-sync — P2 (Backend de sync)** — `SyncService` (idempotencia,
      replay vía `InventoryService.sale`, dedup de clientes, `build_pull`) + endpoints
      `/sync/register|push|conflicts`. Implementado por OpenCode, auditado en rojo
      (2.50/5.00 — 3 hallazgos **críticos** de aislamiento multi-tenant: IDOR en
      `register_device`, `password_hash` sin cifrar en el pull, `movement_allocations`/
      `sales_document_items` sin filtro de negocio). **Corregido directamente por Claude**
      (no vía OpenCode) + re-review con mutation-testing (detectó y corrigió una regresión
      propia: scope de `client_id` roto por el mismo patrón que el ADR-012 de SKU) +
      **re-auditado a 3.70/5.00 🟡**. 267 unit verdes. Ver
      `docs/auditorias/2026-08-28-p2-sync-backend-offline-sync*.md`.
      **Commiteado** (`e4e03a2`, en `develop`).
- [x] **Fix bug de prod: diagnóstico ZARA irrelevante en factura AUTODOC** — `_zara_pdf_diagnostics()`
      en `ui_purchases.py` se disparaba en cualquier importación con ≤3 líneas sin verificar
      que el PDF fuera realmente de ZARA (debug leftover). Gateado a `"ZARA" in full_text`.
      261 unit verdes. **Commiteado** (`b5c1c85`, en `develop`).
- [x] **Épico offline-desktop-sync — P3, P4, P5 + auditoría integral (2026-08-29)** — ver
      sesión del mismo día más abajo para el detalle completo. Resumen: P3 cerrado (3 bugs
      reales encontrados por e2e y corregidos), P5 reescrito de cero (auditado 1.40/5.00 🔴,
      el test nunca había corrido), P4 auditado por primera vez y encontrado ~100% roto (5
      bugs de crash garantizado + 7 más en una segunda ronda, todos corregidos). Auditoría
      integral del épico completo: **2.15/5.00 🔴 → 4.05/5.00 🟢**. Todo commiteado
      (`41adf1d`, `13f7af1`, `88edf08`, `cb83e2e`, en `develop`). **Pendiente:** validación
      manual en Windows de `packaging/desktop.spec`/`installer.iss` (PyInstaller/Inno Setup no
      instalables en este entorno Linux) y de SQLCipher real (verificado solo contra stock
      `sqlite3`, que ignora `PRAGMA key` silenciosamente). Informes:
      `docs/auditorias/2026-08-29-offline-desktop-sync-epic-full-audit{,-reaudit}.md`.
- [x] **Serena MCP habilitado para el proyecto (2026-08-29)** — onboarding corrido, memorias
      escritas (`core`, `tech_stack`, `suggested_commands`, `conventions`, `task_completion`,
      `known_bug_patterns`) en `.serena/memories/`. Decisión del usuario: uso obligatorio de
      las tools de Serena (símbolos/referencias/edición semántica) en vez de `Read`/`Grep`/
      `Edit` nativos para explorar y editar código en todo proyecto, no solo este.


## Reglas para el agente

1. **No inventar APIs** — leer `app/routers/` y `app/schemas.py` antes de asumir endpoints o schemas
2. **Testing** — siempre correr `pytest -v` después de cambios en lógica de negocio. **Toda feature nueva lleva tests e2e** (`tests/e2e/`, Playwright) que validen el flujo completo; no se da por terminada sin ellos (ver DoD)
3. **Documentación** — actualizar este `CLAUDE.md` si cambia la estructura o el estado
4. **Commits** — mensajes en inglés (English), formato conventional commits
5. **Base de datos** — nunca borrar `inventario.db` sin backup
6. **Routers** — hay 18 routers, revisar `app/main.py` para ver cuáles están registrados
7. **Push** — siempre a `gitea` (192.168.1.21). Gitea tiene mirror automático a GitHub, así que NO pushear a GitHub directamente.
8. **Flujo de cambios (permanente)** — los cambios van **siempre a `develop`**, se prueban en el entorno de desarrollo (`dev.gestion.legatumbp.com`, LAN interno), y solo después se mergean a `main` y se despliegan en producción (`gestion.legatumbp.com`). Nunca directo a `main`.
9. **Modelo de ejecución** — ver [[loop-desarrollo-agentes]] v4 (único loop): idea + Requirements/Design + revisión en **Opus** (`cc_claude_code`); Envelopes/Assembly en **DeepSeek Pro** (`cc_deepseek`); implementación/correcciones/primer barrido en **cc_local** (Claude Code + modelo local **Qwen3.6-35B-A3B** `qwen36_35b_coder_mtp` vía llama-swap). El ejecutor local ya NO es OpenCode (sustituido por `cc_local`).
10. **Copiloto IA en runtime** — el copiloto usa **modelo local en dev** (default `AGENT_LLM_BASE_URL=http://localhost:8080/v1`, `AGENT_LLM_MODEL=qwen3_8b_instruct`) y **API LLM externa en producción** (env `AGENT_LLM_*` en `docker-compose-homelab.yml`). Las env vars viven en `app/settings.py`.


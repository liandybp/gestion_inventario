# CLAUDE.md — Gestión de Inventario

## Resumen del proyecto

App web FastAPI + HTMX para gestión de inventario multi-negocio con soporte multi-ubicación (CENTRAL + POS), control FIFO de lotes, documentos de venta (facturas/presupuestos), clientes, compras, gastos, transferencias y reportes.

- **Versión actual:** `0.4.12` (ver `VERSION`) — commiteada en `develop` el 2026-10-10 junto con `0.4.11`, **sin push** (pendiente de probar en dev); `0.4.10` es la desplegada en dev y prod
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
| Testing | pytest (377 unit + 44 e2e Playwright) |

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
│   ├── templates/           ← Jinja2 + HTMX partials
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
| **Desarrollo** | `http://dev.gestion.legatumbp.com` | **Interno (LAN)** — NO sale a internet | `develop` | host `ai-lab-home` (192.168.1.162) | uvicorn `:10000` |
| **Producción** | `https://gestion.legatumbp.com` | **Público** (Cloudflare Tunnel) | `main` | vm-apps (192.168.1.20) | Docker `gestion-inventario` `:10000` |

- **Dev es interno**: se resuelve vía **AdGuard DNS** (192.168.1.23) con rewrite `dev.gestion.legatumbp.com → 192.168.1.20` (vm-apps). El **Nginx de vm-apps** (`default.conf`) redirige `dev.gestion.legatumbp.com` → `192.168.1.162:10000` (host dev). NO usa Cloudflare Tunnel.
  - Ruta: `navegador → AdGuard → vm-apps:80 (Nginx) → host:10000 (uvicorn)`.
- **Deploy dev/prod**: **Gitea Actions CD** (`.gitea/workflows/deploy.yml`). En `push` corre `test` (CI) y, si pasa, `deploy-dev` (rama `develop`) o `deploy-prod` (rama `main`) en runners self-hosted. Dev: `git reset` en `~/staging/gestion_inventario` + `systemctl restart gestion-inventario-dev.service` (systemd `Restart=always`). Prod: `docker compose -f docker-compose-homelab.yml up -d --build`. Proceso general reutilizable en `~/obsidian-lab/04-Harness/deploy-gitea-actions.md`.
- **BD dev**: PostgreSQL `localhost:5433/inventario` (host). **BD prod**: `192.168.1.20:5432/inventario` (vm-apps).

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

## Estado actual

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
      (e2e validados por mutación). **Pendiente:** push a `develop` y prueba manual en dev (Ventas con un POS
      sin stock de un producto; Compras con un producto posterior al 200).
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

## Sesiones

- **2026-10-10 (sesión 17)** — **Piloto de `cc_local` (Claude Code + Qwen3.6 local) como ejecutor de PAEs.**
  (1) v0.4.11: quitar "Crear producto manualmente" de Compras; tres rondas de review + re-auditoría (3.95 🟡);
  commiteado (`9b4b430`, `6fce595`), sin push. (2) v0.4.12: PAE de los pendientes (H-012 con búsqueda
  dinámica, H-104..H-109), validado en un worktree (encontró 2 bugs del propio PAE: T9b y T16) e implementado
  por `cc_local` en 61 min, fiel al PAE. Review + auditoría 3.75 🟡 con H-201..H-210 (el principal, error de
  diseño del PAE: Ventas/Traspasos filtran por stock). Corregidos todos por Claude Code el mismo día
  (re-auditoría 4.15 🟡, 377 unit + 44 e2e), sin subir versión; commiteado en `develop`, sin push.
  Conclusión del piloto: `cc_local` ejecuta bien; los fallos vienen del diseño del PAE. Detalle:
  `.claude/session-notes.md`.
- **2026-10-09 (sesión 16)** — **Deploy a producción del backlog acumulado +
  `paid_by_transfer` + fix de CD de prod.** La nota de Obsidian del proyecto estaba
  desactualizada (decía P2 del épico offline-desktop-sync sin commitear; en realidad
  P1-P5 llevaban commiteados en `develop` desde el 2026-08-29, y la sesión 15 del
  copiloto tampoco se había pusheado). Al intentar desplegar a `main`, el job
  `deploy-prod` de `.gitea/workflows/deploy.yml` seguía apuntando a la ruta de la VM
  vieja `vm-apps` (`~/PyCharmMiscProject/gestion_inventario`) — nunca se había
  actualizado tras la migración a `laptop-server` del 2026-09-07 porque no hubo
  ningún push a `main` desde entonces. Reescrito para clonar por HTTP-git en LAN con
  el token del job (sin SSH keys), construir la imagen en el host, recrear solo el
  contenedor en `~/apps`, verificar health con la versión esperada y rollback
  automático a la imagen anterior si falla — probado dos veces en producción real
  (v0.4.9 y v0.4.10), ambas con éxito. `Dockerfile` ahora copia `VERSION` (cierra uno
  de los 2 bugs de healthcheck pendientes desde hacía semanas). Con el CD arreglado:
  deploy de v0.4.9 a prod (esquema v4→v7, código de agosto→septiembre). Después,
  implementación completa de `paid_by_transfer` a partir del PAE del 2026-08-18
  nunca ejecutado (ver entrada en Estado actual arriba para el detalle, incluido el
  bug real de FIFO/fecha descubierto por el e2e nuevo) — v0.4.10, también desplegado
  y verificado en vivo en dev y prod. De paso: una falsa alarma de seguridad
  (`admin`/`admin` funcionaba en prod en una verificación, no en la siguiente — el
  usuario había borrado esa cuenta manualmente entre ambas pruebas, confirmado
  leyendo `users` en prod directamente). Detalle completo: `.claude/session-notes.md`.
- **2026-09-21 (sesión 15)** — **Copiloto: diagnóstico + memoria de tool previa + eval
  de precisión.** Usuario reportó que el copiloto del dashboard dinámico "a veces da
  resultados que no pide". Recap de arquitectura (LangGraph, `_plan_node` →
  `_execute_node` → `_build_node` → `_narrate_node`, prompt heurístico de ~30 reglas,
  33 tools) → auditoría de `app/agent/` con `auditoria-ingenieria`
  (`docs/auditorias/2026-09-21-agent-copiloto.md`, 2.85/5.00 🔴, 8 hallazgos) →
  usuario aprobó atacar 3: H-001 (logging), H-005 (memoria de tool previa), H-004
  (eval set) → PAE vía SDD-Agent (`specs/agent-copiloto-diagnostico/`, 8 task
  envelopes) → **implementado directamente por Claude Code** (pedido explícito del
  usuario — "arranca tu mismo", no vía OpenCode, excepción puntual al flujo estándar
  del proyecto). Verificación: 295 unit + 22 e2e verdes, eval real contra Qwen3-8B
  local (96% tool / 96% type / 88% args accuracy). `/code-review` (medium, primera
  ronda) encontró 5 hallazgos: el más grave, `app/routers/ui_agent.py:117`, el guard
  de `last_tool` solo protegía el primer turno tras "nueva conversación" — turnos
  posteriores sin widget propio en esa conversación seguían filtrando el widget más
  reciente del negocio (de cualquier conversación, incluso ya cerrada). Corregido
  comparando `widget.created_at` contra `prior[0].created_at` (primer turno de ESTA
  conversación) — al implementar el fix se descubrió y reprodujo un bug real de
  infraestructura: SQLite/SQLAlchemy formatea `CURRENT_TIMESTAMP` sin microsegundos
  pero bindea cualquier `datetime` Python como parámetro **siempre con** `.000000`,
  rompiendo la comparación de fechas en SQL incluso para instantes idénticos (no
  aplica a PostgreSQL/prod) — resuelto comparando en Python tras el fetch en vez de
  en el `WHERE`. Los otros 4 hallazgos: test de regresión sin cobertura del
  escenario real (agregado), y 3 gaps de calidad en el eval (duplicaba
  `app/utils.py::month_range` y el fallback de `_execute_node`, no detectaba
  circuit-breaker-cooldown como fallo de conectividad, recompilaba el grafo 24
  veces) — los 4 corregidos. Segunda ronda de `/code-review`: **0 hallazgos**.
  Re-auditoría: **4.35/5.00 🟢** (era 2.85 🔴). 2 commits en `develop` (`eb8509a`
  feature, `1fc4d7c` bump 0.4.8→0.4.9), **sin push** (pendiente de confirmación del
  usuario). Pendiente explícito: decisión sobre H-003 (routing en dos pasos /
  few-shot para el prompt heurístico), ahora con baseline real del eval. Informes:
  `docs/auditorias/2026-09-21-agent-copiloto{,-diagnostico-postimpl,-diagnostico-reaudit}.md`.
  Detalle completo: `.claude/session-notes.md`.
- **2026-08-29 (sesión 13)** — **Cierre del épico offline-desktop-sync (P3/P4/P5) + auditoría
  integral + Serena habilitado.** Continuación de la sesión 12 el mismo día. Cuatro tramos:
  (1) **P3 cierre**: `/code-review` + `auditoria-ingenieria` sobre el commit `41adf1d` (3
  quick wins: `app_mode` hardcodeado, `_PULL_ORDER` sin fuente compartida con
  `SyncService.build_pull()` → nuevo `app/sync/tables.py::SYNC_TABLE_ORDER`, dead init) + e2e
  Playwright nuevo (`tests/e2e/test_sync_e2e.py`) que encontró **3 bugs reales adicionales**
  en `apply_pull()` que 15 tests unitarios nunca habían detectado: FK bloqueada por tablas
  locales no sincronizadas (`audit_logs` et al., nunca antes ejercitado porque ningún test
  creaba una fila de auditoría junto al usuario), datetimes llegando como string ISO tras
  cruzar JSON por HTTP (`TypeError` en SQLite), e inserts cross-tabla sin `relationship()`
  ORM que un solo `flush()` final no reordenaba por FK. Commiteado (`13f7af1`, `88edf08`).
  (2) **Auditoría del épico completo** (los 5 slices como sistema integrado, no uno por uno)
  pedida por el usuario: **2.15/5.00 🔴** — reveló que **P4 nunca había sido auditado ni
  ejecutado por nadie**. (3) **Corrección de P5**: `tests/e2e/test_sync_multinode_e2e.py`
  nunca había corrido con éxito (`NameError` inmediato); su fixture `central_server` apuntaba
  a la BD real de dev + el puerto 10000 del servidor de dev real corriendo; `verify.sh` nunca
  ejecutaba el e2e pese a afirmarlo. Reescrito de punta a punta (nodos como emisores HTTP
  contra un servidor central real aislado, mismo patrón seguro que `live_server`).
  (4) **Corrección de P4** (auditado por primera vez, encontrado ~100% roto): derivación de
  clave SQLCipher crasheaba garantizado (`Path.as_bytes()` no existe), launcher.py crasheaba
  garantizado (`__import__` mal usado), `desktop.spec`/`installer.iss` con API de
  PyInstaller/Inno Setup inventada — 5 hallazgos críticos, todos corregidos y verificados por
  ejecución directa (conexión SQLite real de punta a punta en modo desktop). Una segunda
  ronda de `/code-review` sobre esos fixes encontró 7 hallazgos más (fuga de estado entre
  tests, hilo de uvicorn muriendo en silencio, orden de PRAGMA vs SQLCipher, clave de entorno
  sin normalizar, fallback DPAPI→base64 peligroso, hidden imports de pywebview faltantes),
  todos corregidos. Re-auditoría del épico completo: **4.05/5.00 🟢**. Todo commiteado
  (`cb83e2e`). 286 unit + 22 e2e verdes al cierre (era 265 al inicio de la sesión 12).
  Informes: `docs/auditorias/2026-08-29-{p3-sync-client-offline-sync-closeout,
  p5-multinode-e2e-offline-sync{,-closeout},offline-desktop-sync-epic-full-audit{,-reaudit}}.md`.
  **Fuera de alcance de este entorno** (documentado explícitamente, no oculto): build real de
  PyInstaller/Inno Setup y verificación contra SQLCipher real requieren una máquina Windows.
  **Serena habilitado**: instalado a nivel `user` (todos los proyectos), onboarding corrido
  para este repo, memorias escritas en `.serena/memories/` (ver `mem:core` y demás) —
  decisión del usuario de usar sus tools de código semántico en vez de `Read`/`Grep`/`Edit`
  nativos de acá en adelante, en todo proyecto. `.serena/` **sin commitear** (working tree
  local, decisión de commit no pedida esta sesión). Detalle completo:
  `.claude/session-notes.md`.
- **2026-08-29 (sesión 12)** — **Fix bug de prod: diagnóstico ZARA irrelevante en importación
  de factura AUTODOC**: usuario reportó texto de debug (`Diagnóstico ZARA: p1: refs_text=0
  refs_words=0 words=211`) pegado al mensaje de éxito al importar una factura del negocio de
  recambios. Causa real: `_zara_pdf_diagnostics()` en `ui_purchases.py` se disparaba en
  cualquier importación con ≤3 líneas, sin verificar si el PDF era de ZARA — debug leftover
  del parser de ZARA nunca gateado. `parse_invoice_pdf()` en sí funcionaba bien (prueba
  H&M → ZARA → AUTODOC, cayó correctamente a AUTODOC, 3 líneas importadas OK); el bug era
  puramente de UI. Fix: la función retorna `None` si el texto extraído no contiene "ZARA"
  (+3 líneas, diff aislado). 261 unit verdes. **Sin commitear** — mismo working tree que P2
  (sesión 11, también sin commitear); decisión de commit pendiente del usuario para ambos.
  Detalle: `.claude/session-notes.md`.
- **2026-08-28 (sesión 11)** — **Épico offline-desktop-sync: brainstorming + 5 PAEs vía
  SDD-Agent + implementación P1 + auditoría (score 2.90 🔴, no mergeado):** brainstorming
  completo de la nueva app de escritorio Windows offline-first con sync bidireccional
  (alcance: venta+cliente offline, compras/traspasos/LLM online-only, rangos de SKU 10k por
  negocio, dedup de clientes por teléfono/email, pywebview + SQLCipher/DPAPI). Design doc en
  `docs/plans/2026-08-28-app-escritorio-offline-sync-design.md`. SDD-Agent generó el épico
  completo en `specs/offline-desktop-sync/`: `requirements.md` (20 REQs) descompuesto en
  **5 PAEs secuenciales** (P1 Fundamentos, P2 Backend de sync, P3 Cliente+escritura offline,
  P4 Desktop+cifrado, P5 e2e multi-nodo), 47 task envelopes en total. OpenCode implementó
  **P1** (outbox `pending_sync_events` con hash encadenado + rangos de SKU de 10k por
  negocio) — 233 unit verdes, scope respetado. `/code-review` (8 ángulos, 1-vote verify) +
  `auditoria-ingenieria` (10 dimensiones) sobre el código real: **score 2.90/5.00 🔴**,
  **NO mergeado**. Hallazgo crítico **H-001**: `next_sku_in_range` tiene un boundary bug
  (`n < ceiling` estricto) que causa colisión de SKU **permanente** al llegar al SKU #10.000
  de un negocio — reproduce la misma clase de incidente que este código debía prevenir
  (v0.4.6). Más 3 hallazgos altos (H-002 hash del outbox no cubre todos los campos, H-003
  cero logging/try-except en `app/sync/` pese a que el propio pre-mortem del diseño lo
  exigía, H-004 race condition latente en `append_event`). Informe completo:
  `docs/auditorias/2026-08-28-p1-foundations-offline-sync.md`. **P1 corregido y
  commiteado** en el resto de la sesión — re-auditoría 3.80/5.00 🟡 (informe
  `-reaudit.md`), commit `2dbad8a` en `develop` (241 unit verdes).
  **Continuación (misma sesión) — P2 implementado, auditado, corregido y re-auditado:**
  OpenCode implementó P2 (`SyncService`, endpoints `/sync/register|push|conflicts`).
  `/code-review` + `auditoria-ingenieria` (score **2.50/5.00 🔴**) encontraron 3 hallazgos
  **críticos** de aislamiento multi-tenant: IDOR en `register_device` (no validaba
  pertenencia del `business_id`, cualquier admin/owner podía aprovisionar un token para el
  negocio de otro), `password_hash` de usuarios viajando sin cifrar en el pull, y
  `movement_allocations`/`sales_document_items` volcados **sin filtro de negocio** (fuga de
  datos comerciales entre todos los negocios del sistema en cada sincronización). Más 3
  altos (`event_id` único global permitía bloqueo cruzado entre negocios, `device.location_id`
  nunca se usaba para restringir ventas, token de dispositivo sin rate limit ni expiración).
  Informe: `docs/auditorias/2026-08-28-p2-sync-backend-offline-sync.md`. **Claude corrigió
  los 8 hallazgos directamente** (sin pasar por OpenCode, a pedido del usuario). Una segunda
  ronda de `/code-review` con **mutation-testing real** (revertir cada fix y confirmar que
  el test falla) detectó que la propia corrección había introducido una **regresión**: el
  chequeo de colisión de `client_id` se scopeó por negocio, pero `Customer.client_id` sigue
  siendo `unique=True` **global** (misma familia de bug que el ADR-012 de SKU, la tercera
  vez que este patrón aparece en el proyecto) — corregida en el mismo ciclo, con test de
  regresión añadido. Re-auditoría: **3.70/5.00 🟡** (informe `-reaudit.md`), 267 unit verdes.
  **P2 queda sin commitear** — el usuario cerró la sesión antes de confirmar el commit;
  retomar la decisión la próxima sesión antes de avanzar a P3. Detalle completo:
  `.claude/session-notes.md`.
- **2026-08-19 (sesión 10)** — **Fix bug de prod: compra de producto nuevo bloqueada
  (v0.4.6) + arreglo de Cloudflare Access sobre el runner de CD compartido**: usuario
  reportó el mismo síntoma que el bug de `27f9da2` (17-ago) pero causa distinta —
  `list_sku_prefixes()` scopeaba por `business_id` desde ese fix, pero `products.sku`
  (y `inventory_lots.lot_code`) son `unique=True` **global** en la BD, no compuestos
  (documentación previa de este archivo era incorrecta en ese punto, ya corregida).
  Confirmado con datos reales de prod: `cerveza_mariela` (SKU000496–501) chocaba con
  `bazar` (SKU000354–509) en el candidato `SKU000502` — bloqueaba la compra de producto
  nuevo en todos los negocios menos el de mayor SKU global. Revertido a numeración
  global (decisión explícita del usuario, ver ADR-012 en Obsidian). 222 unit verdes,
  verificado en vivo en dev (curl login+compra) y prod (health + BD sin colisiones).
  En paralelo, el deploy automático estaba roto: Cloudflare Access puesto delante de
  `gitea.legatumbp.com` (hardening del lab, sesión distinta) bloqueaba tanto el
  `FetchTask` del runner de Gitea Actions como el `git fetch` del checkout de cada
  workflow — afecta a **todos** los repos que comparten ese runner, no solo este.
  Guiado el arreglo por chat (política Bypass + 3 Destinations: `api/actions/*`,
  `info/refs`, `git-upload-pack`) tras varias vueltas por cambios no documentados en la
  UI de Cloudflare. Pipeline push→test→deploy-dev verificado de punta a punta sin
  intervención manual tras el fix. Deploy a prod (`develop`→`main`, `73b912a`) se hizo
  manualmente (bypass temporal, mismos pasos que haría el CD) antes de que el runner
  quedara sano — no se re-verificó el job `deploy-prod` automático después. Detalle:
  `.claude/session-notes.md`, `~/obsidian-lab/00-Inicio/problemas-conocidos.md`.
- **2026-08-18 (sesión 9)** — **PAE: método de pago (transferencia) en venta rápida** —
  usuario pidió poder identificar si una venta se pagó por transferencia. Exploración del
  flujo real de venta (`ui_sales.py` → `InventoryService.sale/update_sale` →
  `InventoryMovement`, no `SalesDocument`) + 3 preguntas de alcance al usuario (scope
  solo venta rápida, visible+filtrable en historial, editable después) → SDD-Agent (4
  fases: requirements con 8 REQs Gherkin, design con pre-mortem de las 10 dimensiones de
  auditoría, 14 task envelopes, PAE assembly). Hallazgo del pre-mortem: `update_sale()`
  tiene un segundo caller no evidente a primera vista (`ui_profit_items.py`) que hubiera
  sufrido una regresión silenciosa sin el trade-off `Optional[bool] = None` (documentado
  como D3 en `design.md` y REQ-8 de no-regresión). Ninguna línea de código de producción
  tocada esta sesión — solo planificación. PAE en `specs/sale-payment-method/`, sin
  commitear (decisión explícita del usuario: revisar/ajustar mañana antes de
  implementar). Detalle: `.claude/session-notes.md`.
- **2026-08-18 (sesión 8)** — **Fix bug de prod: sugerencias de venta vacías en Bazar Mariela
  (v0.4.5)**: diagnóstico inicial equivocado en dev (asumí problema de `is_default` mal
  configurado, corregido en la BD de dev) hasta notar que ningún request de la sesión del
  usuario llegaba al servidor dev — el usuario operaba en producción. En prod, el negocio bazar
  tenía dos ubicaciones "central": una con el stock real (`role='central'`, 5255 uds/154 lotes)
  y una huérfana con 0 stock (`role=NULL`, código `BUSINESS_CENTRAL`) creada por un bug de
  auto-creación de ubicaciones (`LocationRepository` usaba el prefijo `BUSINESS_` en vez del
  código real del negocio porque `InventoryService` nunca le pasaba `business_code`). Una segunda
  bug en `get_business_locations()` dejaba que esa huérfana "ganara" como central resuelta por
  coincidir con la convención de código `_CENTRAL`, así que el panel de venta arrancaba
  apuntando a la ubicación vacía. Fix de datos inmediato en prod (borrado de la huérfana, sin
  datos asociados) + fix de los 2 bugs de código + versión 0.4.5. 254 unit verdes.
- **2026-08-17 (sesión 7)** — **Auditoría de cobertura de tools + 4 tools nuevas + fix bug de
  prod (v0.4.2→0.4.4)**: fix inicial de `profit_by_product` (ignoraba `limit`, ordenaba por
  ventas no ganancia) → auditoría de cobertura de las 29 tools del copiloto contra el modelo →
  SDD-Agent genera PAE para P0/P1 (`specs/agent-tools-p0-p1/`) → OpenCode-style: 4 tools nuevas
  (`dead_stock`, `inventory_turnover`, `profit_by_category`, `profit_by_location`) + filtro
  `location` en 3 tools existentes → `/code-review` (5 hallazgos) + `auditoria-ingenieria`
  (3.20→4.05/5.00) → deploy dev → **deploy prod → bug en vivo reportado por el usuario**
  (chat responde texto pero sin gráfica) → diagnosticado (`PlanModel.tool` vs `widgets`
  desincronizados) y corregido con retry+síntesis de widget → deploy dev → deploy prod (0.4.4).
  **Aprendizaje de proceso**: se saltó la validación en dev antes del primer deploy a prod
  ("ponemos en producción" sin confirmar el paso intermedio) y así llegó el bug — la segunda
  vuelta si esperó confirmación explícita en dev antes de mergear a `main`. 221 unit verdes
  (200 unit del arranque de sesión). Ambos deploys via `develop` → `main` (el trabajo había
  quedado sobre `main` local por error de rama, corregido con `git stash`+`checkout develop`
  antes de cada commit). Detalle: `.claude/session-notes.md`.
- **2026-08-17 (sesión 6)** — **Fixes prod + Coach admin-only + deploy develop**: (1) regresión Fase C en compras (SKU auto-generado fallaba registrando producto nuevo sin SKU manual; desempaquetado mal en `_generate_auto_sku()` tras f6e25dd), (2) scope `list_sku_prefixes()` por business_id evita numeración SKU compartida, (3) Coach restricción RBAC temporal a admin (validación previa), (4) investigación fuga ubicaciones en Cerveza = sin bug (datos limpios en BD, probable caché cliente). 205 unit + 20 e2e verdes. Commit `27f9da2` pusheado a `gitea/develop` (dispara CI/CD automático). Detalle: `.claude/session-notes.md`.
- **2026-08-15 (sesión 4)** — **Fases B/C/D del refactor de auditoría** (SDD → OpenCode): B (SoC/capas, services extraídos) 3.85, C (RBAC API + escalabilidad + repos + e2e en CI) 4.00, D (contract tests + backoff LLM + rate limiter IP+user) 4.10. **Arreglo pipeline CI/CD (4 causas raíz)**: el "CI verde" documentado nunca funcionó en el runner host (los deploys eran manuales) — `apt-get` sin sudo, `--no-cov` sin `pytest-cov`, test en `runs-on: ubuntu-latest` (inexistente), 4 e2e flaky eliminados. 149 unit + 16 e2e verdes. Deploy dev + prod `success`. Detalle en `.claude/session-notes.md`
- **2026-08-15 (sesión 3)** — **Negocio sin POS** (Cervezas Mariela, deploy prod: `CERVEZA_MARIELA_POS1` eliminada) + **auditoría quick wins** (XSS H-029/034, credenciales H-036 fail-fast, índices H-053) + **code-review (8 ángulos)** que encontró y corrigió 11 hallazgos propios (dropdown Documentos vacío, traspasos 422 sin-POS, "None" en edit form, Location vacía, default POS1, seed log-only, `.conf` legacy, etc.) + **auditoría 2.95** (`docs/auditorias/2026-08-15-negocio-sin-pos.md`). Detalle en `.claude/session-notes.md`
- **2026-08-15 (sesión 2)** — Traspaso masivo (modal + 6 fixes + campo fecha) + code-review + e2e Playwright (institucionalizado en DoD) + copiloto (3 bugs + Tier 1/2 = 27 tools) + despliegue CD con Gitea Actions (runners self-hosted + systemd + test-gate; cron watcher y Dokploy eliminados). Detalle en `.claude/session-notes.md`
- **2026-08-15** — Fix 3 bugs de prod (tema/burbuja por cache `?v=40` congelado, LLM 400 por `strict:true`, `NameError` en restock-table) + code-review + auditoría 2.70 + tests de regresión. Detalle en `.claude/session-notes.md`
- **2026-08-14/15** — Refactor config de negocio `.conf` → BD (fuente única de verdad): `Business` 22 columnas + `Location` `role`/`is_default`/`position`, `business_repository.py`, 47 callers migrados, `.conf` legacy (backfill schema v4), deploy a producción. ⚠️ **Bugs en prod detectados 2026-08-15 (pendientes de fix)**: (1) `NameError: name 'db' is not defined` en `_location_name_for_code` → `/ui/restock-table` 500; (2) LLM API 400 → copiloto "no disponible" en prod; (3) tema claro/oscuro + burbuja copiloto no responden en prod (en investigación; código idéntico dev/prod). Detalle en `.claude/session-notes.md`
- **2026-08-14** — Copiloto IA implementado (LangGraph + 9 tools + widget persistente), code-review (10 findings corregidos), auditoría 3.45, quick wins (security headers, formato moneda, tests, settings), modelo local dev / API prod. Detalle en `.claude/session-notes.md`
- **2026-08-13** — Features config de negocio + multi-negocio owners + code-review + auditoría + fix borrado usuario + version bump 0.4.0 + deploy dev y producción. Detalle en `.claude/session-notes.md`
- **2026-08-11** — DBeaver + PostgreSQL client + restauración BD + migración SQLite→PG + deploy producción + fix CI/CD Gitea + acceso en homepage. Detalle en `.claude/session-notes.md`
- **2026-08-04/05** — Feature Negocios + auditoría (22 hallazgos) + deploy Gitea. Detalle en `.claude/session-notes.md`

## Reglas para el agente

1. **No inventar APIs** — leer `app/routers/` y `app/schemas.py` antes de asumir endpoints o schemas
2. **Testing** — siempre correr `pytest -v` después de cambios en lógica de negocio. **Toda feature nueva lleva tests e2e** (`tests/e2e/`, Playwright) que validen el flujo completo; no se da por terminada sin ellos (ver DoD)
3. **Documentación** — actualizar este `CLAUDE.md` si cambia la estructura o el estado
4. **Commits** — mensajes en inglés (English), formato conventional commits
5. **Base de datos** — nunca borrar `inventario.db` sin backup
6. **Routers** — hay 18 routers, revisar `app/main.py` para ver cuáles están registrados
7. **Push** — siempre a `gitea` (192.168.1.21). Gitea tiene mirror automático a GitHub, así que NO pushear a GitHub directamente.
8. **Flujo de cambios (permanente)** — los cambios van **siempre a `develop`**, se prueban en el entorno de desarrollo (`dev.gestion.legatumbp.com`, LAN interno), y solo después se mergean a `main` y se despliegan en producción (`gestion.legatumbp.com`). Nunca directo a `main`.
9. **Modelo de ejecución** — Claude Code (DeepSeek API `deepseek-v4-pro[1m]`) planifica y genera el SDD/PAE; OpenCode + modelo local **Qwen3.6-35B-A3B** (`qwen36_35b_coder_mtp` vía llama-swap) implementa los task envelopes. El modelo local NO es GLM (GLM-4.7-Flash quedó como legado/fallback desde 2026-07-25).
10. **Copiloto IA en runtime** — el copiloto usa **modelo local en dev** (default `AGENT_LLM_BASE_URL=http://localhost:8080/v1`, `AGENT_LLM_MODEL=qwen3_8b_instruct`) y **API LLM externa en producción** (env `AGENT_LLM_*` en `docker-compose-homelab.yml`). Las env vars viven en `app/settings.py`.

- [x] **Copiloto: diagnóstico + memoria de tool previa + eval de precisión (v0.4.9)** —
      usuario reportó que el copiloto "a veces da resultados que no pide". Auditoría
      de `app/agent/` (`docs/auditorias/2026-09-21-agent-copiloto.md`, 2.85/5.00 🔴,
      8 hallazgos) → PAE vía SDD-Agent (`specs/agent-copiloto-diagnostico/`) para 3
      de ellos: **H-001** logging estructurado en `_plan_node` (mensaje, tool,
      tool_args, reintentos — antes 1 sola línea de log en todo el módulo);
      **H-005** memoria de la tool/args del turno anterior de la MISMA conversación,
      vía nuevo `app/agent/widgets.py::latest_widget_meta(db, business_id, since=...)`
      pasado a `build_system_prompt(last_tool=...)`; **H-004** eval set de 24 casos
      dorados (`tests/eval/test_agent_tool_selection_eval.py`, marker `agent_eval`,
      `pytest tests/eval -m agent_eval -v -s`) corrido contra el LLM real, resultado
      96% tool / 96% type / 88% args accuracy — baseline para decidir sobre H-003
      (routing en dos pasos / few-shot para el prompt heurístico de 33 tools, aún
      pendiente). **Implementado directamente por Claude Code**, no vía OpenCode
      (pedido explícito del usuario, excepción puntual al flujo estándar). Dos
      rondas de `/code-review` + `auditoria-ingenieria`: la primera encontró que el
      guard de `last_tool` solo protegía el primer turno tras "nueva conversación"
      (turnos posteriores sin widget propio seguían filtrando el widget más
      reciente del negocio, de cualquier conversación) — corregido comparando
      fechas **en Python, no en SQL**, tras descubrir un bug real de
      SQLite/SQLAlchemy (`CURRENT_TIMESTAMP` se guarda sin microsegundos pero un
      `datetime` bindeado como parámetro siempre lleva `.000000`, así que
      `WHERE created_at >= :since` fallaba incluso para instantes iguales — no
      aplica a PostgreSQL/prod). Segunda ronda: 0 hallazgos. Auditoría final
      **4.35/5.00 🟢**. 295 unit + 22 e2e verdes. 2 commits en `develop`
      (`eb8509a`, `1fc4d7c`, bump 0.4.8→0.4.9), **sin push**.

## Épico offline-desktop-sync (v0.4.8) — estado real por slice

**Score integrado (2026-08-29, re-auditoría tras corregir P4 por completo):
🟢 4.05/5.00** (antes 2.15/5.00 🔴 el mismo día — ver
`docs/auditorias/2026-08-29-offline-desktop-sync-epic-full-audit{,-reaudit}.md`).

**⚠️ CAMBIO DE REQUERIMIENTO (2026-09-07):** usuario solicitó que **gastos operativos** también funcionen en offline (actualmente están marcados como "online-only" en los specs). Pendiente de especificación formal vía SDD-Agent antes de implementar.

| PAE | slice | Estado | Comentario |
|-----|-------|--------|------------|
| **P1** | Fundamentos | 🟡 3.80/5.00 | Outbox `pending_sync_events` + rangos SKU 10k. Commiteado (`2dbad8a`) |
| **P2** | Backend de sync | 🟡 3.70/5.00 | `SyncService`, endpoints `/sync/*`. Commiteado (`e4e03a2`) |
| **P3** | Cliente de sync | 🟡 funcional | `sync()`/`apply_pull` con 3 bugs reales adicionales encontrados por e2e y corregidos (2026-08-29). Commiteado (`41adf1d`, `13f7af1`, `88edf08`) |
| **P4** | Empaquetado desktop | 🟢 dentro del score integrado | Auditado por primera vez el 2026-08-29 (nunca antes): 5 bugs de crash garantizado (derivación de clave SQLCipher, launcher, `desktop.spec`, `installer.iss`) + 7 hallazgos de una segunda ronda de code-review — **todos corregidos y verificados por ejecución directa** (incluida una conexión SQLite real de punta a punta en modo desktop). Commiteado (`cb83e2e`) |
| **P5** | e2e multi-nodo | 🟢 dentro del score integrado | Auditado en 1.40/5.00 🔴 (el test nunca había corrido), reescrito por completo, verde. Commiteado (`cb83e2e`) |

**Épico completo desplegado en producción el 2026-10-09** (v0.4.9, esquema v7) —
ver sesión 16. **Versión actual del repo:** `0.4.10` (ver `VERSION`) — todo incremento
sigue siendo PATCH salvo pedido explícito del usuario (regla dura del `CLAUDE.md` global).

**Build Windows:** el instalador se construye manualmente en Windows usando
`pyinstaller --spec-file packaging/desktop.spec` + Inno Setup (`packaging/README-desktop.md`).
`desktop.spec`/`installer.iss` se reescribieron contra la sintaxis real de ambas herramientas
(la versión anterior usaba API inventada, no hubiera compilado), pero **ninguna de las dos
build reales se pudo ejecutar en este entorno** (Linux, sin PyInstaller ni Inno Setup
instalados) — sigue siendo, por diseño del propio épico, una validación manual en Windows
antes de confiar en que el instalador funciona de punta a punta.

**Script de verificación:** `specs/offline-desktop-sync/verify.sh` corre unit (286 tests) +
e2e de sync (`test_sync_e2e.py` + `test_sync_multinode_e2e.py`, 2 tests) con `set -e` como
único gate real. **Corre de punta a punta con exit 0 por primera vez en su historia**
(2026-08-29) — antes fallaba siempre en el paso 1 (test roto de P4) o nunca llegaba a ejecutar
el e2e (afirmaba validarlo sin hacerlo).

**Documentación viva:** esta sección se corrigió dos veces el 2026-08-29: primero para
reflejar que "P5 🟢 Verde" era falso (el test nunca había corrido), después para reflejar que
P4 — nunca antes auditado — estaba igual de roto y ya se corrigió. Lección para sesiones
futuras: no declarar un slice "verde" en este archivo sin haber corrido el comando que lo
prueba, y auditar TODO el alcance de un épico, no solo las partes que ya tuvieron atención.

**Pendiente antes de considerar el épico 100% cerrado:**
1. Validación manual real en Windows de `desktop.spec` (PyInstaller) e `installer.iss`
   (Inno Setup) — inherentemente fuera del alcance de este entorno Linux.
2. Verificar la derivación de clave y `PRAGMA key` contra SQLCipher real
   (`sqlcipher3-binary`), no solo contra stock `sqlite3` (que ignora `PRAGMA key`
   silenciosamente y no puede detectar un fallo real de cifrado).
3. Decidir si P1-P3 (🟡, sin llegar a 🟢 individualmente) necesitan otra pasada de auditoría
   antes de mergear `develop`→`main`, o si el score integrado del épico (4.05 🟢) es
   suficiente para el usuario.
4. Commitear P4 y P5 (working tree, sin commitear todavía).

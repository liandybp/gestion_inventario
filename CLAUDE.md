# CLAUDE.md — Gestión de Inventario

## Resumen del proyecto

App web FastAPI + HTMX para gestión de inventario multi-negocio con soporte multi-ubicación (CENTRAL + POS), control FIFO de lotes, documentos de venta (facturas/presupuestos), clientes, compras, gastos, transferencias y reportes.

- **Versión actual:** `0.4.6` (ver `VERSION`)
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
| Testing | pytest (222 unit + 20 e2e Playwright) |

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
- [ ] **PAE generado: método de pago (transferencia) en venta rápida** — checkbox "Pagado
      por transferencia" en `InventoryMovement` (endpoints `/ui/sale`, `/ui/sale/barcode`,
      `/ui/movement/sale/{id}/update`), desmarcado por defecto, editable, visible y
      filtrable en el historial de ventas (tab Ventas). **NO** aplica a `SalesDocument`
      (facturas/presupuestos) — confirmado con el usuario. Generado vía SDD-Agent
      (`specs/sale-payment-method/`, 14 task envelopes: modelo + migración SQLite/PG +
      schemas + repo + service + 2 routers + 3 templates + tests unit/e2e + version bump).
      Decisión de diseño clave: `InventoryService.update_sale()` recibe
      `paid_by_transfer: Optional[bool] = None` (no `bool` obligatorio) porque tiene un
      segundo caller (`ui_profit_items.py`, tab "Ganancia por artículo") que no pasa el
      campo — un default `False` fijo hubiera reseteado el valor en cada edición desde ese
      tab. **Sin implementar todavía** — PAE aprobado por el usuario, ejecución con
      OpenCode pendiente para la próxima sesión.
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
- [ ] **Épico offline-desktop-sync (app de escritorio offline-first + sync bidireccional)**
      — 5 PAEs generados vía SDD-Agent en `specs/offline-desktop-sync/` (P1 Fundamentos, P2
      Backend de sync, P3 Cliente+escritura offline, P4 Desktop+cifrado Windows, P5 e2e
      multi-nodo). **P1 implementado por OpenCode pero NO mergeado** — auditoría (score
      2.90/5.00 🔴) encontró un boundary bug crítico en la numeración de SKU por rango
      (`next_sku_in_range`, colisión permanente al SKU #10.000 de un negocio) + 3 hallazgos
      altos (hash de integridad del outbox incompleto, cero logging, race condition
      latente). Ver `docs/auditorias/2026-08-28-p1-foundations-offline-sync.md`. Pendiente:
      aplicar los 4 quick wins y re-auditar antes de generar P2.

## Sesiones

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
  `docs/auditorias/2026-08-28-p1-foundations-offline-sync.md`. **Pendiente:** corregir
  H-001/H-002/H-003/H-006 (quick wins, ~1h) antes de avanzar a P2. Detalle:
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

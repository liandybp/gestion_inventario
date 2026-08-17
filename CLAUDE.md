# CLAUDE.md — Gestión de Inventario

## Resumen del proyecto

App web FastAPI + HTMX para gestión de inventario multi-negocio con soporte multi-ubicación (CENTRAL + POS), control FIFO de lotes, documentos de venta (facturas/presupuestos), clientes, compras, gastos, transferencias y reportes.

- **Versión actual:** `0.4.1` (ver `VERSION`)
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
| Testing | pytest (149 unit + 16 e2e Playwright) |

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
- Constraints unique compuestos por `(business_id, sku)` y `(business_id, lot_code)`

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

## Sesiones

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

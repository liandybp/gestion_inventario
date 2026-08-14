# CLAUDE.md — Gestión de Inventario

## Resumen del proyecto

App web FastAPI + HTMX para gestión de inventario multi-negocio con soporte multi-ubicación (CENTRAL + POS), control FIFO de lotes, documentos de venta (facturas/presupuestos), clientes, compras, gastos, transferencias y reportes.

- **Versión actual:** `0.4.0` (ver `VERSION`)
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
| Testing | pytest (80 tests, SQLite en memoria, cobertura 86%) |

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
│   │                           runner, widgets, schemas, prompts, llm
│   ├── repositories/        ← Data access layer
│   │   ├── inventory_repository.py
│   │   └── product_repository.py
│   ├── services/            ← Business logic
│   │   ├── inventory_service.py
│   │   └── product_service.py
│   ├── routers/             ← 18 routers (API + UI, incl. ui_agent)
│   ├── templates/           ← Jinja2 + HTMX partials
│   └── static/              ← CSS, JS, uploads
├── tests/
│   ├── conftest.py
│   └── unit/                ← 12 test files (~50 tests)
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

# Tests (SQLite en memoria)
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -v
```

## Entornos (dev vs producción)

| Entorno | URL | Acceso | Rama | Dónde corre | Servidor |
|---|---|---|---|---|---|
| **Desarrollo** | `http://dev.gestion.legatumbp.com` | **Interno (LAN)** — NO sale a internet | `develop` | host `ai-lab-home` (192.168.1.162) | uvicorn `:10000` |
| **Producción** | `https://gestion.legatumbp.com` | **Público** (Cloudflare Tunnel) | `main` | vm-apps (192.168.1.20) | Docker `gestion-inventario` `:10000` |

- **Dev es interno**: se resuelve vía **AdGuard DNS** (192.168.1.23) con rewrite `dev.gestion.legatumbp.com → 192.168.1.20` (vm-apps). El **Nginx de vm-apps** (`default.conf`) redirige `dev.gestion.legatumbp.com` → `192.168.1.162:10000` (host dev). NO usa Cloudflare Tunnel.
  - Ruta: `navegador → AdGuard → vm-apps:80 (Nginx) → host:10000 (uvicorn)`.
- **Deploy dev**: script `~/deploy-watcher-gestion.sh` (cron cada 2 min) hace `git fetch + reset --hard origin/develop` en `~/staging/gestion_inventario` y reinicia uvicorn `:10000`. Siempre asegura que el servidor esté vivo.
- **Deploy prod**: manual — `docker compose -f docker-compose-homelab.yml up -d --build` en vm-apps (rama `main`).
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
- [ ] Sistema agéntico con LLM local multi-tool (20+ tools, insights proactivos) — fase 2 planificada

## Sesiones

- **2026-08-14** — Copiloto IA implementado (LangGraph + 9 tools + widget persistente), code-review (10 findings corregidos), auditoría 3.45, quick wins (security headers, formato moneda, tests, settings), modelo local dev / API prod. Detalle en `.claude/session-notes.md`
- **2026-08-13** — Features config de negocio + multi-negocio owners + code-review + auditoría + fix borrado usuario + version bump 0.4.0 + deploy dev y producción. Detalle en `.claude/session-notes.md`
- **2026-08-11** — DBeaver + PostgreSQL client + restauración BD + migración SQLite→PG + deploy producción + fix CI/CD Gitea + acceso en homepage. Detalle en `.claude/session-notes.md`
- **2026-08-04/05** — Feature Negocios + auditoría (22 hallazgos) + deploy Gitea. Detalle en `.claude/session-notes.md`

## Reglas para el agente

1. **No inventar APIs** — leer `app/routers/` y `app/schemas.py` antes de asumir endpoints o schemas
2. **Testing** — siempre correr `pytest -v` después de cambios en lógica de negocio
3. **Documentación** — actualizar este `CLAUDE.md` si cambia la estructura o el estado
4. **Commits** — mensajes en inglés (English), formato conventional commits
5. **Base de datos** — nunca borrar `inventario.db` sin backup
6. **Routers** — hay 18 routers, revisar `app/main.py` para ver cuáles están registrados
7. **Push** — siempre a `gitea` (192.168.1.21). Gitea tiene mirror automático a GitHub, así que NO pushear a GitHub directamente.
8. **Flujo de cambios (permanente)** — los cambios van **siempre a `develop`**, se prueban en el entorno de desarrollo (`dev.gestion.legatumbp.com`, LAN interno), y solo después se mergean a `main` y se despliegan en producción (`gestion.legatumbp.com`). Nunca directo a `main`.
9. **Modelo de ejecución** — Claude Code (DeepSeek API `deepseek-v4-pro[1m]`) planifica y genera el SDD/PAE; OpenCode + modelo local **Qwen3.6-35B-A3B** (`qwen36_35b_coder_mtp` vía llama-swap) implementa los task envelopes. El modelo local NO es GLM (GLM-4.7-Flash quedó como legado/fallback desde 2026-07-25).
10. **Copiloto IA en runtime** — el copiloto usa **modelo local en dev** (default `AGENT_LLM_BASE_URL=http://localhost:8080/v1`, `AGENT_LLM_MODEL=qwen3_8b_instruct`) y **API LLM externa en producción** (env `AGENT_LLM_*` en `docker-compose-homelab.yml`). Las env vars viven en `app/settings.py`.

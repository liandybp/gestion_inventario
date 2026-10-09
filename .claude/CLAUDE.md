# gestion_inventario

> Contexto de proyecto para OpenCode + Qwen3.6-35B-A3B
> Generado desde: ~/obsidian-lab/Templates/template-claude-md.md
> Leer completo antes de tocar cualquier archivo.

---

## Stack

| Capa | Tecnología | Versión |
|------|-----------|---------|
| API | FastAPI | 0.115.6 |
| ORM | SQLAlchemy | 2.0.36 |
| DB driver | psycopg (binary) | 3.2.3 |
| Validación | Pydantic | 2.10.3 |
| Templates | Jinja2 | 3.1.4 |
| PDF | fpdf2 / pdfplumber | 2.7.9 / 0.11.0 |
| Sessions | itsdangerous | 2.2.0 |
| Server | uvicorn (standard) | 0.32.1 |
| Python | 3.12 | — |
| Dependencias | requirements.txt | — |

Branch activa: `develop`
Repo: `https://github.com/liandybp/gestion_inventario`

---

## Mapa de directorios

```
gestion_inventario/
├── app/
│   ├── main.py                  # Entrypoint FastAPI, registro routers, SessionMiddleware
│   ├── db.py                    # Engine SQLAlchemy, SessionLocal, get_session(), Base
│   ├── models.py                # Todos los modelos SQLAlchemy (13 modelos)
│   ├── schemas.py               # Pydantic schemas (request/response)
│   ├── auth.py                  # PBKDF2 password hashing, authenticate(), get_user_by_username()
│   ├── security.py              # Roles: admin/owner/operator, helpers is_admin() etc.
│   ├── deps.py                  # session_dep(), require_user_api, require_admin_api, require_active_business_id
│   ├── migrations.py            # Seed admin/operator users, run_migrations()
│   ├── utils.py                 # generate_session_secret()
│   ├── audit.py                 # AuditLog model y helpers
│   ├── business_config.py       # BusinessConfig Pydantic model, carga desde .conf/.json
│   ├── business_config.conf     # Config INI por defecto (Auto Sandero)
│   ├── invoice_parsers.py       # Parseo de facturas PDF
│   ├── sales_document_pdf.py    # Generación PDF de documentos de venta
│   ├── routers/                 # ~25 routers (3 API JSON + ~22 UI HTMX)
│   │   ├── health.py, products.py, inventory.py   # API JSON
│   │   ├── ui.py                # Mount point routers UI
│   │   ├── ui_auth.py, ui_home.py, ui_stock.py    # Dashboard + login
│   │   ├── ui_products.py, ui_purchases.py, ui_sales.py
│   │   ├── ui_transfers.py, ui_expenses.py, ui_extractions.py
│   │   ├── ui_customers.py, ui_users.py, ui_businesses.py
│   │   ├── ui_sales_documents.py, ui_dividends.py, ui_profit.py, ui_profit_items.py
│   │   ├── ui_activity.py, ui_history.py, ui_agent.py, ui_coach.py
│   │   └── ui_common.py, ui_tabs_common.py
│   ├── repositories/            # business, customer, inventory,
│   │   │                           location, product, purchase, user
│   ├── services/                # coach, coach_rules, finance, inventory, product,
│   │   │                           reporting, sales_document
│   ├── static/                  # CSS, JS, uploads
│   └── templates/               # Jinja2 templates (partials/, components/)
├── tests/                       # 149 unit (SQLite memoria) + 16 e2e (Playwright)
├── docker-compose.yml           # ⚠️ No editar sin confirmación explícita
├── docker-compose-homelab.yml
├── Dockerfile
├── Caddyfile
├── requirements.txt
├── requirements-dev.txt
├── VERSION                      # 0.4.0
├── CHANGELOG.md
└── .bumpversion.cfg
```

---

## Dependencias críticas — firmas reales

```python
# app/db.py — sesión de base de datos
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase

class Base(DeclarativeBase): ...

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+pysqlite:///./inventario.db")
# Engine: SQLite → check_same_thread=False; PostgreSQL → pool_size=10, max_overflow=20
# pool_pre_ping=True, autoflush=False, autocommit=False

def get_session() -> Session:
    return SessionLocal()
```

```python
# app/deps.py — dependencias FastAPI
from app.db import get_session

def session_dep() -> Generator[Session, None, None]:
    db = get_session()
    try:
        yield db
    finally:
        db.close()

# require_user_api → 401 si no hay sesión
# require_admin_api → además verifica role == "admin"
# require_active_business_id → business_id desde session (admin) o user.business_id (owner/operator)
```

```python
# app/auth.py — autenticación
from app.auth import authenticate, get_user_by_username, hash_password, verify_password

def authenticate(db: Session, username: str, password: str) -> User | None:
    """Devuelve User si credenciales válidas y activo, None si no."""
    ...

def hash_password(plain: str) -> str:
    """PBKDF2-HMAC-SHA256, 200k iteraciones. Formato: pbkdf2_sha256$iter$salt$digest"""
    ...
```

```python
# app/security.py — roles y permisos
from app.security import is_admin, is_owner, is_operator, can_manage_users, can_change_business, can_view_activity, can_access_full_dashboard

# Roles: "admin", "owner", "operator"
# Session dict: {"username": str, "role": str, "business_id": int}
```

```python
# app/models.py — modelos principales (13 modelos)
# Business, User, Customer, Location, Product, InventoryMovement,
# InventoryLot, MovementAllocation, SalesDocument, SalesDocumentItem,
# MoneyExtraction, OperatingExpense, AuditLog
# Todos heredan de Base (app.db.Base)
```

```python
# app/business_config.py — configuración multi-negocio
# BusinessConfig con: IssuerConfig, CurrencyConfig, SalesDocumentsConfig,
# DividendsConfig, LocationsConfig, InventoryConfig
# Carga desde INI .conf o .json, soporta overrides por business code
```

---

## Patrones del proyecto

### Cómo se definen endpoints (HTMX)

```python
# app/routers/ui_products.py
from fastapi import APIRouter, Depends, Request, Form
from app.deps import session_dep, require_active_business_id

router = APIRouter(prefix="/ui", tags=["ui-products"])

@router.post("/product")
async def create_product(
    request: Request,
    sku: str = Form(...),
    name: str = Form(...),
    # ... más Form fields
    db: Session = Depends(session_dep),
    business_id: int = Depends(require_active_business_id),
):
    # Lógica con repo/service
    # Retorna HTML partial para HTMX swap
```

### Cómo se definen endpoints API JSON

```python
# app/routers/products.py
from fastapi import APIRouter, Depends
from app.schemas import ProductCreate, ProductRead

router = APIRouter(tags=["products"])

@router.post("/products", response_model=ProductRead)
async def create(data: ProductCreate, db: Session = Depends(session_dep)):
    ...

@router.get("/products", response_model=list[ProductRead])
async def list_all(db: Session = Depends(session_dep)):
    ...
```

### Cómo se registran routers

```python
# app/main.py
from app.routers import products, inventory, health
from app.routers.ui import mount_ui_routers

app.include_router(products.router)
app.include_router(inventory.router)
app.include_router(health.router)
mount_ui_routers(app)
```

### Patrón de auth en UI

```python
# SessionMiddleware en main.py
# Cookie: inventario_session, max_age=24h, same_site="lax"
# Middleware ui_auth_middleware protege /ui/* excepto /ui/login y /ui/logout
# Inactivity timeout: 60 minutos (detecta X-User-Activity header para HTMX)
```

### Cómo se corren los tests

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: pytest -v
# 149 tests unitarios (SQLite en memoria) + 16 e2e (Playwright), CI/CD Gitea verde.
```

### Cómo se levanta el proyecto en local

```bash
# Con Docker:
docker compose up -d

# Sin Docker:
pip install -r requirements.txt
uvicorn app.main:app --reload --host 127.0.0.1 --port 10000
```

---

## Invariantes — nunca violar

1. **docker-compose.yml / docker-compose-homelab.yml**: no modificar sin confirmación explícita.
2. **.env**: no leer, no modificar, no imprimir su contenido.
3. **requirements.txt**: no añadir dependencias sin confirmación. Listarlas y pedir aprobación.
4. **app/auth.py**: no cambiar la lógica de hash_password (PBKDF2 200k iteraciones) ni verify_password sin confirmación.
5. **app/db.py**: todos los modelos heredan de `Base` de `app/db.py`. No crear bases alternativas.
6. **app/deps.py**: no modificar require_user_api, require_admin_api, require_active_business_id — son el control de acceso centralizado.
7. **Routers UI**: `ui_tabs.py` fue dividido en `ui_stock.py`/`ui_profit.py`/`ui_activity.py`/`ui_history.py`/`ui_dividends.py`/`ui_home.py`/`ui_tabs_common.py` (Fase B). El dashboard ya no es monolítico.
8. **Migraciones**: no modificar archivos existentes en migrations — solo crear nuevos.

---

## Qué NO existe todavía

- No hay sistema de roles/permisos granular por recurso (solo admin/owner/operator global)
- No hay caché Redis
- No hay sistema de emails/notificaciones
- No hay API REST documentada con OpenAPI tags organizados
- Fase 2 del agente (insights proactivos + LLM local multi-tool) — planificada (el copiloto ya tiene 27 tools Tier 1/2)

---

## Estado actual del proyecto

```
Tests:        304 unit (SQLite en memoria) + 24 e2e (Playwright), todos verdes
              + eval de precisión del copiloto (tests/eval/, marker agent_eval,
              corre a demanda contra el LLM real: pytest tests/eval -m agent_eval -v -s)
Endpoints:    API JSON: /health, /products CRUD, /movements (purchase/sale/transfer/adjustment),
              /sync/{register,push,conflicts} (offline-desktop-sync)
              UI HTMX: dashboard completo + copiloto IA (33 tools) + tab Coach + sync banner
              + checkbox "pagado por transferencia" en venta rápida (v0.4.10)
Config:       BD como fuente única de verdad (Business +22 columnas + Location role/pos),
              .conf legacy (backfill schema v4) — ver CLAUDE.md raíz
Migraciones:  inline en migrations.py (schema v8: sync_state + device_tokens + paid_by_transfer)
Versión:      0.4.10 (ver VERSION) — commiteado en main (ce3ccf6), DESPLEGADO Y VERIFICADO
              en dev (ai-lab-home) y prod (laptop-server, gestion.legatumbp.com) el 2026-10-09
Docker:       docker-compose.yml (postgres + app + caddy) + docker-compose-homelab.yml
CD prod:      `.gitea/workflows/deploy.yml::deploy-prod` reescrito 2026-10-09 para la
              estructura real de laptop-server (clona por git-HTTP en LAN, build local,
              recrea el contenedor en ~/apps, rollback automático si el health falla) —
              el job viejo apuntaba a la ruta de la VM vm-apps, eliminada en septiembre,
              y nunca se había vuelto a probar por falta de push a main hasta hoy.
Serena:       MCP habilitado 2026-08-29, memorias en .serena/memories/ (mem:core es el punto
              de entrada) — usar sus tools (find_symbol/get_symbols_overview/
              replace_symbol_body) en vez de Read/Grep/Edit nativos para código.
```

## Copiloto IA — observabilidad y memoria de conversación (2026-09-21)

`app/agent/graph.py::_plan_node` loguea (INFO/WARNING/ERROR) el plan elegido, los
reintentos por formato, los fallos duros de LLM y los widgets sintetizados en cada
turno de chat — antes de esta sesión solo había una línea de log en todo `app/agent/`.
El planner también recibe, cuando corresponde, la tool/args del último widget de
**esta misma conversación** (`app/agent/widgets.py::latest_widget_meta(db,
business_id, since=...)`, filtrado por fecha en Python — no en SQL, por un bug real
de comparación de `DateTime` en SQLite/SQLAlchemy, ver
`docs/auditorias/2026-09-21-agent-copiloto-diagnostico-reaudit.md`). Auditoría del
diff: 4.35/5.00 🟢. Pendiente: decisión sobre H-003 (reescribir el prompt heurístico
de 33 tools / routing en dos pasos) usando `tests/eval/test_agent_tool_selection_eval.py`
como vara de medir (baseline: 96% tool / 88% args accuracy).

## Épico offline-desktop-sync — resumen (detalle completo en CLAUDE.md raíz)

Los 5 slices (P1-P5) están commiteados en `develop` (`2dbad8a`, `e4e03a2`, `41adf1d`,
`13f7af1`, `88edf08`, `cb83e2e`). Auditoría integral del épico completo (2026-08-29):
**2.15/5.00 🔴 → 4.05/5.00 🟢** tras corregir P4 (nunca antes auditado, encontrado ~100%
roto: derivación de clave SQLCipher, launcher, `desktop.spec`/`installer.iss` todos con bugs
de crash garantizado) y reescribir P5 por completo (su e2e nunca había corrido con éxito).
Informes: `docs/auditorias/2026-08-29-offline-desktop-sync-epic-full-audit{,-reaudit}.md`.

**Pendiente, fuera de alcance de este entorno Linux:** validación manual en Windows de
`packaging/desktop.spec` (PyInstaller) e `installer.iss` (Inno Setup), y verificación contra
SQLCipher real (`sqlcipher3-binary`) — solo probado contra stock `sqlite3` hasta ahora.

**Épico desplegado en producción el 2026-10-09** (v0.4.9, esquema v4→v7) junto con la
sesión 15 del copiloto — ver sesión 16 en `CLAUDE.md` raíz y `.claude/session-notes.md`.

## `paid_by_transfer` en venta rápida (v0.4.10, 2026-10-09)

PAE de `specs/sale-payment-method/` (2026-08-18) ejecutado 7 semanas después —
checkbox en `InventoryMovement`, filtro en el historial, propagado al outbox de sync.
Bug real descubierto por el e2e nuevo: `_rebuild_product_fifo` podía rechazar una
edición de venta válida ("Insufficient stock") si una compra y la venta caían en el
mismo minuto — el round-trip del campo Fecha por el form de edición tenía solo
precisión de minuto. Corregido con milisegundos, **acotado a `ui_sales.py` +
`sale_panel.html` + `sale_edit_form.html`** — el mismo helper `dt_to_local_input`
(sin el fix) sigue en uso por compras/traspasos/gastos/extracciones/dividendos, con
el mismo riesgo latente, sin corregir a propósito (fuera de alcance de esa sesión).

**Otros pendientes acumulados de sesiones previas:** fix estructural de SKU único
compuesto `(business_id, sku)` pospuesto por decisión explícita del usuario; verde
definitivo del copiloto (≥4.2, timeout global del agente); `.conf` legacy sin
renombrar a `.bak`; probe `agent_llm` del healthcheck sin auth header (1 de 2 bugs de
healthcheck corregido esta sesión — `Dockerfile` ya copia `VERSION`).

---

## Reglas operacionales para OpenCode

- **Scope**: solo tocar archivos listados en el PAE activo (sección 3.2).
- **Archivos nuevos**: reportar nombre y ubicación antes de crear.
- **Archivos temporales**: eliminar `_tmp`, `_test`, `_draft`, `_vN` cuando el código definitivo funcione.
- **Type hints**: obligatorios en todas las funciones nuevas.
- **Imports**: no añadir dependencias externas no listadas en requirements.txt sin reportar.
- **Sin autoridad de diseño**: si algo del PAE activo es ambiguo, reportar — no asumir.
- **Verificación de scope**: antes de cerrar cada fase, ejecutar `git diff --name-only` y confirmar que solo aparecen los archivos esperados.
- **Invariantes**: releer la sección "Invariantes" antes de cada fase, no solo al inicio.

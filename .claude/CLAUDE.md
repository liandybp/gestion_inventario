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
Tests:        222 unit (SQLite en memoria) + 20 e2e (Playwright), CI/CD Gitea verde
Endpoints:    API JSON: /health, /products CRUD, /movements (purchase/sale/transfer/adjustment)
              UI HTMX: dashboard completo + copiloto IA (33 tools) + tab Coach (score + recomendaciones)
Config:       BD como fuente única de verdad (Business +22 columnas + Location role/pos),
              .conf legacy (backfill schema v4) — ver CLAUDE.md raíz
Migraciones:  inline en migrations.py (schema_version=4, backfill config desde .conf)
Versión:      0.4.6
Docker:       docker-compose.yml (postgres + app + caddy) + docker-compose-homelab.yml
Hecho (08-15): traspaso masivo, negocio sin POS, copiloto 27 tools, auditoría A/B/C/D (4.10)
Hecho (08-16): coach financiero-operativo (determinista + chat vía agente), auditoría (3.65) +
               code-review (6 hallazgos corregidos), 24 task envelopes, v0.4.1
Hecho (08-17): copiloto 27→33 tools (dead_stock, inventory_turnover, profit_by_category,
               profit_by_location + filtro location), auditoría 3.20→4.05 (7 hallazgos
               corregidos), v0.4.3. Fix bug de prod: PlanModel.tool sin widgets (chat sin
               gráfica) → retry+síntesis en _plan_node, v0.4.4.
Hecho (08-18): fix bug de prod: sugerencias de artículos vacías en venta (Bazar Mariela) —
               ubicación huérfana (`role=NULL`) generada por `LocationRepository` auto-creando
               con prefijo `BUSINESS_` (InventoryService nunca pasaba business_code real)
               "ganaba" como central por convención de código en `get_business_locations()`.
               Fix en business_repository.py + location_repository.py, dato huérfano borrado
               en prod, v0.4.5.
PAE generado (08-18, sesión 9): sale-payment-method — checkbox "pagado por transferencia"
               en venta rápida (InventoryMovement, no SalesDocument), desmarcado por defecto,
               editable, visible y filtrable en historial. specs/sale-payment-method/ (14
               task envelopes vía SDD-Agent), sin implementar todavía — sin commitear.
Hecho (08-19): fix bug de prod: compra de producto nuevo bloqueada (v0.4.6). Regresión del
               fix 27f9da2 (08-17): list_sku_prefixes() pasó a filtrar por business_id, pero
               products.sku es unique=True GLOBAL (no compuesto, pese a lo documentado antes
               en CLAUDE.md raíz). El candidato de SKU autogenerado de un negocio con pocos
               productos colisionaba con SKUs de otro negocio → 409 en 5/5 negocios menos el
               de mayor SKU global. Revertido a numeración global en
               purchase_repository.py::list_sku_prefixes. Regression test añadido. 222 unit
               verdes (conteo real verificado; "254 unit" de sesiones previas era incorrecto).
               Deploy dev+prod verificado en vivo. Misma sesión: Cloudflare Access puesto
               delante de gitea.legatumbp.com (sesión de hardening del lab en paralelo) rompió
               el runner de CD compartido (FetchTask + git checkout de todos los repos, no solo
               este) — arreglado con 3 excepciones de Bypass (api/actions/*, info/refs,
               git-upload-pack), pipeline push→test→deploy-dev verificado sin intervención
               manual. Deploy a main se hizo manual antes del fix de Access (mismos pasos que
               el CD), sin re-verificar el job deploy-prod automático después. Ver
               ~/obsidian-lab/00-Inicio/problemas-conocidos.md y
               ~/obsidian-lab/04-Harness/deploy-gitea-actions.md (sección 5).
Pendiente:    verificar que deploy-prod también corre solo tras el fix de Access, fix
              estructural (SKU único compuesto (business_id, sku) + migración + auditar
              call sites que asumen sku global en ui_common.py/ui_products.py — decisión
              explícita del usuario de posponerlo), implementar specs/sale-payment-method/
              (14 envelopes), verde definitivo ≥4.2, estacionalidad [B]/REQ-8 (no
              implementada), fase 2 agente, renombrar .conf→.bak, Dockerfile no copia VERSION
              (health siempre "unknown"), healthcheck agent_llm sin auth header (falso
              "degraded" en prod), auditar otros negocios por ubicaciones huérfanas similares
              (mismo patrón de bug), COMMITEAR P2 del épico offline-desktop-sync (ver abajo,
              código ya corregido y re-auditado 3.70/5.00, esperando confirmación del
              usuario) antes de generar el detalle de P3
Deuda téc.:   ui_tabs.py dividido en ui_stock/ui_profit/... (Fase B) — ya no es monolítico

Épico offline-desktop-sync (08-28): 5 PAEs generados vía SDD-Agent en
              specs/offline-desktop-sync/ (P1 Fundamentos, P2 Backend de sync central, P3
              Cliente+escritura offline, P4 Desktop Windows+cifrado SQLCipher/DPAPI, P5 e2e
              multi-nodo), 47 task envelopes.
              P1 (outbox pending_sync_events + rangos de SKU 10k/negocio): implementado por
              OpenCode → auditado en rojo (2.90/5.00, H-001 crítico: next_sku_in_range con
              boundary bug que causaba colisión de SKU PERMANENTE al SKU #10.000 de un
              negocio, mismo incidente que v0.4.6) → CORREGIDO y re-auditado a 3.80/5.00 🟡
              → COMMITEADO (2dbad8a, en develop). 241 unit verdes en ese commit. Informes:
              docs/auditorias/2026-08-28-p1-foundations-offline-sync{,-reaudit}.md.
              P2 (SyncService: idempotencia, replay vía InventoryService.sale, dedup de
              clientes, build_pull; endpoints /sync/register|push|conflicts): implementado
              por OpenCode → auditado en rojo (2.50/5.00, PEOR que P1 — 3 hallazgos
              CRÍTICOS de aislamiento multi-tenant: IDOR en register_device sin validar
              pertenencia del business_id, password_hash de usuarios viajando sin cifrar en
              el pull, movement_allocations/sales_document_items volcados SIN filtro de
              negocio — fuga de datos comerciales entre todos los negocios del sistema) →
              CORREGIDO DIRECTAMENTE POR CLAUDE (no vía OpenCode, a pedido del usuario) →
              segunda ronda de /code-review con mutation-testing real detectó que la propia
              corrección había introducido una REGRESIÓN (client_id de Customer scopeado por
              negocio pese a ser unique=True GLOBAL — mismo patrón que el ADR-012 de SKU,
              tercera vez que aparece este tipo de bug en el proyecto) → regresión corregida
              en el mismo ciclo con test de regresión añadido → re-auditado a 3.70/5.00 🟡.
              267 unit verdes. Informes:
              docs/auditorias/2026-08-28-p2-sync-backend-offline-sync{,-reaudit}.md.
              **P2 SIN COMMITEAR** — sesión cerrada antes de confirmar el commit (usuario
              dijo "cerramos" sin responder sí/no a la pregunta de commitear). Retomar la
              decisión de commit ANTES de tocar más código la próxima sesión.
```

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

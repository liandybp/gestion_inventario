# gestion_inventario

> Contexto de proyecto para OpenCode + GLM-4.7-Flash.
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
│   ├── routers/
│   │   ├── health.py            # GET /health
│   │   ├── products.py          # CRUD API JSON productos
│   │   ├── inventory.py         # API JSON movimientos (purchase/sale/transfer/adjustment)
│   │   ├── ui.py                # Mount point routers UI
│   │   ├── ui_auth.py           # Login/logout, cambio contraseña obligatorio
│   │   ├── ui_tabs.py           # Dashboard principal, 14 tabs, stock table (2392 líneas)
│   │   ├── ui_products.py       # CRUD productos HTMX
│   │   ├── ui_purchases.py      # Compras HTMX + labels
│   │   ├── ui_sales.py          # Ventas HTMX + barcode
│   │   ├── ui_transfers.py      # Transferencias HTMX
│   │   ├── ui_expenses.py       # Gastos operativos HTMX
│   │   ├── ui_extractions.py    # Extracciones de dinero HTMX
│   │   ├── ui_customers.py      # Clientes HTMX
│   │   ├── ui_users.py          # Gestión usuarios HTMX
│   │   ├── ui_sales_documents.py # Documentos de venta + carrito HTMX
│   │   └── ui_common.py         # Shared utils para routers UI
│   ├── repositories/
│   │   ├── inventory_repository.py
│   │   └── product_repository.py
│   ├── services/
│   │   ├── inventory_service.py
│   │   └── product_service.py
│   ├── static/                  # CSS, JS, uploads
│   └── templates/               # Jinja2 templates (partials/, components/)
├── tests/                       # ⚠️ VACÍO — no hay tests escritos
├── docker-compose.yml           # ⚠️ No editar sin confirmación explícita
├── docker-compose-homelab.yml
├── Dockerfile
├── Caddyfile
├── requirements.txt
├── requirements-dev.txt
├── VERSION                      # 0.3.4
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
# ⚠️ No hay tests escritos todavía. tests/ está vacío.
# pytest no está en requirements-dev.txt
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
7. **app/routers/ui_tabs.py**: archivo de 2392 líneas, tocar con cuidado — contiene el dashboard principal.
8. **Migraciones**: no modificar archivos existentes en migrations — solo crear nuevos.

---

## Qué NO existe todavía

- No hay tests — `tests/` está vacío, pytest no está en requirements-dev.txt
- No hay sistema de roles/permisos granular por recurso (solo admin/owner/operator global)
- No hay rate limiting
- No hay caché Redis
- No hay sistema de emails/notificaciones
- No hay API REST documentada con OpenAPI tags organizados
- No hay CI/CD pipeline

---

## Estado actual del proyecto

```
Tests:        0 tests (tests/ vacío)
Endpoints:    API JSON: /health, /products CRUD, /movements (purchase/sale/transfer/adjustment)
              UI HTMX: 14 tabs en dashboard, CRUD completo (products, purchases, sales,
              transfers, expenses, extractions, customers, users, sales documents)
Migraciones:  Seed manual via migrations.py (admin + operator users)
Versión:      0.3.4
Docker:       docker-compose.yml (postgres + app + caddy)
Pendiente:    Tests, API docs, rate limiting
Deuda téc.:   ui_tabs.py demasiado grande (2392 líneas), tests/ vacío
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

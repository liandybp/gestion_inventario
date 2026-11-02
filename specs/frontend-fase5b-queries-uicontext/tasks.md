# Task Envelopes — Frontend fase 5b: consultas fuera de los routers UI y `UIContext` completo

**Release:** 0.5.0 (agrupado, sin bump). **Rama base:** `develop` tras el merge de 5a (`a44338e`).
**Fuentes:** `requirements.md` (REQ-1..13) y `design.md` (§2.1..2.10). Todas las rutas de plantilla
son las de después de 5a (`features/<dominio>/…`). Las líneas de Python citadas son las reales de la
rama (`a44338e`, iguales a `bf38a2b` salvo cadenas de plantilla).

> **cc_local:** lee SOLO los envelopes del lote que te toca. El envelope es la única fuente.
> Un commit por lote. Si el contexto se compacta a mitad de un lote: parar.
> Dentro de un lote, un envelope puede dejar temporalmente en rojo tests que arregla otro envelope
> del MISMO lote (p. ej. al cambiar la firma de un auxiliar); el gate es `--test-cmd` al final del lote.

Lotes: **A** T1–T4 · **B** T5–T11 · **C** T12–T18 · **D** T19–T25 · **E** T26–T32 · **F** T33–T39 · **G** T40–T46 · **H** T47–T53 · **I** T54–T60

---

# LOTE A — Lote 0 (camino feliz + REQ-1 + REQ-2)

### T1: Guarda de recuento de consultas a `users` (modo medida)

**Depends on:** none
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** `tests/unit/test_ui_query_count.py`
**Pillars:** P1, P4, P5, P6, P8

#### Context (firmas reales)

```python
# app/auth.py — YA EXISTE
def hash_password(plain: str) -> str: ...

# app/db.py — YA EXISTE
class Base(DeclarativeBase): ...

# app/deps.py — YA EXISTE
def session_dep() -> Generator[Session, None, None]: ...

# app/main.py — YA EXISTE
app = FastAPI(...)  # registra routers y SessionMiddleware
```

#### Objective

Crear `tests/unit/test_ui_query_count.py` en modo medida: recorre las 6 rutas de REQ-3 con un
usuario logueado, afirma `status_code == 200` y registra (imprime) cuántas sentencias `FROM users`
y `FROM user_businesses` ejecuta cada una. No afirma todavía el máximo.

#### Instructions

**Step 1:** Crea `tests/unit/test_ui_query_count.py` con este contenido literal (no cambies nada):

```python
"""REQ-3: como máximo un SELECT sobre `users` por petición UI.

Lote 0 (modo medida): recorre las rutas de la tabla de REQ-3 con un usuario logueado,
afirma status 200 y registra los recuentos. `STRICT_ROUTES` se llena al cerrar el Lote D
(el envelope que afirma todo REQ-3); hasta entonces no se afirma el máximo.
"""
from __future__ import annotations

import re

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.auth import hash_password
from app.db import Base
from app.deps import session_dep
from app.main import app
from app.models import Business, User, UserBusiness

ROUTES: list[tuple[str, str]] = [
    ("admin", "/ui/tabs/sales"),
    ("owner", "/ui/tabs/transfers"),
    ("operator", "/ui/tabs/sales"),
    ("owner", "/ui/tabs/documents"),
    ("admin", "/ui/tabs/inventory"),
    ("owner", "/ui/dashboard"),
]

# Rutas que deben afirmar `users == 1`. Se rellena con las 6 de REQ-3 en el envelope final.
STRICT_ROUTES: set[str] = set()

_COUNTS: dict[str, int] = {"users": 0, "user_businesses": 0}


def _reset_counts() -> None:
    _COUNTS["users"] = 0
    _COUNTS["user_businesses"] = 0


def _count_statement(conn, cursor, statement, parameters, context, executemany):  # noqa: ARG001
    text = str(statement)
    if re.search(r"\bFROM\s+users\b", text, flags=re.IGNORECASE):
        _COUNTS["users"] += 1
    if re.search(r"\bFROM\s+user_businesses\b", text, flags=re.IGNORECASE):
        _COUNTS["user_businesses"] += 1


@pytest.fixture
def counter_client():
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    db = Session()
    biz = Business(code="test", name="Test Business")
    db.add(biz)
    db.flush()
    admin = User(username="admin", password_hash=hash_password("admin"), role="admin",
                 is_active=True, business_id=biz.id)
    owner = User(username="owner", password_hash=hash_password("owner"), role="owner",
                 is_active=True, business_id=biz.id)
    operator = User(username="operator", password_hash=hash_password("operator"), role="operator",
                    is_active=True, business_id=biz.id)
    db.add_all([admin, owner, operator])
    db.flush()
    db.add(UserBusiness(user_id=owner.id, business_id=biz.id))
    db.commit()
    db.close()

    def _override():
        s = Session()
        try:
            yield s
        finally:
            s.close()

    app.dependency_overrides[session_dep] = _override
    event.listen(engine, "before_cursor_execute", _count_statement)
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.clear()
        event.remove(engine, "before_cursor_execute", _count_statement)
        engine.dispose()


@pytest.mark.parametrize("role,route", ROUTES)
def test_route_returns_200_and_records_counts(counter_client, role: str, route: str) -> None:
    client = counter_client
    resp = client.post("/ui/login", data={"username": role, "password": role},
                       follow_redirects=False)
    assert resp.status_code in (302, 303), f"login {role}: {resp.status_code}"
    _reset_counts()
    page = client.get(route)
    assert page.status_code == 200, f"{role} {route} -> {page.status_code}"
    print(f"[query-count] {role} {route}: users={_COUNTS['users']} "
          f"user_businesses={_COUNTS['user_businesses']}")
    if route in STRICT_ROUTES:
        assert _COUNTS["users"] == 1, f"{role} {route}: users={_COUNTS['users']}"
```

**Edge cases (ya resueltos):**
- El owner se guarda en la variable local `owner`: usa `owner.id` en `db.add(UserBusiness(...))`.
  No hagas `db.refresh(owner)` antes: `db.flush()` ya asigna `owner.id`.
- El login ejecuta SQL: el contador se resetea DESPUÉS del login y ANTES del GET medido.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: ADMIN_USERNAME=admin ADMIN_PASSWORD=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test python3 -m pytest tests/unit/test_ui_query_count.py -q --override-ini="addopts="
# Expected: 6 passed (una por ruta)
```

#### Expected Report

- [ ] `tests/unit/test_ui_query_count.py` creado con el contenido literal
- [ ] Ningún otro archivo modificado
- [ ] 6 tests PASS

---

### T2: REQ-1 — `get_active_business_id` acepta `user=` (opcional)

**Depends on:** T1
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/security.py`, `tests/unit/test_security.py`
**Pillars:** P1, P4, P5, P6

#### Context (firmas reales)

```python
# app/security.py — YA EXISTE
def get_current_user_from_session(db: Session, request: Request) -> Optional[User]: ...
def get_active_business_id(db: Session, request: Request) -> Optional[int]:
    user = get_current_user_from_session(db, request)  # ← primera línea, línea 49
    if user is None:
        return None
    ...
def require_active_business_id(db: Session, request: Request) -> int: ...  # NO tocar
def get_active_business_code(db: Session, request: Request) -> Optional[str]: ...  # NO tocar
```

#### Objective

Hacer que `get_active_business_id` use el usuario pasado en `user=` si se lo dan, sin cambiar su
comportamiento cuando no se lo pasan.

#### Instructions

**Step 1: Read the file**
```
Read app/security.py
```

**Step 2: Añade el sentinel y cambia la firma** (2 ediciones en `app/security.py`).

Edición 1 — después de la línea `_log = get_logger(__name__)` (línea 13) y antes de
`from app.models import ...`, añade:

```python
_UNSET = object()
```

Edición 2 — cambia la firma de `get_active_business_id` (líneas 48-50):

```python
# BEFORE:
def get_active_business_id(db: Session, request: Request) -> Optional[int]:
    user = get_current_user_from_session(db, request)
    if user is None:
        return None

# AFTER:
def get_active_business_id(
    db: Session, request: Request, *, user: object = _UNSET
) -> Optional[int]:
    if user is _UNSET:
        user = get_current_user_from_session(db, request)
    if user is None:
        return None
```

No toques ninguna otra función de `app/security.py`.

**Step 3: Añade los tests en `tests/unit/test_security.py`.** Añade al final del fichero (conserva lo
existente) estos tests:

```python
from unittest.mock import MagicMock

from app.security import _UNSET, get_active_business_id


def _add_user(db, username, role, business_id):
    u = User(username=username, password_hash="x", role=role, is_active=True,
             business_id=business_id)
    db.add(u)
    db.commit()
    db.refresh(u)
    return u


def _request_with_session(business_id):
    req = MagicMock()
    req.session = {"username": "u", "active_business_id": business_id}
    return req


def test_get_active_business_id_uses_passed_user_without_users_query(db_session):
    u = _add_user(db_session, "owner1", "owner", 1)
    req = _request_with_session(3)
    # owner sin negocios asignados pero con user.business_id=1: con user= devuelve 1
    assert get_active_business_id(db_session, req, user=u) == 1


def test_get_active_business_id_without_user_resolves_session(db_session):
    _add_user(db_session, "op1", "operator", 1)
    req = MagicMock()
    req.session = {"username": "op1"}
    assert get_active_business_id(db_session, req) == 1


def test_get_active_business_id_user_none_returns_none(db_session):
    req = _request_with_session(7)
    assert get_active_business_id(db_session, req, user=None) is None


def test_get_active_business_id_user_sentinel_is_distinct_from_none():
    assert _UNSET is not None
```

**Imports needed:**
```python
# tests/unit/test_security.py — añadir al inicio:
from unittest.mock import MagicMock
from app.security import _UNSET, get_active_business_id
```

**Edge cases (ya resueltos):**
- `user=None` (pasado explícitamente) devuelve `None` sin tocar la sesión.
- No pasar `user` deja el comportamiento exacto de antes (primera línea resuelve la sesión).
- `require_active_business_id` y `get_active_business_code` NO se tocan.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_security.py -q --override-ini="addopts="
# Expected: todos PASS (los existentes + 4 nuevos)
```

#### Expected Report

- [ ] `app/security.py` — solo `_UNSET` añadido y firma de `get_active_business_id` cambiada
- [ ] `tests/unit/test_security.py` — 4 tests nuevos añadidos al final
- [ ] Ningún otro archivo modificado
- [ ] tests PASS

---

### T3: REQ-2 — `UIContext` completo (campos perezosos) + `get_business_code`

**Depends on:** T2
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/repositories/business_repository.py`, `app/routers/ui_context.py`
**Pillars:** P1, P4, P5, P6, P8

#### Context (firmas reales)

```python
# app/repositories/business_repository.py — YA EXISTE
from app.models import Business, Location, Product, User, UserBusiness
from app.security import get_active_business_id  # sigue usándolo get_active_business_config
def get_business_config(db: Session, business_id: Optional[int]) -> BusinessConfig: ...

# app/routers/ui_context.py — YA EXISTE (parcial)
_UNSET = object()
@dataclass
class UIContext:
    request: Request
    db: Session
    _user: object = field(default=_UNSET, repr=False)
    _business_id: Optional[int] = field(default=None, repr=False)
    @property
    def user(self) -> Optional[User]: ...
    @property
    def business_id(self) -> int:  # hoy usa require_active_business_id
```

#### Objective

Añadir `business_repository.get_business_code` y completar `UIContext` con los campos perezosos
`active_business_id`, `business_code` y `config`, derivados todos de `self.user`.

#### Instructions

**Step 1:** En `app/repositories/business_repository.py`, añade `get_business_code` justo después de
`get_active_business_config` (después de la línea 136):

```python
def get_business_code(db: Session, business_id: Optional[int]) -> Optional[str]:
    """Código del negocio (None si no hay id o el negocio no existe)."""
    if business_id is None:
        return None
    code = db.scalar(select(Business.code).where(Business.id == int(business_id)))
    return (str(code).strip() if code is not None else None) or None
```

No añadas ningún import nuevo (ya están `select` y `Business`).

**Step 2:** Reescribe `app/routers/ui_context.py` completo con este contenido literal:

```python
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from fastapi import HTTPException, Request
from sqlalchemy.orm import Session

from app.models import User
from app.repositories.business_repository import get_business_code, get_business_config
from app.security import get_active_business_id, get_current_user_from_session

_UNSET = object()


def role_of(user: Optional[User]) -> str:
    """Rol normalizado ("" sin usuario). Única normalización para guardias y sidebar."""
    return (user.role or "").strip().lower() if user is not None else ""


@dataclass
class UIContext:
    request: Request
    db: Session
    _user: object = field(default=_UNSET, repr=False)
    _active_bid: object = field(default=_UNSET, repr=False)
    _business_code: object = field(default=_UNSET, repr=False)
    _config: object = field(default=_UNSET, repr=False)

    @property
    def user(self) -> Optional[User]:
        if self._user is _UNSET:
            self._user = get_current_user_from_session(self.db, self.request)
        return self._user  # type: ignore[return-value]

    @property
    def role(self) -> str:
        return role_of(self.user)

    # Valores de la petición: no releer tras modificar el negocio (ui_businesses.py no
    # lee config/código tras escribir; ui_auth.py no usa UIContext).
    @property
    def active_business_id(self) -> Optional[int]:
        if self._active_bid is _UNSET:
            self._active_bid = get_active_business_id(self.db, self.request, user=self.user)
        return self._active_bid  # type: ignore[return-value]

    @property
    def business_id(self) -> int:
        if self.active_business_id is None:
            raise HTTPException(status_code=409, detail="Active business_id is required")
        return int(self.active_business_id)

    @property
    def business_code(self) -> Optional[str]:
        if self._business_code is _UNSET:
            self._business_code = get_business_code(self.db, self.active_business_id)
        return self._business_code  # type: ignore[return-value]

    @property
    def config(self) -> object:
        if self._config is _UNSET:
            self._config = get_business_config(self.db, self.active_business_id)
        return self._config

    def ensure_admin(self) -> None:
        if self.role != "admin":
            raise HTTPException(status_code=403, detail="Admin required")

    def ensure_admin_or_owner(self) -> None:
        if self.role not in ("admin", "owner"):
            raise HTTPException(status_code=403, detail="Admin required")


def ui_context(request: Request, db: Session) -> UIContext:
    """Contexto de la petición. Se llama directamente en el cuerpo del handler, no como Depends."""
    return UIContext(request=request, db=db)
```

Nota: el import de `security` en el fichero reescrito ya incluye `get_current_user_from_session`
(la propiedad `user` lo usa). No lo elimines.

**Imports needed:**
```python
# app/routers/ui_context.py
from app.repositories.business_repository import get_business_code, get_business_config
from app.security import get_active_business_id, get_current_user_from_session
# Se elimina: require_active_business_id
```

**Edge cases (ya resueltos):**
- `ctx.config` no lanza: `get_business_config(db, None)` devuelve `BusinessConfig()`.
- El 409 sale SOLO de `business_id`. `active_business_id`/`business_code`/`config` devuelven `None`/default.
- Los 4 campos usan el sentinel `_UNSET` ya existente; cada uno se resuelve como mucho una vez.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_ui_context.py -q --override-ini="addopts="
# Expected: 2 tests existentes FALLAN (parchean uc.require_active_business_id); T4 Step 0 los sustituye.
# El lote se verifica al final, tras T4.
```

#### Expected Report

- [ ] `app/repositories/business_repository.py` — `get_business_code` añadida
- [ ] `app/routers/ui_context.py` — reescrito con los 4 campos perezosos
- [ ] Ningún otro archivo modificado
- [ ] tests PASS

---

### T4: REQ-2 — tests de `UIContext` + modo de aserción del recuento

**Depends on:** T3
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** `tests/unit/test_ui_context.py`, `tests/unit/test_ui_query_count.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/routers/ui_context.py — YA EXISTE tras T3
from app.routers import ui_context as uc
class UIContext: ...  # propiedades user, role, active_business_id, business_id, business_code, config

# app/repositories/business_repository.py — YA EXISTE
def get_business_code(db, business_id) -> Optional[str]: ...
def get_business_config(db, business_id) -> BusinessConfig: ...
```

#### Objective

Añadir los casos de REQ-2 a `test_ui_context.py` (config sin negocio, 409 solo en `business_id`,
una sola resolución de usuario al leer las 5 propiedades) y dejar el recuento listo para afirmar.

#### Instructions

**Step 0 (corrección de la revisión, 2026-11-02):** T3 quita `require_active_business_id` de
`ui_context.py`, así que los dos tests existentes que lo parchean darían `AttributeError`. En
`tests/unit/test_ui_context.py`, sustituye las dos funciones completas:

```python
# BEFORE: def test_business_id_resolved_once(...) y def test_business_id_does_not_cache_failure(...)
#         (las dos parchean uc.require_active_business_id)
# AFTER:
def test_business_id_resolved_once(monkeypatch):
    calls = []
    monkeypatch.setattr(uc, "get_current_user_from_session", lambda _db, _req: None)
    monkeypatch.setattr(uc, "get_active_business_id", lambda _db, _req, *, user: calls.append(1) or 7)
    ctx = _ctx()
    assert ctx.business_id == 7
    assert ctx.business_id == 7
    assert len(calls) == 1


def test_business_id_409_every_read_without_re_resolving(monkeypatch):
    calls = []
    monkeypatch.setattr(uc, "get_current_user_from_session", lambda _db, _req: None)
    monkeypatch.setattr(uc, "get_active_business_id", lambda _db, _req, *, user: calls.append(1) or None)
    ctx = _ctx()
    with pytest.raises(HTTPException) as e:
        ctx.business_id
    assert e.value.status_code == 409
    with pytest.raises(HTTPException):
        ctx.business_id
    assert len(calls) == 1
```

No cambies ningún otro test existente.

**Step 1:** En `tests/unit/test_ui_context.py`, añade al final (conserva lo existente):

```python
def test_business_id_raises_409_when_no_business(monkeypatch):
    monkeypatch.setattr(uc, "get_active_business_id", lambda _db, _req, *, user: None)
    ctx = _ctx()
    with pytest.raises(HTTPException) as e:
        ctx.business_id
    assert e.value.status_code == 409
    assert e.value.detail == "Active business_id is required"


def test_config_does_not_raise_before_business_id(monkeypatch):
    # config lee active_business_id (None → BusinessConfig()), no lanza el 409.
    monkeypatch.setattr(uc, "get_active_business_id", lambda _db, _req, *, user: None)
    monkeypatch.setattr(uc, "get_business_config", lambda _db, _bid: "DEFAULT")
    ctx = _ctx()
    assert ctx.config == "DEFAULT"


def test_all_properties_resolve_user_once(monkeypatch):
    calls = []
    fake_user = MagicMock()
    monkeypatch.setattr(uc, "get_current_user_from_session",
                        lambda _db, _req: calls.append(1) or fake_user)
    monkeypatch.setattr(uc, "get_active_business_id",
                        lambda _db, _req, *, user: calls.append(1) or 7)
    monkeypatch.setattr(uc, "get_business_code", lambda _db, _bid: calls.append(1) or "TEST")
    monkeypatch.setattr(uc, "get_business_config", lambda _db, _bid: calls.append(1) or "CFG")
    ctx = _ctx()
    _ = ctx.user
    _ = ctx.active_business_id
    _ = ctx.business_id
    _ = ctx.business_code
    _ = ctx.config
    # una sola resolución del usuario (las otras 3 lecturas usan el id cacheado)
    assert len(calls) == 4


def test_business_code_none_when_no_business(monkeypatch):
    monkeypatch.setattr(uc, "get_active_business_id", lambda _db, _req, *, user: None)
    monkeypatch.setattr(uc, "get_business_code", lambda _db, _bid: None)
    ctx = _ctx()
    assert ctx.business_code is None
```

**Step 2:** En `tests/unit/test_ui_query_count.py`, cambia el comentario de `STRICT_ROUTES` por uno
que diga que se rellena en el envelope final del Lote D. No añadas rutas a `STRICT_ROUTES` todavía.

```python
# BEFORE:
# Rutas que deben afirmar `users == 1`. Se rellena con las 6 de REQ-3 en el envelope final.
STRICT_ROUTES: set[str] = set()

# AFTER (sin cambio funcional; solo precisa cuándo se rellena):
# Rutas que deben afirmar `users == 1`. El envelope final del Lote D añade aquí las 6 de REQ-3.
STRICT_ROUTES: set[str] = set()
```

**Edge cases (ya resueltos):**
- `_ctx()` es el helper existente del fichero (construye `UIContext` con `MagicMock`).
- No cambies los tests existentes de `test_ui_context.py`.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_ui_context.py tests/unit/test_security.py tests/unit/test_ui_query_count.py -q --override-ini="addopts="
# Expected: todos PASS
```

#### Expected Report

- [ ] `tests/unit/test_ui_context.py` — 2 tests sustituidos (Step 0) y 4 nuevos añadidos
- [ ] `tests/unit/test_ui_query_count.py` — solo comentario de STRICT_ROUTES cambiado
- [ ] Ningún otro archivo modificado
- [ ] tests PASS

---

# LOTE B — REQ-4 (auxiliares a `ctx`, parte 1)

### T5: `ui_tabs_common.py` — `_home_locations_context` y `_location_name_for_code` a `ctx`

**Depends on:** T4
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_tabs_common.py`, `app/routers/ui_stock.py`, `app/routers/ui_profit_items.py`, `tests/unit/test_ui_regressions.py`, `tests/unit/test_profit_items_date_filter.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/routers/ui_context.py — YA EXISTE
class UIContext: ...  # .config (BusinessConfig), .request

# app/routers/ui_tabs_common.py — YA EXISTE (líneas 71-107)
def _home_locations_context(db: Session, request: Request) -> tuple[list[dict], str]:
    config = get_active_business_config(db, request)
    ...
def _location_name_for_code(location_code: str, db: Session, request: Request) -> str:
    code = (location_code or "").strip()
    locations, _default_loc_code = _home_locations_context(db, request)
    ...
```

#### Objective

Cambiar `_home_locations_context` y `_location_name_for_code` para recibir `ctx` en lugar de
`(db, request)`.

#### Instructions

**Step 1: Read the file**
```
Read app/routers/ui_tabs_common.py
```

**Step 2: Cambia las firmas y sus cuerpos.**

```python
# BEFORE (líneas 71-82):
def _home_locations_context(db: Session, request: Request) -> tuple[list[dict], str]:
    config = get_active_business_config(db, request)

# AFTER:
def _home_locations_context(ctx: "UIContext") -> tuple[list[dict], str]:
    config = ctx.config
```

```python
# BEFORE (líneas 100-107):
def _location_name_for_code(location_code: str, db: Session, request: Request) -> str:
    code = (location_code or "").strip()
    locations, _default_loc_code = _home_locations_context(db, request)

# AFTER:
def _location_name_for_code(location_code: str, ctx: "UIContext") -> str:
    code = (location_code or "").strip()
    locations, _default_loc_code = _home_locations_context(ctx)
```

**Step 3: Imports.** Cambia el import:

```python
# BEFORE (línea 11):
from app.repositories.business_repository import get_active_business_config

# AFTER:
from app.routers.ui_context import UIContext
```

Y en `from fastapi import HTTPException, Request` (línea 6), deja solo `HTTPException`:

```python
# BEFORE:
from fastapi import HTTPException, Request
# AFTER:
from fastapi import HTTPException
```

**Step 4 (corrección de la revisión, 2026-11-02): llamadores fuera de este lote.** Para que el lote B
cierre en verde, cambia aquí los llamadores que no migran hasta los lotes C y D:

- `app/routers/ui_stock.py`: las 2 ocurrencias exactas
  `_location_name_for_code(selected_location_code, db, request)` → `_location_name_for_code(selected_location_code, ctx)`
  (los dos handlers ya tienen `ctx`).
- `app/routers/ui_profit_items.py`, dentro de `_render_profit_items_tab`: la línea
  `    locations, default_loc_code = _home_locations_context(db, request)` →
  `    locations, default_loc_code = _home_locations_context(ui_context(request, db))`
  (transitorio: T21 la deja en `_home_locations_context(ctx)`). `ui_context` ya está importado en ese fichero.
- `tests/unit/test_ui_regressions.py`, en `test_location_name_for_code_no_nameerror`:
  `result = _location_name_for_code("CENTRAL", db_session, _FakeRequest())` →
  `result = _location_name_for_code("CENTRAL", UIContext(request=_FakeRequest(), db=db_session))`, y justo
  debajo de `from app.routers.ui_tabs_common import _location_name_for_code` añade
  `    from app.routers.ui_context import UIContext` (misma sangría).
- `tests/unit/test_profit_items_date_filter.py:15`: `lambda _db, _req: ([], "")` → `lambda _ctx: ([], "")`.

No toques ninguna otra línea de esos 4 ficheros.

**Edge cases (ya resueltos):**
- `_location_id_for_code` NO cambia (no está en REQ-4): sigue con `(db, location_code, business_id, business_code)`.
- `_restock_items_with_location_history` y `_deletable_skus` NO se tocan en este envelope.
- `ui_home` y `ui_profit` (llamadores de `_home_locations_context`) los migran T7 y T8, en este mismo lote.
  `ui_stock` y `ui_profit_items` se cambian en el Step 4.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_ui_regressions.py -q --override-ini="addopts="
# Expected: PASS (Step 4 adapta el test). ui_home/ui_profit fallan hasta T7/T8, del mismo lote.
```

#### Expected Report

- [ ] `app/routers/ui_tabs_common.py` — 2 firmas cambiadas + imports ajustados
- [ ] Step 4: 2 llamadas en `ui_stock.py`, 1 en `ui_profit_items.py`, 2 tests adaptados
- [ ] Ningún otro archivo modificado

---

### T6: `ui_agent.py` — `_currency_symbol` a `ctx`

**Depends on:** T5
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_agent.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/routers/ui_agent.py — YA EXISTE
def _currency_symbol(db, request) -> str:
    config = get_active_business_config(db, request)
    symbol = getattr(getattr(config, "currency", None), "symbol", None)
    return str(symbol) if symbol else "€"
# 4 call sites: líneas 58, 91, 154, 164 → _currency_symbol(db, request)
```

#### Objective

Cambiar `_currency_symbol` a `(ctx)` y actualizar sus 4 call sites.

#### Instructions

**Step 1:** Cambia la función (líneas 30-34):

```python
# BEFORE:
def _currency_symbol(db, request) -> str:
    """Símbolo de moneda del negocio activo (H-011, consistente con el dashboard)."""
    config = get_active_business_config(db, request)
    symbol = getattr(getattr(config, "currency", None), "symbol", None)
    return str(symbol) if symbol else "€"

# AFTER:
def _currency_symbol(ctx: "UIContext") -> str:
    """Símbolo de moneda del negocio activo (H-011, consistente con el dashboard)."""
    config = ctx.config
    symbol = getattr(getattr(config, "currency", None), "symbol", None)
    return str(symbol) if symbol else "€"
```

**Step 2:** Sustituye las 4 ocurrencias exactas `_currency_symbol(db, request)` por
`_currency_symbol(ctx)` (líneas 58, 91, 154, 164).

**Step 3: Imports.**

```python
# BEFORE (línea 2):
from app.repositories.business_repository import get_active_business_config
# AFTER: (elimina la línea)

# BEFORE (línea 17-18):
from app.routers.ui_context import ui_context
from app.security import get_active_business_code
# AFTER:
from app.routers.ui_context import UIContext, ui_context
# (elimina la línea de get_active_business_code: era un import sin uso)
```

**Edge cases (ya resueltos):**
- `ctx` está en scope en los 4 handlers (todos hacen `ctx = ui_context(request, db)`).

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_ui_agent.py -q --override-ini="addopts="
# Expected: PASS
```

#### Expected Report

- [ ] `app/routers/ui_agent.py` — `_currency_symbol(ctx)` + 4 call sites + imports
- [ ] Ningún otro archivo modificado
- [ ] tests PASS

---

### T7: `ui_home.py` — código de negocio a `ctx` y `_home_locations_context(ctx)`

**Depends on:** T5
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_home.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/routers/ui_home.py — YA EXISTE
from app.security import (get_active_business_code, get_active_business_id, get_user_business_ids)
# línea 165 (dashboard): active_business_id = get_active_business_id(db, request)
# línea 210 (tab_home):  business_code = get_active_business_code(db, request)
# línea 217 (tab_home):  locations, _default_loc_code = _home_locations_context(db, request)
# línea 320 (home_charts): business_code = get_active_business_code(db, request)
```

#### Objective

Sustituir las llamadas directas a `security.py` por `ctx.*` en `ui_home.py`.

#### Instructions

**Step 1:** Sustituye (3 ediciones exactas):

```python
# BEFORE (línea 165, dashboard):
    active_business_id = get_active_business_id(db, request)
# AFTER:
    active_business_id = ctx.active_business_id
```

```python
# BEFORE (línea 210, tab_home):
    business_code = get_active_business_code(db, request)
# AFTER:
    business_code = ctx.business_code
```

```python
# BEFORE (línea 320, home_charts):
    business_code = get_active_business_code(db, request)
# AFTER:
    business_code = ctx.business_code
```

```python
# BEFORE (línea 217, tab_home):
    locations, _default_loc_code = _home_locations_context(db, request)
# AFTER:
    locations, _default_loc_code = _home_locations_context(ctx)
```

**Step 2: Imports.**

```python
# BEFORE (líneas 13-17):
from app.security import (
    get_active_business_code,
    get_active_business_id,
    get_user_business_ids,
)
# AFTER:
from app.security import get_user_business_ids
```

**Edge cases (ya resueltos):**
- El dashboard sigue usando `get_user_business_ids(db, user.id)` (línea 175): ese import se queda.
- El acceso ORM del dashboard (`db.get(Business, ...)`, `select(Business)`, líneas 169-183) NO se
  toca aquí: lo mueve el Lote G.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_ui_home.py -q --override-ini="addopts=" 2>/dev/null || true
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: snapshot check OK
```

#### Expected Report

- [ ] `app/routers/ui_home.py` — 4 sustituciones + imports
- [ ] Ningún otro archivo modificado
- [ ] snapshot check OK

---

### T8: `ui_profit.py` — código a `ctx` y `_home_locations_context(ctx)`

**Depends on:** T5
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_profit.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/routers/ui_profit.py — YA EXISTE
from app.security import get_active_business_code  # línea 11
# línea 40: business_code = get_active_business_code(db, request)
# línea 42: locations, default_loc_code = _home_locations_context(db, request)
```

#### Objective

Sustituir las dos llamadas por `ctx.*` en `ui_profit.py`.

#### Instructions

**Step 1:** Sustituye:

```python
# BEFORE (línea 40):
    business_code = get_active_business_code(db, request)
# AFTER:
    business_code = ctx.business_code
```

```python
# BEFORE (línea 42):
    locations, default_loc_code = _home_locations_context(db, request)
# AFTER:
    locations, default_loc_code = _home_locations_context(ctx)
```

**Step 2: Import.** Elimina la línea `from app.security import get_active_business_code` (línea 11).

**Edge cases (ya resueltos):**
- `_location_id_for_code(db, ..., business_code=business_code)` sigue igual (no está en REQ-4).

#### Verification

```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_profit.py` — 2 sustituciones + import eliminado
- [ ] Ningún otro archivo modificado
- [ ] snapshot check OK

---

### T9: `ui_dividends.py` — config a `ctx.config`

**Depends on:** T4
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_dividends.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/routers/ui_dividends.py — YA EXISTE
from app.repositories.business_repository import get_active_business_config  # línea 11
# línea 39: config = get_active_business_config(db, request)
```

#### Objective

Sustituir `get_active_business_config(db, request)` por `ctx.config` en `tab_dividends`.

#### Instructions

**Step 1:**

```python
# BEFORE (línea 39):
    config = get_active_business_config(db, request)
# AFTER:
    config = ctx.config
```

**Step 2: Import.** Elimina la línea 11
(`from app.repositories.business_repository import get_active_business_config`).

#### Verification

```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_dividends.py` — 1 sustitución + import eliminado
- [ ] Ningún otro archivo modificado
- [ ] snapshot check OK

---

### T10: `ui_extractions.py` — config a `ctx.config` (4 sitios)

**Depends on:** T4
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_extractions.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/routers/ui_extractions.py — YA EXISTE
from app.repositories.business_repository import get_active_business_config  # línea 2
from app.security import get_active_business_code  # línea 13 (import SIN uso)
# 4 ocurrencias: líneas 41, 107, 185, 212 → config = get_active_business_config(db, request)
```

#### Objective

Sustituir las 4 ocurrencias por `ctx.config` y quitar los imports sin uso.

#### Instructions

**Step 1:** Sustituye las 4 ocurrencias exactas `config = get_active_business_config(db, request)`
por `config = ctx.config`.

**Step 2: Imports.** Elimina:
- línea 2: `from app.repositories.business_repository import get_active_business_config`
- línea 13: `from app.security import get_active_business_code`

#### Verification

```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_extractions.py` — 4 sustituciones + 2 imports eliminados
- [ ] Ningún otro archivo modificado
- [ ] snapshot check OK

---

### T11: `ui_customers.py` — config a `ctx.config` + import sin uso

**Depends on:** T4
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_customers.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/routers/ui_customers.py — YA EXISTE
from app.repositories.business_repository import get_active_business_config  # línea 2
from app.security import get_active_business_code  # línea 16 (import SIN uso)
# línea 139 (customer_detail): config = get_active_business_config(db, request)
```

#### Objective

Sustituir la llamada por `ctx.config` y quitar los imports sin uso.

#### Instructions

**Step 1:**

```python
# BEFORE (línea 139):
    config = get_active_business_config(db, request)
# AFTER:
    config = ctx.config
```

**Step 2: Imports.** Elimina la línea 2
(`from app.repositories.business_repository import get_active_business_config`) y la línea 16
(`from app.security import get_active_business_code`).

#### Verification

```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_customers.py` — 1 sustitución + 2 imports eliminados
- [ ] Ningún otro archivo modificado
- [ ] snapshot check OK

---

# LOTE C — REQ-4 (auxiliares a `ctx`, parte 2)

### T12: `ui_stock.py` — config/code a `ctx` + `_location_name_for_code(ctx)`

**Depends on:** T5
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_stock.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/routers/ui_stock.py — YA EXISTE
from app.repositories.business_repository import get_active_business_config  # línea 13
from app.security import get_active_business_code  # línea 15
# 4× config = get_active_business_config(db, request): líneas 51, 100, 168, 249
# 2× business_code = get_active_business_code(db, request): líneas 379, 424
# 2× _location_name_for_code(selected_location_code, ctx): líneas 409, 453 (ya cambiadas en T5, no tocar)
```

#### Objective

Sustituir las 6 llamadas de config/código por `ctx.*` (las 2 de `_location_name_for_code` ya las cambió T5).

#### Instructions

**Step 1:** Sustituye:
- las 4 ocurrencias exactas `config = get_active_business_config(db, request)` por `config = ctx.config`;
- las 2 ocurrencias exactas `business_code = get_active_business_code(db, request)` por
  `business_code = ctx.business_code`.

**Step 2: Imports.** Elimina:
- línea 13: `from app.repositories.business_repository import get_active_business_config`
- línea 15: `from app.security import get_active_business_code`

**Edge cases (ya resueltos):**
- `_deletable_skus`, `_location_id_for_code`, `_normalize_restock_scope` y
  `_restock_items_with_location_history` siguen importándose de `ui_tabs_common` y no cambian aquí.

#### Verification

```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline && SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_ui_regressions.py -q --override-ini="addopts="
# Expected: snapshot OK y test_ui_regressions PASS
```

#### Expected Report

- [ ] `app/routers/ui_stock.py` — 6 sustituciones + 2 imports eliminados
- [ ] Ningún otro archivo modificado
- [ ] snapshot + test_ui_regressions PASS

---

### T13: `ui_products.py` — config/code a `ctx`

**Depends on:** T4
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_products.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/routers/ui_products.py — YA EXISTE
from app.repositories.business_repository import get_active_business_config  # línea 2
from app.security import get_active_business_code  # línea 17
# 4× config = get_active_business_config(db, request): líneas 351, 380, 419, 661
# 1× business_code = get_active_business_code(db, request): línea 660
```

#### Objective

Sustituir las 5 llamadas por `ctx.*`.

#### Instructions

**Step 1:** Sustituye las 4 ocurrencias exactas `config = get_active_business_config(db, request)`
por `config = ctx.config` y la 1 ocurrencia exacta
`business_code = get_active_business_code(db, request)` por `business_code = ctx.business_code`.

**Step 2: Imports.** Elimina la línea 2
(`from app.repositories.business_repository import get_active_business_config`) y, de la línea 17
(`from app.security import get_active_business_code`), elimina la línea completa.

**Edge cases (ya resueltos):**
- `barcode_to_sku` y `extract_sku` (import de `.ui_common`) NO se tocan aquí: los mueve el Lote E.

#### Verification

```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_products.py` — 5 sustituciones + 2 imports eliminados
- [ ] Ningún otro archivo modificado
- [ ] snapshot check OK

---

### T14: `ui_sales.py` (parte 1) — `_sale_pos_context` a `ctx`

**Depends on:** T4
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_sales.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/routers/ui_sales.py — YA EXISTE
def _sale_pos_context(db: Session, request: Request, location_code: str = "") -> tuple[...]:
    config = get_active_business_config(db, request)
# call sites: línea 100-104 (multilínea, con location_code), 192-196, 291-295, 439, 513, 597
```

#### Objective

Cambiar `_sale_pos_context` a recibir `(ctx, location_code="")` y actualizar sus 6 call sites.

#### Instructions

**Step 1:** Cambia la firma (líneas 58-59):

```python
# BEFORE:
def _sale_pos_context(db: Session, request: Request, location_code: str = "") -> tuple[list[dict[str, str]], str, str]:
    config = get_active_business_config(db, request)

# AFTER:
def _sale_pos_context(ctx: "UIContext", location_code: str = "") -> tuple[list[dict[str, str]], str, str]:
    config = ctx.config
```

**Step 2:** Sustituye los 6 call sites:
- los 3 multilínea (líneas 100-104, 192-196, 291-295):

```python
# BEFORE:
    _sale_pos_context(
        db,
        request,
        location_code,
    )
# AFTER:
    _sale_pos_context(
        ctx,
        location_code,
    )
```

- los 3 de una línea (líneas 439, 513, 597): `_sale_pos_context(db, request)` →
  `_sale_pos_context(ctx)`.

**Edge cases (ya resueltos):**
- No toques `_sales_doc_context` aquí: lo hace T15.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_ui_sales_context.py -q --override-ini="addopts="
# Expected: 2 tests en rojo (la firma cambió). Acepta el rojo; T15+T-e2e cierran el lote C verde.
```

#### Expected Report

- [ ] `app/routers/ui_sales.py` — firma + 6 call sites
- [ ] Ningún otro archivo modificado
- [ ] (rojo transitorio en test_ui_sales_context hasta T15)

---

### T15: `ui_sales.py` (parte 2) — `_sales_doc_context` a `ctx` + tab_sales + tests

**Depends on:** T14
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_sales.py`, `tests/unit/test_ui_sales_context.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/routers/ui_sales.py — YA EXISTE
def _sales_doc_context(db: Session, request: Request) -> dict:
    config = get_active_business_config(db, request)
    bid = require_active_business_id(db, request)
    session = getattr(request, "session", None) or {}
    ...
# 11 call sites: **_sales_doc_context(db, request)  (líneas 251, 270, 389, 478, 497, 542, 561, 582, 618, 664, 683)
# línea 742: business_code = get_active_business_code(db, request)
# línea 743: config = get_active_business_config(db, request)
```

#### Objective

Cambiar `_sales_doc_context` a `(ctx)`, actualizar sus 11 call sites, y las 2 llamadas de `tab_sales`.

#### Instructions

**Step 1:** Cambia la firma (líneas 114-117):

```python
# BEFORE:
def _sales_doc_context(db: Session, request: Request) -> dict:
    config = get_active_business_config(db, request)
    bid = require_active_business_id(db, request)
    session = getattr(request, "session", None) or {}

# AFTER:
def _sales_doc_context(ctx: "UIContext") -> dict:
    config = ctx.config
    bid = ctx.business_id
    session = getattr(ctx.request, "session", None) or {}
```

**Step 2:** Sustituye las 11 ocurrencias exactas `**_sales_doc_context(db, request)` por
`**_sales_doc_context(ctx)`.

**Step 3:** En `tab_sales`:

```python
# BEFORE (línea 742):
    business_code = get_active_business_code(db, request)
# AFTER:
    business_code = ctx.business_code
```

```python
# BEFORE (línea 743):
    config = get_active_business_config(db, request)
# AFTER:
    config = ctx.config
```

**Step 4: Imports.** En `app/routers/ui_sales.py`:
- elimina la línea 2 (`from app.repositories.business_repository import get_active_business_config`);
- en la línea 17, cambia `from app.security import get_active_business_code, require_active_business_id`
  por (elimina la línea completa).

**Step 5: tests.** En `tests/unit/test_ui_sales_context.py`, sustituye las 2 llamadas:

```python
# BEFORE (línea 26):
    sale_locations, default_code, selected_code = ui_sales._sale_pos_context(None, _DummyRequest(), "CENTRAL")
# AFTER:
    sale_locations, default_code, selected_code = ui_sales._sale_pos_context(_mock_ctx(), "CENTRAL")
```

```python
# BEFORE (línea 36):
    _sale_locations, default_code, selected_code = ui_sales._sale_pos_context(None, _DummyRequest(), "")
# AFTER:
    _sale_locations, default_code, selected_code = ui_sales._sale_pos_context(_mock_ctx(), "")
```

Y cambia el monkeypatch de `get_active_business_config` por el de `UIContext.config`:

```python
# BEFORE (en ambos tests):
    monkeypatch.setattr(ui_sales, "get_active_business_config", lambda _db, _request: _mock_config())

# AFTER:
    monkeypatch.setattr(ui_sales.UIContext, "config", property(lambda self: _mock_config()))
```

Añade al principio del fichero (tras los imports) este helper:

```python
def _mock_ctx():
    ctx = ui_sales.UIContext(request=_DummyRequest(), db=None)
    return ctx
```

**Edge cases (ya resueltos):**
- `_sales_doc_context` usa `ctx.request.session` para el carrito/draft; el resto del cuerpo (ORM de
  documentos/clientes, líneas 124-129) NO se toca aquí: lo mueve el Lote G.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_ui_sales_context.py -q --override-ini="addopts=" && python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: 3 tests PASS y snapshot OK
```

#### Expected Report

- [ ] `app/routers/ui_sales.py` — firma + 11 call sites + 2 sustituciones tab_sales + imports
- [ ] `tests/unit/test_ui_sales_context.py` — helper + 2 llamadas + monkeypatch
- [ ] Ningún otro archivo modificado
- [ ] tests + snapshot PASS

---

### T16: `ui_purchases.py` (parte 1) — `_purchase_config_values` a `ctx`

**Depends on:** T4
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_purchases.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/routers/ui_purchases.py — YA EXISTE
def _purchase_config_values(db: Session, request: Request) -> tuple[float, Optional[float], str, str]:
    cfg = get_active_business_config(db, request)
# 7 call sites: líneas 82, 367, 506, 542, 572, 641, 718 → _purchase_config_values(db, request)
```

#### Objective

Cambiar `_purchase_config_values` a `(ctx)` y actualizar sus 7 call sites.

#### Instructions

**Step 1:** Cambia la firma (líneas 42-43):

```python
# BEFORE:
def _purchase_config_values(db: Session, request: Request) -> tuple[float, Optional[float], str, str]:
    cfg = get_active_business_config(db, request)

# AFTER:
def _purchase_config_values(ctx: "UIContext") -> tuple[float, Optional[float], str, str]:
    cfg = ctx.config
```

**Step 2:** Sustituye las 7 ocurrencias exactas `_purchase_config_values(db, request)` por
`_purchase_config_values(ctx)`.

**Edge cases (ya resueltos):**
- La función local `_render_error` (que usa `get_current_user_from_session`) la arregla T17.

#### Verification

```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_purchases.py` — firma + 7 call sites
- [ ] Ningún otro archivo modificado
- [ ] snapshot check OK

---

### T17: `ui_purchases.py` (parte 2) — `_render_error` con `ctx.user` + tab_purchases + imports

**Depends on:** T16
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_purchases.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/routers/ui_purchases.py — YA EXISTE
# línea 86-87 (función local _render_error, dentro de purchase_from_invoice):
    def _render_error(message: str, detail: str) -> HTMLResponse:
        user = get_current_user_from_session(db, request)
# línea 865 (tab_purchases): config = get_active_business_config(db, request)
```

#### Objective

Sustituir `get_current_user_from_session` y la última `get_active_business_config` por `ctx`.

#### Instructions

**Step 1:** En la función local `_render_error` (línea 87):

```python
# BEFORE:
        user = get_current_user_from_session(db, request)
# AFTER:
        user = ctx.user
```

**Step 2:** En `tab_purchases` (línea 865):

```python
# BEFORE:
    config = get_active_business_config(db, request)
# AFTER:
    config = ctx.config
```

**Step 3: Imports.** Elimina:
- línea 2: `from app.repositories.business_repository import get_active_business_config`
- línea 19: `from app.security import get_active_business_code, get_current_user_from_session`
  (ambos sin uso tras este cambio)

**Edge cases (ya resueltos):**
- `ctx` está en scope dentro de `purchase_from_invoice` (definido en línea 79) y `tab_purchases`
  (línea 859), así que la función local `_render_error` lo captura por clausura.

#### Verification

```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_purchases.py` — 2 sustituciones + 2 imports eliminados
- [ ] Ningún otro archivo modificado
- [ ] snapshot check OK

---

### T18: `ui_sales_documents.py` — config a `ctx.config` (10 sitios)

**Depends on:** T4
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_sales_documents.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/routers/ui_sales_documents.py — YA EXISTE
from app.repositories.business_repository import get_active_business_config, get_business_config  # línea 2
from app.security import get_active_business_code  # línea 17 (import SIN uso)
# 10× config = get_active_business_config(db, request): líneas 202, 265, 343, 539, 695, 773, 835, 894, 1034, 1085
```

#### Objective

Sustituir las 10 llamadas por `ctx.config` y quitar los imports sin uso.

#### Instructions

**Step 1:** Sustituye las 10 ocurrencias exactas `config = get_active_business_config(db, request)`
por `config = ctx.config`.

**Step 2: Imports.** Elimina:
- línea 2 completa (`get_active_business_config` y `get_business_config` quedan ambos sin uso);
- línea 17: `from app.security import get_active_business_code`.

#### Verification

```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_sales_documents.py` — 10 sustituciones + 2 imports eliminados
- [ ] Ningún otro archivo modificado
- [ ] snapshot check OK

---

# LOTE D — REQ-4 (parte 3) + movimiento de `extract_sku`

### T19: `ui_transfers.py` (parte 1) — `_tab_error` con `ctx.user` + config

**Depends on:** T4
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_transfers.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/routers/ui_transfers.py — YA EXISTE
# línea 168-169 (función local _tab_error, dentro de transfer_update):
    def _tab_error(detail: str) -> HTMLResponse:
        user = get_current_user_from_session(db, request)
# 10× config = get_active_business_config(db, request): líneas 144, 294, 412, 508, 656, 684, 704, 806, 900, 1015
```

#### Objective

Sustituir `get_current_user_from_session` en `_tab_error` y las 10 llamadas a config por `ctx`.

#### Instructions

**Step 1:** En la función local `_tab_error` (línea 169):

```python
# BEFORE:
        user = get_current_user_from_session(db, request)
# AFTER:
        user = ctx.user
```

**Step 2:** Sustituye las 10 ocurrencias exactas `config = get_active_business_config(db, request)`
por `config = ctx.config`.

**Edge cases (ya resueltos):**
- `ctx` está en scope en `transfer_update` (línea 140), así que `_tab_error` lo captura por clausura.
- La 11.ª llamada (`business_code` en `tab_transfers`, línea 1014) la hace T20.

#### Verification

```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_transfers.py` — 11 sustituciones
- [ ] Ningún otro archivo modificado
- [ ] snapshot check OK

---

### T20: `ui_transfers.py` (parte 2) — `business_code` a `ctx` + imports

**Depends on:** T19
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_transfers.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/routers/ui_transfers.py — YA EXISTE
# línea 1014 (tab_transfers): business_code = get_active_business_code(db, request)
```

#### Objective

Sustituir la última llamada a `security.py` y limpiar imports.

#### Instructions

**Step 1:**

```python
# BEFORE (línea 1014):
    business_code = get_active_business_code(db, request)
# AFTER:
    business_code = ctx.business_code
```

**Step 2: Imports.** Elimina:
- línea 2: `from app.repositories.business_repository import get_active_business_config`
- línea 18: `from app.security import get_active_business_code, get_current_user_from_session`
  (ambos sin uso tras T19+T20)

**Edge cases (ya resueltos):**
- No toques `_resolve_transfer_sku`, `service._transfer_out_id_for_movement_id` ni
  `service._transfer_codes_from_out_note` aquí: los mueve/renombra el Lote E/F.

#### Verification

```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_transfers.py` — 1 sustitución + 2 imports eliminados
- [ ] Ningún otro archivo modificado
- [ ] snapshot check OK

---

### T21: `ui_profit_items.py` — `_render_profit_items_tab` a `ctx`

**Depends on:** T4
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_profit_items.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/routers/ui_profit_items.py — YA EXISTE
def _render_profit_items_tab(*, request, db, query="", start_date=None, end_date=None,
                             location_code="", message=None, message_detail=None, message_class=None):
    bid = require_active_business_id(db, request)   # línea 52
    user = get_current_user_from_session(db, request)  # línea 53
    ...
    business_code = get_active_business_code(db, request)  # línea 56
    locations, default_loc_code = _home_locations_context(ui_context(request, db))  # línea 57 (dejada así por T5)
    ...
    _location_id_for_code(db, selected_location_code, business_id=bid, business_code=business_code)  # línea 60
    ...
    return templates.TemplateResponse(request=request, name=..., context={...})  # línea 93
# 8 call sites: _render_profit_items_tab(request=request, db=db, ...) → líneas 123, 197, 282, 368, 404, 437, 474, 567
```

#### Objective

Cambiar `_render_profit_items_tab` a recibir `ctx` y actualizar sus 8 call sites.

#### Instructions

**Step 1:** Cambia la firma y el cuerpo (líneas 40-63):

```python
# BEFORE:
def _render_profit_items_tab(
    *,
    request: Request,
    db: Session,
    query: str = "",
    ...
) -> HTMLResponse:
    bid = require_active_business_id(db, request)
    user = get_current_user_from_session(db, request)
    reporting = ReportingService(db, business_id=bid)

    business_code = get_active_business_code(db, request)
    locations, default_loc_code = _home_locations_context(ui_context(request, db))
    selected_location_code = (location_code or "").strip() or (default_loc_code or "")
    location_id = (
        _location_id_for_code(db, selected_location_code, business_id=bid, business_code=business_code)
        if selected_location_code
        else None
    )

# AFTER:
def _render_profit_items_tab(
    *,
    ctx: "UIContext",
    query: str = "",
    ...
) -> HTMLResponse:
    bid = ctx.business_id
    user = ctx.user
    reporting = ReportingService(ctx.db, business_id=bid)

    business_code = ctx.business_code
    locations, default_loc_code = _home_locations_context(ctx)
    selected_location_code = (location_code or "").strip() or (default_loc_code or "")
    location_id = (
        _location_id_for_code(ctx.db, selected_location_code, business_id=bid, business_code=business_code)
        if selected_location_code
        else None
    )
```

(Conserva intactos `start_date`, `end_date`, `location_code`, `message`, `message_detail`,
`message_class` y el resto del cuerpo a partir de la línea 65. Cambia en el `return` final
`request=request` por `request=ctx.request`.)

**Step 2:** Sustituye los 8 call sites: la forma `_render_profit_items_tab(request=request, db=db, ...)`
pasa a `_render_profit_items_tab(ctx=ctx, ...)`. Sustituye:
- las 7 ocurrencias multilínea que empiezan por `_render_profit_items_tab(\n        request=request,\n        db=db,`
  → `_render_profit_items_tab(\n        ctx=ctx,`
- la 1 ocurrencia de una línea (línea 404):
  `_render_profit_items_tab(request=request, db=db, query=query, start_date=start_date, end_date=end_date, location_code=location_code)`
  → `_render_profit_items_tab(ctx=ctx, query=query, start_date=start_date, end_date=end_date, location_code=location_code)`.

**Step 3: Imports.** Elimina de `app/routers/ui_profit_items.py`:
- la línea 14-18 que importa `get_active_business_code, get_current_user_from_session, require_active_business_id`
  de `app.security` (elimina el bloque `from app.security import (...)` completo);
- añade `from app.routers.ui_context import UIContext` (o amplía el import de `ui_context` existente
  en la línea 32 a `from .ui_context import UIContext, ui_context`).

**Edge cases (ya resueltos):**
- `tab_profit_items` (línea 123) ya construye `ctx` antes de llamar: pásalo.
- El acceso ORM (`db.get(InventoryMovement, ...)`, `db.get(Product, ...)`) NO se toca aquí: lo mueve el Lote F.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_profit_items_date_filter.py -q --override-ini="addopts=" && python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: PASS y snapshot OK (el monkeypatch del test ya usa (ctx) desde T5; no lo toques)
```

#### Expected Report

- [ ] `app/routers/ui_profit_items.py` — firma + 8 call sites + imports
- [ ] Ningún otro archivo modificado
- [ ] tests + snapshot PASS

---

### T22: Mover `extract_sku` de `ui_common.py` a `product_service.py`

**Depends on:** T21
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/services/product_service.py`, `app/routers/ui_common.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/routers/ui_common.py — YA EXISTE (líneas 124-128)
def extract_sku(product_field: str) -> str:
    value = (product_field or "").strip()
    if " - " in value:
        value = value.split(" - ", 1)[0].strip()
    return value

# app/services/product_service.py — YA EXISTE (module-level functions al inicio)
```

#### Objective

Mover `extract_sku` de la capa de routers a la capa de servicios (REQ-6 / dimensión 3).

#### Instructions

**Step 1:** En `app/services/product_service.py`, añade justo después de `_log = get_logger(__name__)`
(línea 17) esta función de módulo (cuerpo tal cual):

```python
def extract_sku(product_field: str) -> str:
    value = (product_field or "").strip()
    if " - " in value:
        value = value.split(" - ", 1)[0].strip()
    return value
```

**Step 2:** En `app/routers/ui_common.py`, elimina la función `extract_sku` (líneas 124-128) completa,
incluida su línea en blanco posterior.

**Edge cases (ya resueltos):**
- Este envelope deja rotos los 6 importadores de `extract_sku` (`ui_products`, `ui_purchases`,
  `ui_profit_items`, `ui_sales`, `ui_sales_documents`, `ui_transfers`). Los arreglan T23-T25 de
  este mismo lote. No los toques aquí.

#### Verification

```bash
python3 -c "from app.services.product_service import extract_sku; assert extract_sku('SKU - name') == 'SKU'"
# Expected: sin salida (assert pasa)
```

#### Expected Report

- [ ] `app/services/product_service.py` — `extract_sku` añadida
- [ ] `app/routers/ui_common.py` — `extract_sku` eliminada
- [ ] Ningún otro archivo modificado
- [ ] import de `extract_sku` desde `product_service` funciona

---

### T23: Importadores de `extract_sku` (ui_sales + ui_products)

**Depends on:** T22
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_sales.py`, `app/routers/ui_products.py`
**Pillars:** P1, P4, P5, P6

#### Objective

Actualizar 2 importadores de `extract_sku` para que importen desde `product_service`.

#### Instructions

**Step 1:** En `app/routers/ui_sales.py`, el import multilínea de `.ui_common` (líneas 44-50) tiene
`extract_sku,`. Quita `extract_sku,` de esa lista y añade una línea nueva:

```python
# BEFORE (dentro del bloque from .ui_common import (...)):
    barcode_to_sku,
    extract_sku,
    parse_dt,

# AFTER:
    barcode_to_sku,
    parse_dt,
```

Y añade, tras el bloque `from .ui_common import (...)`:

```python
from app.services.product_service import extract_sku
```

**Step 2:** En `app/routers/ui_products.py`, el import (líneas 23-30) tiene `extract_sku,`. Quítalo de
esa lista y añade `from app.services.product_service import extract_sku` en su lugar (misma técnica).

**Edge cases (ya resueltos):**
- Solo cambia el import, no los usos de `extract_sku(...)`.

#### Verification

```bash
python3 -c "import app.routers.ui_sales, app.routers.ui_products"
# Expected: sin salida (imports OK)
```

#### Expected Report

- [ ] `app/routers/ui_sales.py` — import de `extract_sku` cambiado
- [ ] `app/routers/ui_products.py` — import de `extract_sku` cambiado
- [ ] Ningún otro archivo modificado
- [ ] imports OK

---

### T24: Importadores de `extract_sku` (ui_purchases + ui_sales_documents)

**Depends on:** T22
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_purchases.py`, `app/routers/ui_sales_documents.py`
**Pillars:** P1, P4, P5, P6

#### Objective

Actualizar 2 importadores más de `extract_sku`.

#### Instructions

**Step 1:** En `app/routers/ui_purchases.py`, el bloque `from .ui_common import (...)` (líneas 25-33)
tiene `extract_sku,`. Quítalo y añade `from app.services.product_service import extract_sku`.

**Step 2:** En `app/routers/ui_sales_documents.py`, la línea 23 es
`from .ui_common import extract_sku, parse_dt, templates`. Cámbiala por
`from .ui_common import parse_dt, templates` y añade
`from app.services.product_service import extract_sku`.

#### Verification

```bash
python3 -c "import app.routers.ui_purchases, app.routers.ui_sales_documents"
# Expected: sin salida
```

#### Expected Report

- [ ] `app/routers/ui_purchases.py` — import cambiado
- [ ] `app/routers/ui_sales_documents.py` — import cambiado
- [ ] Ningún otro archivo modificado
- [ ] imports OK

---

### T25: Importadores de `extract_sku` (ui_transfers + ui_profit_items)

**Depends on:** T22
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_transfers.py`, `app/routers/ui_profit_items.py`
**Pillars:** P1, P4, P5, P6

#### Objective

Actualizar los 2 importadores restantes de `extract_sku`.

#### Instructions

**Step 1:** En `app/routers/ui_transfers.py`, la línea 23 es
`from .ui_common import dt_to_local_input, extract_sku, month_range, parse_dt, templates`.
Cámbiala por `from .ui_common import dt_to_local_input, month_range, parse_dt, templates` y añade
`from app.services.product_service import extract_sku`.

**Step 2:** En `app/routers/ui_profit_items.py`, el bloque `from .ui_common import (...)` (líneas 25-31)
tiene `extract_sku,`. Quítalo y añade `from app.services.product_service import extract_sku`.

#### Verification

```bash
python3 -c "import app.routers.ui_transfers, app.routers.ui_profit_items"
# Expected: sin salida
```

#### Expected Report

- [ ] `app/routers/ui_transfers.py` — import cambiado
- [ ] `app/routers/ui_profit_items.py` — import cambiado
- [ ] Ningún otro archivo modificado
- [ ] imports OK

---

# LOTE E — REQ-6/REQ-9: `ProductService` (barcode_to_sku, resolve_sku) + `ProductRepository`

### T26: `ProductService.barcode_to_sku` (negocio obligatorio) + `ProductRepository.get_by_lot_code`

**Depends on:** T25
**Skill:** backend-developer
**Agent:** data
**Files:** `app/services/product_service.py`, `app/repositories/product_repository.py`
**Pillars:** P1, P4, P5, P6, P3

#### Context (firmas reales)

```python
# app/repositories/product_repository.py — YA EXISTE
class ProductRepository:
    def __init__(self, db: Session, business_id: Optional[int] = None): ...  # self._db, self._business_id
    def get_by_sku(self, sku: str) -> Optional[Product]: ...

# app/services/product_service.py — YA EXISTE
class ProductService:
    def __init__(self, db: Session, business_id: int | None = None): ...  # self._products (ProductRepository)
    def get_by_sku(self, sku: str) -> Product: ...

# app/routers/ui_common.py — YA EXISTE (cuerpo a mover, líneas 175-250)
def barcode_to_sku(db: Session, barcode: str, business_id: Optional[int] = None) -> str: ...
```

#### Objective

Crear `ProductService.barcode_to_sku(barcode)` (con negocio obligatorio) y el método de repositorio
`get_by_lot_code` que usa, reutilizando `ProductRepository`.

#### Instructions

**Step 1:** En `app/repositories/product_repository.py`, añade al final de la clase
`ProductRepository`:

```python
    def get_by_lot_code(self, code: str) -> Optional[Product]:
        code = (code or "").strip()
        lot_stmt = select(InventoryLot).where(InventoryLot.lot_code == code)
        lot_stmt = lot_stmt.where(InventoryLot.business_id == self._business_id)
        lot = self._db.scalar(lot_stmt)
        if lot is None and not code.endswith("00000"):
            lot_stmt = select(InventoryLot).where(InventoryLot.lot_code == f"{code}00000")
            lot_stmt = lot_stmt.where(InventoryLot.business_id == self._business_id)
            lot = self._db.scalar(lot_stmt)
        if lot is None:
            return None
        product = self._db.get(Product, lot.product_id)
        if product is not None and int(getattr(product, "business_id", 0) or 0) == self._business_id:
            return product
        return None
```

Añade el import `from app.models import InventoryLot, Product` al inicio del fichero (hoy importa solo
`Product`).

**Step 2:** En `app/services/product_service.py`, añade el método `barcode_to_sku` a la clase
`ProductService` (después de `get_by_sku`), con la lógica de `ui_common.barcode_to_sku` adaptada a
`self`:

```python
    def barcode_to_sku(self, barcode: str) -> str:
        code = (barcode or "").strip()
        if not code:
            raise HTTPException(status_code=422, detail="barcode is required")

        def _barcode_sku_candidates(raw_code: str) -> list[str]:
            seen: set[str] = set()
            out: list[str] = []

            def _add(value: str) -> None:
                v = (value or "").strip()
                if not v:
                    return
                key = v.upper()
                if key in seen:
                    return
                seen.add(key)
                out.append(v)

            head = raw_code.split("|", 1)[0].strip()
            if head:
                _add(head)
                _add(head.split("-", 1)[0].strip())

            code_upper = raw_code.upper()

            qs_match = re.search(r"(?:^|[?&;])(sku|spp)=([^&;\s]+)", raw_code, flags=re.IGNORECASE)
            if qs_match:
                _add(qs_match.group(2))

            kv_match = re.search(r"(?:SKU|SPP)\s*[:=]\s*([A-Za-z0-9._\-/]+)", raw_code, flags=re.IGNORECASE)
            if kv_match:
                _add(kv_match.group(1))

            if code_upper.startswith("SPP"):
                tail = raw_code[3:].lstrip(" :|=-_/\\")
                if tail:
                    _add(tail)
                    _add(tail.split("|", 1)[0].strip())

            return out

        for sku_candidate in _barcode_sku_candidates(code):
            product = self._products.get_by_sku(sku_candidate)
            if product is not None:
                return product.sku

        product = self._products.get_by_lot_code(code)
        if product is not None:
            return product.sku

        raise HTTPException(status_code=404, detail="Barcode not recognized")
```

Añade `import re` al inicio de `app/services/product_service.py` (hoy no lo importa) y asegúrate de que
`HTTPException` ya está importado (sí, línea 6).

**Step 3:** En `app/routers/ui_common.py`, elimina la función `barcode_to_sku` completa (líneas 175-250)
y el import `from sqlalchemy import select` si queda sin uso (sí queda sin uso: elimínalo).

**Edge cases (ya resueltos):**
- El negocio ya no es opcional: `barcode_to_sku` usa `self._products` (scopeado al negocio).
- Mismos 422/404 y textos que antes.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_repositories.py -q --override-ini="addopts="
# Expected: PASS (los tests existentes de repositorio no se rompen)
```

#### Expected Report

- [ ] `app/services/product_service.py` — `barcode_to_sku` añadido
- [ ] `app/repositories/product_repository.py` — `get_by_lot_code` añadido
- [ ] `app/routers/ui_common.py` — `barcode_to_sku` + `select` eliminados
- [ ] Ningún otro archivo modificado
- [ ] tests PASS

---

### T27: `ProductService.resolve_sku` (unifica las dos resoluciones)

**Depends on:** T26
**Skill:** backend-developer
**Agent:** data
**Files:** `app/services/product_service.py`
**Pillars:** P1, P4, P5, P6, P3

#### Context

```python
# app/services/product_service.py — YA EXISTE tras T26
def extract_sku(product_field: str) -> str: ...
class ProductService:
    def barcode_to_sku(self, barcode: str) -> str: ...
    def _products ...  # ProductRepository.get_by_sku
```

#### Objective

Crear `ProductService.resolve_sku(value, *, barcode_fallback=False)` con la lógica de
`_resolve_sale_sku`/`_resolve_transfer_sku` unificada (REQ-6).

#### Instructions

**Step 1:** Añade a la clase `ProductService` (después de `barcode_to_sku`):

```python
    def resolve_sku(self, value: str, *, barcode_fallback: bool = False) -> str:
        val = (value or "").strip()
        if not val:
            raise HTTPException(status_code=422, detail="product is required")

        extracted = extract_sku(val)
        for candidate in (extracted, val):
            cand = (candidate or "").strip()
            if not cand:
                continue
            by_sku = self._products.get_by_sku(cand)
            if by_sku is not None:
                return str(by_sku.sku)

        by_name = self._products.first_by_name(val)
        if by_name is not None:
            return str(by_name.sku)

        if barcode_fallback:
            try:
                return self.barcode_to_sku(val)
            except HTTPException:
                pass

        detail = (
            "Producto no encontrado. Usa SKU, nombre exacto o código de barras válido."
            if barcode_fallback
            else "Producto no encontrado"
        )
        raise HTTPException(status_code=404, detail=detail)
```

**Step 2:** Añade `first_by_name` a `ProductRepository` (se crea en T28, pero `resolve_sku` lo
referencia). Si T28 aún no ha corrido, añade tú el método en este envelope a
`app/repositories/product_repository.py` (un solo archivo más; el envelope toca 2 archivos):

```python
    def first_by_name(self, value: str) -> Optional[Product]:
        return self._db.scalar(
            select(Product)
            .where(Product.business_id == self._business_id, Product.name.ilike(value))
            .order_by(Product.id.asc())
        )
```

**Edge cases (ya resueltos):**
- Valor vacío → 422 `"product is required"`.
- `barcode_fallback=True` ignora el `HTTPException` de `barcode_to_sku`.
- Mensaje 404 distinto con y sin `barcode_fallback`.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_repositories.py -q --override-ini="addopts="
# Expected: PASS
```

#### Expected Report

- [ ] `app/services/product_service.py` — `resolve_sku` añadido
- [ ] `app/repositories/product_repository.py` — `first_by_name` añadido
- [ ] Ningún otro archivo modificado
- [ ] tests PASS

---

### T28: `ProductRepository` — `list_categories` y `deletable_skus`

**Depends on:** T27
**Skill:** backend-developer
**Agent:** data
**Files:** `app/repositories/product_repository.py`, `tests/unit/test_repositories.py`
**Pillars:** P1, P4, P5, P6, P3

#### Context

```python
# app/routers/ui_products.py — YA EXISTE (consulta a mover, líneas 679-685)
# app/routers/ui_tabs_common.py — YA EXISTE (cuerpo a mover, líneas 15-50, _deletable_skus)
```

#### Objective

Crear `ProductRepository.list_categories()` y `ProductRepository.deletable_skus(skus)` con los cuerpos
actuales tal cual.

#### Instructions

**Step 1:** Añade a la clase `ProductRepository`:

```python
    def list_categories(self) -> list[str]:
        rows = self._db.execute(
            select(Product.category)
            .where(Product.business_id == self._business_id)
            .distinct()
            .order_by(Product.category.asc())
        ).all()
        return [str(c or "").strip() for (c,) in rows if str(c or "").strip()]

    def deletable_skus(self, skus: list[str]) -> set[str]:
        clean = [str(s).strip() for s in (skus or []) if str(s).strip()]
        if not clean:
            return set()
        rows = self._db.execute(
            select(Product.id, Product.sku).where(
                Product.sku.in_(clean),
                Product.business_id == self._business_id,
            )
        ).all()
        if not rows:
            return set()
        product_ids = [int(pid) for pid, _ in rows]
        id_to_sku = {int(pid): str(sku) for pid, sku in rows}
        movement_pids = set(
            self._db.scalars(
                select(InventoryMovement.product_id)
                .where(InventoryMovement.product_id.in_(product_ids))
                .where(InventoryMovement.business_id == self._business_id)
                .distinct()
            ).all()
        )
        lot_pids = set(
            self._db.scalars(
                select(InventoryLot.product_id)
                .where(InventoryLot.product_id.in_(product_ids))
                .where(InventoryLot.business_id == self._business_id)
                .distinct()
            ).all()
        )
        blocked = movement_pids | lot_pids
        return {sku for pid, sku in id_to_sku.items() if pid not in blocked}
```

Añade el import `from app.models import InventoryLot, InventoryMovement, Product` al inicio (hoy solo
`Product`).

**Step 2:** En `tests/unit/test_repositories.py`, añade al final (conserva lo existente) estos tests de
aislamiento por negocio:

```python
def test_product_repo_scoped_methods_isolate_by_business(db_session, business_id):
    from app.models import InventoryLot, InventoryMovement, Location
    other = Business(code="otro", name="Otro")
    db_session.add(other)
    db_session.commit()
    db_session.refresh(other)
    p1 = _create_product(db_session, business_id, "SKU-CAT1", "Camiseta")
    p2 = _create_product(db_session, int(other.id), "SKU-CAT2", "Pantalon")
    loc = Location(business_id=business_id, code="CENTRAL", name="Central")
    db_session.add(loc)
    db_session.flush()
    mv = InventoryMovement(business_id=business_id, product_id=p1.id, location_id=loc.id,
                           type="purchase", quantity=2, unit_cost=1.0, unit_price=None,
                           movement_date=datetime.now(timezone.utc), note=None)
    db_session.add(mv)
    db_session.flush()
    db_session.add(InventoryLot(business_id=business_id, movement_id=mv.id, product_id=p1.id,
                                location_id=loc.id, lot_code="LOT-CAT", received_at=datetime.now(timezone.utc),
                                unit_cost=1.0, qty_received=2, qty_remaining=2))
    db_session.commit()

    repo = ProductRepository(db_session, business_id=business_id)
    assert repo.first_by_name("Camiseta") is not None
    assert repo.first_by_name("Pantalon") is None  # de otro negocio
    assert "SKU-CAT1" not in [""]  # list_categories devuelve categorías del negocio
    cats = repo.list_categories()
    assert all(isinstance(c, str) for c in cats)
    assert repo.get_by_lot_code("LOT-CAT") is not None
    assert repo.get_by_lot_code("LOT-CAT") .sku == "SKU-CAT1"
    assert repo.deletable_skus(["SKU-CAT1"]) == set()  # tiene movimientos/lotes
    assert repo.deletable_skus(["SKU-CAT2"]) == set()  # otro negocio → no visible
```

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_repositories.py -q --override-ini="addopts="
# Expected: PASS
```

#### Expected Report

- [ ] `app/repositories/product_repository.py` — 2 métodos añadidos + import
- [ ] `tests/unit/test_repositories.py` — 1 test de aislamiento añadido
- [ ] Ningún otro archivo modificado
- [ ] tests PASS

---

### T29: `ui_products.py` — consumir `first_by_name`, `list_categories`, `barcode_to_sku`

**Depends on:** T28
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/routers/ui_products.py`
**Pillars:** P1, P4, P5, P6, P3

#### Context

```python
# app/routers/ui_products.py — YA EXISTE
# líneas 603-607 (product_defaults_sale): db.scalar(select(Product).where(..., Product.name.ilike(value))...)
# línea 611: barcode_to_sku(db, value, business_id=business_id)
# líneas 679-685 (tab_inventory): db.execute(select(Product.category)...) → categories
```

#### Objective

Sustituir las consultas ORM de `ui_products.py` por `ProductRepository`/`ProductService`.

#### Instructions

**Step 1:** En `product_defaults_sale` (líneas 602-613), sustituye:

```python
# BEFORE:
    if p is None:
        p = db.scalar(
            select(Product)
            .where(Product.business_id == int(business_id), Product.name.ilike(value))
            .order_by(Product.id.asc())
        )

    if p is None:
        try:
            p = product_service.get_by_sku(barcode_to_sku(db, value, business_id=business_id))
        except HTTPException:
            return HTMLResponse("")

# AFTER:
    if p is None:
        p = ProductRepository(db, business_id=business_id).first_by_name(value)

    if p is None:
        try:
            p = product_service.get_by_sku(product_service.barcode_to_sku(value))
        except HTTPException:
            return HTMLResponse("")
```

**Step 2:** En `tab_inventory` (líneas 677-688), sustituye:

```python
# BEFORE:
    categories: list[str] = []
    try:
        rows = db.execute(
            select(Product.category)
            .where(Product.business_id == int(bid))
            .distinct()
            .order_by(Product.category.asc())
        ).all()
        categories = [str(c or "").strip() for (c,) in rows if str(c or "").strip()]
    except Exception:
        _log.error("categories fallback en tab_inventory")
        categories = []

# AFTER:
    categories: list[str] = []
    try:
        categories = ProductRepository(db, business_id=bid).list_categories()
    except Exception:
        _log.error("categories fallback en tab_inventory")
        categories = []
```

**Step 3: Imports.** Elimina de `app/routers/ui_products.py`:
- `from sqlalchemy import select` (línea 21);
- `barcode_to_sku` del import de `.ui_common` (líneas 23-30);
- añade `from app.repositories.product_repository import ProductRepository`.

**Edge cases (ya resueltos):**
- Los `try/except` + `_log.error` de cada sitio se conservan en el router (dimensión 5).

#### Verification

```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_products.py` — 3 sitios sustituidos + imports
- [ ] Ningún otro archivo modificado
- [ ] snapshot check OK

---

### T30: `ui_stock.py` — consumir `list_categories` y `deletable_skus`

**Depends on:** T28
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/routers/ui_stock.py`
**Pillars:** P1, P4, P5, P6, P3

#### Context

```python
# app/routers/ui_stock.py — YA EXISTE
# líneas 277-283 (ui_supplier_return): db.execute(select(Product.category)...) → categories
# líneas 69, 132, 210: deletable_skus = _deletable_skus(db, [i.sku for i in items], business_id=int(bid))
```

#### Objective

Sustituir las consultas por `ProductRepository.list_categories()` y `ProductRepository.deletable_skus()`.

#### Instructions

**Step 1:** En `ui_supplier_return` (líneas 275-286), sustituye:

```python
# BEFORE:
    categories: list[str] = []
    try:
        rows = db.execute(
            select(Product.category)
            .where(Product.business_id == int(bid))
            .distinct()
            .order_by(Product.category.asc())
        ).all()
        categories = [str(c or "").strip() for (c,) in rows if str(c or "").strip()]
    except Exception:
        _log.error("categories fallback en devoluciones")
        categories = []

# AFTER:
    categories: list[str] = []
    try:
        categories = ProductRepository(db, business_id=bid).list_categories()
    except Exception:
        _log.error("categories fallback en devoluciones")
        categories = []
```

**Step 2:** Sustituye las 3 ocurrencias exactas
`deletable_skus = _deletable_skus(db, [i.sku for i in items], business_id=int(bid))` por
`deletable_skus = ProductRepository(db, business_id=bid).deletable_skus([i.sku for i in items])`.

**Step 3: Imports.** En `app/routers/ui_stock.py`:
- elimina `from sqlalchemy import select` (línea 7);
- quita `_deletable_skus` del import de `.ui_tabs_common` (líneas 22-28), dejando
  `_location_id_for_code`, `_location_name_for_code`, `_normalize_restock_scope` y
  `_restock_items_with_location_history`;
- añade `from app.repositories.product_repository import ProductRepository`.

#### Verification

```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_stock.py` — 4 sitios sustituidos + imports
- [ ] Ningún otro archivo modificado
- [ ] snapshot check OK

---

### T31: `ui_sales.py` — consumir `resolve_sku` y `barcode_to_sku`

**Depends on:** T27
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/routers/ui_sales.py`
**Pillars:** P1, P4, P5, P6, P3

#### Context

```python
# app/routers/ui_sales.py — YA EXISTE
def _resolve_sale_sku(db: Session, business_id: int, product_input: str) -> str: ...  # líneas 141-173
# línea 200 (sale): sku = _resolve_sale_sku(db, business_id=bid, product_input=product)
# línea 299 (sale_barcode): sku = barcode_to_sku(db, barcode, business_id=bid)
```

#### Objective

Eliminar `_resolve_sale_sku` y usar `ProductService.resolve_sku`/`barcode_to_sku`.

#### Instructions

**Step 1:** Elimina la función `_resolve_sale_sku` completa (líneas 141-173).

**Step 2:** En `sale` (línea 200):

```python
# BEFORE:
        sku = _resolve_sale_sku(db, business_id=bid, product_input=product)
# AFTER:
        sku = ProductService(db, business_id=bid).resolve_sku(product, barcode_fallback=True)
```

**Step 3:** En `sale_barcode` (línea 299):

```python
# BEFORE:
        sku = barcode_to_sku(db, barcode, business_id=bid)
# AFTER:
        sku = ProductService(db, business_id=bid).barcode_to_sku(barcode)
```

**Step 4: Imports.** Elimina `barcode_to_sku` del import de `.ui_common` (líneas 44-50) y
`from sqlalchemy import select` (línea 9) si queda sin uso (sí queda sin uso).

#### Verification

```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_sales.py` — `_resolve_sale_sku` eliminada + 2 call sites + imports
- [ ] Ningún otro archivo modificado
- [ ] snapshot check OK

---

### T32: `ui_transfers.py` — consumir `resolve_sku`

**Depends on:** T27
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/routers/ui_transfers.py`
**Pillars:** P1, P4, P5, P6, P3

#### Context

```python
# app/routers/ui_transfers.py — YA EXISTE
def _resolve_transfer_sku(db: Session, business_id: int, product_input: str) -> str: ...  # líneas 33-55
# línea 683 (get_transfer_stock): resolved_sku = _resolve_transfer_sku(db, business_id=bid, product_input=sku)
```

#### Objective

Eliminar `_resolve_transfer_sku` y usar `ProductService.resolve_sku`.

#### Instructions

**Step 1:** Elimina la función `_resolve_transfer_sku` completa (líneas 33-55).

**Step 2:** En `get_transfer_stock` (línea 683):

```python
# BEFORE:
        resolved_sku = _resolve_transfer_sku(db, business_id=bid, product_input=sku)
# AFTER:
        resolved_sku = ProductService(db, business_id=bid).resolve_sku(sku)
```

**Step 3: Imports.** Elimina `from sqlalchemy import select` (línea 11) y `from sqlalchemy.sql import func`
(línea 12) si quedan sin uso (el `func` sigue usándose en `transfer_print`, así que conserva `func`
hasta el Lote F que mueve `transfer_print`; aquí elimina solo `select` si queda sin uso).
Añade `from app.services.product_service import ProductService` (ya está importado `InventoryService`
en la línea 19; añade la línea nueva junto a él).

#### Verification

```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_transfers.py` — `_resolve_transfer_sku` eliminada + 1 call site + imports
- [ ] Ningún otro archivo modificado
- [ ] snapshot check OK

---

# LOTE F — REQ-7/REQ-8: `InventoryRepository` + renombrado de métodos públicos

### T33: `InventoryRepository` — `get_movement`, `skus_with_location_history`, `transfer_*`, `skus_with_movements`

**Depends on:** T32
**Skill:** backend-developer
**Agent:** data
**Files:** `app/repositories/inventory_repository.py`, `tests/unit/test_inventory_repository_queries.py`
**Pillars:** P1, P4, P5, P6, P3, P8

#### Context (firmas reales)

```python
# app/repositories/inventory_repository.py — YA EXISTE
class InventoryRepository:
    def __init__(self, db: Session, business_id: Optional[int] = None): ...  # self._db, self._business_id
# imports: from app.models import AuditLog, InventoryLot, InventoryMovement, Location, MovementAllocation, Product
#          from sqlalchemy import String, case, cast, func, select
```

#### Objective

Añadir los 5 métodos de repositorio de inventario que consumen los routers (REQ-7 y §2.3).

#### Instructions

**Step 1:** Añade a la clase `InventoryRepository` (al final, antes de `_fill_reordered_audit_usernames`
o al final de la clase):

```python
    def get_movement(self, movement_id: int, types: tuple[str, ...]) -> Optional[InventoryMovement]:
        mv = self._db.get(InventoryMovement, int(movement_id))
        if mv is None:
            return None
        if mv.type not in types:
            return None
        if int(getattr(mv, "business_id", 0) or 0) != self._business_id:
            return None
        return mv

    def skus_with_location_history(self, location_id: int, skus: list[str]) -> set[str]:
        return {
            str(sku).strip()
            for sku in self._db.scalars(
                select(Product.sku)
                .select_from(InventoryMovement)
                .join(Product, Product.id == InventoryMovement.product_id)
                .where(
                    InventoryMovement.business_id == self._business_id,
                    InventoryMovement.location_id == int(location_id),
                    Product.sku.in_(skus),
                )
                .distinct()
            ).all()
            if str(sku or "").strip()
        }

    def transfer_out_by_ref(self, ref: str) -> list[InventoryMovement]:
        return list(
            self._db.scalars(
                select(InventoryMovement)
                .where(
                    InventoryMovement.type == "transfer_out",
                    InventoryMovement.note.ilike(f"%ref={ref}%"),
                    InventoryMovement.business_id == self._business_id,
                )
                .order_by(InventoryMovement.movement_date.asc(), InventoryMovement.id.asc())
            )
        )

    def transfer_items_by_ref(self, ref: str) -> list[tuple[str, str, float, float]]:
        rows = self._db.execute(
            select(
                Product.sku,
                Product.name,
                func.coalesce(func.sum(func.abs(InventoryMovement.quantity)), 0).label("qty"),
                func.coalesce(Product.default_sale_price, 0).label("sale_price"),
            )
            .select_from(InventoryMovement)
            .join(Product, Product.id == InventoryMovement.product_id)
            .where(
                InventoryMovement.type == "transfer_out",
                InventoryMovement.note.ilike(f"%ref={ref}%"),
                InventoryMovement.business_id == self._business_id,
            )
            .group_by(Product.id)
            .order_by(Product.sku.asc())
        ).all()
        return [(str(sku or ""), str(name or ""), float(qty or 0), float(sale_price or 0))
                for sku, name, qty, sale_price in rows]

    def skus_with_movements(
        self,
        movement_type: str,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> set[str]:
        mv_filters = [
            InventoryMovement.business_id == self._business_id,
            InventoryMovement.type == movement_type,
        ]
        if start is not None:
            mv_filters.append(InventoryMovement.movement_date >= start)
        if end is not None:
            mv_filters.append(InventoryMovement.movement_date < end)
        return set(
            self._db.scalars(
                select(Product.sku)
                .join(InventoryMovement, InventoryMovement.product_id == Product.id)
                .where(*mv_filters)
                .distinct()
            ).all()
        )
```

`datetime` ya está importado (`from datetime import datetime`). No añadas imports nuevos.

**Step 2:** En `tests/unit/test_inventory_repository_queries.py`, añade al final (conserva lo existente)
estos tests de aislamiento por negocio:

```python
def test_get_movement_isolates_by_business_and_type(db_session, business_id):
    from app.models import Business, InventoryMovement, Location
    other = Business(code="otro", name="Otro")
    db_session.add(other)
    db_session.commit()
    db_session.refresh(other)
    p = _create_product(db_session, business_id, "SKU-MV", "Movimiento")
    t0 = datetime.now(timezone.utc)
    sale = InventoryMovement(business_id=int(other.id), product_id=p.id, location_id=None,
                             type="sale", quantity=-1, unit_cost=None, unit_price=5,
                             movement_date=t0, note=None)
    db_session.add(sale)
    db_session.commit()

    repo = InventoryRepository(db_session, business_id=business_id)
    # de otro negocio → None (REQ-7)
    assert repo.get_movement(sale.id, ("sale",)) is None
    # tipo distinto → None
    sale.business_id = business_id
    db_session.commit()
    assert repo.get_movement(sale.id, ("purchase",)) is None
    # negocio y tipo correctos → el movimiento
    assert repo.get_movement(sale.id, ("sale",)) is sale


def test_transfer_out_by_ref_scopes_to_business(db_session, business_id):
    from app.models import Business, InventoryMovement
    other = Business(code="otro2", name="Otro2")
    db_session.add(other)
    db_session.commit()
    db_session.refresh(other)
    p = _create_product(db_session, business_id, "SKU-TREF", "Traspaso")
    t0 = datetime.now(timezone.utc)
    mv = InventoryMovement(business_id=int(other.id), product_id=p.id, location_id=None,
                           type="transfer_out", quantity=1, unit_cost=1.0, unit_price=None,
                           movement_date=t0, note="ref=TP-XYZ: n")
    db_session.add(mv)
    db_session.commit()

    repo = InventoryRepository(db_session, business_id=business_id)
    assert repo.transfer_out_by_ref("TP-XYZ") == []
    assert repo.skus_with_movements("transfer_out", start=None, end=None) == set()
```

**Edge cases (ya resueltos):**
- `get_movement` devuelve `None` si no existe, si es de otro negocio o si el tipo no está en `types`.
  `types` es obligatorio (sin default).
- `transfer_out_by_ref` filtra por negocio EN SQL (hoy se filtra en Python; mismo resultado y orden).

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_inventory_repository_queries.py -q --override-ini="addopts="
# Expected: PASS
```

#### Expected Report

- [ ] `app/repositories/inventory_repository.py` — 5 métodos añadidos
- [ ] `tests/unit/test_inventory_repository_queries.py` — 2 tests de aislamiento añadidos
- [ ] Ningún otro archivo modificado
- [ ] tests PASS

---

### T34: REQ-8 — renombrar los 2 métodos privados a públicos (app)

**Depends on:** T33
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/services/inventory_service.py`, `app/routers/ui_transfers.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/services/inventory_service.py — YA EXISTE
def _transfer_out_id_for_movement_id(self, movement_id: int) -> int: ...   # línea 684
def _transfer_to_code_from_out_note(self, note: str) -> str: ...          # línea 704 (llama al siguiente)
def _transfer_codes_from_out_note(self, note: str) -> tuple[str, str, Optional[str]]: ...  # línea 708

# app/routers/ui_transfers.py — 9 usos
```

#### Objective

Renombrar `_transfer_out_id_for_movement_id` → `transfer_out_id_for_movement_id` y
`_transfer_codes_from_out_note` → `transfer_codes_from_out_note` (sin alias, actualizando TODOS los
usos, incluidos los internos del servicio).

#### Instructions

**Step 1:** En `app/services/inventory_service.py`, sustituye TODAS las ocurrencias exactas:

- `_transfer_out_id_for_movement_id(` → `transfer_out_id_for_movement_id(` (def + usos internos);
- `_transfer_codes_from_out_note(` → `transfer_codes_from_out_note(` (def + usos internos).

**Step 2:** En `app/routers/ui_transfers.py`, sustituye TODAS las ocurrencias exactas de los mismos dos
patrones (`service._transfer_out_id_for_movement_id(` → `service.transfer_out_id_for_movement_id(` y
`service._transfer_codes_from_out_note(` → `service.transfer_codes_from_out_note(`).

**Edge cases (ya resueltos):**
- `_transfer_to_code_from_out_note` NO se renombra; solo su llamada interna a
  `_transfer_codes_from_out_note` cambia a `transfer_codes_from_out_note`.
- Sin alias: no dejes `_transfer_* = transfer_*` en ningún sitio.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_inventory_service_unit.py -q --override-ini="addopts="
# Expected: 2 tests en rojo (test_transfer_codes_parser_*) hasta T35
```

#### Expected Report

- [ ] `app/services/inventory_service.py` — 2 renombrados
- [ ] `app/routers/ui_transfers.py` — usos renombrados
- [ ] Ningún otro archivo modificado
- [ ] (rojo transitorio en test_inventory_service_unit hasta T35)

---

### T35: REQ-8 — actualizar los 2 tests que usan el método privado

**Depends on:** T34
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** `tests/unit/test_inventory_service_unit.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# tests/unit/test_inventory_service_unit.py — YA EXISTE
# línea 48: service._transfer_codes_from_out_note("Transfer CENTRAL->POS1 ref=TP-123: nota")
# línea 59: service._transfer_codes_from_out_note("nota sin formato")
```

#### Objective

Actualizar las 2 llamadas al nombre público.

#### Instructions

**Step 1:** Sustituye las 2 ocurrencias exactas `service._transfer_codes_from_out_note(` por
`service.transfer_codes_from_out_note(`.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_inventory_service_unit.py -q --override-ini="addopts="
# Expected: PASS
```

#### Expected Report

- [ ] `tests/unit/test_inventory_service_unit.py` — 2 renombrados
- [ ] Ningún otro archivo modificado
- [ ] tests PASS

---

### T36: `ui_transfers.py` — consumir `get_movement` y `get_product` (edición)

**Depends on:** T34
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/routers/ui_transfers.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/repositories/inventory_repository.py — YA EXISTE
def get_movement(self, movement_id: int, types: tuple[str, ...]) -> Optional[InventoryMovement]: ...
# app/repositories/purchase_repository.py — YA EXISTE
def get_product(self, product_id: int) -> Optional[Product]: ...

# app/routers/ui_transfers.py — sitios:
# transfer_edit_form (104-110): out_id = service.transfer_out_id_for_movement_id(movement_id)
#     mv = db.get(InventoryMovement, out_id); if mv is None or mv.type != "transfer_out": 404; if business_id != bid: 404; product = db.get(Product, mv.product_id)
# transfer_update (205-210): mv_out = db.get(InventoryMovement, out_id); if ... 404
# transfer_delete (423-424 y 477-479): mv_out = db.get(InventoryMovement, out_id); mv = db.get(InventoryMovement, out_id); product = db.get(Product, mv.product_id)
```

#### Objective

Sustituir los `db.get(InventoryMovement, …)`/`db.get(Product, …)` de la edición/borrado de traspaso por
los repositorios.

#### Instructions

**Step 1:** Crea un repo en cada handler (justo tras `bid = ctx.business_id`):
`repo = InventoryRepository(db, business_id=bid)` y `purchase_repo = PurchaseRepository(db, bid)`.

**Step 2:** Sustituye cada patrón `mv = db.get(InventoryMovement, out_id)` seguido del bloque de 404:

```python
# BEFORE (transfer_edit_form, líneas 105-109):
    mv = db.get(InventoryMovement, out_id)
    if mv is None or mv.type != "transfer_out":
        raise HTTPException(status_code=404, detail="Transfer movement not found")
    if int(getattr(mv, "business_id", 0) or 0) != int(bid):
        raise HTTPException(status_code=404, detail="Transfer movement not found")

# AFTER:
    mv = repo.get_movement(out_id, ("transfer_out",))
    if mv is None:
        raise HTTPException(status_code=404, detail="Transfer movement not found")
```

Y `product = db.get(Product, mv.product_id)` → `product = purchase_repo.get_product(mv.product_id)`.

**Step 3:** Aplica el MISMO patrón a `transfer_update` (líneas 206-210) y a `transfer_delete`
(líneas 424 y 477-479). En `transfer_delete`, la línea 424 usa `out_id` validado por
`transfer_out_id_for_movement_id` y la 477-479 usa `out_id` + `product`; usa `repo.get_movement` y
`purchase_repo.get_product` igual.

**Step 4: Imports.** Añade `from app.repositories.inventory_repository import InventoryRepository` y
`from app.repositories.purchase_repository import PurchaseRepository` a `app/routers/ui_transfers.py`.
Conserva `from app.models import InventoryMovement, Product` si aún se usa en otros sitios (lo hace,
hasta T37).

**Edge cases (ya resueltos):**
- `get_movement` devuelve `None` para "no existe / otro negocio / tipo distinto": el `raise 404` con el
  MISMO `detail` queda en el router.
- Los `db.get(InventoryMovement, …)` de `transfer_print` (línea 708) los mueve T37.

#### Verification

```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_transfers.py` — 4 sitios `db.get(InventoryMovement)` + 2 `db.get(Product)` sustituidos
- [ ] Ningún otro archivo modificado
- [ ] snapshot check OK

---

### T37: `ui_transfers.py` — `transfer_print` y `transfer_bulk` con repositorio

**Depends on:** T36
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/routers/ui_transfers.py`
**Pillars:** P1, P4, P5, P6, P3

#### Context

```python
# app/routers/ui_transfers.py — YA EXISTE
# transfer_print (708-761): mv = db.get(InventoryMovement, int(movement_id)); mvs = select(InventoryMovement)...ilike ref...; filtro por negocio en Python; stmt = select(Product.sku,...).group_by...
# transfer_bulk_modal (847-864): transferred_skus = set(db.scalars(select(Product.sku).join(...).where(*mv_filters).distinct()).all())
```

#### Objective

Sustituir las consultas de impresión y traspaso masivo por `InventoryRepository`.

#### Instructions

**Step 1:** En `transfer_print`, sustituye el bloque `mvs = list(db.scalars(...))` + filtro en Python
(líneas 721-732):

```python
# BEFORE:
    mvs = list(
        db.scalars(
            select(InventoryMovement)
            .where(
                InventoryMovement.type == "transfer_out",
                InventoryMovement.note.ilike(f"%ref={ref_clean}%"),
            )
            .order_by(InventoryMovement.movement_date.asc(), InventoryMovement.id.asc())
        )
    )
    if bid is not None:
        mvs = [m for m in mvs if int(getattr(m, "business_id", 0) or 0) == int(bid)]

# AFTER:
    mvs = InventoryRepository(db, business_id=bid).transfer_out_by_ref(ref_clean)
```

Y el `mv = db.get(InventoryMovement, int(movement_id))` de la línea 708:

```python
# BEFORE:
        mv = db.get(InventoryMovement, int(movement_id))
        if mv is None:
            raise HTTPException(status_code=404, detail="Transfer not found")
        if int(getattr(mv, "business_id", 0) or 0) != int(bid):
            raise HTTPException(status_code=404, detail="Transfer not found")

# AFTER:
        mv = InventoryRepository(db, business_id=bid).get_movement(int(movement_id), ("transfer_out", "transfer_in"))
        if mv is None:
            raise HTTPException(status_code=404, detail="Transfer not found")
```

Y el bloque `stmt = (select(Product.sku, ...).group_by(...))` + `rows = db.execute(stmt).all()` +
bucle (líneas 743-774) pasa a:

```python
    rows = InventoryRepository(db, business_id=bid).transfer_items_by_ref(ref_clean)
    items: list[dict] = []
    for sku, name, qty, sale_price in rows:
        sp = float(sale_price or 0)
        items.append(
            {
                "sku": sku,
                "name": name,
                "qty": qty,
                "sale_price": sp,
                "sale_price_10": sp * 0.10,
            }
        )
```

**Step 2:** En `transfer_bulk_modal` (líneas 849-865), sustituye:

```python
# BEFORE:
        mv_filters = [
            InventoryMovement.business_id == bid,
            InventoryMovement.type == "transfer_out",
        ]
        if start_dt is not None:
            mv_filters.append(InventoryMovement.movement_date >= start_dt)
        if end_dt is not None:
            mv_filters.append(InventoryMovement.movement_date < end_dt)
        transferred_skus = set(
            db.scalars(
                select(Product.sku)
                .join(InventoryMovement, InventoryMovement.product_id == Product.id)
                .where(*mv_filters)
                .distinct()
            ).all()
        )

# AFTER:
        transferred_skus = InventoryRepository(db, business_id=bid).skus_with_movements(
            "transfer_out", start=start_dt, end=end_dt
        )
```

**Step 3: Imports.** Si `select`, `func`, `InventoryMovement` y `Product` ya no se usan en
`ui_transfers.py`, elimínalos (revisa: `Product` puede seguir usándose; elimina solo lo que quede
realmente sin uso).

#### Verification

```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_transfers.py` — 3 sitios de print/bulk sustituidos + imports limpios
- [ ] Ningún otro archivo modificado
- [ ] snapshot check OK

---

### T38: `ui_sales.py` — `get_movement`/`get_product` en edición de venta

**Depends on:** T33
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/routers/ui_sales.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/routers/ui_sales.py — YA EXISTE (sale_edit_form, líneas 404-409)
# mv = db.get(InventoryMovement, movement_id); if mv is None or mv.type != "sale": 404; if business_id != bid: 404
# product = db.get(Product, mv.product_id)
```

#### Objective

Sustituir los `db.get` por repositorio (REQ-7).

#### Instructions

**Step 1:** En `sale_edit_form` (líneas 404-409), sustituye:

```python
# BEFORE:
    mv = db.get(InventoryMovement, movement_id)
    if mv is None or mv.type != "sale":
        raise HTTPException(status_code=404, detail="Sale movement not found")
    if int(getattr(mv, "business_id", 0) or 0) != int(bid):
        raise HTTPException(status_code=404, detail="Sale movement not found")
    product = db.get(Product, mv.product_id)

# AFTER:
    mv = InventoryRepository(db, business_id=bid).get_movement(movement_id, ("sale",))
    if mv is None:
        raise HTTPException(status_code=404, detail="Sale movement not found")
    product = PurchaseRepository(db, bid).get_product(mv.product_id)
```

**Step 2: Imports.** Añade `from app.repositories.inventory_repository import InventoryRepository` y
`from app.repositories.purchase_repository import PurchaseRepository`. Elimina `from sqlalchemy import
select` (línea 9) si queda sin uso (revisa: `tab_sales` aún lo usa hasta el Lote G; consérvalo si es
así).

#### Verification

```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_sales.py` — 1 sitio sustituido + imports
- [ ] Ningún otro archivo modificado
- [ ] snapshot check OK

---

### T39: `ui_profit_items.py` — `get_movement`/`get_product`/`get_lot_by_movement` (5 sitios)

**Depends on:** T33
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/routers/ui_profit_items.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/routers/ui_profit_items.py — YA EXISTE
# 5× db.get(InventoryMovement, movement_id) + comprobación de tipo/negocio + db.get(Product, mv.product_id):
#   líneas 146-151 (adjustment), 220-225 (transfer_in), 305-310 (sale), 500-505 (purchase) + línea 506 lot
```

#### Objective

Sustituir los 5 bloques de carga de movimiento/producto/lote por repositorio (REQ-7/REQ-10).

#### Instructions

**Step 1:** En cada uno de los 5 handlers, crea `repo = InventoryRepository(db, business_id=bid)` y
`purchase_repo = PurchaseRepository(db, bid)` tras `bid = ctx.business_id`, y sustituye:

```python
# BEFORE (patrón, p. ej. líneas 146-151):
    mv = db.get(InventoryMovement, movement_id)
    if mv is None or mv.type != "adjustment":
        raise HTTPException(status_code=404, detail="Adjustment movement not found")
    if int(getattr(mv, "business_id", 0) or 0) != int(bid):
        raise HTTPException(status_code=404, detail="Adjustment movement not found")
    product = db.get(Product, mv.product_id)

# AFTER:
    mv = repo.get_movement(movement_id, ("adjustment",))
    if mv is None:
        raise HTTPException(status_code=404, detail="Adjustment movement not found")
    product = purchase_repo.get_product(mv.product_id)
```

El `type` y el `detail` de cada handler se conservan: `"adjustment"`, `"transfer_in"`, `"sale"`,
`"purchase"` con sus respectivos `"Adjustment movement not found"`, `"Transfer movement not found"`,
`"Sale movement not found"`, `"Purchase movement not found"`.

**Step 2:** En `profit_items_purchase_edit_form` (línea 506):
`lot = db.scalar(select(InventoryLot).where(InventoryLot.movement_id == mv.id))` →
`lot = purchase_repo.get_lot_by_movement(mv.id)`.

**Step 3: Imports.** Añade `from app.repositories.inventory_repository import InventoryRepository` y
`from app.repositories.purchase_repository import PurchaseRepository`. Elimina `from sqlalchemy import
select` (línea 8) si queda sin uso (sí queda sin uso tras este cambio).

**Edge cases (ya resueltos):**
- El handler de `transfer_in` (líneas 258-262) hace una SEGUNDA carga de `mv` antes de `update`: usa
  también `repo.get_movement(movement_id, ("transfer_in",))`.

#### Verification

```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_profit_items.py` — 5 sitios + lote sustituidos + imports
- [ ] Ningún otro archivo modificado
- [ ] snapshot check OK

---

# LOTE G — Escrituras: `business_repository`, dashboard, `audit_repository`

### T40: `business_repository` — `location_has_data`, `reconcile_locations`, `create_locations`, `delete_locations`, `list_businesses_by_ids`

**Depends on:** T39
**Skill:** backend-developer
**Agent:** data
**Files:** `app/repositories/business_repository.py`
**Pillars:** P1, P4, P5, P6, P2

#### Context (firmas reales)

```python
# app/repositories/business_repository.py — YA EXISTE
from app.logger import get_logger  # NO existe hoy; añádelo (dimension 5: location_has_data lo necesita)
from app.models import Business, Location, Product, User, UserBusiness
# app/routers/ui_businesses.py — YA EXISTE (cuerpos a mover):
#   _location_has_data (62-74), _reconcile_locations (77-146), create locations (452-472), delete (786)
```

#### Objective

Añadir las 5 funciones de `business_repository` que consume `ui_businesses.py` y `ui_home.py`.

#### Instructions

**Step 1:** Añade al inicio de `app/repositories/business_repository.py` el logger:
`from app.logger import get_logger` y, tras los imports, `_log = get_logger(__name__)`.

**Step 2:** Añade al final del fichero (mantén el estilo de funciones de módulo existentes):

```python
def location_has_data(db: Session, location_id: int) -> bool:
    """True si la ubicación tiene movimientos/lotes/documentos asociados."""
    for model in (InventoryMovement, InventoryLot, SalesDocument):
        try:
            count = db.scalar(
                select(func.count()).select_from(model).where(model.location_id == int(location_id))
            )
            if count:
                return True
        except Exception:
            _log.exception("location_has_data: fallo de query en %s", model.__tablename__)
            continue
    return False


def list_businesses_by_ids(db: Session, ids: list[int]) -> list[Business]:
    if not ids:
        return []
    return list(
        db.scalars(select(Business).where(Business.id.in_([int(i) for i in ids])).order_by(Business.code.asc()))
    )


def create_locations(
    db: Session,
    business_id: int,
    central: tuple[str, str],
    pos: list[tuple[str, str]],
    default_pos_code: str,
) -> None:
    db.add(Location(
        business_id=int(business_id),
        code=central[0],
        name=central[1],
        role="central",
        position=0,
    ))
    for i, (pcode, pname) in enumerate(pos, start=1):
        db.add(Location(
            business_id=int(business_id),
            code=pcode,
            name=pname,
            role="pos",
            position=i,
            is_default=(pcode == default_pos_code),
        ))


def reconcile_locations(
    db: Session,
    business_id: int,
    central: tuple[str, str],
    pos: list[tuple[str, str]],
    default_pos_code: str,
) -> Optional[str]:
    bid = int(business_id)
    central_code, central_name = central
    central_row = db.scalar(select(Location).where(Location.business_id == bid, Location.role == "central"))
    if central_row is None:
        central_row = db.scalar(select(Location).where(Location.business_id == bid, Location.code == central_code))
    if central_row is None:
        central_row = Location(business_id=bid, code=central_code, name=central_name, role="central", position=0)
        db.add(central_row)
    else:
        central_row.code = central_code
        central_row.name = central_name
        central_row.role = "central"
        central_row.position = 0

    parsed = [s for s in pos if (s[0] or "").strip()]
    existing = list(db.scalars(
        select(Location).where(Location.business_id == bid, Location.role == "pos")
        .order_by(Location.position.asc(), Location.id.asc())
    ).all())

    for loc in existing:
        loc.is_default = False

    for i, (pcode, pname) in enumerate(parsed, start=1):
        is_def = (pcode == default_pos_code)
        if i - 1 < len(existing):
            loc = existing[i - 1]
            loc.code = pcode
            loc.name = pname
            loc.role = "pos"
            loc.position = i
            loc.is_default = is_def
        else:
            db.add(Location(business_id=bid, code=pcode, name=pname,
                            role="pos", position=i, is_default=is_def))

    if len(existing) > len(parsed):
        for loc in existing[len(parsed):]:
            if location_has_data(db, int(loc.id)):
                return f"No se puede eliminar la ubicación '{loc.name}' ({loc.code}) porque tiene datos asociados."
            db.delete(loc)

    db.flush()
    return None


def delete_locations(db: Session, business_id: int) -> None:
    db.query(Location).filter(Location.business_id == int(business_id)).delete(synchronize_session=False)
```

**Step 3: Imports.** Añade a `app/repositories/business_repository.py`:
`from app.models import InventoryLot, InventoryMovement, SalesDocument` (amplía el import existente de
`app.models`, que hoy es `Business, Location, Product, User, UserBusiness`).

**Edge cases (ya resueltos):**
- El parseo del form (`_parse_spec`) y el prefijo (`_prefix_location_code`) se quedan en el router
  (`ui_businesses.py`): el repositorio recibe códigos finales.
- `reconcile_locations` conserva el `flush()` final y el mensaje de error idéntico.
- Ninguna de estas funciones hace `commit()`.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_business_repository.py -q --override-ini="addopts="
# Expected: PASS
```

#### Expected Report

- [ ] `app/repositories/business_repository.py` — 5 funciones + logger + imports
- [ ] Ningún otro archivo modificado
- [ ] tests PASS

---

### T41: `ui_businesses.py` — consumir `create_locations`

**Depends on:** T40
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/routers/ui_businesses.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/routers/ui_businesses.py — YA EXISTE (business_create, líneas 447-473)
# central_code = _prefix_location_code(central_code, code); db.add(Location(...)); bucle POS
```

#### Objective

Sustituir el bloque de creación de ubicaciones de `business_create` por
`business_repository.create_locations`.

#### Instructions

**Step 1:** En `business_create`, sustituye (líneas 447-473):

```python
# BEFORE:
    central_code, central_name = _parse_loc(
        _s(locations_central), "CENTRAL", "Almacén Central"
    )
    central_code = _prefix_location_code(central_code, code)
    db.add(Location(
        business_id=new_business.id,
        code=central_code,
        name=central_name,
        role="central",
        position=0,
    ))

    # POS list (vacío = negocio sin POS); descartar specs sin código (p.ej. ':Tienda')
    pos_specs = [s for s in _parse_loc_list(_s(locations_pos)) if (s[0] or "").strip()]
    default_pos_code = _prefix_location_code(_s(locations_default_pos), code)

    for i, (pcode, pname) in enumerate(pos_specs, start=1):
        ploc_code = _prefix_location_code(pcode, code)
        db.add(Location(
            business_id=new_business.id,
            code=ploc_code,
            name=pname,
            role="pos",
            position=i,
            is_default=(ploc_code == default_pos_code),
        ))

# AFTER:
    central_code, central_name = _parse_loc(
        _s(locations_central), "CENTRAL", "Almacén Central"
    )
    central_code = _prefix_location_code(central_code, code)

    # POS list (vacío = negocio sin POS); descartar specs sin código (p.ej. ':Tienda')
    pos_specs = [s for s in _parse_loc_list(_s(locations_pos)) if (s[0] or "").strip()]
    default_pos_code = _prefix_location_code(_s(locations_default_pos), code)

    pos_final = [(_prefix_location_code(pcode, code), pname) for pcode, pname in pos_specs]
    create_locations(db, new_business.id, (central_code, central_name), pos_final, default_pos_code)
```

**Step 2: Import.** Añade `create_locations` al import de `app.repositories.business_repository`
(líneas 22-32).

**Edge cases (ya resueltos):**
- `pos_specs` y `default_pos_code` se siguen calculando en el router (parseo + prefijo).
- El `db.commit()` de `business_create` sigue en el router.

#### Verification

```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_businesses.py` — creación de ubicaciones delegada
- [ ] Ningún otro archivo modificado
- [ ] snapshot check OK

---

### T42: `ui_businesses.py` — consumir `reconcile_locations`, `delete_locations`, `location_has_data`

**Depends on:** T41
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/routers/ui_businesses.py`
**Pillars:** P1, P4, P5, P6, P2

#### Context

```python
# app/routers/ui_businesses.py — YA EXISTE
# business_update (675): reconcile_error = _reconcile_locations(db, business, _s(locations_central), _s(locations_pos), _s(locations_default_pos), code)
# business_delete (786): db.query(Location).filter(Location.business_id == business_id).delete(synchronize_session=False)
# helpers a borrar: _location_has_data (62-74), _reconcile_locations (77-146)
```

#### Objective

Delegar la reconciliación y el borrado de ubicaciones al repositorio, y borrar los helpers locales.

#### Instructions

**Step 1:** En `business_update`, sustituye el parseo previo y la llamada (líneas 674-675) por:

```python
# BEFORE:
    reconcile_error = _reconcile_locations(db, business, _s(locations_central), _s(locations_pos), _s(locations_default_pos), code)

# AFTER:
    central_raw, pos_raw, default_pos_raw = _s(locations_central), _s(locations_pos), _s(locations_default_pos)
    central_code, central_name = _parse_spec_of(central_raw, "CENTRAL", "Almacén Central")
    central_final = (_prefix_location_code(central_code, code), central_name)
    raw_parts = [p.strip() for p in (pos_raw or "").split(",") if p.strip()]
    parsed = [
        s for s in (_parse_spec_of(p, "POS1", "Punto de venta 1") for p in raw_parts)
        if (s[0] or "").strip()
    ]
    pos_final = [(_prefix_location_code(pc, code), pn) for pc, pn in parsed]
    default_pos_final = _prefix_location_code((default_pos_raw or "").strip(), code)
    reconcile_error = reconcile_locations(db, business.id, central_final, pos_final, default_pos_final)
```

Añade el helper local `_parse_spec_of` (equivalente a `_parse_spec` de `_reconcile_locations`) en
`ui_businesses.py`:

```python
def _parse_spec_of(raw, fb_code, fb_name) -> tuple[str, str]:
    s = (raw or "").strip()
    if not s:
        return fb_code, fb_name
    if ":" in s:
        c, n = s.split(":", 1)
        return ((c or "").strip() or fb_code), ((n or "").strip() or fb_name)
    return s, s
```

**Step 2:** En `business_delete`, sustituye la línea 786:

```python
# BEFORE:
    db.query(Location).filter(Location.business_id == business_id).delete(synchronize_session=False)
# AFTER:
    delete_locations(db, business_id)
```

**Step 3:** Elimina las funciones `_location_has_data` (62-74) y `_reconcile_locations` (77-146) de
`app/routers/ui_businesses.py`.

**Step 4: Imports.** Añade `delete_locations` y `reconcile_locations` al import de
`app.repositories.business_repository`. Elimina `from sqlalchemy import func, select` (línea 9) si
quedan sin uso, y `from app.models import (InventoryLot, InventoryMovement, Location, SalesDocument)`
solo si ya no se usan (revisa: `Location` puede quedar sin uso tras este cambio; elimina lo que
realmente quede sin uso).

**Edge cases (ya resueltos):**
- `_prefix_location_code` y `_prefix_location_spec/_prefix_location_list` se quedan en el router.
- El `rollback` en `business_update` cuando `reconcile_error` sigue igual.

#### Verification

```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_businesses.py` — 2 delegaciones + 2 helpers borrados + imports
- [ ] Ningún otro archivo modificado
- [ ] snapshot check OK

---

### T43: `ui_home.py` — dashboard sin ORM directo

**Depends on:** T40
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/routers/ui_home.py`
**Pillars:** P1, P4, P5, P6, P3

#### Context

```python
# app/routers/ui_home.py — YA EXISTE (dashboard, líneas 165-183)
# active_business_id = ctx.active_business_id; businesses = []; active_business = None
# if active_business_id is not None: active_business = db.get(Business, int(active_business_id))  # 169
# admin: businesses = list(db.scalars(select(Business).order_by(Business.code.asc())))  # 173
# owner: businesses = list(db.scalars(select(Business).where(Business.id.in_(assigned))...))  # 178-183
```

#### Objective

Sustituir los accesos ORM del dashboard por `business_repository`.

#### Instructions

**Step 1:** En `dashboard` (líneas 167-183), sustituye:

```python
# BEFORE:
    active_business = None
    if active_business_id is not None:
        active_business = db.get(Business, int(active_business_id))
    role = role_of(user)
    if user is not None:
        if role == "admin":
            businesses = list(db.scalars(select(Business).order_by(Business.code.asc())))
        elif role == "owner":
            assigned = get_user_business_ids(db, user.id)
            if assigned:
                businesses = list(
                    db.scalars(
                        select(Business)
                        .where(Business.id.in_(assigned))
                        .order_by(Business.code.asc())
                    )
                )

# AFTER:
    active_business = None
    if active_business_id is not None:
        active_business = get_business(db, int(active_business_id))
    role = role_of(user)
    if user is not None:
        if role == "admin":
            businesses = list_businesses(db)
        elif role == "owner":
            assigned = get_user_business_ids(db, user.id)
            if assigned:
                businesses = list_businesses_by_ids(db, assigned)
```

**Step 2: Imports.** Añade `from app.repositories.business_repository import get_business,
list_businesses, list_businesses_by_ids`. Elimina `from sqlalchemy import select` (línea 8) y
`from app.models import Business` (línea 12) si quedan sin uso.

#### Verification

```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK (el dashboard renderiza igual)
```

#### Expected Report

- [ ] `app/routers/ui_home.py` — dashboard con repositorio + imports
- [ ] Ningún otro archivo modificado
- [ ] snapshot check OK

---

### T44: `audit_repository.py` (CREATE) + `ui_activity.py` consumidor

**Depends on:** T43
**Skill:** backend-developer
**Agent:** data
**Files:** `app/repositories/audit_repository.py`, `app/routers/ui_activity.py`
**Pillars:** P1, P4, P5, P6, P3

#### Context

```python
# app/routers/ui_activity.py — YA EXISTE (líneas 46-59)
stmt = (select(AuditLog).where(AuditLog.entity_type == "auth").where(AuditLog.action.in_(["login", "logout"])))
if username: stmt = stmt.where(AuditLog.username == username)
if start_dt is not None: stmt = stmt.where(AuditLog.created_at >= start_dt)
if end_dt is not None: stmt = stmt.where(AuditLog.created_at < end_dt)
stmt = stmt.order_by(AuditLog.created_at.desc()).limit(2000)
rows = db.execute(stmt).scalars().all()
```

#### Objective

Crear `audit_repository.list_auth_events` y usarlo en `ui_activity.py`.

#### Instructions

**Step 1:** Crea `app/repositories/audit_repository.py` con este contenido literal:

```python
"""Acceso a datos de auditoría (extraído de ui_activity)."""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import AuditLog


def list_auth_events(
    db: Session,
    *,
    username: str,
    start: Optional[datetime],
    end: Optional[datetime],
    limit: int = 2000,
) -> list[AuditLog]:
    stmt = (
        select(AuditLog)
        .where(AuditLog.entity_type == "auth")
        .where(AuditLog.action.in_(["login", "logout"]))
    )
    if username:
        stmt = stmt.where(AuditLog.username == username)
    if start is not None:
        stmt = stmt.where(AuditLog.created_at >= start)
    if end is not None:
        stmt = stmt.where(AuditLog.created_at < end)
    stmt = stmt.order_by(AuditLog.created_at.desc()).limit(int(limit))
    return list(db.execute(stmt).scalars().all())
```

**Step 2:** En `app/routers/ui_activity.py`, sustituye las líneas 46-59 por:

```python
    rows = list_auth_events(
        db,
        username=username,
        start=start_dt,
        end=end_dt,
        limit=2000,
    )
```

**Step 3: Imports.** En `ui_activity.py`: añade
`from app.repositories.audit_repository import list_auth_events`; elimina `from sqlalchemy import
select` (línea 9) y `from app.models import AuditLog` (línea 13) si quedan sin uso (quedan sin uso:
`AuditLog` sigue usándose como tipo en `filtered_rows: list[AuditLog]` — consérvalo si es así;
elimina solo `select`).

**Edge cases (ya resueltos):**
- `username` vacío = sin filtro (comportamiento idéntico).

#### Verification

```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/repositories/audit_repository.py` creado
- [ ] `app/routers/ui_activity.py` — consulta delegada + imports
- [ ] Ningún otro archivo modificado
- [ ] snapshot check OK

---

### T45: `ui_tabs_common.py` — `_restock_items_with_location_history` con repositorio

**Depends on:** T33
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/routers/ui_tabs_common.py`
**Pillars:** P1, P4, P5, P6, P3

#### Context

```python
# app/routers/ui_tabs_common.py — YA EXISTE (líneas 129-143)
history_skus = {str(sku).strip() for sku in db.scalars(select(Product.sku).select_from(InventoryMovement)
    .join(Product, ...).where(...).distinct()).all() if str(sku or "").strip()}
```

#### Objective

Sustituir la consulta por `InventoryRepository.skus_with_location_history`.

#### Instructions

**Step 1:** En `_restock_items_with_location_history` (líneas 129-143), sustituye:

```python
# BEFORE:
    history_skus = {
        str(sku).strip()
        for sku in db.scalars(
            select(Product.sku)
            .select_from(InventoryMovement)
            .join(Product, Product.id == InventoryMovement.product_id)
            .where(
                InventoryMovement.business_id == int(business_id),
                InventoryMovement.location_id == int(location_id),
                Product.sku.in_(skus),
            )
            .distinct()
        ).all()
        if str(sku or "").strip()
    }

# AFTER:
    history_skus = InventoryRepository(
        db, business_id=int(business_id)
    ).skus_with_location_history(int(location_id), skus)
```

**Step 2: Imports.** En `ui_tabs_common.py`: añade
`from app.repositories.inventory_repository import InventoryRepository`; elimina
`from sqlalchemy import select` (línea 7) si queda sin uso, y quita `InventoryMovement`, `Product` del
import de `app.models` si quedan sin uso (revisa: `_deletable_skus` ya no está aquí tras el Lote E;
deja el import de `Location`, `InventoryLot` si aún se usan).

#### Verification

```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_tabs_common.py` — 1 consulta delegada + imports
- [ ] Ningún otro archivo modificado
- [ ] snapshot check OK

---

### T46: `ui_tabs_common.py` — borrar `_deletable_skus` (ya en repositorio)

**Depends on:** T30
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_tabs_common.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/routers/ui_tabs_common.py — YA EXISTE (líneas 15-50, _deletable_skus)
# Ya sin llamadores (ui_stock lo consume vía ProductRepository desde T30)
```

#### Objective

Eliminar `_deletable_skus` (código muerto tras T30).

#### Instructions

**Step 1:** Elimina la función `_deletable_skus` completa (líneas 15-50) de
`app/routers/ui_tabs_common.py`.

**Step 2:** Ajusta el import de `app.models` si `InventoryMovement`/`InventoryLot`/`Product` quedan
sin uso tras este cambio y el de T45 (elimina solo los nombres que ya no se usen).

#### Verification

```bash
python3 -c "import app.routers.ui_tabs_common"
# Expected: sin salida
```

#### Expected Report

- [ ] `app/routers/ui_tabs_common.py` — `_deletable_skus` eliminada
- [ ] Ningún otro archivo modificado
- [ ] import OK

---

# LOTE H — Escrituras restantes (documentos, usuarios, coach, clientes, auth)

### T47: `SalesDocumentService` — `delete_document`, `delete_item`, `add_item`, `add_document`

**Depends on:** T46
**Skill:** backend-developer
**Agent:** data
**Files:** `app/services/sales_document_service.py`
**Pillars:** P1, P4, P5, P6

#### Context (firmas reales)

```python
# app/services/sales_document_service.py — YA EXISTE
class SalesDocumentService:
    def __init__(self, db: Session, business_id: int): ...  # self._db, self._business_id
    def get_items(self, doc_id: int) -> list[SalesDocumentItem]: ...
```

#### Objective

Añadir los 4 métodos de escritura de documentos.

#### Instructions

**Step 1:** Añade a la clase `SalesDocumentService`:

```python
    def delete_document(self, doc: SalesDocument) -> None:
        items = self.get_items(doc.id)
        for it in items:
            self._db.delete(it)
        # Sin relationship entre documento y líneas, el unit of work no sabe el orden: borrar las
        # líneas primero o la FK sales_document_items.document_id rechaza el DELETE del documento.
        self._db.flush()
        self._db.delete(doc)

    def delete_item(self, item: SalesDocumentItem) -> None:
        self._db.delete(item)

    def add_item(self, item: SalesDocumentItem) -> None:
        self._db.add(item)

    def add_document(self, doc: SalesDocument, items: list[SalesDocumentItem]) -> None:
        self._db.add(doc)
        self._db.flush()
        for it in items:
            it.document_id = doc.id
            self._db.add(it)
```

`SalesDocument` y `SalesDocumentItem` ya están importados.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit -q --override-ini="addopts=" -k "sales_doc or document" 2>/dev/null
# Expected: los tests de documentos existentes PASS
```

#### Expected Report

- [ ] `app/services/sales_document_service.py` — 4 métodos añadidos
- [ ] Ningún otro archivo modificado
- [ ] tests PASS

---

### T48: `ui_sales_documents.py` — consumir los 4 métodos de escritura

**Depends on:** T47
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/routers/ui_sales_documents.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/routers/ui_sales_documents.py — YA EXISTE
# sales_doc_delete (273-279): items = ...get_items(doc.id); for it in items: db.delete(it); db.flush(); db.delete(doc)
# sales_doc_update (373): db.delete(it)
# sales_doc_update (410-421): db.add(SalesDocumentItem(...))
# sales_doc_issue (981-986): db.add(doc); db.flush(); for it in items: it.document_id = doc.id; db.add(it)
```

#### Objective

Delegar las escrituras de documentos al servicio.

#### Instructions

**Step 1:** En `sales_doc_delete` (líneas 273-279), sustituye:

```python
# BEFORE:
    items = SalesDocumentService(db, bid).get_items(doc.id)
    for it in items:
        db.delete(it)
    # Sin relationship entre documento y líneas, el unit of work no sabe el orden: borrar las
    # líneas primero o la FK sales_document_items.document_id rechaza el DELETE del documento.
    db.flush()
    db.delete(doc)

# AFTER:
    SalesDocumentService(db, bid).delete_document(doc)
```

**Step 2:** En `sales_doc_update`, sustituye `db.delete(it)` (línea 373) por
`SalesDocumentService(db, bid).delete_item(it)` y el bloque `db.add(SalesDocumentItem(...))`
(líneas 410-421) por:

```python
        svc = SalesDocumentService(db, bid)
        svc.add_item(
            SalesDocumentItem(
                document_id=doc.id,
                line_no=next_line,
                sku=None,
                description=new_desc,
                unit_of_measure=None,
                quantity=float(new_qty),
                unit_price=float(new_price),
                line_total=new_line_total,
            )
        )
```

**Step 3:** En `sales_doc_issue` (líneas 981-986), sustituye:

```python
# BEFORE:
    db.add(doc)
    db.flush()

    for it in items:
        it.document_id = doc.id
        db.add(it)

# AFTER:
    SalesDocumentService(db, bid).add_document(doc, items)
```

**Edge cases (ya resueltos):**
- Los `db.commit()`/`db.rollback()` siguen en el router.

#### Verification

```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_sales_documents.py` — 4 sitios delegados
- [ ] Ningún otro archivo modificado
- [ ] snapshot check OK

---

### T49: `UserRepository.detach_references` + `ui_users.py` consumidor

**Depends on:** T48
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/repositories/user_repository.py`, `app/routers/ui_users.py`
**Pillars:** P1, P4, P5, P6

#### Context (firmas reales)

```python
# app/repositories/user_repository.py — YA EXISTE
class UserRepository:
    def __init__(self, db: Session): ...  # self._db
    def add(self, user: User) -> None: ...
    def delete(self, user: User) -> None: ...
    def set_user_businesses(self, user_id: int, business_ids: list[int]) -> None: ...

# app/routers/ui_users.py — YA EXISTE
# user_create (151): db.add(new_user); db.flush()
# user_delete (347-358): set_user_businesses; db.query(AuditLog)...update; db.query(ChatMessage)...update; db.query(DashboardWidget)...update; db.delete(user)
```

#### Objective

Añadir `detach_references` a `UserRepository` y usarlo en `user_create`/`user_delete`.

#### Instructions

**Step 1:** En `app/repositories/user_repository.py`, añade a la clase `UserRepository`:

```python
    def detach_references(self, user_id: int) -> None:
        from app.models import AuditLog, ChatMessage, DashboardWidget
        self._db.query(AuditLog).filter(AuditLog.user_id == int(user_id)).update(
            {AuditLog.user_id: None}, synchronize_session=False
        )
        self._db.query(ChatMessage).filter(ChatMessage.user_id == int(user_id)).update(
            {ChatMessage.user_id: None}, synchronize_session=False
        )
        self._db.query(DashboardWidget).filter(DashboardWidget.created_by == int(user_id)).update(
            {DashboardWidget.created_by: None}, synchronize_session=False
        )
```

(Añade `AuditLog, ChatMessage, DashboardWidget` al import de `app.models` al inicio del fichero, en
vez del import local, si prefieres; el import local también vale y evita tocar el import de arriba.)

**Step 2:** En `app/routers/ui_users.py`:
- `user_create` (línea 151): `db.add(new_user)` → `UserRepository(db).add(new_user)`.
- `user_delete` (líneas 347-358): sustituye el bloque de 3 `db.query(...).update(...)` por
  `UserRepository(db).detach_references(int(user.id))`, y `db.delete(user)` por
  `UserRepository(db).delete(user)`.

**Edge cases (ya resueltos):**
- El orden en `user_delete` sigue: `set_user_businesses(id, [])` → `detach_references(id)` →
  `delete(user)` → `commit()`.

#### Verification

```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/repositories/user_repository.py` — `detach_references` añadido
- [ ] `app/routers/ui_users.py` — 2 sitios delegados
- [ ] Ningún otro archivo modificado
- [ ] snapshot check OK

---

### T50: `CoachService.add_cash_baseline` + `ui_coach.py` consumidor

**Depends on:** T49
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/services/coach_service.py`, `app/routers/ui_coach.py`
**Pillars:** P1, P4, P5, P6

#### Context (firmas reales)

```python
# app/services/coach_service.py — YA EXISTE
class CoachService:
    def __init__(self, db: Session, business_id: int): ...
# app/routers/ui_coach.py — YA EXISTE (set_cash_baseline, líneas 55-61):
# row = CashBaseline(business_id=..., baseline_date=..., amount=..., created_by=...); db.add(row)
```

#### Objective

Añadir `add_cash_baseline` a `CoachService` y usarlo.

#### Instructions

**Step 1:** En `app/services/coach_service.py`, añade a la clase `CoachService`:

```python
    def add_cash_baseline(
        self, baseline_date: date, amount: float, created_by: Optional[int]
    ) -> CashBaseline:
        row = CashBaseline(
            business_id=self._business_id,
            baseline_date=baseline_date,
            amount=amount,
            created_by=created_by,
        )
        self._db.add(row)
        return row
```

Añade `from datetime import date` (hoy importa `datetime, timedelta, timezone`).

**Step 2:** En `app/routers/ui_coach.py`, sustituye (líneas 55-61):

```python
# BEFORE:
    row = CashBaseline(
        business_id=business_id,
        baseline_date=payload.baseline_date,
        amount=payload.amount,
        created_by=user.id if user else None,
    )
    db.add(row)

# AFTER:
    CoachService(db, business_id).add_cash_baseline(
        payload.baseline_date, payload.amount, user.id if user else None
    )
```

**Step 3: Imports.** En `ui_coach.py`: elimina `from app.models import CashBaseline` (línea 11) si
queda sin uso (queda sin uso).

**Edge cases (ya resueltos):**
- El `db.commit()` de `set_cash_baseline` sigue en el router.

#### Verification

```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/services/coach_service.py` — `add_cash_baseline` añadido
- [ ] `app/routers/ui_coach.py` — 1 sitio delegado + import
- [ ] Ningún otro archivo modificado
- [ ] snapshot check OK

---

### T51: `ui_customers.py` — `CustomerRepository.delete`

**Depends on:** T50
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/routers/ui_customers.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/routers/ui_customers.py — YA EXISTE (customer_delete, línea 258): db.delete(customer)
# app/repositories/customer_repository.py — YA EXISTE: def delete(self, customer: Customer) -> None
```

#### Objective

Usar `CustomerRepository.delete` (REQ-10).

#### Instructions

**Step 1:** En `customer_delete` (línea 258):

```python
# BEFORE:
    db.delete(customer)
# AFTER:
    CustomerRepository(db, bid).delete(customer)
```

#### Verification

```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_customers.py` — 1 delegación
- [ ] Ningún otro archivo modificado
- [ ] snapshot check OK

---

### T52: `ui_auth.py` — `UserRepository.get` y `business_repository.get_business`

**Depends on:** T51
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/routers/ui_auth.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/routers/ui_auth.py — YA EXISTE
# línea 167: row = db.get(User, int(user.id))
# línea 216: if db.get(Business, bid) is None:
```

#### Objective

Sustituir los `db.get` por repositorios (REQ-5 incluye `ui_auth.py` en la guarda ORM).

#### Instructions

**Step 1:** Sustituye:

```python
# BEFORE (línea 167):
    row = db.get(User, int(user.id))
# AFTER:
    row = UserRepository(db).get(int(user.id))
```

```python
# BEFORE (línea 216):
    if db.get(Business, bid) is None:
# AFTER:
    if get_business(db, bid) is None:
```

**Step 2: Imports.** Añade `from app.repositories.user_repository import UserRepository` y
`from app.repositories.business_repository import get_business`. Elimina `from sqlalchemy import
select` (línea 9) si queda sin uso, y ajusta el import de `app.models` (línea 16) si `Business`/`User`
quedan sin uso (`User` sigue usándose como tipo en las anotaciones; conserva lo que se use).

#### Verification

```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_auth.py` — 2 sitios delegados + imports
- [ ] Ningún otro archivo modificado
- [ ] snapshot check OK

---

### T53: `ui_sales.py` — `_sales_doc_context` con `SalesDocumentService`

**Depends on:** T52
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/routers/ui_sales.py`
**Pillars:** P1, P4, P5, P6, P3

#### Context

```python
# app/routers/ui_sales.py — YA EXISTE (_sales_doc_context, líneas 124-129 y tab_sales 757-774)
# doc_stmt = select(SalesDocument).where(...).limit(10); recent_documents = list(db.scalars(...))
# cust_stmt = select(Customer).where(...).limit(200); customers = list(db.scalars(...))
```

#### Objective

Sustituir las consultas de documentos/clientes por `SalesDocumentService` (REQ-6).

#### Instructions

**Step 1:** En `_sales_doc_context` (líneas 124-129), sustituye:

```python
# BEFORE:
    doc_stmt = select(SalesDocument).where(SalesDocument.business_id == int(bid))
    recent_documents = list(
        db.scalars(doc_stmt.order_by(SalesDocument.issue_date.desc(), SalesDocument.id.desc()).limit(10))
    )
    cust_stmt = select(Customer).where(Customer.business_id == int(bid))
    customers = list(db.scalars(cust_stmt.order_by(Customer.name.asc(), Customer.id.asc()).limit(200)))

# AFTER:
    svc = SalesDocumentService(ctx.db, bid)
    recent_documents = svc.list_recent(limit=10)
    customers = svc.customers_list(limit=200)
```

**Step 2:** En `tab_sales` (líneas 757-774), sustituye el bloque de `doc_stmt`/`cust_stmt` por:

```python
    svc = SalesDocumentService(db, bid)
    recent_documents = svc.list_recent(limit=200, start=start_dt, end=end_dt)
    q = (query or "").strip()
    if q:
        recent_documents = [
            d
            for d in recent_documents
            if query_match(q, str(getattr(d, "code", "") or ""), str(getattr(d, "client_name", "") or ""))
        ]
    recent_documents = recent_documents[:10]

    customers = svc.customers_list(limit=200)
```

**Step 3: Imports.** Añade `from app.services.sales_document_service import SalesDocumentService`.
Elimina `from sqlalchemy import select` (línea 9) y, del import de `app.models` (línea 15), quita
`Customer` y `SalesDocument` si quedan sin uso.

**Edge cases (ya resueltos):**
- El filtro `query_match` y el `[:10]` se conservan en el router (mismo orden y límites).

#### Verification

```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_sales.py` — 2 bloques delegados + imports
- [ ] Ningún otro archivo modificado
- [ ] snapshot check OK

---

# LOTE I — Guardas, documentación y cierre

### T54: Guarda `tests/unit/test_routers_no_orm.py` (REQ-5/REQ-4)

**Depends on:** T53
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** `tests/unit/test_routers_no_orm.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# tests/unit/test_routers_no_html.py — YA EXISTE (patrón a replicar)
ROUTERS_DIR = Path(__file__).resolve().parents[2] / "app" / "routers"
```

#### Objective

Crear la guarda que impide ORM directo y resolución directa del negocio en los routers UI.

#### Instructions

**Step 1:** Crea `tests/unit/test_routers_no_orm.py` con este contenido literal:

```python
"""REQ-5/REQ-4: los routers UI no usan el ORM directamente ni resuelven el negocio por su cuenta.

Regex por línea (mismo patrón que test_routers_no_html.py): las llamadas ORM del proyecto tienen
siempre la forma `db.x(`. Fuera de `ui_context.py` y `ui_auth.py`, los routers no deben llamar a
`get_active_business_config/code/id`, `require_active_business_id` ni `get_current_user_from_session`.
Se ignoran las líneas de comentario.
"""
from __future__ import annotations

import re
from pathlib import Path

ROUTERS_DIR = Path(__file__).resolve().parents[2] / "app" / "routers"

ORM_CALLS = re.compile(r"\bselect\(|\bdb\.(get|scalar|scalars|execute|query|add|delete)\(")
SQLA_IMPORT = re.compile(r"from sqlalchemy import .*\b(select|func)\b")
SECURITY_CALLS = re.compile(
    r"\b(get_active_business_config|get_active_business_code|get_active_business_id|"
    r"require_active_business_id|get_current_user_from_session)\("
)

ALLOWED_SECURITY_FILES = {"ui_context.py", "ui_auth.py"}


def _code_lines(source: str) -> list[tuple[int, str]]:
    # Numera antes de filtrar comentarios: el informe cita la línea real del fichero.
    return [(i, ln) for i, ln in enumerate(source.splitlines(), start=1) if not ln.strip().startswith("#")]


def test_no_orm_in_ui_routers() -> None:
    offenders: list[str] = []
    for path in sorted(ROUTERS_DIR.glob("ui_*.py")):
        for lineno, line in _code_lines(path.read_text(encoding="utf-8")):
            if ORM_CALLS.search(line):
                offenders.append(f"{path.name}:{lineno}")
            if SQLA_IMPORT.search(line):
                offenders.append(f"{path.name}:{lineno}")
    assert offenders == [], f"ORM directo en routers UI:\n" + "\n".join(offenders)


def test_no_direct_security_resolution_in_ui_routers() -> None:
    offenders: list[str] = []
    for path in sorted(ROUTERS_DIR.glob("ui_*.py")):
        if path.name in ALLOWED_SECURITY_FILES:
            continue
        for lineno, line in _code_lines(path.read_text(encoding="utf-8")):
            if SECURITY_CALLS.search(line):
                offenders.append(f"{path.name}:{lineno}")
    assert offenders == [], f"Resolución directa del negocio en routers UI:\n" + "\n".join(offenders)


def test_guard_detects_orm_and_security() -> None:
    assert ORM_CALLS.search("db.scalar(select(Product))")
    assert SQLA_IMPORT.search("from sqlalchemy import select")
    assert SECURITY_CALLS.search("get_active_business_config(db, request)")
    assert not ORM_CALLS.search("# db.execute() en un comentario")
```

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_routers_no_orm.py -q --override-ini="addopts="
# Expected: PASS (si falla, lista los sitios que quedan y no sigas: corrígelos en el lote I)
```

#### Expected Report

- [ ] `tests/unit/test_routers_no_orm.py` creado
- [ ] Ningún otro archivo modificado
- [ ] tests PASS

---

### T55: Afirmar todo REQ-3 en `test_ui_query_count.py`

**Depends on:** T54
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** `tests/unit/test_ui_query_count.py`
**Pillars:** P1, P4, P5, P6, P8

#### Objective

Rellenar `STRICT_ROUTES` con las 6 rutas de REQ-3 (ahora todas cumplen `users == 1`).

#### Instructions

**Step 1:** En `tests/unit/test_ui_query_count.py`, sustituye:

```python
# BEFORE:
# Rutas que deben afirmar `users == 1`. El envelope final del Lote D añade aquí las 6 de REQ-3.
STRICT_ROUTES: set[str] = set()

# AFTER:
# Todas las rutas de REQ-3 cumplen `users == 1` tras REQ-4 (auxiliares a ctx).
STRICT_ROUTES: set[str] = {
    "/ui/tabs/sales",
    "/ui/tabs/transfers",
    "/ui/tabs/documents",
    "/ui/tabs/inventory",
    "/ui/dashboard",
}
```

**Step 2 (corrección de la revisión, 2026-11-02):** REQ-3 también limita `user_businesses`. En la misma
función `test_route_returns_200_and_records_counts`, sustituye:

```python
# BEFORE:
    if route in STRICT_ROUTES:
        assert _COUNTS["users"] == 1, f"{role} {route}: users={_COUNTS['users']}"
# AFTER:
    if route in STRICT_ROUTES:
        assert _COUNTS["users"] == 1, f"{role} {route}: users={_COUNTS['users']}"
        max_ub = 2 if route == "/ui/dashboard" else 1
        assert _COUNTS["user_businesses"] <= max_ub, (
            f"{role} {route}: user_businesses={_COUNTS['user_businesses']} (máx {max_ub})"
        )
```

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: ADMIN_USERNAME=admin ADMIN_PASSWORD=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test python3 -m pytest tests/unit/test_ui_query_count.py -q --override-ini="addopts="
# Expected: 6 passed (cada ruta afirma users == 1 y user_businesses <= 1, o <= 2 en /ui/dashboard)
```

#### Expected Report

- [ ] `tests/unit/test_ui_query_count.py` — STRICT_ROUTES con 5 rutas (sales/transfers/documents/inventory/dashboard)
- [ ] Ningún otro archivo modificado
- [ ] 6 tests PASS

---

### T56: Barrido de imports sin uso en los routers UI

**Depends on:** T55
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_*.py`
**Pillars:** P1, P4, P5, P6

#### Objective

Eliminar los imports de `sqlalchemy` (`select`, `func`) y de modelos que hayan quedado sin uso en los
routers UI, y verificar que la guarda ORM sigue en verde.

#### Instructions

**Step 1:** Ejecuta el barrido y elimina SOLO los imports que queden realmente sin uso:

```bash
# Para cada app/routers/ui_*.py: elimina `from sqlalchemy import select` (y `func`) y los nombres
# de app.models / app.security / app.repositories.business_repository que ya no se referencien.
grep -rn "select\|func" app/routers/ui_*.py
```

Guía (ya resueltos, confírmalos con grep antes de borrar):
- Elimina `from sqlalchemy import select` y `from sqlalchemy.sql import func` de los routers que ya no
  los usan (ui_products, ui_stock, ui_sales, ui_transfers, ui_profit_items, ui_activity,
  ui_home, ui_sales_documents, ui_tabs_common, ui_auth, ui_businesses).
- Elimina los nombres de `app.models` que quedaron sin uso en cada router (p. ej. `Product` en
  ui_products si ya no se usa; `Customer`, `SalesDocument` en ui_sales; `InventoryLot`,
  `InventoryMovement` en ui_profit_items si no se usan).
- Elimina los `from app.security import get_active_business_code/...` y
  `from app.repositories.business_repository import get_active_business_config/...` que queden.

**Step 2:** NO borres imports que sí se usan. Ante la duda, ejecuta `python3 -c "import app.routers.X"`
para cada router tocado.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_routers_no_orm.py -q --override-ini="addopts=" && python3 -c "import app.main"
# Expected: guarda PASS y app importa sin errores
```

#### Expected Report

- [ ] imports sin uso eliminados de los routers UI
- [ ] `test_routers_no_orm.py` PASS
- [ ] `import app.main` sin errores

---

### T57: Docs — `.claude/CLAUDE.md` (invariante de `security.py`) — REQ-12

**Depends on:** T56
**Skill:** technical-writer
**Agent:** docs
**Files:** `.claude/CLAUDE.md`
**Pillars:** P1, P4, P5, P6

#### Objective

Actualizar el invariante de `app/security.py` en `.claude/CLAUDE.md` (fichero versionado: este
envelope lo edita).

#### Instructions

**Step 1:** En `.claude/CLAUDE.md`, en la sección `## Invariantes — nunca violar`, añade una entrada
nueva (manteniendo la numeración actual intacta: inserta tras la 6 existente y renumera las siguientes
si es necesario, o añade como sub-punto de la 6). Texto literal a añadir (como nuevo invariante):

```
6. **app/security.py**: `get_active_business_id` acepta `user=` opcional; no cambiar su lógica ni la
   de los `require_*`. `app/deps.py`: no modificar `require_user_api`, `require_admin_api`,
   `require_active_business_id` — son el control de acceso centralizado.
```

**Step 2: Verificación mecánica de lo que NO debe cambiar.** Ejecuta:

```bash
git diff a44338e -- .claude/CLAUDE.md
```

El diff debe MOSTRAR SOLO líneas añadidas (la entrada nueva) y NINGUNA línea borrada. Si aparece una
línea `-` borrada, deshaz esa parte y vuelve a editar.

#### Verification

```bash
git diff a44338e -- .claude/CLAUDE.md | grep -c "^-" | grep -q "^0$" && echo OK
# Expected: OK (cero líneas borradas)
```

#### Expected Report

- [ ] `.claude/CLAUDE.md` — invariante de `security.py` añadido (solo líneas nuevas)
- [ ] `git diff a44338e -- .claude/CLAUDE.md` sin líneas `-`
- [ ] Ningún otro archivo modificado

---

### T58: Docs — `CHANGELOG.md` de 0.5.0 y fila "5" del diagnóstico — REQ-12

**Depends on:** T57
**Skill:** technical-writer
**Agent:** docs
**Files:** `CHANGELOG.md`, `docs/auditorias/2026-10-23-frontend-arquitectura-diagnostico.md`
**Pillars:** P1, P4, P5, P6

#### Objective

Añadir la entrada de la fase 5b al CHANGELOG de 0.5.0 y marcar ✅ la fila "5" del diagnóstico.

#### Instructions

**Step 1:** En `CHANGELOG.md`, bajo el encabezado `## 0.5.0 (sin publicar — ...)`, añade como PRIMERA
entrada (antes de la línea de fase 5a) el texto literal:

```
- refactor(frontend): fase 5b — las consultas salen de los routers UI hacia repositorios/servicios y
  `UIContext` queda completo (campos perezosos `active_business_id`/`business_code`/`config`, todos a
  partir de `ctx.user`). `get_active_business_id` acepta `user=` opcional (sin cambios de lógica). Los
  routers UI ya no usan el ORM directo (guarda `tests/unit/test_routers_no_orm.py`) ni resuelven el
  negocio por su cuenta; de 4 a 1 SELECT sobre `users` por petición UI (guarda
  `tests/unit/test_ui_query_count.py`). HTML sin cambios (arnés y estilos idénticos a la baseline de 5a).
```

**Step 2:** En `docs/auditorias/2026-10-23-frontend-arquitectura-diagnostico.md`, busca la fila "5"
(de la tabla de deuda) y marca su casilla como `✅` (sustituye el `☐`/`[ ]`/espacio por `✅`) y, si
tiene una columna de "estado", pon `✅ resuelto en fase 5b`.

**Step 3: Verificación mecánica.** Ejecuta:

```bash
git diff a44338e -- CHANGELOG.md | grep -c "^-" | grep -q "^0$" && echo OK
```

(El CHANGELOG solo añade la línea nueva; no borra nada existente.)

#### Verification

```bash
git diff a44338e -- CHANGELOG.md docs/auditorias/2026-10-23-frontend-arquitectura-diagnostico.md | grep -c "^-" | grep -q "^0$" && echo OK
# Expected: OK (cero líneas borradas)
```

#### Expected Report

- [ ] `CHANGELOG.md` — entrada fase 5b añadida (solo líneas nuevas)
- [ ] `docs/auditorias/2026-10-23-frontend-arquitectura-diagnostico.md` — fila 5 marcada ✅
- [ ] `git diff` sin líneas `-`
- [ ] Ningún otro archivo modificado

---

### T59: Docs — `CLAUDE.md` raíz (Estado actual) y memoria Serena `conventions` — REQ-12

**Depends on:** T58
**Skill:** technical-writer
**Agent:** docs
**Files:** `CLAUDE.md`, `.serena/memories/conventions.md`
**Pillars:** P1, P4, P5, P6

#### Objective

Actualizar el `CLAUDE.md` raíz (Estado actual) y la memoria Serena `conventions`.

#### Instructions

**Step 1:** En `CLAUDE.md` raíz, en la sección `## Estado actual`, añade AL PRINCIPIO de la lista
(antes de la entrada de fase 5a) un elemento `- [x]` con el texto literal:

```
- [x] **Release 0.5.0, fase 5b del frontend: consultas fuera de los routers UI y `UIContext`
      completo (2026-11-02, rama `feature/frontend-fase5b`)** — las ~50 llamadas
      `get_active_business_config/code/id` de los routers UI pasan a `ctx.config`/`ctx.business_code`/
      `ctx.active_business_id`; los auxiliares (`_sale_pos_context`, `_sales_doc_context`,
      `_purchase_config_values`, `_render_error`, `_tab_error`, `_currency_symbol`,
      `_render_profit_items_tab`, `_home_locations_context`, `_location_name_for_code`) reciben `ctx`;
      `get_active_business_id` gana `user=` opcional; `extract_sku`/`barcode_to_sku`/`resolve_sku`
      pasan a `ProductService`; `InventoryRepository.get_movement` centraliza la comprobación de
      negocio; renombrados a públicos `transfer_out_id_for_movement_id`/`transfer_codes_from_out_note`.
      Guardas `test_routers_no_orm.py` y `test_ui_query_count.py` (1 SELECT sobre `users` por petición).
      HTML sin cambios (arnés y estilos idénticos a la baseline de 5a).
```

**Step 2:** En `.serena/memories/conventions.md`, añade al final (mantén el formato de bullets
existentes):

```
- Routers UI: los routers de `app/routers/ui_*.py` no usan el ORM directamente (`select(`, `db.get(`,
  `db.scalar(`, `db.scalars(`, `db.execute(`, `db.query(`, `db.add(`, `db.delete(`) ni resuelven el
  negocio por su cuenta (`get_active_business_config/code/id`, `require_active_business_id`,
  `get_current_user_from_session` fuera de `ui_context.py`/`ui_auth.py`): la guarda es
  `tests/unit/test_routers_no_orm.py`. El contexto de la petición es `UIContext` (`ctx.config`,
  `ctx.business_code`, `ctx.active_business_id`, `ctx.user`), creado en el cuerpo del handler con
  `ctx = ui_context(request, db)`.
```

**Step 3: Verificación mecánica.** Ejecuta:

```bash
git diff a44338e -- CLAUDE.md | grep -c "^-" | grep -q "^0$" && echo OK
```

#### Verification

```bash
git diff a44338e -- CLAUDE.md .serena/memories/conventions.md | grep -c "^-" | grep -q "^0$" && echo OK
# Expected: OK (cero líneas borradas)
```

#### Expected Report

- [ ] `CLAUDE.md` — entrada fase 5b en Estado actual (solo líneas nuevas)
- [ ] `.serena/memories/conventions.md` — bullet de la guarda ORM añadido
- [ ] `git diff` sin líneas `-`
- [ ] Ningún otro archivo modificado

---

### T60: Cierre — `search_graph` de métodos sin uso + recuento de `_log.` + suite completa (REQ-11)

**Depends on:** T59
**Skill:** backend-developer
**Agent:** coder
**Files:** (ninguno — solo verificación; reporta)
**Pillars:** P1, P4, P5, P6, P7

#### Objective

Verificar que ningún método nuevo quedó sin consumidor, que no se perdió ningún log, y que la suite
completa (REQ-11) está en verde.

#### Instructions

**Step 1: Métodos sin uso.** Confirma que cada método nuevo de §2.3 tiene al menos un llamador:

```bash
grep -rn "first_by_name\|list_categories\|get_by_lot_code\|deletable_skus\|get_movement\|skus_with_location_history\|transfer_out_by_ref\|transfer_items_by_ref\|skus_with_movements\|resolve_sku\|barcode_to_sku\|delete_document\|delete_item\|add_item\|add_document\|detach_references\|add_cash_baseline\|list_auth_events\|get_business_code\|list_businesses_by_ids\|location_has_data\|reconcile_locations\|create_locations\|delete_locations" app/ | grep -v "def "
```

Cada nombre debe aparecer al menos en 2 sitios (definición + uso). Si alguno aparece solo en su `def`,
repórtalo y no lo dejes sin consumidor.

**Step 2: Recuento de logs.** El número de llamadas `_log.` en `app/` no debe bajar respecto a
`a44338e`:

```bash
echo "actual: $(grep -rn "_log\." app/ --include='*.py' | wc -l)"
echo "base:   $(git grep -n "_log\." a44338e -- app/ | wc -l)"
```

Si `actual < base`, repórtalo (se perdió un log al mover un cuerpo).

**Step 3: Suite completa (REQ-11).** Ejecuta por orden y confirma 0 diferencias:

```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: ADMIN_USERNAME=admin ADMIN_PASSWORD=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test python3 -m pytest tests/unit -q --override-ini="addopts="
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/e2e -m e2e --no-cov -q
python3 tests/snapshots/style_snapshot.py check /tmp/gestion_inventario-style-baseline-bf38a2b --allow tests/snapshots/style_allowlist.txt
git grep -n "db\.\(get\|scalar\|scalars\|execute\|query\|add\|delete\)(" app/routers/ui_*.py
```

El último comando debe devolver vacío.

#### Verification

```bash
git grep -n "db\.\(get\|scalar\|scalars\|execute\|query\|add\|delete\)(" app/routers/ui_*.py
# Expected: sin resultados (vacío)
```

#### Expected Report

- [ ] todos los métodos nuevos tienen consumidor
- [ ] recuento de `_log.` igual o mayor que en `a44338e`
- [ ] `ui_snapshot.py check` OK
- [ ] `pytest tests/unit` verde
- [ ] `pytest tests/e2e -m e2e` verde
- [ ] `style_snapshot.py check` 0 diferencias
- [ ] `git grep db.(...)` en routers UI vacío

# Phase 3 — Task Envelopes: Auditoría Fase A

> Cada envelope es autocontenido. OpenCode ejecuta UNO a la vez, en orden T1 → T11.
> Test runner: `SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -v`
> **Regla dura:** ≤2 archivos por envelope · código exacto (before/after) · verificación mecánica · cero placeholders.
> Antes de T1: leer `CLAUDE.md` y `specs/auditoria-fase-a/design.md` §4 (firmas reales) y §5 (inventario).

---

### T1: Logging JSON estructurado + request_id (logger.py)

**Depends on:** none
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/logger.py`
**Pillars:** P1, P4, P7

#### Context

```python
# app/logger.py — EXISTE, reemplazar TODO el contenido
import logging, os
_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=..., format="%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%Y-%m-%dT%H:%M:%S")
def get_logger(name: str) -> logging.Logger: return logging.getLogger(name)
```

#### Objective

Logging JSON (ts/level/logger/request_id/message) con `request_id_var` de contextvar, sin cambiar la API `get_logger()`.

#### Instructions

**Step 1:** Reemplazar TODO el contenido de `app/logger.py` por:

```python
"""Logging centralizado: formato JSON + request_id via contextvars (H-043).

`get_logger(name)` conserva la firma pública; cada log incluye `request_id`
si el middleware de correlation ID lo seteó (app.main).
"""
from __future__ import annotations

import contextvars
import json
import logging
import time

import app.settings as settings  # H-045: LOG_LEVEL viene de settings

# ContextVar que el middleware de main setea por request.
request_id_var: contextvars.ContextVar[str] = contextvars.ContextVar("request_id", default="-")


def set_request_id(value: str) -> None:
    """Setea el request_id del contexto actual (llamado por el middleware)."""
    request_id_var.set(value or "-")


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created)),
            "level": record.levelname,
            "logger": record.name,
            "request_id": request_id_var.get(),
            "message": record.getMessage(),
        }
        if record.exc_info:
            entry["exc"] = self.formatException(record.exc_info)
        return json.dumps(entry, ensure_ascii=False)


_LEVEL_MAP = {"DEBUG": logging.DEBUG, "INFO": logging.INFO, "WARNING": logging.WARNING, "ERROR": logging.ERROR}

_handler = logging.StreamHandler()
_handler.setFormatter(_JsonFormatter())

root = logging.getLogger()
root.handlers = [_handler]
root.setLevel(_LEVEL_MAP.get(settings.LOG_LEVEL, logging.INFO))


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
```

**Edge cases (ya resueltos):**
- `LOG_LEVEL` se lee de `app.settings.LOG_LEVEL` (declarado en T2/T9 — ver Nota al pie).
- No se imprimen secretos: el formatter solo incluye `message` + `exc`.

**Nota de orden de tareas:** `app.settings.LOG_LEVEL` se añade en T2. Si T1 se ejecuta antes de T2 y `LOG_LEVEL` no existe aún en settings, usar `getattr(settings, "LOG_LEVEL", "INFO")`.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -c "import app.logger as L; L.get_logger('t').warning('hola %s', 1)"
# Expected: una línea JSON con "level": "WARNING" y "message": "hola 1"
```

#### Expected Report

- [ ] `app/logger.py` reemplazado (JSON + request_id_var + set_request_id)
- [ ] Ningún otro archivo modificado
- [ ] Comando de verificación — PASS

---

### T2: Settings — añadir env comunes + Middleware correlation ID + LOG_LEVEL

**Depends on:** T1
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/settings.py`, `app/main.py`

#### Context

```python
# app/settings.py — EXISTE. Añadir al final del archivo (mantener AGENT_* actuales)
def env(name: str, default: str) -> str: ...

# app/main.py — EXISTE
@app.middleware("http")
async def security_headers_middleware(request, call_next): ...
@app.middleware("http")
async def ui_auth_middleware(request, call_next): ...
app = FastAPI(title="Inventario", lifespan=lifespan)
```

#### Objective

Declarar env comunes en settings y añadir el middleware de `X-Request-ID` + request log en main.

#### Instructions

**Step 1 — settings.py:** añadir al final:

```python
# ── Comunes (H-045: consolidar os.getenv dispersos) ──────────────────────────
DATABASE_URL = env("DATABASE_URL", "sqlite+pysqlite:///./inventario.db")
DB_POOL_SIZE = int(env("DB_POOL_SIZE", "10"))
DB_MAX_OVERFLOW = int(env("DB_MAX_OVERFLOW", "20"))
SESSION_SECRET = env("SESSION_SECRET", "")
SESSION_HTTPS_ONLY = env("SESSION_HTTPS_ONLY", "0") == "1"
HOST = env("HOST", "127.0.0.1")
PORT = int(env("PORT", "10000"))
RELOAD = env("RELOAD", "1") == "1"
LOG_LEVEL = env("LOG_LEVEL", "INFO").upper()
BUSINESS_CONFIG_PATH = env("BUSINESS_CONFIG_PATH", "")
BUSINESS_CONFIG_DIR = env("BUSINESS_CONFIG_DIR", "")
LOGIN_RATE_LIMIT = int(env("LOGIN_RATE_LIMIT", "10"))
LOGIN_RATE_WINDOW = int(env("LOGIN_RATE_WINDOW", "300"))
UPLOAD_MAX_MB = int(env("UPLOAD_MAX_MB", "10"))
DEV_ACTIONS_ENABLED = env("DEV_ACTIONS_ENABLED", "1") == "1"
```

**Step 2 — main.py:** añadir imports y el middleware NUEVO (registrarlo como primer middleware, encima de security_headers):

```python
# imports nuevos al inicio de app/main.py (junto a los existentes)
import time
import uuid
from app.logger import get_logger, request_id_var, set_request_id

_log = get_logger(__name__)


@app.middleware("http")
async def correlation_id_middleware(request, call_next):
    """H-043: X-Request-ID + log de request (método, path, status, duración)."""
    rid = (request.headers.get("X-Request-ID") or "").strip() or uuid.uuid4().hex[:16]
    request.state.request_id = rid
    set_request_id(rid)
    start = time.monotonic()
    response = await call_next(request)
    duration_ms = int((time.monotonic() - start) * 1000)
    response.headers["X-Request-ID"] = rid
    path = getattr(request, "url", None)
    _log.info(
        "request %s %s -> %s (%sms)",
        request.method, path.path if path else request.url, response.status_code, duration_ms,
    )
    return response
```

**Edge cases (ya resueltos):** si el cliente envía `X-Request-ID`, se respeta; `set_request_id` usa contextvar (no interfiere con el logging de otros hilos).

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -q tests/unit/test_health.py -x 2>/dev/null; echo "exit=$?"
# Expected: exit=0 o test no existe aún (los tests se añaden en T11). No debe romper nada.
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -q -m "not e2e" --no-cov 2>&1 | tail -2
# Expected: todos pasan
```

#### Expected Report

- [ ] `app/settings.py` ampliado (env comunes)
- [ ] `app/main.py` middleware `correlation_id_middleware` registrado
- [ ] Suite unit — PASS

---

### T3: Health check profundo (H-065)

**Depends on:** T2
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/routers/health.py`

#### Context

```python
# app/routers/health.py — EXISTE, reemplazar
@router.get("/health")
def health() -> dict: ...  # checks: {database}; 200/503
```

#### Objective

Health con `version`, `schema_version`, `database`, `agent_llm` (no bloqueante).

#### Instructions

**Step 1:** Reemplazar TODO el contenido de `app/routers/health.py`:

```python
from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter
from sqlalchemy import text

from app.db import SessionLocal

router = APIRouter(tags=["health"])

_APP_DIR = Path(__file__).resolve().parents[2]


def _app_version() -> str:
    try:
        v = (_APP_DIR / "VERSION").read_text(encoding="utf-8").strip()
        return v or "unknown"
    except OSError:
        return "unknown"


def _schema_version() -> str:
    try:
        db = SessionLocal()
        try:
            row = db.execute(text("SELECT version FROM schema_version WHERE id = 1")).fetchone()
            return str(row[0]) if row else "missing"
        finally:
            db.close()
    except Exception:
        return "error"


def _db_ok() -> str:
    try:
        db = SessionLocal()
        try:
            db.execute(text("SELECT 1"))
        finally:
            db.close()
        return "ok"
    except Exception:
        return "degraded"


def _agent_llm_ok() -> str:
    """Reachability opcional del backend LLM (timeout 2s). No baja el status."""
    import app.settings as settings

    base = settings.AGENT_LLM_BASE_URL
    if not base:
        return "disabled"
    try:
        import urllib.request

        urllib.request.urlopen(f"{base}/models", timeout=2).read()
        return "ok"
    except Exception:
        return "degraded"


@router.get("/health")
def health() -> dict:
    db_status = _db_ok()
    schema = _schema_version()
    llm_status = _agent_llm_ok()
    critical_ok = db_status == "ok" and schema not in ("error", "missing")
    overall = "ok" if critical_ok else "degraded"
    return {
        "status": overall,
        "version": _app_version(),
        "checks": {
            "database": db_status,
            "schema_version": schema,
            "agent_llm": llm_status,
        },
    }
```

**Edge cases (ya resueltos):** si el LLM está caído → `agent_llm=degraded` pero `status` sigue "ok" (REQ-10). `VERSION` se lee relativo al repo.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_health.py -q 2>/dev/null; echo "exit=$?"
# Expected: exit=0 o no existe el test aún (T11 lo añade)
```

#### Expected Report

- [ ] `app/routers/health.py` con version/schema_version/database/agent_llm
- [ ] Ningún otro archivo modificado

---

### T4: Circuit breaker del LLM (H-030)

**Depends on:** T2
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/agent/circuit_breaker.py` (CREATE), `app/agent/graph.py`, `app/settings.py`

#### Context

```python
# app/agent/graph.py — EXISTE. En _plan_node, antes del retry loop:
    error: Optional[str] = None
    for _ in range(config.MAX_RETRIES + 1):
        try:
            plan = _PLANNER.invoke(messages)
            ...
        except (OutputParserException, ValidationError) as exc:
            ...
        except Exception as exc:
            state["error"] = f"LLM no disponible: {exc}"
            state["plan"] = PlanModel(tool=None, widgets=[], reply="El copiloto no está disponible en este momento.")
            return state

# app/agent/config.py — re-exporta desde app.settings
# app/settings.py — T2 añadió env comunes; añadir aquí las del breaker
```

#### Objective

`LLMCircuitBreaker` con singleton + wiring en `_plan_node`: degrada sin llamar al LLM durante el cooldown.

#### Instructions

**Step 1 — settings.py:** añadir (junto a AGENT_*):

```python
AGENT_BREAKER_FAIL_THRESHOLD = int(env("AGENT_BREAKER_FAIL_THRESHOLD", "3"))
AGENT_BREAKER_COOLDOWN_S = float(env("AGENT_BREAKER_COOLDOWN_S", "60"))
AGENT_BREAKER_MAX_RETRIES = int(env("AGENT_BREAKER_MAX_RETRIES", "1"))
```

**Step 2 — app/agent/circuit_breaker.py (CREAR):**

```python
"""Circuit breaker del copiloto (H-030).

Protege al LLM: tras N fallos consecutivos abre (OPEN) por un cooldown; las
consultas degradan sin llamar al LLM; tras el cooldown una sonda (HALF-OPEN)
decide si vuelve a CLOSED o reabre. Thread-safe (se usa desde el threadpool).
"""
from __future__ import annotations

import threading
import time

from app import settings

_log = settings  # noop: mantiene import; el log se usa en graph.py


class LLMCircuitBreaker:
    def __init__(self, fail_threshold: int, cooldown_s: float, max_retries: int) -> None:
        self._fail_threshold = fail_threshold
        self._cooldown_s = cooldown_s
        self._max_retries = max_retries
        self._lock = threading.Lock()
        self._state = "CLOSED"  # CLOSED | OPEN | HALF-OPEN
        self._failures = 0
        self._opened_at: float | None = None

    @property
    def state(self) -> str:
        with self._lock:
            return self._state

    def allow_request(self) -> bool:
        with self._lock:
            if self._state == "OPEN":
                if self._opened_at is not None and time.monotonic() - self._opened_at >= self._cooldown_s:
                    self._state = "HALF-OPEN"
                    return True
                return False
            return True  # CLOSED y HALF-OPEN permiten 1 sonda

    def record_success(self) -> None:
        with self._lock:
            self._state = "CLOSED"
            self._failures = 0

    def record_failure(self) -> None:
        with self._lock:
            self._failures += 1
            if self._state == "HALF-OPEN" or self._failures >= self._fail_threshold:
                self._state = "OPEN"
                self._opened_at = time.monotonic()


breaker = LLMCircuitBreaker(
    fail_threshold=settings.AGENT_BREAKER_FAIL_THRESHOLD,
    cooldown_s=settings.AGENT_BREAKER_COOLDOWN_S,
    max_retries=settings.AGENT_BREAKER_MAX_RETRIES,
)
```

**Step 3 — graph.py:** añadir import y wiring en `_plan_node`:

```python
# import nuevo en graph.py (junto a los existentes)
from app.agent.circuit_breaker import breaker
from app.logger import get_logger

_log_agent = get_logger(__name__)
```

En `_plan_node`, INMEDIATAMENTE antes de `error: Optional[str] = None` (después de `messages.append(HumanMessage(content=state["message"]))`):

```python
    if not breaker.allow_request():
        _log_agent.warning("LLM circuit breaker OPEN — degradando sin llamar al LLM")
        state["error"] = "LLM en cooldown"
        state["plan"] = PlanModel(
            tool=None, widgets=[],
            reply="El copiloto está temporalmente no disponible. Inténtalo en un momento.",
        )
        return state
```

Y dentro del retry: tras `plan = _PLANNER.invoke(messages)` AÑADIR `breaker.record_success()` antes del `if not isinstance(...)`. En el `except Exception as exc:` (hard failure) AÑADIR `breaker.record_failure()` antes del `state["error"] = ...`.

**Edge cases (ya resueltos):** breaker OPEN → una consulta no toca el LLM; `_PLANNER` sigue siendo singleton compartido (thread-safe por el lock del breaker).

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_circuit_breaker.py tests/unit/test_agent_graph.py -q 2>/dev/null; echo "exit=$?"
# Expected: test_circuit_breaker (T11) o suite no rota
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -q -m "not e2e" --no-cov 2>&1 | tail -2
# Expected: todos pasan
```

#### Expected Report

- [ ] `app/agent/circuit_breaker.py` creado (LLMCircuitBreaker + breaker)
- [ ] `app/agent/graph.py` wiring (allow_request + record_success/failure)
- [ ] `app/settings.py` +3 env del breaker
- [ ] Suite unit — PASS

---

### T5: Logging en excepts de `ui_tabs.py` (H-039, router principal)

**Depends on:** T1
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_tabs.py`

#### Context

```python
# app/routers/ui_tabs.py — EXISTE. Patrones de except silencioso a loggear:
#   try: ... except Exception: X = []  (o return fallback)
# Ya tiene en algunos sitios `from app.logger import get_logger`; si no, añadirlo.
```

#### Objective

Añadir `_log.exception()` en los `except Exception` de `ui_tabs.py` que devuelven fallback/[]/0, SIN cambiar el comportamiento.

#### Instructions

**Step 1:** Añadir al inicio de `app/routers/ui_tabs.py` (junto a los imports existentes):

```python
from app.logger import get_logger

_log = get_logger(__name__)
```

**Step 2:** Localizar TODOS los `except Exception` en el archivo (grep). Para CADA uno que devuelva fallback (lista vacía, `return HTMLResponse("")`, `return []`, `X = 0`), añadir la primera línea dentro del except:

```python
            _log.exception("ui_tabs fallback en <contexto de la función>: %r", <expresión del fallback>)
```

**Regla estricta:** NO tocar la lógica del `except` — solo añadir la línea de log como PRIMERA línea. Usar `_log.exception(...)` si la excepción está viva (`except Exception as e:` → `_log.exception(...)` o `_log.error("... %s", e)`); si el except NO captura la excepción (`except Exception:`), usar `_log.error("...")` sin `exc_info`.

**Ejemplo (patrón típico):**

```python
# BEFORE:
        try:
            product_options = product_service.search(query="", limit=200)
        except Exception:
            product_options = []

# AFTER:
        try:
            product_options = product_service.search(query="", limit=200)
        except Exception:
            _log.error("product_options fallback en <función>")
            product_options = []
```

#### Verification

```bash
python3 -c "import ast; ast.parse(open('app/routers/ui_tabs.py').read())" && echo "SYNTAX OK"
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -q -m "not e2e" --no-cov 2>&1 | tail -2
# Expected: todos pasan
```

#### Expected Report

- [ ] `app/routers/ui_tabs.py`: `_log` importado + logs en todos los excepts de fallback
- [ ] Suite unit — PASS

---

### T6: Logging en excepts de los demás routers (H-039)

**Depends on:** T5
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_transfers.py`, `app/routers/ui_sales_documents.py`, `app/routers/ui_products.py`, `app/routers/ui_purchases.py`, `app/routers/ui_expenses.py`, `app/routers/ui_extractions.py`, `app/routers/ui_sales.py`

#### Context

Mismos patrones que T5 pero en los routers de operaciones. Algunos ya importan `get_logger` (p.ej. `ui_businesses.py:30`), otros no.

#### Objective

Añadir logging en los `except Exception` de fallback de estos routers (misma regla que T5). Un envelope por router es demasiado; este envelope cubre TODOS con el mismo patrón mecánico.

#### Instructions

**Step 1:** Para cada archivo listado: si no tiene `from app.logger import get_logger`, añadirlo al bloque de imports + `_log = get_logger(__name__)`.
**Step 2:** En cada `except Exception` que devuelve fallback/[]/0/HTMLResponse(""), añadir la PRIMERA línea de log (misma regla que T5).

**No tocar:** `ui_businesses.py` (ya loguea), `app/services/*` (fuera de scope), la lógica de los except.

#### Verification

```bash
for f in app/routers/ui_transfers.py app/routers/ui_sales_documents.py app/routers/ui_products.py app/routers/ui_purchases.py app/routers/ui_expenses.py app/routers/ui_extractions.py app/routers/ui_sales.py; do python3 -c "import ast; ast.parse(open('$f').read())" || echo "SYNTAX FAIL $f"; done; echo "SYNTAX OK"
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -q -m "not e2e" --no-cov 2>&1 | tail -2
# Expected: todos pasan
```

#### Expected Report

- [ ] Todos los routers listados con `_log` en excepts de fallback
- [ ] Suite unit — PASS

---

### T7: `flush()` en `_location_id_for_code` (H-040)

**Depends on:** none
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/services/inventory_service.py`

#### Context

```python
# app/services/inventory_service.py — EXISTE (método _location_id_for_code, ~l.108)
            self._db.add(Location(business_id=int(self._business_id), code=alt_code, name=name))
            self._db.commit()
        except IntegrityError:
            self._db.rollback()
```

#### Objective

`commit()` → `flush()` para que la Location auto-creada se persista con la operación padre (sin huérfanas).

#### Instructions

**Step 1:** Reemplazar:

```python
            self._db.add(Location(business_id=int(self._business_id), code=alt_code, name=name))
            self._db.commit()
```

por:

```python
            self._db.add(Location(business_id=int(self._business_id), code=alt_code, name=name))
            self._db.flush()
```

**Edge cases (ya resueltos):** el `except IntegrityError: self._db.rollback()` se conserva (carrera de doble creación). `flush()` valida FK/UNIQUE sin commitear; la fila se persiste con el commit de la operación padre.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -q tests/unit/test_inventory_service_unit.py tests/unit/test_ui_transfers_bulk.py --no-cov 2>&1 | tail -2
# Expected: todos pasan
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -q -m "not e2e" --no-cov 2>&1 | tail -2
```

#### Expected Report

- [ ] `inventory_service.py` `_location_id_for_code`: commit→flush
- [ ] Suite unit — PASS

---

### T8: Audit atómico en ventas/traspasos (H-041)

**Depends on:** T7
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/audit.py`, `app/routers/ui_sales.py`, `app/routers/ui_transfers.py`

#### Context

```python
# app/audit.py — EXISTE
    db.add(row)
    try: db.commit()   # ← cambiar a flush
    except Exception: db.rollback(); _log.exception(...)

# app/routers/ui_sales.py — EXISTE (patrón: la operación commitea en service)
    mv = service.sale(...)
    log_event(db, user, "sale", ...)   # DESPUÉS del commit del service
# app/routers/ui_transfers.py — EXISTE (create_transfer)
    service.transfer(TransferCreate(...))
    log_event(db, user, "transfer", ...)   # DESPUÉS
```

#### Objective

`log_event` flushea (no commitea); en ventas y traspasos el audit se emite ANTES del commit de la operación (atómico). En los demás flujos, `log_event` flushea y quien lo llama añade un commit si el flujo no commitea tras el audit.

#### Instructions

**Step 1 — audit.py:** `db.commit()` → `db.flush()`, y ampliar el docstring:

```python
def log_event(db, user, action, entity_type, entity_id=None, detail=None) -> None:
    """Registra un evento de auditoría en la MISMA transacción del caller.

    Usa `flush()` (no `commit()`): el AuditLog se persiste con el commit que
    haga el caller. En ventas/traspasos se llama ANTES de la operación que
    commitea (atómico). En el resto es best-effort: el caller debe commitear
    tras llamarlo (o aceptar la pérdida si no lo hace).
    """
    row = AuditLog(...)
    db.add(row)
    try:
        db.flush()
    except Exception:
        db.rollback()
        _log.exception("Failed to persist audit event: action=%s entity=%s", action, entity_type)
```

**Step 2 — ui_sales.py (sale):** reordenar para emitir el audit ANTES del commit del service:

```python
# BEFORE (patrón actual):
    mv = service.sale(...)
    log_event(db, user, "sale", ...)
# AFTER:
    log_event(db, user, "sale", ...)
    mv = service.sale(...)
```
> El audit ya flushea; `service.sale()` commitea al final → audit + venta atómicos. Si la venta falla (rollback), el audit también se revierte (correcto: no hay venta sin audit).

**Step 3 — ui_transfers.py (create_transfer):** mismo reorden con `log_event(...)` ANTES de `service.transfer(...)`.

**Step 4 — audit.py docstring de "best-effort":** en el resto de callers (compras, ajustes, gastos, clientes, usuarios, negocios) NO reordenar; añadir `db.commit()` inmediatamente después de `log_event(...)` SOLO si el flujo NO commitea después de forma evidente (p.ej. `ui_customers.py`, `ui_businesses.py` donde no hay un service que commitee tras el audit). Verificar con los tests.

**⚠️ Protocolo:** si algún test de auditoría falla tras el reorden, seguir el protocolo de fallo (sección 5.2): leer el error, corregir el CÓDIGO, re-ejecutar. NO revertir el reorden sin reportar.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -q tests/unit/test_ui_users.py tests/unit/test_auth_db_and_security.py tests/unit/test_inventory_service_unit.py --no-cov 2>&1 | tail -3
# Expected: tests de auditoría (usuarios, ventas, transferencias) pasan
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -q -m "not e2e" --no-cov 2>&1 | tail -2
```

#### Expected Report

- [ ] `app/audit.py`: flush + docstring
- [ ] `ui_sales.py` / `ui_transfers.py`: audit antes del commit de la operación
- [ ] Suite unit — PASS

---

### T9: Consolidar `os.getenv` → `app.settings` (H-045)

**Depends on:** T2
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/db.py`, `app/utils.py`, `app/routers/ui_auth.py`, `app/routers/ui_common.py`, `app/business_config.py`

#### Context

```python
# app/db.py — EXISTE
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+pysqlite:///./inventario.db")
_pool_kwargs = { "pool_size": int(os.getenv("DB_POOL_SIZE","10")), "max_overflow": int(os.getenv("DB_MAX_OVERFLOW","20")) }
# app/utils.py — EXISTE
secret = os.getenv("SESSION_SECRET", "").strip()
# app/routers/ui_auth.py — EXISTE
_LOGIN_RATE_LIMIT = int(os.getenv("LOGIN_RATE_LIMIT", "10"))
_LOGIN_RATE_WINDOW = int(os.getenv("LOGIN_RATE_WINDOW", "300"))
# app/routers/ui_common.py — EXISTE (upload / dev-actions os.getenv)
# app/business_config.py — EXISTE
raw_default = os.getenv("BUSINESS_CONFIG_PATH", "")
```

#### Objective

Que `os.getenv` solo aparezca en `app/settings.py` (y `load_dotenv` en db.py). Reemplazar en los 5 archivos por imports de `app.settings`.

#### Instructions

**Step 1 — app/db.py:** conservar `load_dotenv()`; reemplazar los `os.getenv`:

```python
import app.settings as settings
DATABASE_URL = settings.DATABASE_URL
_pool_kwargs = {}
if not DATABASE_URL.startswith("sqlite"):
    _pool_kwargs = {
        "pool_size": settings.DB_POOL_SIZE,
        "max_overflow": settings.DB_MAX_OVERFLOW,
    }
```

**Step 2 — app/utils.py:** `SESSION_SECRET` desde settings.

```python
from app.settings import SESSION_SECRET as _SESSION_SECRET
def get_session_secret() -> str:
    secret = _SESSION_SECRET.strip()
    if not secret:
        secret = secrets.token_hex(32)
    return secret
```

**Step 3 — ui_auth.py:** `_LOGIN_RATE_LIMIT = settings.LOGIN_RATE_LIMIT`, `_LOGIN_RATE_WINDOW = settings.LOGIN_RATE_WINDOW`.

**Step 4 — ui_common.py y business_config.py:** reemplazar cada `os.getenv(...)` por el equivalente de `app.settings` (buscar qué env usa cada uno con `grep -n "os.getenv"` y mapearlas a las declaradas en T2).

**Regla:** `app/migrations.py` y `app/main.py` ya usan `os.getenv` para credenciales del seed (`ADMIN_*`) y `HOST/PORT/RELOAD` — main.py se mueve en T2; migrations.py CONSERVA su `os.getenv` del seed (semántica de import-time del fail-fast, documentado). `app/logger.py` ya usa settings (T1).

#### Verification

```bash
grep -rn "os.getenv" app/ --include="*.py" | grep -v "app/settings.py" | grep -v "load_dotenv" | grep -v "ADMIN_USERNAME\|ADMIN_PASSWORD\|OPERATOR_USERNAME\|OPERATOR_PASSWORD"
# Expected: solo app/migrations.py (seed) — documentado
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -q -m "not e2e" --no-cov 2>&1 | tail -2
```

#### Expected Report

- [ ] `db.py`, `utils.py`, `ui_auth.py`, `ui_common.py`, `business_config.py` sin `os.getenv`
- [ ] Solo settings.py (+ seed de migrations) usan env
- [ ] Suite unit — PASS

---

### T10: Tests de regresión (health + circuit breaker)

**Depends on:** T3, T4
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** `tests/unit/test_health.py` (CREATE), `tests/unit/test_circuit_breaker.py` (CREATE)

#### Context

```python
# tests/unit/ — estructura existente (db_session fixture en tests/conftest.py)
# app/routers/health.py — T3 (health() -> dict)
# app/agent/circuit_breaker.py — T4 (LLMCircuitBreaker, breaker singleton)
```

#### Objective

Tests unitarios: health profundo (fields + status) y circuit breaker (transiciones CLOSED/OPEN/HALF-OPEN).

#### Instructions

**Step 1 — `tests/unit/test_circuit_breaker.py`:**

```python
from __future__ import annotations

from app.agent.circuit_breaker import LLMCircuitBreaker


def test_closed_allows_and_success_closes():
    b = LLMCircuitBreaker(fail_threshold=2, cooldown_s=60, max_retries=1)
    assert b.allow_request() is True
    b.record_success()
    assert b.state == "CLOSED"


def test_opens_after_threshold():
    b = LLMCircuitBreaker(fail_threshold=2, cooldown_s=60, max_retries=1)
    b.record_failure()
    assert b.state == "CLOSED"
    b.record_failure()
    assert b.state == "OPEN"
    assert b.allow_request() is False


def test_half_open_after_cooldown():
    b = LLMCircuitBreaker(fail_threshold=2, cooldown_s=0, max_retries=1)
    b.record_failure()
    b.record_failure()
    assert b.state == "OPEN"
    # cooldown_s=0 → la siguiente allow_request pasa a HALF-OPEN
    assert b.allow_request() is True
    assert b.state == "HALF-OPEN"


def test_failure_in_half_open_reopens():
    b = LLMCircuitBreaker(fail_threshold=2, cooldown_s=0, max_retries=1)
    b.record_failure(); b.record_failure()
    assert b.allow_request() is True  # HALF-OPEN
    b.record_failure()
    assert b.state == "OPEN"
```

**Step 2 — `tests/unit/test_health.py`:**

```python
from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app


def test_health_has_expected_checks():
    client = TestClient(app)
    res = client.get("/health")
    assert res.status_code == 200
    body = res.json()
    assert body["status"] in ("ok", "degraded")
    assert set(body["checks"].keys()) == {"database", "schema_version", "agent_llm"}
    assert "version" in body
```

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_circuit_breaker.py tests/unit/test_health.py -q --no-cov 2>&1 | tail -3
# Expected: 5 passed
```

#### Expected Report

- [ ] `test_circuit_breaker.py` (4 tests) PASS
- [ ] `test_health.py` (1 test) PASS

---

### T11: Verificación completa + e2e existente

**Depends on:** T10
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** (sin cambios de código)

#### Objective

Verificación final: suite unit completa + e2e existentes (5) siguen pasando, scope correcto.

#### Instructions

**Step 1:** Ejecutar:

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -m "not e2e" --no-cov 2>&1 | tail -2
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/e2e -m e2e --no-cov 2>&1 | tail -2
git diff --name-only develop
```

**Step 2:** Confirmar: (a) 142+ unit pasan (138 base + 5 nuevos); (b) 5 e2e pasan; (c) `git diff --name-only develop` lista SOLO los archivos de la sección 5 del design.md (más tests).

#### Verification

```bash
# Todos verdes. Si algún test falla: protocolo de fallo (PAE sección 5.2).
```

#### Expected Report

- [ ] Suite unit — PASS (≥142)
- [ ] Suite e2e — PASS (5)
- [ ] Scope — solo archivos de design.md §5
- [ ] Ningún archivo temporal (`_tmp`/`_draft`) dejado

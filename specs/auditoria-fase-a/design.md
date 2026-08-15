# Fase A — Design (SDD Phase 2)

> Decisiones cerradas. OpenCode NO decide nada de esta sección.

## D1 — Correlation ID + logging JSON (H-043)

- **Middleware** en `app/main.py` (nuevo `@app.middleware("http")`), registrado ANTES de `security_headers_middleware` (se ejecuta primero).
- Genera `X-Request-ID` (`uuid4().hex[:16]`) si el cliente no lo envía; lo guarda en `request.state.request_id` y lo setea en un `contextvars.ContextVar`.
- Log de request: método, path, status, duración_ms, request_id — a nivel INFO.
- **`app/logger.py`**: formateador JSON (`ts, level, logger, request_id, message`) + `request_id_var` de contextvar + helper `set_request_id()`.
- `request_id_var` vive en `app/logger.py` (lo usan `get_logger` y el middleware de main).
- Formato JSON SIEMPRE (no opt-in): cumple H-043 y simplifica. `LOG_LEVEL` se conserva.

## D2 — Health check profundo (H-065)

- `GET /health` amplía `checks`: `database` (SELECT 1), `schema_version` (SELECT version FROM schema_version WHERE id=1), `version` (archivo `VERSION`), `agent_llm` (reachability opcional, timeout 2s).
- `status` = "ok" solo si database+version+schema_version OK; `agent_llm=degraded` NO baja el status (REQ-10). Status 503 si database o schema_version falla.

## D3 — Circuit breaker LLM (H-030)

- Nuevo módulo `app/agent/circuit_breaker.py` con clase `LLMCircuitBreaker` y singleton `breaker`.
- Estados: CLOSED → (N fallos consecutivos) → OPEN (cooldown) → HALF-OPEN (1 sonda) → CLOSED (éxito) / OPEN (fallo de la sonda).
- Config vía `app/settings.py` (nuevas env): `AGENT_BREAKER_FAIL_THRESHOLD=3`, `AGENT_BREAKER_COOLDOWN_S=60`, `AGENT_BREAKER_MAX_RETRIES=1`.
- En `_plan_node` de `graph.py`: antes de cada `_PLANNER.invoke`, `breaker.allow_request()`; si False → degradar sin llamar LLM. `record_success()`/`record_failure()`.
- **Thread-safety:** se usa dentro de `asyncio.to_thread` (threadpool); el breaker protege con `threading.Lock` (el acceso es cross-thread).

## D4 — Logging en excepts de routers (H-039)

- Añadir `from app.logger import get_logger` + `_log = get_logger(__name__)` en los routers que no lo tengan, y `_log.exception(...)` (o `_log.error` si no hay excepción viva) en cada `except Exception` que devuelve fallback/error.
- **Regla:** NUNCA cambiar el comportamiento (status code, fallback, mensaje) — solo añadir el log.
- Router por envelope (ui_tabs → ui_transfers → ui_sales_documents → ui_products/ui_purchases → ui_expenses/ui_extractions/ui_sales).

## D5 — `flush()` en `_location_id_for_code` (H-040)

- `app/services/inventory_service.py` `_location_id_for_code`: `self._db.commit()` → `self._db.flush()` en el bloque de auto-creación de Location.
- El `except IntegrityError: self._db.rollback()` se conserva (carrera de doble creación).
- Efecto: la Location solo se persiste si la operación padre commitea → sin huérfanas.

## D6 — Audit atómico en ventas/traspasos (H-041)

- `app/audit.py log_event`: `db.commit()` → `db.flush()` (no persiste por separado).
- **Ventas** (`ui_sales.py`) y **Traspasos** (`ui_transfers.py`): reordenar la llamada a `log_event` para que ocurra ANTES de `service.sale()`/`service.transfer()` (que commitean al final). Así el audit commitea con la operación.
- **Otros flujos** (compras, ajustes, gastos, clientes, usuarios, negocios): se añade un `db.commit()` explícito inmediatamente después de `log_event` (si el flujo no commitea tras el audit) para no perder el registro; el docstring de `log_event` documenta "best-effort out-of-band" donde no se reordena.
- ⚠️ Envelope T8 verifica cada call site contra los tests existentes; si algún test de auditoría falla, seguir el protocolo de fallo (sección 5.2 del PAE).

## D7 — Consolidar os.getenv (H-045)

- `app/settings.py` declara (además de las AGENT_* existentes): `DATABASE_URL`, `DB_POOL_SIZE`, `DB_MAX_OVERFLOW`, `SESSION_SECRET`, `SESSION_HTTPS_ONLY`, `HOST`, `PORT`, `RELOAD`, `LOG_LEVEL`, `BUSINESS_CONFIG_PATH`, `BUSINESS_CONFIG_DIR`, `LOGIN_RATE_LIMIT`, `LOGIN_RATE_WINDOW`, `UPLOAD_MAX_MB`, `DEV_ACTIONS_ENABLED` (según existan).
- Módulos que pasan a importar de `app.settings` (dejan de usar `os.getenv`): `app/db.py`, `app/logger.py`, `app/main.py`, `app/utils.py`, `app/routers/ui_auth.py`, `app/routers/ui_common.py`, `app/business_config.py`.
- **Excepciones permitidas:** `app/db.py` conserva `load_dotenv()` (necesario para cargar el `.env`); `app/migrations.py` conserva `os.getenv("ADMIN_USERNAME"/"ADMIN_PASSWORD"/...)` para el seed (semántica de fail-fast ya implementada, NO mover al import-time).

## D8 — Fuera de scope: H-046 compose env

- NO se toca `docker-compose.yml` en este PAE (invariante). Pendiente de decisión del usuario.

---

## 4. Firmas reales del repo (NO modificar salvo la tarea que lo indica)

```python
# app/logger.py — EXISTE
import logging, os
_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=..., format="%(asctime)s [%(levelname)s] %(name)s: %(message)s", ...)
def get_logger(name: str) -> logging.Logger: return logging.getLogger(name)

# app/main.py — EXISTE (middlewares)
@app.middleware("http")
async def security_headers_middleware(request, call_next): ...
@app.middleware("http")
async def ui_auth_middleware(request, call_next): ...
# lifespan() llama run_startup_tasks(); app = FastAPI(title="Inventario", lifespan=lifespan)
# uvicorn main: host=os.getenv("HOST","127.0.0.1"), port=int(os.getenv("PORT","10000")), reload=os.getenv("RELOAD","1")=="1"

# app/routers/health.py — EXISTE
@router.get("/health")
def health() -> dict: ...  # checks: {database}, status ok|degraded, 200/503

# app/agent/llm.py — EXISTE
def build_llm(**kwargs) -> ChatOpenAI: ...  # timeout=config.LLM_TIMEOUT_S
def build_planner(model: Type[BaseModel]) -> ChatOpenAI: ...  # _PLANNER se construye así

# app/agent/graph.py — EXISTE (plan_node)
_PLANNER = _Planner(build_planner(PlanModel))  # o similar en _build_graph()
# En _plan_node: for _ in range(config.MAX_RETRIES + 1): try: plan = _PLANNER.invoke(messages) ...
#   except OutputParserException/ValidationError -> retry con feedback
#   except Exception as exc -> state["error"]=..., degradar, return
# _execute_node / _build_node / _widgets_node también existen; _PLANNER es compartido (singleton a nivel módulo)

# app/agent/runner.py — EXISTE
_GRAPH = build_graph()  # compiled once
async def run_agent(business_id, message, history=None) -> dict:  # asyncio.to_thread(_run)

# app/services/inventory_service.py — EXISTE (H-040)
def _location_id_for_code(self, code: str) -> int:
    ...
    self._db.add(Location(business_id=int(self._business_id), code=alt_code, name=name))
    self._db.commit()          # ← T7: commit() → flush()
    except IntegrityError:
        self._db.rollback()
    ...

# app/audit.py — EXISTE (H-041)
def log_event(db, user, action, entity_type, entity_id=None, detail=None) -> None:
    db.add(row); try: db.commit()  # ← T8: commit() → flush()
    except Exception: db.rollback(); _log.exception(...)

# app/routers/ui_sales.py — EXISTE (H-041 reorder)
#   sale() del router llama: mv = service.sale(...); log_event(db, user, "sale", ...)  (DESPUÉS del commit)
# app/routers/ui_transfers.py — idem con service.transfer(...); log_event(...)  (DESPUÉS)

# app/settings.py — EXISTE (H-045, se amplía)
def env(name: str, default: str) -> str: ...
AGENT_LLM_* / AGENT_MAX_* (ya declarados)

# app/db.py — EXISTE (H-045)
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+pysqlite:///./inventario.db")  # → app.settings
_pool_kwargs = { "pool_size": int(os.getenv("DB_POOL_SIZE","10")), "max_overflow": int(os.getenv("DB_MAX_OVERFLOW","20")) }
```

## 5. Inventario de archivos (scope duro)

| Archivo | Operación | Responsabilidad |
|---------|-----------|-----------------|
| `app/logger.py` | MODIFY | JSON formatter + `request_id_var` + `set_request_id()` |
| `app/main.py` | MODIFY | Middleware de correlation ID + request log |
| `app/routers/health.py` | MODIFY | Health profundo (version, schema_version, agent_llm) |
| `app/agent/circuit_breaker.py` | CREATE | `LLMCircuitBreaker` + singleton `breaker` |
| `app/agent/graph.py` | MODIFY | Wire del breaker en `_plan_node` |
| `app/settings.py` | MODIFY | Nuevas env vars (breaker, DB, session, host/port, log, business_config, rate, upload) |
| `app/services/inventory_service.py` | MODIFY | `_location_id_for_code`: commit→flush |
| `app/audit.py` | MODIFY | `log_event`: commit→flush + docstring best-effort |
| `app/routers/ui_sales.py` | MODIFY | Reorder `log_event` antes de `service.sale()` |
| `app/routers/ui_transfers.py` | MODIFY | Reorder `log_event` antes de `service.transfer()` (create_transfer) |
| `app/routers/ui_tabs.py` | MODIFY | `_log.exception()` en excepts silenciosos |
| `app/routers/ui_transfers.py` | MODIFY | idem (excepts restantes) |
| `app/routers/ui_sales_documents.py` | MODIFY | idem |
| `app/routers/ui_products.py` | MODIFY | idem |
| `app/routers/ui_purchases.py` | MODIFY | idem |
| `app/routers/ui_expenses.py` / `ui_extractions.py` / `ui_sales.py` | MODIFY | idem |
| `app/db.py` | MODIFY | Importar DATABASE_URL/DB_POOL de settings |
| `app/utils.py` | MODIFY | SESSION_SECRET desde settings |
| `app/routers/ui_auth.py` | MODIFY | Rate limit desde settings |
| `app/routers/ui_common.py` | MODIFY | Uploads/dev-actions desde settings |
| `app/business_config.py` | MODIFY | BUSINESS_CONFIG_PATH/DIR desde settings |
| `tests/unit/test_health.py` | CREATE | Health profundo |
| `tests/unit/test_circuit_breaker.py` | CREATE | Breaker states |
| `tests/unit/test_agent_graph.py` | MODIFY | Breaker en plan_node (si aplica) |

> **NO se tocan:** `docker-compose*.yml`, `app/auth.py`, `app/deps.py`, `app/models.py`,
> `app/schemas.py`, `app/templates/*`, `app/agent/runner.py` (salvo que el envelope lo diga).

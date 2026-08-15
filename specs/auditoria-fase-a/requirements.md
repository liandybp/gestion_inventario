# Fase A — Requirements (SDD Phase 1)

> Feature: subir el score de la auditoría de 2.95 → ~3.45 atacando 4 dimensiones.
> Fuente: `docs/auditorias/2026-08-15-negocio-sin-pos.md` (hallazgos H-030, H-039, H-040,
> H-041, H-043, H-045, H-065).

## REQs (MoSCoW)

### Must (M)

| ID | Requisito | Criterio de aceptación (Gherkin) |
|----|-----------|-----------------------------------|
| REQ-1 | **Correlation ID por request (H-043)** | Given un request HTTP, When pasa por el middleware, Then la respuesta lleva `X-Request-ID` y todos los logs de ese request (y sus subtareas) incluyen `request_id`. |
| REQ-2 | **Logging estructurado JSON (H-043)** | Given el logger del proyecto, When se emite un log, Then el formato es JSON con `ts/level/logger/request_id/message` y NO el formato de texto plano actual. |
| REQ-3 | **Health check profundo (H-065)** | Given `GET /health`, When responde, Then incluye `version` (archivo VERSION), `schema_version` (tabla schema_version), `database`, y reachability del LLM (`agent_llm`), con `status` 503 si algo crítico falla. |
| REQ-4 | **Circuit breaker del LLM (H-030)** | Given el copiloto, When el LLM falla N veces consecutivas, Then el breaker pasa a OPEN y durante el cooldown las consultas degradan inmediatamente ("copiloto temporalmente no disponible") sin llamar al LLM; tras el cooldown una sonda (HALF-OPEN) decide CLOSED/OPEN. |
| REQ-5 | **Logging en `except Exception` de routers (H-039)** | Given un `except Exception` en un router que devuelve fallback/error, When se dispara, Then registra `_log.exception()`/`_log.error()` con el contexto, sin cambiar el comportamiento (fallback/status code intacto). |
| REQ-6 | **`flush()` en `_location_id_for_code` (H-040)** | Given una operación que resuelve una Location inexistente, When la crea, Then NO la commitea por separado (usa `flush()`), y se persiste solo si la operación padre commitea (sin huérfanas). |
| REQ-7 | **Audit atómico en ventas/traspasos (H-041)** | Given una venta/traspaso, When se ejecuta y commitea, Then su `AuditLog` se persiste en el MISMO commit (si la operación hace rollback, el audit también). |
| REQ-8 | **Consolidar `os.getenv` → `settings.py` (H-045)** | Given el repo, When se busca `os.getenv`, Then solo aparece en `app/settings.py` (y en `app/db.py` el `load_dotenv`), no disperso en 9+ módulos. |

### Should (S)

| ID | Requisito | Criterio de aceptación |
|----|-----------|------------------------|
| REQ-9 | **Envolver el retry del LLM con el breaker** | Given el breaker CLOSED, When el planner falla, Then se reintenta según `MAX_RETRIES`; cuando está OPEN, NO se reintenta (degrade inmediato). |
| REQ-10 | **`agent_llm` en health no bloquea el health check** | Given `GET /health`, When el LLM está caído, Then `agent_llm=degraded` pero `status` sigue 200 si BD y migraciones OK (el health no debe caer por el copiloto). |

### Won't (W — fuera de scope Fase A)

| ID | Excluido | Razón |
|----|----------|-------|
| W-1 | **H-046** compose env obligatoria (`${VAR:?must-be-set}`) | Toca `docker-compose.yml` (invariante del proyecto); requiere decisión del usuario. Pendiente fuera de este PAE. |
| W-2 | **H-026/H-023** dividir `ui_tabs.py` / `InventoryService` | Fase B (estructural). |
| W-3 | **H-028** mover ORM de 13 routers a repos | Fase B (estructural). |
| W-4 | **H-033** paginación + cache SS/ROP | Fase C (escalabilidad). |

## NFRs

- **Rendimiento:** el middleware de correlation ID y el logging JSON no deben añadir >1ms por request; el health check con LLM usa timeout ≤2s.
- **Compatibilidad:** todos los tests existentes (138 unit + 5 e2e) deben seguir pasando. El cambio de `commit()`→`flush()` en `_location_id_for_code` NO debe alterar el comportamiento observable.
- **Seguridad:** los logs JSON NO deben incluir secretos ni datos PII no necesarios (nunca la contraseña, el SESSION_SECRET, ni las API keys). El `request_id` se propaga vía `contextvars` (no se pasa por parámetro a cada función).
- **Observabilidad:** el `X-Request-ID` se genera con `uuid4().hex[:16]` si el cliente no lo envía; se respeta el header entrante (trazabilidad cross-service).
- **No-regresión de auditoría:** `log_event` en flujos NO reordenados (todos excepto ventas/traspasos) mantiene el comportamiento actual (best-effort documentado).

## Fuentes de verdad

- Firmas reales: `specs/auditoria-fase-a/design.md` §4 (leer antes de codificar).
- Envelopes: `specs/auditoria-fase-a/tasks.md`.
- Invariantes del proyecto: `CLAUDE.md` (no tocar `docker-compose*.yml`, `auth.py`, `deps.py`, `db.py` — salvo las tareas que lo indican explícitamente).

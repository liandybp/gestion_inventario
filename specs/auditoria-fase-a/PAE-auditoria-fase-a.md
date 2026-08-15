# PAE-auditoria-fase-a: Subir score de auditoría 2.95 → ~3.45

<!-- ═══════════════════════════════════════════════════════════════
     INSTRUCCIONES PARA OPENCODE — LEER ANTES DE CUALQUIER ACCIÓN
     1. Leer CLAUDE.md del proyecto. Si no existe, detenerse y reportar.
     2. Leer sección 5 (Arquitectura objetivo / design.md) completa antes de tocar archivos.
     3. EJECUTAR VERIFICACIÓN PRE-IMPLEMENTACIÓN (sección 7.0) — sin excepción.
     4. Una fase a la vez (T1→T11 en orden). Sin fase N verificada, no empieza N+1.
     5. Si algo no está especificado aquí o en tasks.md: no se inventa, se reporta.
     6. Archivos fuera del inventario (5.2): no se tocan bajo ninguna circunstancia.
     7. CADA tarea incluye verificación. Si falla: corregir, no avanzar.
     8. Al terminar: ejecutar script de verificación completa (sección 7.2).
     9. docker-compose*.yml NO se toca (invariante) — ver W-1.
     ═══════════════════════════════════════════════════════════════ -->

## 1. Metadatos

| Campo | Valor |
|-------|-------|
| **Proyecto** | gestion_inventario |
| **Rama base** | `develop` |
| **Rama de trabajo** | `feature/auditoria-fase-a` |
| **Fecha** | 2026-08-15 |
| **Versión actual** | v0.4.0 |
| **Versión objetivo** | v0.4.1 |
| **PAE anterior** | `traspaso-masivo` |
| **Fase** | ejecución (OpenCode + `qwen36_35b_coder_mtp` local) |
| **Test runner** | `SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -v` |
| **E2E runner** | `SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/e2e -m e2e --no-cov` |

## 2. Documentos fuente

| Fase SDD | Output | Contenido |
|----------|--------|-----------|
| Phase 1 | `specs/auditoria-fase-a/requirements.md` | REQ-1..10 (MoSCoW) + NFRs + W (fuera de scope) |
| Phase 2 | `specs/auditoria-fase-a/design.md` | Decisiones D1-D8, firmas reales, inventario de archivos |
| Phase 3 | `specs/auditoria-fase-a/tasks.md` | **envelopes T1..T11 con código exacto** |

## 3. Objetivo

Subir el score de la auditoría de **2.95 → ~3.45** (amarillo) atacando 4 dimensiones
de bajo score con quick wins concretos (ver `docs/auditorias/2026-08-15-negocio-sin-pos.md`):

| Dimensión | Hoy | Objetivo | Hallazgos |
|-----------|-----|----------|-----------|
| 7. Observabilidad | 2 | 4 | H-043 (correlation_id + JSON), H-065 (health profundo) |
| 5. Robustez | 3 | 4 | H-030 (circuit breaker LLM), H-039 (logs en excepts) |
| 6. Transaccional | 3 | 4 | H-040 (flush), H-041 (audit atómico ventas/traspasos) |
| 8. Configuración | 3 | 4 | H-045 (os.getenv → settings) |

**Valor esperado:** score 2.95 → ~3.45; debugging de prod con trazabilidad por
request; copiloto que no bloquea al LLM caído; sin ubicaciones huérfanas; audit
fiable en ventas/traspasos; un solo punto de config de env.

**Qué NO se toca:** `docker-compose*.yml` (W-1 H-046, invariante), `app/auth.py`,
`app/deps.py`, `app/models.py`, `app/schemas.py`, `app/templates/*`,
`app/agent/runner.py`, `.env`.

## 4. Decisiones de diseño (cerradas — ver design.md para detalle)

| # | Decisión |
|---|----------|
| D1 | Correlation ID vía middleware + `contextvars` en `logger.py` (JSON siempre) |
| D2 | Health profundo: version/schema_version/database/agent_llm; agent_llm no baja status |
| D3 | Circuit breaker LLM: módulo nuevo `app/agent/circuit_breaker.py`, singleton, thread-safe |
| D4 | Logging en excepts: SOLO añadir log, nunca cambiar comportamiento |
| D5 | `_location_id_for_code`: commit→flush (persiste con la operación padre) |
| D6 | Audit atómico en ventas/traspasos (flush + reorden); best-effort documentado en el resto |
| D7 | Consolidar os.getenv → settings.py; migrations.py conserva seed env (fail-fast) |
| D8 | H-046 compose FUERA de scope (invariante; requiere decisión del usuario) |

## 5. Arquitectura objetivo

### 5.1 Diagrama

```
[Cliente] ──▶ [correlation_id_middleware ★ (X-Request-ID + log JSON)]
                  │  contextvars.request_id
                  ▼
             [ui_auth_middleware · security_headers_middleware] (· sin cambios)
                  │
                  ▼
             [app.logger ★ JSON: ts/level/logger/request_id/message]
                  │
                  ▼
             [health ★ version/schema_version/database/agent_llm]
                  │
                  ▼
             [copiloto: _plan_node ★ → breaker ★ (OPEN→degrade sin LLM)]
                  │
                  ▼
             [inventory_service._location_id_for_code ★ flush()]
                  │
                  ▼
             [audit.log_event ★ flush() + reorden ventas/traspasos]
```

### 5.2 Inventario de archivos (scope duro — ver design.md §5)

- CREATE: `app/agent/circuit_breaker.py`, `tests/unit/test_health.py`, `tests/unit/test_circuit_breaker.py`
- MODIFY: `app/logger.py`, `app/main.py`, `app/routers/health.py`, `app/agent/graph.py`,
  `app/settings.py`, `app/services/inventory_service.py`, `app/audit.py`,
  `app/routers/ui_sales.py`, `app/routers/ui_transfers.py`, `app/routers/ui_tabs.py`,
  `app/routers/ui_sales_documents.py`, `app/routers/ui_products.py`,
  `app/routers/ui_purchases.py`, `app/routers/ui_expenses.py`,
  `app/routers/ui_extractions.py`, `app/db.py`, `app/utils.py`,
  `app/routers/ui_auth.py`, `app/routers/ui_common.py`, `app/business_config.py`

### 5.3 Firmas reales

Ver `specs/auditoria-fase-a/design.md` §4 (obligatorio leer antes de T1).

## 6. Fases de implementación (Task Envelopes)

> Envelopes completos con código exacto en `specs/auditoria-fase-a/tasks.md`.
> Orden estricto T1→T11. Cada tarea tiene su `Verification`.

| Envelope | Título | Files |
|----------|--------|-------|
| T1 | Logging JSON + request_id (logger.py) | `app/logger.py` |
| T2 | Settings comunes + middleware correlation ID | `app/settings.py`, `app/main.py` |
| T3 | Health check profundo | `app/routers/health.py` |
| T4 | Circuit breaker LLM | `app/agent/circuit_breaker.py`, `app/agent/graph.py`, `app/settings.py` |
| T5 | Logging en excepts de ui_tabs.py | `app/routers/ui_tabs.py` |
| T6 | Logging en excepts de los demás routers | 7 routers |
| T7 | flush() en _location_id_for_code | `app/services/inventory_service.py` |
| T8 | Audit atómico ventas/traspasos | `app/audit.py`, `ui_sales.py`, `ui_transfers.py` |
| T9 | Consolidar os.getenv → settings | `db.py`, `utils.py`, `ui_auth.py`, `ui_common.py`, `business_config.py` |
| T10 | Tests (health + circuit breaker) | `tests/unit/test_health.py`, `test_circuit_breaker.py` |
| T11 | Verificación completa + e2e | (sin código) |

## 7. Plan de tests y verificación

### 7.0 Verificación pre-implementación (OBLIGATORIO)

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -m "not e2e" --no-cov 2>&1 | tail -2
# Expected: 138 passed
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/e2e -m e2e --no-cov 2>&1 | tail -2
# Expected: 5 passed
```

Si falla, NO empezar. Reportar.

### 7.1 Por task envelope

Cada envelope de `tasks.md` incluye su `Verification` (comando exacto → PASS).

### 7.2 Script de verificación completa

```bash
#!/bin/bash
set -e
echo "=== Pre-check: tests before changes ==="
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -m "not e2e" --no-cov 2>&1 | tail -2
echo "=== Post-check: all unit tests ==="
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -m "not e2e" --no-cov 2>&1 | tail -2
echo "=== Post-check: e2e ==="
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/e2e -m e2e --no-cov 2>&1 | tail -2
echo "=== os.getenv dispersion check (H-045) ==="
grep -rn "os.getenv" app/ --include="*.py" | grep -v "app/settings.py" | grep -v "load_dotenv" || true
echo "=== Scope check ==="
git diff --name-only develop
echo "=== ALL CHECKS PASSED ==="
```

### 7.3 Protocolo de fallo

1. Leer el error completo. 2. `git diff` para ver el cambio. 3. Corregir el código, no el test.
4. Re-ejecutar. 5. Si tras 3 intentos falla: reportar error + diff + intentos. NO avanzar.

## 8. Riesgos y mitigaciones

| # | Riesgo | Prob. | Impacto | Mitigación |
|---|--------|-------|---------|------------|
| R1 | T8 (audit) rompe tests de auditoría | Media | Medio | Protocolo de fallo; reorden SOLO en ventas/traspasos; resto flush+commit documentado |
| R2 | T2 (correlation middleware) añade overhead | Baja | Bajo | Solo uuid4 + 1 log por request (<1ms) |
| R3 | JSON logging rompe algún parser | Baja | Bajo | No hay parser de logs en el repo; docker logs siguen capturando JSON |
| R4 | T4 (breaker) degrada el copiloto en dev si el modelo local tarda | Media | Medio | Umbral 3 fallos + cooldown 60s; HALF-OPEN recupera solo |
| R5 | T9 (os.getenv→settings) cambia import-time de db.py | Media | Medio | `load_dotenv()` se conserva ANTES de importar settings en db.py; tests cubren |

**Rollback general:** `git revert` del commit de la fase; la rama `feature/auditoria-fase-a` se mergea solo con tests verdes.

## 9. Definición de hecho (DoD)

### 9.1 Checklist pre-commit (en orden)

- [ ] **Pre-check:** 138 unit + 5 e2e pasaban antes (guardar output)
- [ ] **Tests:** ≥142 unit + 5 e2e pasan después
- [ ] **e2e:** `tests/e2e -m e2e` — 5 pasan
- [ ] **App:** `uvicorn app.main:app` arranca (health 200 con campos nuevos)
- [ ] **os.getenv:** solo en `app/settings.py` (+ seed de migrations documentado)
- [ ] **Invariantes:** no se tocó `docker-compose*.yml`, `auth.py`, `deps.py`, `models.py`
- [ ] **Scope:** `git diff --name-only develop` = solo archivos de 5.2
- [ ] **Limpieza:** sin `_tmp`/`_draft`/`_vN`

### 9.2 Commits esperados

```
feat(observability): JSON logging + correlation id + health profundo (H-043/H-065)
feat(agent): circuit breaker LLM (H-030)
feat(logging): log en excepts de routers (H-039)
fix(inventory): flush en _location_id_for_code (H-040)
fix(audit): audit atomico en ventas/traspasos (H-041)
refactor(config): consolidar os.getenv en settings.py (H-045)
test: health + circuit breaker
docs: actualizar CLAUDE.md estado + session notes
```

---

> Fuente: `Templates/template-pae.md` · Generado por SDD Lead (Phase 4 — PAE Assembly)
> Skill: `SDD-Agent` · Firma del SDD Lead: Claude Code (DeepSeek API)

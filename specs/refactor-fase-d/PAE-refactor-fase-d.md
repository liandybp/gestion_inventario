# PAE-refactor-fase-d: Empujón al verde (contract tests + backoff LLM + rate limiter)

<!-- ═══════════════════════════════════════════════════════════════
     INSTRUCCIONES PARA OPENCODE — LEER ANTES DE CUALQUIER ACCIÓN
     1. Leer CLAUDE.md del proyecto. Si no existe, detenerse y reportar.
     2. Leer design.md §4 (firmas reales) y §5 (inventario) ANTES de tocar archivos.
     3. EJECUTAR VERIFICACIÓN PRE-IMPLEMENTACIÓN (sección 7.0) — sin excepción.
     4. Fases en orden: D1 (T1-3) → D2 (T4) → D3 (T5) → T6 verificación.
     5. Si algo no está especificado: no se inventa, se reporta.
     6. Archivos fuera del inventario (design.md §5): no se tocan.
     7. CADA tarea incluye verificación. Si falla: corregir, no avanzar.
     8. docker-compose*.yml NO se toca (invariante).
     ═══════════════════════════════════════════════════════════════ -->

## 1. Metadatos

| Campo | Valor |
|-------|-------|
| **Proyecto** | gestion_inventario |
| **Rama base** | `develop` |
| **Rama de trabajo** | `feature/refactor-fase-d` |
| **Fecha** | 2026-08-15 |
| **Versión actual** | v0.4.0 |
| **Versión objetivo** | v0.4.1 |
| **PAE anterior** | `refactor-fase-c` |
| **Fase** | ejecución (OpenCode + `qwen36_35b_coder_mtp` local) |
| **Test runner** | `SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -v` |

## 2. Documentos fuente

| Fase SDD | Output | Contenido |
|----------|--------|-----------|
| Phase 1 | `specs/refactor-fase-d/requirements.md` | REQ-1..7 + NFRs + W |
| Phase 2 | `specs/refactor-fase-d/design.md` | Decisiones D1-D3, firmas, inventario |
| Phase 3 | `specs/refactor-fase-d/tasks.md` | **envelopes T1..T6** |

## 3. Objetivo

Cruzar a **verde (≥4.2)** desde el 4.00 actual con 3 frentes:

| Frente | Cambio | Impacto |
|--------|--------|---------|
| **D1 Testing** | API contract tests (RBAC + schemas + error paths) + e2e a clientes/compras/documentos | dim9 4→5 |
| **D2 Robustez** | LLM retry con backoff exponencial + jitter | dim5 4→5 |
| **D3 Seguridad** | Rate limiter login keyed por (IP real + username) | dim4 (hacia 5) |

**Objetivo de score:** 4.00 → **4.25 🟢** (dim9 +0.10 + dim5 +0.15; dim4 como bonus).

**Qué NO se toca:** `docker-compose*.yml`, `app/auth.py` (hash), `app/deps.py`,
`app/models.py`, `app/schemas.py`, `app/templates/*`, `app/agent/llm.py`, `app/agent/runner.py`.

## 4. Decisiones de diseño (cerradas — ver design.md)

| # | Decisión |
|---|----------|
| D1 | Contract tests en e2e (`page.request` + live_server) para evitar el problema del SQLite in-memory |
| D2 | Backoff `min(base * 2^n, max) + jitter` en `_plan_node`; config `AGENT_RETRY_*` |
| D3 | `_real_ip(request)` desde X-Forwarded-For (primer valor) + fallback client.host; key `ip|username` |

## 5. Fases de implementación (Task Envelopes)

> Envelopes completos en `specs/refactor-fase-d/tasks.md`. Orden D1→D2→D3.

| Envelope | Título | Files |
|----------|--------|-------|
| T1 | API contract tests (RBAC + schemas) | test_api_contracts_e2e.py |
| T2 | API error paths (4xx no 5xx) | test_api_contracts_e2e.py |
| T3 | e2e flujos: cliente/compra/documento | test_flows_e2e.py |
| T4 | LLM retry backoff/jitter | graph.py, settings.py, config.py |
| T5 | Rate limiter IP+username | ui_auth.py |
| T6 | Verificación completa | (verificación) |

## 6. Riesgos y mitigaciones

| # | Riesgo | Prob. | Impacto | Mitigación |
|---|--------|-------|---------|------------|
| R1 | Contract tests con payloads de schema equivocado | Media | Medio | Leer `app/schemas.py` antes; 4xx es aceptable si no 5xx |
| R2 | T4 (backoff) duerme el thread en retry | Baja | Bajo | Backoff ≤5s, solo en retry (no primero); breaker ya limita |
| R3 | T5 (rate limiter) bloquea legítimos tras proxy | Baja | Medio | Fallback a client.host; key por IP+usuario (no solo IP) |

**Rollback:** cada hito es commit atómico; `git revert`.

## 7. Plan de tests y verificación

### 7.0 Pre-implementación (OBLIGATORIO)

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -m "not e2e" --no-cov 2>&1 | tail -2
# Expected: 145 passed
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/e2e -m e2e --no-cov 2>&1 | tail -2
# Expected: 14 passed
```

### 7.2 Verificación completa

```bash
#!/bin/bash
set -e
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -m "not e2e" --no-cov 2>&1 | tail -2
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/e2e -m e2e --no-cov 2>&1 | tail -2
echo "=== contract tests ==="
grep -c "def test_" tests/e2e/test_api_contracts_e2e.py
echo "=== flows e2e ==="
grep -c "def test_" tests/e2e/test_flows_e2e.py
echo "=== scope ==="
git diff --name-only develop
echo "=== ALL CHECKS PASSED ==="
```

### 7.3 Protocolo de fallo

1. Leer error. 2. `git diff`. 3. Corregir código. 4. Re-ejecutar. 5. Tras 3 intentos: reportar, NO avanzar.

## 8. Definición de hecho (DoD)

### 8.1 Checklist pre-commit

- [ ] **Pre-check:** 145 unit + 14 e2e pasaban antes
- [ ] **Contract tests:** ≥3 (RBAC admin/operator + schemas + error paths)
- [ ] **e2e flows:** ≥7 (gasto/extracción/profit/venta + cliente/compra/documento)
- [ ] **Backoff LLM:** retries con backoff+jitter (no inmediatos)
- [ ] **Rate limiter:** key `ip|username` con X-Forwarded-For
- [ ] **Tests:** suite unit + e2e pasan
- [ ] **Invariantes:** no se tocó compose/auth/deps/models/templates
- [ ] **Scope:** `git diff --name-only develop` = solo design.md §5
- [ ] **Limpieza:** sin temporales

### 8.2 Commits esperados

```
test(api): contract tests (RBAC + schemas + error paths)
test(e2e): flujos cliente/compra/documento
feat(agent): LLM retry con backoff/jitter
fix(security): rate limiter login keyed por IP+usuario
docs: actualizar CLAUDE.md estado + session notes + informe auditoria
```

---

> Fuente: `Templates/template-pae.md` · Generado por SDD Lead (Phase 4 — PAE Assembly)
> Skill: `SDD-Agent` · Firma del SDD Lead: Claude Code (DeepSeek API)

# PAE-refactor-fase-b: SoC + Capas (dividir ui_tabs + inventory_service + LocationRepository)

<!-- ═══════════════════════════════════════════════════════════════
     INSTRUCCIONES PARA OPENCODE — LEER ANTES DE CUALQUIER ACCIÓN
     1. Leer CLAUDE.md del proyecto. Si no existe, detenerse y reportar.
     2. Leer design.md §4 (firmas reales) y §5 (inventario) ANTES de tocar archivos.
     3. EJECUTAR VERIFICACIÓN PRE-IMPLEMENTACIÓN (sección 7.0) — sin excepción.
     4. Fases en orden: B1 (T1..T10) → B2 (T11..T15) → B3 (T16..T18). Sin N verificada, no N+1.
     5. Si algo no está especificado: no se inventa, se reporta.
     6. Archivos fuera del inventario (design.md §5): no se tocan.
     7. CADA tarea incluye verificación. Si falla: corregir, no avanzar.
     8. Al terminar: script de verificación completa (sección 7.2).
     9. Las rutas /ui/tabs/* y el HTML servido deben ser IDÉNTICOS antes y después.
     ═══════════════════════════════════════════════════════════════ -->

## 1. Metadatos

| Campo | Valor |
|-------|-------|
| **Proyecto** | gestion_inventario |
| **Rama base** | `develop` |
| **Rama de trabajo** | `feature/refactor-fase-b` |
| **Fecha** | 2026-08-15 |
| **Versión actual** | v0.4.0 |
| **Versión objetivo** | v0.4.1 |
| **PAE anterior** | `auditoria-fase-a` |
| **Fase** | ejecución (OpenCode + `qwen36_35b_coder_mtp` local) |
| **Test runner** | `SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -v` |
| **E2E runner** | `SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/e2e -m e2e --no-cov` |

## 2. Documentos fuente

| Fase SDD | Output | Contenido |
|----------|--------|-----------|
| Phase 1 | `specs/refactor-fase-b/requirements.md` | REQ-1..7 + NFRs + W |
| Phase 2 | `specs/refactor-fase-b/design.md` | Mapa de movimiento D1, extracción D2, LocationRepository D3, firmas, inventario |
| Phase 3 | `specs/refactor-fase-b/tasks.md` | **envelopes T1..T18** |

## 3. Objetivo

Subir **dim1 SoC** y **dim3 Capas** de 2 a 4 (score global → ~3.85) mediante:

- **B1 — Dividir `ui_tabs.py`** (2437 ln, 36 handlers) en routers por dominio
  (`ui_home`, `ui_stock`, `ui_profit_items`, `ui_history`, `ui_activity`,
  `ui_dividends`, `ui_profit`, `ui_tabs_common` + mover `tab_*` a sus routers
  existentes). Rutas EXACTAS preservadas.
- **B2 — Extraer `FinanceService` y `ReportingService`** de `InventoryService`
  (3066 → <2300 ln). Verificado: los métodos a mover no dependen del core FIFO.
- **B3 — `LocationRepository`** único (H-049): resolver código→id en un solo lugar.

**Valor:** merge conflicts casi nulos por feature (ya no un solo archivo de 2400 ln);
testing de un tab sin levantar el dashboard entero; lógica de finance/reporting
reutilizable; fin de la divergencia de resolución de ubicaciones.

**Qué NO se toca:** `docker-compose*.yml`, `app/auth.py`, `app/deps.py`,
`app/db.py`, `app/models.py`, `app/schemas.py`, `app/templates/*`,
`app/agent/graph.py|runner.py|llm.py|widgets.py`, `app/routers/ui_auth.py|ui_agent.py`.

## 4. Decisiones de diseño (cerradas — ver design.md)

| # | Decisión |
|---|----------|
| D1 | Mover `tab_*` a routers de dominio existentes + nuevos; helpers a `ui_tabs_common.py` |
| D2 | `FinanceService`/`ReportingService` con `(db, business_id)`; InventoryService expone `business_id` |
| D3 | `LocationRepository.location_id_for_code` fuente única; `inventory_service` delega |
| D4 | Fase B NO migra todo el ORM de routers (queda para Fase B2) |

## 5. Fases de implementación (Task Envelopes)

> Envelopes completos en `specs/refactor-fase-b/tasks.md`. Orden B1→B2→B3.

| Envelope | Título | Files |
|----------|--------|-------|
| T1 | `ui_tabs_common.py` (helpers compartidos) | CREATE |
| T2 | `ui_home.py` (/, /dashboard, /tabs/home, /home-charts) | CREATE + ui.py |
| T3 | `ui_stock.py` (stock/restock/return-supplier) | CREATE + ui.py |
| T4 | `ui_profit_items.py` (profit-items + sub-rutas) | CREATE + ui.py |
| T5 | `ui_history.py` + `ui_activity.py` | CREATE + ui.py |
| T6 | `ui_dividends.py` + `ui_profit.py` | CREATE + ui.py |
| T7 | `tab_customers`/`tab_inventory`/`tab_purchases`/`tab_sales`/`tab_documents` → routers existentes | MODIFY 5 routers |
| T8 | `tab_expenses`/`tab_transfers`/`tab_users`/`tab_businesses` → routers existentes | MODIFY 4 routers |
| T9 | Registrar todos los routers nuevos en `ui.py` + eliminar `ui_tabs.py` | MODIFY ui.py, DELETE ui_tabs.py |
| T-e2e-1 | **Smoke e2e de los 16 tabs** (caza rutas rotas del split B1) | CREATE test_tabs_smoke_e2e.py |
| T10 | **Hito B1:** suite + e2e (incl. smoke) verdes | (verificación) |
| T11 | `FinanceService` (expense/extraction/dividends) | CREATE finance_service.py |
| T12 | `ReportingService` (profit/series/metrics) | CREATE reporting_service.py |
| T13 | InventoryService: eliminar métodos movidos + `@property business_id` | MODIFY inventory_service.py |
| T14 | Callers: ui_expenses, ui_extractions, ui_dividends/ui_profit/ui_profit_items | MODIFY |
| T15 | Callers: agent/tools.py + **hito B2** | MODIFY + verificación |
| T-e2e-2 | **Flujos críticos post-B2** (gasto/extracción/profit-items vía UI) | CREATE test_flows_e2e.py |
| T16 | `LocationRepository` (location_id_for_code + location_name_for_code) | CREATE |
| T17 | Migrar inventory_service + ui_sales_documents a LocationRepository | MODIFY 2 |
| T18 | **Hito B3 + verificación completa (incl. e2e nuevos)** | (verificación) |

## 6. Riesgos y mitigaciones

| # | Riesgo | Prob. | Impacto | Mitigación |
|---|--------|-------|---------|------------|
| R1 | Ruta 404 si un router no se registra | Media | Alto | Cada envelope nuevo registra en ui.py + verificación de ruta (TestClient) |
| R2 | Import circular entre ui_tabs_common y routers | Baja | Medio | `ui_tabs_common` NO importa routers; solo models/utils/security |
| R3 | B2 rompe tools del agente | Media | Medio | Envelope T15 actualiza tools.py + tests de agent (test_agent_tools) |
| R4 | Behavior drift al mover handlers (cambio accidental de query) | Media | Alto | Mover el código VERBATIM; no refactorizar durante el movimiento; e2e validan |
| R5 | ui_tabs.py con helpers usados por varios routers | Media | Medio | T1 centraliza primero; cada router importa de ui_tabs_common |

**Rollback:** cada hito (B1/B2/B3) es un commit atómico; `git revert` del hito.

## 7. Plan de tests y verificación

### 7.0 Pre-implementación (OBLIGATORIO)

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -m "not e2e" --no-cov 2>&1 | tail -2
# Expected: 142+ passed (post-Fase A) o los que haya
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/e2e -m e2e --no-cov 2>&1 | tail -2
# Expected: 5 passed
```

### 7.1 Por envelope

Cada envelope de `tasks.md` tiene su `Verification` (comando → PASS). Regla
especial de B1: tras CADA router nuevo, verificar que la ruta responde 200 con
`TestClient` (login mock) o que los templates se importan sin error.

### 7.2 Verificación de hitos

```bash
# Hito B1 (T10): suite + e2e + ui_tabs.py < 200 ln
wc -l app/routers/ui_tabs.py
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -m "not e2e" --no-cov 2>&1 | tail -2
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/e2e -m e2e --no-cov 2>&1 | tail -2

# Hito B2 (T15): inventory_service.py < 2300 ln
wc -l app/services/inventory_service.py
# suite + e2e verdes

# Hito B3 (T18): suite + e2e + scope
grep -rn "_location_id_for_code\|location_id_for_code" app/ --include="*.py" | grep -v location_repository.py
git diff --name-only develop
echo "=== ALL HITOS PASSED ==="
```

### 7.3 Protocolo de fallo

1. Leer error. 2. `git diff`. 3. Corregir código (mover es verbatim, sin refactor).
4. Re-ejecutar. 5. Tras 3 intentos: reportar error + diff + intentos, NO avanzar.

## 8. Definición de hecho (DoD)

### 8.1 Checklist pre-commit

- [ ] **Pre-check:** suite (unit + e2e) pasaba antes
- [ ] **Tests:** suite unit pasan después de cada hito
- [ ] **Smoke e2e:** `test_tabs_smoke_e2e.py` navega los 16 tabs sin error (B1)
- [ ] **Flows e2e:** `test_flows_e2e.py` (gasto/extracción/profit-items) pasan (B2)
- [ ] **e2e completo:** `pytest tests/e2e -m e2e` — 5 existentes + smoke + flujos, todos verdes
- [ ] **`ui_tabs.py`** < 200 ln o eliminado
- [ ] **`inventory_service.py`** < 2300 ln
- [ ] **LocationRepository:** 1 sola implementación de resolución de ubicación
- [ ] **Invariantes:** no se tocó compose/auth/deps/models/templates
- [ ] **Scope:** `git diff --name-only develop` = solo archivos de design.md §5
- [ ] **Limpieza:** sin archivos temporales

### 8.2 Commits esperados

```
refactor(tabs): dividir ui_tabs.py en routers por dominio (H-026)
refactor(services): extraer FinanceService y ReportingService (H-023)
refactor(layers): LocationRepository unico para resolucion de ubicacion (H-049)
test: services extraidos + rutas de tabs
```

---

> Fuente: `Templates/template-pae.md` · Generado por SDD Lead (Phase 4 — PAE Assembly)
> Skill: `SDD-Agent` · Firma del SDD Lead: Claude Code (DeepSeek API)

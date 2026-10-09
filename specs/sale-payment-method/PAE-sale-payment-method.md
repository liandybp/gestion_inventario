# PAE-sale-payment-method: Método de pago (transferencia) en venta rápida

<!-- ═══════════════════════════════════════════════════════════════
     INSTRUCCIONES PARA OPENCODE — LEER ANTES DE CUALQUIER ACCIÓN
     ═══════════════════════════════════════════════════════════════
     1. Leer CLAUDE.md del proyecto (raíz y .claude/CLAUDE.md). Si no existe, detenerse.
     2. Leer sección 3 (Arquitectura objetivo) completa antes de tocar archivos.
     3. EJECUTAR VERIFICACIÓN PRE-IMPLEMENTACIÓN (sección 5.0) — sin excepción.
     4. Una tarea a la vez, en orden T1→T14. Sin T(N) verificada, no empieza T(N+1).
     5. Si algo no está especificado aquí: no se inventa, se reporta.
     6. Archivos fuera del inventario (3.2): no se tocan bajo ninguna circunstancia.
     7. CADA TAREA incluye verificación. Si falla: corregir, no avanzar.
     8. Al terminar: ejecutar script de verificación completa (sección 7.2).
     ═══════════════════════════════════════════════════════════════ -->

## Metadatos

| Campo | Valor |
|-------|-------|
| **Proyecto** | gestion_inventario |
| **Rama base** | develop |
| **Rama de trabajo** | feature/sale-payment-method |
| **Fecha** | 2026-08-18 |
| **Versión actual** | v0.4.5 |
| **Versión objetivo** | v0.4.6 (PATCH — regla dura del proyecto, nunca MINOR) |
| **PAE anterior** | ninguno específico (continúa sobre el fix de sesión de ubicaciones huérfanas, ya mergeado fuera de este PAE) |
| **Fase** | planificación (Claude Code) → ejecución (OpenCode + Qwen3.6-35B-A3B local, GLM-4.7-Flash fallback) |

### Documentos fuente de este PAE

| Fase SDD | Output | Propósito |
|----------|--------|-----------|
| Phase 1 — Requirements | `specs/sale-payment-method/requirements.md` | 8 REQs con Gherkin, MoSCoW, NFRs, out-of-scope |
| Phase 2 — Design | `specs/sale-payment-method/design.md` | Data model, contratos, seguridad, trade-offs (D1-D5), pre-mortem 10 dimensiones |
| Phase 3 — Task Envelopes | `specs/sale-payment-method/tasks.md` | 14 envelopes autocontenidos para OpenCode |

### Skills asignadas a OpenCode

- [ ] `data-engineer` — modelo + migración (T1, T2, T4)
- [ ] `backend-developer` — schemas, service, routers (T3, T5, T6, T7)
- [ ] `frontend-developer` — templates (T8, T9, T10)
- [ ] `test-automation-engineer` — tests unitarios + e2e (T11, T12)
- [ ] `release-manager` — version bump PATCH (T13)
- [ ] `technical-writer` — CHANGELOG (T14)

---

## 1. Objetivo

### 1.1 Qué se construye

Un campo booleano `paid_by_transfer` en el flujo de venta rápida (`InventoryMovement`,
endpoints `POST /ui/sale` y `POST /ui/sale/barcode`): un checkbox "Pagado por
transferencia" — desmarcado por defecto — que se guarda al registrar la venta, se puede
editar después desde "Editar venta", se muestra en el historial de ventas (tab Ventas), y
se puede filtrar ahí ("Todos" / "Transferencia" / "Efectivo/otro").

### 1.2 Por qué

El usuario necesita identificar qué ventas se cobraron por transferencia bancaria para uso
futuro (reportes, conciliación de caja). Hoy `InventoryMovement` no distingue el método de
pago de una venta.

### 1.3 Valor esperado

- Cada venta nueva registrada desde el panel rápido queda etiquetada con su método de pago.
- El historial de ventas permite responder "¿cuánto se cobró por transferencia este mes?"
  filtrando visualmente, sin necesidad de consultar la BD directamente.
- Base de datos lista para reportes agregados futuros (fuera de scope de este PAE).

### 1.4 Qué NO se toca

- `SalesDocument` / `SalesDocumentItem` (facturas y presupuestos) — confirmado
  explícitamente con el usuario que esta feature NO aplica ahí.
- `app/routers/ui_profit_items.py` y `partials/profit_items_sale_edit_form.html` (segundo
  flujo de edición de venta, tab "Ganancia por artículo") — comparten
  `InventoryService.update_sale()` pero su UI no se modifica; se garantiza que no
  regresionan (ver D3 en `design.md` y REQ-8).
- Reportes agregados, dashboards, tools del copiloto IA — el dato solo se captura, edita,
  muestra y filtra en el tab Ventas por ahora.
- Cualquier lógica de conciliación de caja o cuadre efectivo/transferencia.

---

## 2. Decisiones de diseño

| # | Decisión | Alternativas | Justificación | Implicaciones |
|---|----------|-------------|----------------|----------------|
| D1 | Columna `paid_by_transfer: Boolean` en `InventoryMovement` | Enum de método de pago (`cash`/`transfer`/`card`/...) | El usuario pidió un check binario explícito; un enum es sobre-ingeniería para el requisito actual | Migrable a enum más adelante sin romper el booleano |
| D2 | Alcance limitado a `InventoryMovement` (venta rápida) | También `SalesDocument`/`SalesDocumentItem` | Confirmado con el usuario vía pregunta directa | Los documentos de venta no llevan este dato en esta iteración |
| D3 | `update_sale(..., paid_by_transfer: Optional[bool] = None)` — si `None`, no se toca el valor actual | Parámetro `bool` obligatorio con default `False` | `update_sale()` tiene un segundo caller (`ui_profit_items.py`) que no pasa este campo; un default `False` fijo resetearía silenciosamente el valor en cada edición desde ese tab | Sin este trade-off, REQ-8 se rompe (regresión real detectada en el pre-mortem) |
| D4 | Filtro `payment_method` como query param del mismo endpoint `GET /ui/tabs/sales` | Endpoint nuevo dedicado | Consistente con el patrón ya usado para `location_code`/`query`/fechas en ese mismo endpoint | Ningún endpoint nuevo que mantener |
| D5 | Columna nueva al final de la tupla de `recent_sales()` (después de `lot_codes`) | Insertarla junto a `unit_price` | `sales_history.html` es el único consumidor verificado de esa tupla; agregar al final minimiza el diff y el riesgo de desalinear columnas | Ninguna — decisión de bajo riesgo |

---

## 3. Arquitectura objetivo

### 3.1 Diagrama de componentes

```
sale_panel.html (✦ checkbox creación + ✦ filtro historial)
sale_edit_form.html (✦ checkbox edición)
sales_history.html (✦ columna "Pago")
        │
        ▼
ui_sales.py (✦ sale, sale_barcode, sale_update, tab_sales — Form fields + query param)
        │
        ▼
inventory_service.py (✦ sale, update_sale, recent_sales — wiring)
        │
        ▼
inventory_repository.py (✦ recent_sales — columna + filtro SQL)
        │
        ▼
models.py (★ InventoryMovement.paid_by_transfer)
migrations.py (★ ALTER TABLE idempotente, sqlite + postgres)
schemas.py (✦ SaleCreate.paid_by_transfer, MovementRead.paid_by_transfer)

· ui_profit_items.py — SIN CAMBIOS (sigue llamando update_sale() sin el campo nuevo,
  protegido por D3)
```

### 3.2 Inventario de archivos

> **Scope duro:** Solo estos archivos. Ninguno más.
> **Verificación:** `git diff --name-only develop` debe mostrar exactamente estos archivos.

| Archivo | Operación | Responsabilidad |
|---------|-----------|-----------------|
| `app/models.py` | MODIFY | Columna `paid_by_transfer` en `InventoryMovement` |
| `app/migrations.py` | MODIFY | Migración idempotente SQLite + PostgreSQL |
| `app/schemas.py` | MODIFY | `SaleCreate.paid_by_transfer`, `MovementRead.paid_by_transfer` |
| `app/repositories/inventory_repository.py` | MODIFY | `recent_sales()` — columna + filtro `payment_method` |
| `app/services/inventory_service.py` | MODIFY | `sale()`, `update_sale()`, `recent_sales()` — wiring |
| `app/routers/ui_sales.py` | MODIFY | `sale()`, `sale_barcode()`, `sale_update()`, `tab_sales()` — Form fields + query param |
| `app/templates/partials/sale_panel.html` | MODIFY | Checkbox creación + filtro historial |
| `app/templates/partials/sale_edit_form.html` | MODIFY | Checkbox edición |
| `app/templates/partials/sales_history.html` | MODIFY | Columna "Pago" en la tabla |
| `tests/unit/test_inventory_service_unit.py` | MODIFY | 5 tests nuevos |
| `tests/unit/test_schemas.py` | MODIFY | 1 test nuevo |
| `tests/e2e/test_sale_payment_method_e2e.py` | CREATE | 2 tests e2e |
| `VERSION` | MODIFY | `0.4.5` → `0.4.6` |
| `.bumpversion.cfg` | MODIFY | `current_version = 0.4.6` |
| `CHANGELOG.md` | MODIFY | Entrada `0.4.6` |

### 3.3 Dependencias existentes (firmas reales del repo)

```python
# app/models.py — InventoryMovement (línea 221), patrón booleano ya usado en User (línea 64):
must_change_password: Mapped[bool] = mapped_column(
    Boolean, nullable=False, default=False, server_default="false"
)

# app/schemas.py:
class SaleCreate(BaseModel):
    sku: str
    quantity: float
    unit_price: Optional[float] = None
    movement_date: Optional[datetime] = None
    note: Optional[str] = None
    location_code: Optional[str] = None

# app/services/inventory_service.py:
def sale(self, payload: SaleCreate) -> MovementResult: ...
def update_sale(self, movement_id: int, sku: str, quantity: float, unit_price: float,
                 movement_date: Optional[datetime], note: Optional[str]) -> MovementResult: ...
def recent_sales(self, query="", limit=20, start_date=None, end_date=None,
                  month=None, year=None, location_id=None) -> list[tuple]: ...

# app/repositories/inventory_repository.py — SELECT de recent_sales() ya devuelve
# (id, movement_date, sku, name, unit_of_measure, image_url, quantity, unit_price,
#  location_name, lot_codes) — la tupla consumida por sales_history.html.

# app/routers/ui_profit_items.py — SEGUNDO caller de update_sale(), fuera de scope,
# NO se modifica (ver D3):
result = service.update_sale(
    movement_id=movement_id, sku=sku, quantity=quantity, unit_price=unit_price,
    movement_date=parse_dt(movement_date), note=note or None,
)
```

### 3.4 Interfaces nuevas a implementar

Ver `specs/sale-payment-method/tasks.md` — cada envelope (T1-T14) contiene el before/after
exacto de cada firma nueva o modificada. No se crea ningún componente nuevo (repo/service);
todo es extensión de interfaces existentes.

---

## 4. Fases de implementación (Task Envelopes)

> Los 14 task envelopes completos (con Context, Instructions before/after, Verification y
> Expected Report) están en `specs/sale-payment-method/tasks.md`. Ejecutar en orden:

1. **T1** — Columna `paid_by_transfer` en `InventoryMovement` (`app/models.py`)
2. **T2** — Migración SQLite + PostgreSQL (`app/migrations.py`)
3. **T3** — Schemas `SaleCreate` / `MovementRead` (`app/schemas.py`)
4. **T4** — `InventoryRepository.recent_sales()` — columna + filtro (`app/repositories/inventory_repository.py`)
5. **T5** — `InventoryService.sale/update_sale/recent_sales` (`app/services/inventory_service.py`)
6. **T6** — `ui_sales.py` — `sale()` + `sale_barcode()` (creación)
7. **T7** — `ui_sales.py` — `sale_update()` + `tab_sales()` (edición + filtro)
8. **T8** — `sale_panel.html` — checkbox creación + filtro historial
9. **T9** — `sale_edit_form.html` — checkbox edición
10. **T10** — `sales_history.html` — columna "Pago"
11. **T11** — Tests unitarios (`test_inventory_service_unit.py`, `test_schemas.py`)
12. **T12** — Test e2e (`test_sale_payment_method_e2e.py`)
13. **T13** — Version bump PATCH (`VERSION`, `.bumpversion.cfg`)
14. **T14** — CHANGELOG (`CHANGELOG.md`)

---

## 5. Plan de tests y verificación

### 5.0 Verificación pre-implementación (OBLIGATORIO — antes de tocar código)

```bash
# 1. Todos los tests existentes deben pasar ANTES de empezar
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -v

# 2. La app debe arrancar sin errores
SESSION_SECRET=test uvicorn app.main:app --host 0.0.0.0 --port 10000 &
sleep 2 && curl -sf http://localhost:10000/health && kill %1
```

Si alguno de estos falla, **NO empezar la implementación**. Reportar el fallo y esperar
instrucciones.

### 5.1 Verificación por task envelope

Cada envelope en `tasks.md` incluye su propio bloque `#### Verification` con comandos
mecánicos (`pytest -v` → PASS/FAIL). No hay `tests/test_schema_drift.py` ni `seed.py` en
este proyecto — el equivalente es: (a) el modelo se valida vía `Base.metadata.create_all`
en `tests/conftest.py::db_session`, y (b) la migración de arranque se valida en T2 con un
script mecánico contra una BD SQLite temporal real (`app.migrations.run_startup_tasks()`).

### 5.2 Protocolo cuando una verificación falla

1. Leer el error completo — no asumir la causa.
2. Verificar el diff — `git diff` para ver exactamente qué se cambió.
3. Corregir el código — no el test.
4. Re-ejecutar la verificación — repetir hasta que pase.
5. Si después de 3 intentos sigue fallando: reportar el error completo, el diff, y lo
   intentado. NO seguir a la siguiente tarea.

### 5.3 Tests existentes que deben seguir pasando

- [ ] `tests/unit/test_inventory_service_unit.py` — todos los tests existentes sin cambios
- [ ] `tests/unit/test_schemas.py` — todos los tests existentes sin cambios
- [ ] `tests/unit/*` (resto de la suite) — sin regresión
- [ ] `tests/e2e/test_flows_e2e.py::test_sale_flow_reduces_stock` — sigue pasando (el
      checkbox nuevo tiene default `False`, no interfiere con este test existente)
- [ ] `SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -v` — 100% verde antes de empezar

### 5.4 Tests e2e (OBLIGATORIO)

Cubiertos en **T12** (`tests/e2e/test_sale_payment_method_e2e.py`), marcados `@pytest.mark.e2e`
(vía `pytestmark = pytest.mark.e2e`), excluidos del run unitario por defecto. Se ejecutan
con:
```bash
SESSION_SECRET=test python3 -m pytest tests/e2e -m e2e --no-cov -v
```

---

## 6. Riesgos y mitigaciones

| # | Riesgo | Prob. | Impacto | Síntoma | Mitigación | Rollback |
|---|--------|-------|---------|---------|------------|----------|
| R1 | `update_sale()` resetea `paid_by_transfer` a `False` al editar desde el tab "Ganancia por artículo" | Media (sin mitigar) | Medio (dato incorrecto silencioso) | Ventas marcadas como transferencia vuelven a "efectivo" sin que el usuario lo pida | D3: `paid_by_transfer: Optional[bool] = None`, solo se escribe si no es `None` (T5) | Revertir T5 a `bool` obligatorio — reintroduce el riesgo, no recomendado |
| R2 | Migración de `inventory_movements` falla en PostgreSQL prod por lock de tabla grande | Baja | Alto (downtime) | `ALTER TABLE` tarda o bloquea escrituras | Columna nullable-con-default es una operación O(1) metadata-only en PostgreSQL ≥11 (no reescribe la tabla) — mismo patrón ya usado exitosamente para `location_id` y `lead_time_days` en este proyecto | `DROP COLUMN paid_by_transfer` — aditiva, sin dependientes |
| R3 | Template `sales_history.html` rompe si algún otro consumidor no actualizado desempaqueta la tupla vieja de 10 elementos | Baja (verificado con grep) | Alto (500 en tab Ventas) | `ValueError: not enough values to unpack` | Grep confirmó que `sales_history.html` es el único consumidor por posición — T10 lo actualiza en el mismo envelope que T4 lo cambia | Revertir T4 y T10 juntos |
| R4 | Checkbox de filtro (`payment_method`) rompe el `hx-trigger` existente si el `id` no coincide | Baja | Bajo (filtro no dispara, resto de la UI sigue funcionando) | Cambiar método de pago no refresca la tabla sin recargar | T8 usa `id="sales-filter-payment"` consistente entre el `<select>` y el string de `hx-trigger` (mismo patrón que `sales-filter-location`) | N/A — fix de un `id` es trivial |

---

## 7. Definición de hecho (DoD)

### 7.1 Checklist pre-commit (OBLIGATORIO — ejecutar en orden)

- [ ] **Pre-check:** `SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -v` — 100% tests pasaban antes de empezar
- [ ] **Tests:** mismo comando — 100% tests pasan después de los cambios (incluye los 6 nuevos de T11)
- [ ] **e2e:** `SESSION_SECRET=test python3 -m pytest tests/e2e -m e2e --no-cov -v` — incluye los 2 nuevos de T12
- [ ] **Migración:** script mecánico de T2 — "paid_by_transfer column present after 2 runs: OK"
- [ ] **App:** `uvicorn app.main:app --host 0.0.0.0 --port 10000` — arranca sin errores
- [ ] **Invariantes:** ningún archivo de `SalesDocument`/`SalesDocumentItem` tocado; `docker-compose*.yml`, `.env`, `requirements.txt` sin cambios; `app/auth.py`, `app/db.py`, `app/deps.py` sin cambios
- [ ] **Scope:** `git diff --name-only develop` muestra exactamente los 15 archivos de 3.2, nada más
- [ ] **Limpieza:** no hay archivos temporales (`_tmp`, `_test`, `_draft`, `_vN`) sin eliminar (incluyendo `/tmp/mig_test_paid_by_transfer.db` del Step de verificación de T2)
- [ ] **Versión:** `VERSION` y `.bumpversion.cfg` en `0.4.6` (PATCH, no MINOR)
- [ ] **CHANGELOG:** entrada `0.4.6` presente

### 7.2 Script de verificación completa

```bash
#!/bin/bash
# Guardar como specs/sale-payment-method/verify.sh — ejecutar antes de declarar "hecho"
set -e
echo "=== Pre-check: tests before changes ==="
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -v 2>&1 | tee /tmp/pre-tests.log
echo "=== Post-check: all unit tests ==="
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -v
echo "=== Post-check: e2e ==="
SESSION_SECRET=test python3 -m pytest tests/e2e -m e2e --no-cov -v
echo "=== Migration check ==="
rm -f /tmp/mig_test_paid_by_transfer.db
DATABASE_URL=sqlite+pysqlite:////tmp/mig_test_paid_by_transfer.db SESSION_SECRET=test \
ADMIN_USERNAME=admin ADMIN_PASSWORD=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test \
python3 -c "
from app.migrations import run_startup_tasks
from app.db import engine
run_startup_tasks()
with engine.begin() as conn:
    cols = {row[1] for row in conn.exec_driver_sql('PRAGMA table_info(inventory_movements)').fetchall()}
    assert 'paid_by_transfer' in cols, cols
print('Migration OK')
"
rm -f /tmp/mig_test_paid_by_transfer.db
echo "=== Version check ==="
cat VERSION
grep current_version .bumpversion.cfg
echo "=== Scope check ==="
git diff --name-only develop
echo "=== ALL CHECKS PASSED ==="
```

### 7.3 Commits esperados

```
feat(models): add paid_by_transfer column to InventoryMovement
feat(db): add idempotent migration for paid_by_transfer (sqlite + postgresql)
feat(schemas): add paid_by_transfer to SaleCreate and MovementRead
feat(inventory): filter and expose paid_by_transfer in recent_sales
feat(sales): wire paid_by_transfer through sale/update_sale service methods
feat(ui): capture paid_by_transfer checkbox on sale creation
feat(ui): edit paid_by_transfer and filter sales history by payment method
feat(ui): show payment method checkbox/column in sale templates
test(inventory): cover paid_by_transfer create/update/filter behavior
test(e2e): cover sale payment method checkbox flow
chore(release): bump version 0.4.5 → 0.4.6
docs(changelog): add 0.4.6 entry
```

---

> Generado por: SDD Lead (Phase 4 — PAE Assembly)
> Fuente: `Templates/template-pae.md`

## PAE complete at `specs/sale-payment-method/PAE-sale-payment-method.md`.
## 14 task envelopes ready for OpenCode + Qwen local (GLM fallback).
## Awaiting approval before any implementation begins.

# PAE-copiloto-movimientos-articulo: movimientos de un artículo en un período

<!-- ═══════════════════════════════════════════════════════════════
     EJECUCIÓN POR cc_local — NO LEER ESTE PAE ENTERO
     ═══════════════════════════════════════════════════════════════
     Se ejecuta con:  cc_local_pae <repo> specs/copiloto-movimientos-articulo/tasks.md [flags]
     que lanza UN LOTE POR SESIÓN LIMPIA y pasa a cc_local solo los rangos de líneas de sus envelopes.
     El envelope es la única fuente: código exacto, sin consultar notas ni docs genéricas.
     ═══════════════════════════════════════════════════════════════ -->

## Metadatos

| Campo | Valor |
|-------|-------|
| **Proyecto** | gestion_inventario |
| **Rama base** | `develop` (`9655c8c`) |
| **Rama de trabajo** | `feature/copiloto-movimientos-articulo` (ya creada) |
| **Fecha** | 2026-11-02 |
| **Versión actual** | 0.5.0 (agrupada — sin bump) |
| **Versión objetivo** | 0.5.0 (sin bump) |
| **PAE anterior** | `specs/frontend-fase5b-queries-uicontext/` |
| **Fase** | planificación (Claude Code + DeepSeek) → ejecución (cc_local) |

### Documentos fuente de este PAE

| Fase SDD | Output | Propósito |
|----------|--------|-----------|
| Phase 1 — Requirements | `specs/copiloto-movimientos-articulo/requirements.md` | REQ-1..10 con Gherkin, MoSCoW, NFRs |
| Phase 2 — Design | `specs/copiloto-movimientos-articulo/design.md` | Arquitectura, contratos, seguridad, trade-offs, pre-mortem |
| Phase 3 — Task Envelopes | `specs/copiloto-movimientos-articulo/tasks.md` | 14 envelopes autocontenidos para cc_local |

### Skills asignadas a cc_local

- `backend-developer` — repos, `ReportingService`, tool `product_movements`, regla del prompt
- `test-automation-engineer` — tests unitarios, e2e y casos de eval
- `frontend-developer` — nota al pie en `widget_table.html`
- `technical-writer` — `CLAUDE.md` / `.claude/CLAUDE.md` / `CHANGELOG.md`

---

## 1. Objetivo

### 1.1 Qué se construye

Una tool nueva del copiloto IA, `product_movements`, que devuelve una tabla con todos los movimientos de un
artículo concreto en un período: compras, ventas, traspasos, ajustes y devoluciones a proveedor, en todos los
puntos de venta o en uno filtrado, con fila de saldo inicial y columna de saldo acumulado.

### 1.2 Por qué

El usuario pidió (2026-11-02) poder preguntar "el movimiento de un artículo por su código en un período" y
recibir una tabla completa de su kárdex, algo que `movement_history` (solo "últimos movimientos", sin artículo
concreto, sin saldos) no cubre.

### 1.3 Valor esperado

- El copiloto responde con el kárdex de un artículo (saldo inicial + saldo por movimiento) sin inventar números.
- El eval de tool-selection gana 3 casos y no baja de 96/96/92 %.
- Sin regresión en las 34 tools existentes ni en el HTML renderizado.

### 1.4 Qué NO se toca

- `movement_history` y su tool (REQ fuera de alcance).
- Gráfico de evolución del saldo, exportar CSV (REQ-10, Won't Have), filtrar por tipo.
- Sin cambios de BD / migraciones.
- Sin bump de versión (0.5.0 agrupada).

---

## 2. Decisiones de diseño

| # | Decisión | Alternativas | Justificación | Implicaciones |
|---|----------|-------------|---------------|---------------|
| D1 | La lógica vive en `ReportingService.product_movements` | En la tool | Las tools son adaptadores finos; la lógica testeable sin LLM va en servicios | La tool solo resuelve argumentos y llama al servicio |
| D2 | Saldo = suma de movimientos con signo + aviso contra lotes | Reconstruir con lotes | Los lotes no guardan historia por fecha; 1/1.294 descuadres | REQ-7 añade `note` cuando no cuadra |
| D3 | Agrupar traspasos por `out_id` de la nota | Por `ref` | `out_id` es exacto por salida; una `ref` agrupa varias líneas (varios SKU) | Una salida con N entradas = una fila en la vista global |
| D4 | Interpretar la nota con `InventoryService.transfer_codes_from_out_note` + 2 regex nuevas | Regex duplicada | Un solo formato conocido; se captura su 409 | `except Exception` cae al nombre de la ubicación, nunca lanza |
| D5 | Límite 500 filas (constante `PRODUCT_MOVEMENTS_MAX_ROWS`) | Paginación | Máximo real 64 movimientos; la tabla del widget no pagina | Los saldos de las filas mostradas siguen correctos |

---

## 3. Arquitectura objetivo

### 3.1 Diagrama de componentes

```
★ product_movements (tool, app/agent/tools.py)
     │ resolve_sku / get_by_sku / _resolve_location_id / _iso / _parse_end
     ▼
✦ ReportingService.product_movements (app/services/reporting_service.py)
     ├── ✦ InventoryRepository.quantity_before   (saldo inicial < start)
     ├── ·  InventoryRepository.movement_history (filas del período, se invierte)
     ├── ·  InventoryRepository.stock_for_product_id (REQ-7)
     ├── ✦ LocationRepository.labels            (id/code → nombre)
     └── ·  InventoryService.transfer_codes_from_out_note (agrupar traspasos)
✦ widget_table.html (nota al pie) · prompts.py (regla de enrutado)
```

### 3.2 Inventario de archivos

> **Scope duro.** `git diff --name-only develop` debe mostrar exactamente estos 15 archivos.

| Archivo | Operación | Responsabilidad |
|---------|-----------|-----------------|
| `app/repositories/inventory_repository.py` | MODIFY | `quantity_before(product_id, before, location_id=None)` |
| `app/repositories/location_repository.py` | MODIFY | `labels()` → `(by_id, by_code)` |
| `app/services/reporting_service.py` | MODIFY | `import re` + 4 constantes + `product_movements(...)` |
| `app/agent/tools.py` | MODIFY | import `ProductService` + tool `product_movements` |
| `app/agent/prompts.py` | MODIFY | regla de enrutado de `product_movements` |
| `app/templates/features/agent/widget_table.html` | MODIFY | nota al pie `w.payload.note` |
| `tests/unit/test_agent_tools.py` | MODIFY | conteo 35 + 3 tests de la tool |
| `tests/unit/test_repositories.py` | MODIFY | 4 tests de repos |
| `tests/unit/test_ui_xss_escapes.py` | MODIFY | 1 test de escape de la nota |
| `tests/eval/test_agent_tool_selection_eval.py` | MODIFY | 3 casos de eval |
| `tests/unit/test_reporting_product_movements.py` | CREATE | 8 tests del servicio |
| `tests/e2e/test_agent_product_movements_e2e.py` | CREATE | e2e del Lote 0 |
| `CLAUDE.md` | MODIFY | `(34 tools)` → `(35 tools)` (línea 229) |
| `.claude/CLAUDE.md` | MODIFY | `(34 tools)` → `(35 tools)` (línea 287) |
| `CHANGELOG.md` | MODIFY | entrada 0.5.0 |

### 3.3 Dependencias existentes (firmas reales del repo)

```
# app/repositories/inventory_repository.py — YA EXISTE
class InventoryRepository:
    def __init__(self, db: Session, business_id: Optional[int] = None): ...  # self._db, self._business_id
    def movement_history(self, sku=None, query="", movement_type=None, location_id=None,
                         start_date=None, end_date=None, limit=100) -> list[tuple]:
        # join Product, filtra business_id, ordena movement_date DESC/id DESC, limit=None sin límite
    def stock_for_product_id(self, product_id: int, location_id=None) -> float: ...

# MovementHistoryRow(NamedTuple): id, movement_date, type, location_id, sku, name,
#   unit_of_measure, quantity, unit_cost, unit_price, note, username

# app/services/inventory_service.py — YA EXISTE
class InventoryService:
    @property
    def db(self) -> Session: ...
    @property
    def business_id(self) -> int: ...
    def transfer_codes_from_out_note(self, note: str) -> tuple[str, str, Optional[str]]:
        # regex Transfer\s+([^\s:;]+)->([^\s:;]+); HTTPException(409) si no matchea
    def movement_history(self, sku=None, query="", movement_type=None, location_id=None,
                         start_date=None, end_date=None, limit=100) -> list[tuple]: ...

# app/services/product_service.py — YA EXISTE
class ProductService:
    def __init__(self, db: Session, business_id: int | None = None): ...  # self._products (ProductRepository)
    def resolve_sku(self, value: str, *, barcode_fallback: bool = False) -> str: ...  # 422 vacío, 404 inexistente

# app/agent/tools.py — YA EXISTE
def _iso(value: str) -> datetime: ...      # tz-aware
def _parse_end(value: str) -> datetime: ...   # fin inclusivo
def _resolve_location_id(service: InventoryService, location: str) -> int: ...  # 404 si no existe
def tool(name: str, description: str, params: dict) -> Callable: ...  # TOOL_REGISTRY
def tool_schemas() -> list[dict]: ...

# app/agent/widgets.py — YA EXISTE
def add_widget(db, business_id, *, type, title, tool, tool_args, payload, unit, created_by) -> DashboardWidget: ...
```

### 3.4 Interfaces nuevas a implementar

```
# app/repositories/inventory_repository.py
def quantity_before(self, product_id: int, before: datetime, location_id: Optional[int] = None) -> float:
    # suma con signo de InventoryMovement.quantity con movement_date < before, filtrado por business_id
    # (y location_id si se pasa). Devuelve float (0.0 si no hay filas).

# app/repositories/location_repository.py
def labels(self) -> tuple[dict[int, str], dict[str, str]]:
    # ({location_id: nombre}, {code: nombre}) del negocio; nombre vacío cae al código.

# app/services/reporting_service.py
def product_movements(self, *, product_id: int, sku: str, start: datetime, end: datetime,
                      location_id: Optional[int] = None) -> dict:
    # {"columns": [...], "rows": [[...], ...], "note"?}. Fila 0 = "Saldo inicial".
    # Traspasos agrupados en la vista global; desglosados al filtrar ubicación. Nota si >500 filas o descuadre.

# app/agent/tools.py
@tool("product_movements", "...", {"type": "object", "properties": {...}, "required": ["sku"]})
def product_movements(service: InventoryService, args: dict) -> dict: ...
```

---

## 4. Fases de implementación (Task Envelopes)

> Los envelopes completos (BEFORE/AFTER, verification, report) están en `tasks.md`. `cc_local_pae` lee solo
> los rangos de líneas de cada lote. Resumen:

| Lote | Envelope | Files | Qué hace |
|------|----------|-------|----------|
| **A** (Lote 0) | T1 | `inventory_repository.py` | `quantity_before` |
| | T2 | `location_repository.py` | `labels()` |
| | T3 | `reporting_service.py` | `import re` + constantes + `product_movements` |
| | T4 | `tools.py`, `test_agent_tools.py` | tool `product_movements` + conteo 34→35 |
| | T5 | `test_reporting_product_movements.py` | camino feliz REQ-1/2/3 (3 tests) |
| | T6 | `test_agent_product_movements_e2e.py` | e2e sembrado (gate del Lote 0) |
| **B** | T7 | `test_reporting_product_movements.py` | bordes del servicio (5 tests) |
| | T8 | `test_agent_tools.py` | 3 tests de la tool |
| | T9 | `test_repositories.py` | 4 tests de repos |
| **C** | T10 | `widget_table.html`, `test_ui_xss_escapes.py` | nota al pie + escape |
| | T11 | `prompts.py` | regla de enrutado |
| | T12 | `test_agent_tool_selection_eval.py` | 3 casos de eval |
| | T13 | `.claude/CLAUDE.md`, `CLAUDE.md` | 35 tools |
| | T14 | `CHANGELOG.md` | entrada 0.5.0 |

Línea de lotes (en `tasks.md`): `Lotes: **A** T1–T6 · **B** T7–T9 · **C** T10–T14`

---

## 5. Plan de tests y verificación

### 5.1 Verificación por lote (`cc_verify`)

Cada lote se verifica contra el commit anterior con `--base` y `--expect-files` (sus `Files:`). El Lote 0 usa
`--lot0-test-cmd` (e2e nuevo + `test_reporting_product_movements.py`) con `--fail-on-skip`. Todos los lotes usan
`--test-cmd` (pyflakes + snapshots + unit).

### 5.2 Tests nuevos

| Fichero | Tests | REQ |
|---------|-------|-----|
| `test_reporting_product_movements.py` | orden/etiquetas/saldo total, saldo por ubicación, traspaso 2 entradas = 1 fila, entrada sin `out_id`, frontera `start`, aislamiento, límite 500, nota de descuadre | REQ-1..3, REQ-6, REQ-7 |
| `test_agent_tools.py` | SKU 404, ubicación 404, período por defecto | REQ-5, REQ-4, REQ-6 |
| `test_repositories.py` | `quantity_before` (frontera, ubicación, negocio), `labels` | — |
| `test_ui_xss_escapes.py` | nota hostil escapada | REQ-7 |
| `test_agent_product_movements_e2e.py` | tabla con "Saldo inicial", "→" y saldo final | REQ-1..3 |

### 5.3 Tests existentes que deben seguir pasando

- [ ] `tests/unit` completo — 521 unit verdes antes; +15 nuevos, 0 rotos
- [ ] `tests/snapshots/ui_snapshot.py check tests/snapshots/baseline` — HTML idéntico (la tabla sin nota no cambia)
- [ ] `tests/e2e -m e2e` — 94 e2e verdes (más el nuevo)
- [ ] eval `pytest tests/eval -m agent_eval -v -s` — ≥ 96/96/92 % (lo corre la revisión, no `--test-cmd`)

---

## 6. Riesgos y mitigaciones

| # | Riesgo | Prob. | Impacto | Síntoma | Mitigación | Rollback |
|---|--------|-------|---------|---------|------------|----------|
| R1 | Conteo de tools rompe `test_tool_schemas_count` (34) | Alta | Alto | Test rojo al añadir la tool | T4 actualiza el conteo a 35 en el mismo envelope | Revert T4 |
| R2 | Nota de traspaso rara rompe la tabla | Media | Medio | 500 al pedir kárdex | `except Exception` cae al nombre de la ubicación, nunca lanza | Revert T3 |
| R3 | Ventanas distintas en saldo inicial vs filas (doble conteo) | Media | Alto | Saldo final incorrecto | `< start` en `quantity_before`, `>= start`/`< end` en `movement_history`; test de frontera | Revert T1/T3 |
| R4 | `end` naive vs aware → TypeError en REQ-7 | Media | Medio | 500 con fin del período | `end_cmp` normaliza tzinfo antes de comparar | Revert T3 |
| R5 | Import circular `reporting_service` ↔ `inventory_service` | Baja | Alto | ImportError al arrancar | Import local dentro del método (como `_finance()`) | Revert T3 |

---

## 7. Definición de hecho (DoD)

### 7.1 Checklist pre-commit

- [ ] `--test-cmd` verde en los 3 lotes (pyflakes sin "undefined name", snapshots idénticos, unit completo)
- [ ] Lote 0: e2e nuevo verde (`Saldo inicial`, `→`, saldo final 9.0) con `--fail-on-skip`
- [ ] `git diff --name-only develop` = exactamente los 15 archivos de §3.2
- [ ] `test_tool_schemas_count` == 35
- [ ] Sin archivos temporales (`_tmp`, `_test`, `_draft`, `_vN`)
- [ ] Sin bump de versión; sin push/merge/sudo/systemctl
- [ ] eval (revisión, fuera de `--test-cmd`): ≥ 96/96/92 % con los 3 casos nuevos

### 7.2 Criterios de aceptación (Gherkin de requirements.md)

- REQ-1: tabla con compra, traspaso (una fila "origen → destino") y venta en orden, con columnas fijas.
- REQ-2: fila `Saldo inicial` + saldo acumulado correcto (total y por ubicación).
- REQ-3: traspaso con dos entradas = una fila; entrada sin `out_id` = fila propia.
- REQ-4: filtro por ubicación con `_resolve_location_id` (404 si no existe).
- REQ-5: SKU inexistente → 404 (aviso de fallo de tool, PAE-015).
- REQ-6: período por defecto (3 meses) y límite 500 con nota.
- REQ-7: nota de descuadre contra lotes cuando el fin llega a hoy.
- REQ-8: eval elige `product_movements` en los 3 casos nuevos.
- REQ-9: 35 tools en ambos `CLAUDE.md` + entrada 0.5.0 en `CHANGELOG.md`.

---

## Ejecución

```bash
cc_local_pae ~/PyCharmMiscProject/gestion_inventario specs/copiloto-movimientos-articulo/tasks.md \
  --test-cmd '! (.venv/bin/python -m pyflakes app | grep "undefined name" | grep -v "migrations.py:[0-9]*:[0-9]*: undefined name .Session.") && python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline && SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: ADMIN_USERNAME=admin ADMIN_PASSWORD=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test python3 -m pytest tests/unit -q --override-ini="addopts="' \
  --lot0-test-cmd 'SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: ADMIN_USERNAME=admin ADMIN_PASSWORD=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test python3 -m pytest tests/e2e/test_agent_product_movements_e2e.py tests/unit/test_reporting_product_movements.py -q --no-cov --override-ini="addopts="'
```

> Sin `--free-port`: los e2e usan puerto dinámico (`_free_port()` en `tests/e2e/conftest.py`).
> El eval contra el LLM real NO va en `--test-cmd`: lo ejecuta la revisión (5b).

---

> Generado por: SDD Lead (Phase 4 — PAE Assembly, Fases 3-4 en DeepSeek Pro)
> Fuente: `requirements.md`, `design.md`, `tasks.md`

## Revisión de cc_claude_code antes del lanzamiento (2026-11-02)

Revisados los 14 envelopes contra el código (firmas de `movement_history`/`MovementHistoryRow`,
`_fill_reordered_audit_usernames`, esquemas `PurchaseCreate`/`SaleCreate`/`TransferCreate`, fixture `central_server`,
credenciales del servidor e2e, id `widget-{id}` de `widget_grid.html`, `ProductService.__init__`). Correcciones en
`tasks.md`:

1. **T3:** captura `HTTPException` (no `Exception`) al leer la nota de salida, y la etiqueta cae al nombre de la
   ubicación del movimiento ("?" para el destino) en vez de quedar " → ".
2. **T4:** el `product_id` sale de `ProductRepository(...).get_by_sku`, no del atributo privado
   `ProductService._products` (mismo criterio que REQ-8 de la fase 5b: nada de miembros privados desde fuera).

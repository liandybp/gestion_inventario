# Design — P5: e2e multi-nodo + endurecimiento (cierre del épico)

- **Slice:** P5 de 5 (`offline-desktop-sync`)
- **Proyecto:** gestion_inventario · **Rama base:** `develop` · **Versión objetivo:** v0.4.7
- **Fase SDD:** 2 de 4 (Design)
- **REQs cubiertos:** REQ-20 (e2e multi-nodo) + verificación integral del épico + endurecimiento
- **Depende de:** P1-P4 (verdes)
- **Insumo:** `specs/offline-desktop-sync/requirements.md`

---

## 2.1 Codebase Grounding

| Archivo | Operación | Rol |
|---------|-----------|-----|
| `app/sync/client.py` | MODIFY | Enviar `_LATEST_SCHEMA_VERSION` real (no el literal `7`) — endurecimiento dim6 |
| `tests/e2e/conftest.py` | MODIFY | Nueva fixture `central_server` que expone `(base_url, db_path)` |
| `tests/e2e/test_sync_multinode_e2e.py` | CREATE | e2e: 2 nodos + central, con conflicto (REQ-20) |
| `specs/offline-desktop-sync/verify.sh` | CREATE | Script de verificación integral del épico |
| `CLAUDE.md` | MODIFY | Estado actual + entrada del épico (documentación viva) |
| `tests/e2e/conftest.py::live_server` | REFERENCE | Fixture existente (subproceso uvicorn + DB temp + `_seed_stock`) |
| `app/routers/sync.py` | REFERENCE | `/sync/push` (X-Sync-Token), `/sync/conflicts` (P2) |
| `app/models.py` | REFERENCE | `DeviceToken`, `InventoryMovement`, `Customer`, `SyncConflict` |

**Firmas reales verificadas:**
```python
# tests/e2e/conftest.py:113 — @pytest.fixture(scope="function") def live_server() -> str:
#   crea db_path temp, arranca uvicorn en subproceso con DATABASE_URL=sqlite file, _seed_stock(db_path)
#   (siembra business 'recambios' + central + E2E-SKU1 qty 10 + E2E-SKU2 qty 5 + operator/owner), yield base_url
# _seed_stock crea lotes vía InventoryLot/InventoryMovement directamente.
# pytest.ini — addopts = -q -m "not e2e" ; marker e2e registrado. e2e se corre aparte.
# app/sync/client.py — sync() manda {"app_schema_version": 7, ...}  ← literal a single-source
# app/migrations.py — _LATEST_SCHEMA_VERSION (7 tras P3)
```

---

## 2.2 Architecture Approach

**e2e multi-nodo (REQ-20).** El servidor central corre como subproceso (patrón `live_server`).
El test **no** levanta dos procesos-nodo: modela cada nodo como un **emisor de eventos HTTP**
(los eventos que el outbox del cliente habría producido) que hace `POST /sync/push` con su token
de dispositivo. Así ejercita el protocolo completo del servidor (idempotencia, replay vía
services, dedup, conflictos, pull) — que es el corazón convergente de REQ-20. La lógica del
cliente (outbox, apply_pull) ya está cubierta por los unit de P3; la UI desktop se valida
manualmente (P4/README).

**Fixture `central_server`.** Como el test necesita sembrar tokens de dispositivo y aseverar el
estado canónico, se añade una fixture que expone `(base_url, db_path)` (la actual `live_server`
solo devuelve `base_url`). Se **añade**, no se modifica la existente (otros e2e la usan).

**Escenario e2e:**
```
Seed (en db_path): 2 DeviceToken (nodo A, nodo B) para 'recambios' + stock ya sembrado (E2E-SKU1=10).
Nodo A → POST /sync/push [ venta E2E-SKU1 x2 ; cliente phone="600X" client_id="C-A" ]
Nodo B → POST /sync/push [ cliente phone="600X" client_id="C-B" (dedup) ; venta E2E-SKU1 x999 (sin stock) ]
Aserciones (en db_path):
  - hay movimientos de venta aplicados (la de A; NO la de 999 de B)
  - hay UN solo Customer con phone="600X" (dedup conservó C-A)
  - hay un SyncConflict kind="sale_stock" (la venta de 999)
  - reenviar el push de A (mismo event_id) NO duplica (idempotencia)
```

**Endurecimiento (dim6).** `client.py` envía la versión de esquema real
(`_LATEST_SCHEMA_VERSION`), eliminando el número mágico `7` que driftaría en el próximo bump.

**Invariantes:** e2e marcado `@pytest.mark.e2e` (excluido del run unit por defecto). No se toca
lógica de negocio ni el server salvo el single-source de versión (aditivo).

---

## 2.3–2.4 Data Model / API
Ninguno nuevo. P5 solo prueba y endurece lo de P1-P4.

## 2.5 Security Review
El e2e usa tokens de dispositivo reales (seed) y verifica el scoping por negocio. No introduce
superficie nueva. `verify.sh` no expone secretos.

## 2.6 Trade-offs
| # | Decisión | Elegido | Alternativa | Por qué no |
|---|----------|---------|-------------|-----------|
| T1 | Modelar nodos | Emisores de eventos HTTP contra el central | Dos procesos-app completos | El cliente ya está en unit (P3); el valor de REQ-20 es la **convergencia del central**; dos procesos añaden fragilidad sin cubrir más |
| T2 | Fixture | `central_server` nueva `(base_url, db_path)` | Modificar `live_server` | No romper los e2e existentes que dependen de su firma actual |
| T3 | Versión de protocolo | `_LATEST_SCHEMA_VERSION` single-source | Dejar el literal 7 | Evita drift silencioso al próximo bump (dim6) |

## 2.7 Migration & Rollback
Sin migración. Rollback = quitar el test y la fixture; revertir el single-source (trivial).

## 2.9 Pre-mortem contra las 10 dimensiones
| Dim | Riesgo | Mitigación |
|-----|--------|------------|
| **5. Robustez** | e2e flaky por timing del subproceso | Reusar `_wait_ready`/patrón `live_server` ya endurecido en el repo |
| **6. Consistencia** | Versión de protocolo hardcodeada en 2 sitios (server MIN + client send) | client single-source a `_LATEST_SCHEMA_VERSION`; server MIN documentado como piso de compatibilidad |
| **9. Pruebas** | El épico cierra sin validación integral | `verify.sh` corre unit + e2e + scope; DoD del épico lo exige |

## 2.10 Delegation
Ninguna. `test-automation-engineer` cubre el e2e (asignada en el PAE).

## Interfaces nuevas
```python
# tests/e2e/conftest.py
@pytest.fixture
def central_server() -> tuple[str, str]:
    """(base_url, db_path) de un servidor central sembrado — para tests que necesitan la BD."""
# app/sync/client.py — sync() manda app_schema_version = _LATEST_SCHEMA_VERSION (import de migrations)
```

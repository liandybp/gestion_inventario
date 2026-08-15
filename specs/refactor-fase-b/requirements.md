# Fase B — Requirements (SDD Phase 1)

> Refactor estructural (SoC + Capas) para subir dim1 2→4 y dim3 2→4.
> Fuente: `docs/auditorias/2026-08-15-negocio-sin-pos.md` (H-023, H-026, H-028, H-049).

## REQs (MoSCoW)

### Must (M)

| ID | Requisito | Criterio de aceptación |
|----|-----------|------------------------|
| REQ-1 | **`ui_tabs.py` deja de ser un god router** | Given el repo, When se ejecuta B1, Then `ui_tabs.py` queda < 200 ln (o se elimina) y sus handlers viven en routers por dominio registrados en `ui.py`. |
| REQ-2 | **Rutas exactas preservadas** | Given la UI HTMX, When se navegan los 14 tabs y acciones de stock/restock/profit-items, Then las URLs son IDÉNTICAS (sin cambios de cliente ni templates). Verificación: los 5 e2e + navegación manual. |
| REQ-3 | **`InventoryService` pierde finance/reporting** | Given el servicio, When se ejecuta B2, Then `create_expense`/`list_expenses`/`monthly_dividends_report`/`monthly_profit_report`/`sales_metrics_table`/etc. viven en `FinanceService` y `ReportingService`, y `inventory_service.py` baja a < 2300 ln. |
| REQ-4 | **Callers actualizados** | Given `ui_expenses.py`/`ui_extractions.py`/`agent/tools.py`, When ejecutan lógica de finance/reporting, Then construyen `FinanceService`/`ReportingService` (comportamiento idéntico). |
| REQ-5 | **Resolución de ubicación única (H-049)** | Given 3 callers de `_location_id_for_code`, When se ejecuta B3, Then los tres usan `LocationRepository.location_id_for_code` (una sola fuente de verdad, mismo comportamiento). |

### Should (S)

| ID | Requisito | Criterio de aceptación |
|----|-----------|------------------------|
| REQ-6 | **Helpers compartidos centralizados** | Given `_location_id_for_code`/`_restock_items_with_location_history`/etc., When se mueven a `ui_tabs_common.py`, Then no hay copias duplicadas entre routers. |
| REQ-7 | **Nuevos services testeados** | Given `FinanceService`/`ReportingService`, When corren los tests, Then se añaden tests unitarios mínimos que importan y usan los métodos movidos (mismo output que antes). |

### Should (S)

| ID | Requisito | Criterio de aceptación |
|----|-----------|------------------------|
| REQ-8 | **E2E smoke de TODOS los tabs** (valida B1) | Given un login admin, When se navegan los 16 tabs del sidebar, Then cada uno renderiza `#tab-content` sin error (200, sin "Internal Server Error", contenido no vacío). Caza 404s/rutas rotas del split de ui_tabs. |
| REQ-9 | **E2E de flujos críticos post-B2** | Given stock sembrado, When se crean venta/compra/gasto/extracción y se abre profit/profit-items, Then los datos persisten y se muestran (valida FinanceService/ReportingService). |
| REQ-10 | **Suite e2e completa verde al cerrar** | Given la Fase B terminada, When se ejecuta `pytest tests/e2e -m e2e`, Then pasan los e2e existentes (5) + los nuevos (smoke + flujos). |

### Won't (W)

| ID | Excluido | Razón |
|----|----------|-------|
| W-1 | Migrar TODO el ORM de los 13 routers (H-028 completo) | Se centraliza solo la resolución de ubicación en B3; el resto de ORM queda en los routers destino (Fase B2 futura). |
| W-2 | `docker-compose*.yml` | Invariante. |
| W-3 | Paginación/cache (H-033), e2e en CI, H-046 compose | Otras fases. |
| W-4 | E2E de CADA operación CRUD de cada tab (usuarios, negocios, clientes, documentos) | La smoke + flujos críticos cubren el riesgo del refactor; los CRUD unitarios ya existen. |

## NFRs

- **No-regresión de comportamiento:** las respuestas HTML y las rutas son idénticas; los 138 unit + 5 e2e existentes pasan en cada hito de B1.
- **Imports:** no crear ciclos (los routers importan de `ui_tabs_common` y services, nunca al revés; `ui_tabs_common` no importa routers).
- **Exposición pública:** `InventoryService` expone `@property business_id` para que las tools construyan sub-services.
- **Registro:** todo router nuevo se registra en `ui.py`; un router sin registrar = ruta 404 (falla detectable).

## Fuentes de verdad

- Mapa de movimiento: `specs/refactor-fase-b/design.md` (D1 tablas).
- Firmas reales: design.md §4.
- Envelopes: `specs/refactor-fase-b/tasks.md`.

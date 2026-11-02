# Auditoría de Ingeniería — Frontend fase 5b: consultas fuera de los routers UI y `UIContext` completo

- **Fecha:** 2026-11-02
- **Alcance:** `develop...feature/frontend-fase5b` (base `a44338e`, 5a mergeada). Incluye lo que hizo `cc_local_pae`
  (lotes A-H, 60 envelopes) y las correcciones de Claude Code durante la revisión lote a lote. El lote I lo aplicó
  Claude Code.
- **Score:** **4.75 / 5.00 🟢**. Todos los hallazgos de la revisión están corregidos dentro de la rama.
- **Responsable:** Claude Code (Opus 5.5), revisión 5b del loop v4.
- **Specs:** `specs/frontend-fase5b-queries-uicontext/` (requirements, design, tasks T1-T60, PAE con la nota de la
  revisión previa al lanzamiento).

## 1. Contexto y método

| Herramienta | Uso |
|---|---|
| Serena | `find_symbol(include_body)` de los métodos movidos (`get_movement`, `get_by_lot_code`, `transfer_out_by_ref`, `skus_with_movements`, `reconcile_locations`, `profit_items_sale_delete`, `tab_profit`, `InventoryLot`); `find_referencing_symbols` de `_write_business_config` (sin llamadores en `app/`). |
| codebase-memory | Reindexado de la rama; `search_graph` de los 28 métodos/funciones nuevos o renombrados: todos con grado de entrada ≥ 1 (sin capa muerta). |
| `pyflakes` | Nombres indefinidos tras cada lote, que destaparon 2 `NameError` que los tests no cubrían; desde el lote E forma parte del `--test-cmd`. Imports sin uso para el barrido de T56. |
| AST (script) | Todos los `monkeypatch.setattr` de `tests/` cuyo objetivo un envelope elimina; las reexportaciones desde routers antes de borrar imports; `ctx` definido en cada función migrada. |
| Ejecución real | Suites completas tras cada lote. Cierre: `pytest tests/unit` 521, `ui_snapshot.py check` 91, `pytest tests/e2e -m e2e` 94, `style_snapshot.py check` 0 diferencias, `test_ui_query_count.py` en modo estricto. Mutación de la guarda ORM y de los 2 tests de regresión nuevos. |
| `grep` (fuera de cobertura) | Recuento de `_log.` antes y después (129 → 128: la que falta estaba en el código muerto borrado) y ficheros Markdown. |

## 2. Resultado funcional

| REQ | Estado | Evidencia |
|---|---|---|
| REQ-1 `get_active_business_id(user=)` | ✅ | `app/security.py`; 4 tests nuevos en `test_security.py`. |
| REQ-2 `UIContext` completo | ✅ | `app/routers/ui_context.py`; `test_ui_context.py` (409 solo en `business_id`, `config` sin lanzar, resolución única). |
| REQ-3 1 SELECT a `users` por petición | ✅ | `test_ui_query_count.py` estricto: `users=1` en las 6 rutas (antes 4 en `tab_sales`); `user_businesses` ≤ 1 (≤ 2 en `/ui/dashboard`). |
| REQ-4 auxiliares a `ctx` | ✅ | Guarda `test_no_direct_security_resolution_in_ui_routers`. |
| REQ-5 sin ORM en routers UI | ✅ | Guarda `test_no_orm_in_ui_routers` (mutación detectada en `ui_sync.py:71`). |
| REQ-6 duplicados unificados | ✅ | `ProductService.resolve_sku`, `ProductRepository.list_categories`, `SalesDocumentService.list_recent/customers_list`. |
| REQ-7 `get_movement` | ✅ | `InventoryRepository.get_movement` (tipo obligatorio + negocio); 11 sitios migrados. |
| REQ-8 sin métodos privados | ✅ | `transfer_out_id_for_movement_id`, `transfer_codes_from_out_note`. |
| REQ-9 `barcode_to_sku` con negocio | ✅ | `ProductService.barcode_to_sku` (el negocio sale del servicio). |
| REQ-10 repositorios existentes | ✅ | `UserRepository.add/delete`, `CustomerRepository.delete`, `PurchaseRepository.get_product/get_lot_by_movement`. |
| REQ-11 equivalencia | ✅ | 91 snapshots y estilos idénticos, 94 e2e. |
| REQ-12 docs y memorias | ✅ | `.claude/CLAUDE.md` (invariante 6), `CLAUDE.md`, `CHANGELOG.md`, diagnóstico fila 5 ✅, memoria Serena `conventions`. |

## 3. Checklist y scorecard

| # | Dimensión | Peso | Score | Evidencia |
|---|---|---|---|---|
| 1 | SoC / SRP | 10 | 5 | Routers sin ORM; consultas en repositorios y servicios; código muerto borrado (`_write_business_config`, `_prefix_location_list`, `_deletable_skus`, `_resolve_*_sku`). |
| 2 | Cohesión / Acoplamiento | 10 | 5 | Un solo sitio por consulta (REQ-6); `ctx` cachea usuario, negocio, código y configuración. |
| 3 | Capas / Límites | 10 | 5 | `extract_sku` baja a `product_service.py` (antes el servicio habría importado del router); los repositorios no lanzan `HTTPException`. |
| 4 | Seguridad | 15 | 5 | Comprobación de negocio centralizada en `get_movement`; `barcode_to_sku` ya no busca en todos los negocios; test de aislamiento en `ProductRepository`. |
| 5 | Robustez | 15 | 4 | `try/except` + `_log` conservados (recuento). Quedan caminos de handlers sin test: 2 se destaparon en esta fase y ya tienen test de regresión, pero la cobertura por handler no es completa. |
| 6 | Transaccional | 10 | 5 | Ningún método nuevo hace `commit` (corregido `add_cash_baseline`); el orden líneas → `flush` → documento vive en `SalesDocumentService.delete_document`. |
| 7 | Observabilidad | 10 | 5 | Logs intactos; `business_repository` usa el `get_logger` del proyecto. |
| 8 | Configuración | 5 | 5 | Sin cambios. |
| 9 | Pruebas | 10 | 4 | Guardas ORM y de recuento, tests de repositorio y de regresión. Los e2e no cubren todos los POST de error. |
| 10 | Escalabilidad | 5 | 5 | De 4 a 1 consultas a `users` y de 3 a 1 a `user_businesses` por petición. |
| | **Total** | 100 | **475 → 4.75 🟢** | |

## 4. Hallazgos (todos corregidos en la rama)

| ID | Sev. | Lote | Hallazgo | Corrección |
|---|---|---|---|---|
| H-901 | Alto | PAE | T3 quitaba `require_active_business_id` de `ui_context`, pero 8 ficheros de test lo parcheaban. La revisión previa solo detectó `test_ui_context.py`; los otros 7 los destapó la suite completa del lote B. | T4 Step 0 + `643b216`, `cabdd70` (doble vía con `raising=False` donde el router migra después). |
| H-902 | Alto | PAE | T5 rompía llamadores que se migraban 1-2 lotes después (`ui_stock`, `ui_profit_items`) y 2 tests no estaban en ningún `**Files:**`. | Corregido antes del lanzamiento (T5 Step 4). |
| H-903 | Alto | B | `cc_local` dio el lote B por hecho sin hacerlo; en el segundo intento borró `business_code` en `ui_home` (`NameError`), usó `ctx.business_id` (409) en el dashboard, llamó a `_location_id_for_code` con firma errónea en `ui_profit` y creó un `UIContext` por llamada en `ui_agent`. | `e2a4c9e`, `bd38df7`. |
| H-904 | Crítico | D | `cc_local` borró 147 líneas de `ui_transfers.py` (ubicaciones, `_tab_error`, `_safe_product_options`, `movement_ids`) y `ui_profit_items.py:393` quedó con la firma vieja (`TypeError` sin test). | `2d14427`: fichero restaurado desde `9aa5cc6` y T19/T20/T25 aplicados. |
| H-905 | Alto | E | Lote E "OK" con solo los métodos nuevos: los routers seguían con las funciones viejas duplicadas y faltaba el test de T28. El commit de `cc_local` describía métodos que no existen. | `2de020e` (T26 Step 3, T29-T32, test de aislamiento más estricto que el del envelope). |
| H-906 | Medio | F | Faltaban T38/T39; reescritura no pedida de `_stock_options`. | `b8e463d`. |
| H-907 | Medio | G | Faltaba T46; `logging.getLogger` en vez de `get_logger`. | `e018d65`. |
| H-908 | Medio | H | `CoachService.add_cash_baseline` hacía `commit` (frontera transaccional movida); `test_coach_rbac` usaba `ui_coach.CashBaseline`. | `ebfee70`. |
| H-909 | Bajo | I | El autotest de la guarda ORM comprobaba el filtrado de comentarios sobre la regex, no sobre `_code_lines`. | `3d8af74`. |
| H-910 | Bajo | previo | `_write_business_config` (y `_prefix_location_list`): código muerto con `configparser` sin importar desde `d84638d`. | Borrado en `3d8af74`. |
| H-911 | Bajo | previo | `migrations.py:641` anota `"Session"` sin importarlo. | Se mantiene: es una anotación en cadena, sin efecto en tiempo de ejecución, y el invariante 8 prohíbe editar `migrations.py`. |
| H-912 | Bajo | — | `transfer_print` filtra ahora por tipo (`transfer_out`/`transfer_in`) cuando antes no lo hacía. | Aceptado: lo pide T37 y el id llega siempre desde una fila de traspaso; un id de otro tipo da 404 en vez de 422. |

## 5. Lecciones para el loop

- **El "OK" de un lote no prueba que el lote esté hecho.** `cc_verify` comprueba que no se toque nada **fuera** del
  alcance, no que se haga todo lo de **dentro**: el lote E pasó con 4 de 7 envelopes sin aplicar. Para `cc_local_pae`:
  comprobar por envelope que su `BEFORE` ya no aparece en el fichero.
- **Los tests no cubren todos los handlers.** Tres errores de `cc_local` (un `NameError`, una firma errónea y un
  `TypeError`) pasaban la suite. `pyflakes` en el `--test-cmd` cazó los `NameError` desde el lote E; los de firma
  solo se ven leyendo el diff contra el envelope.
- **La revisión del PAE debe incluir los tests que parchean símbolos que el PAE elimina**, con un análisis AST
  (multilínea, `setattr` con ruta en texto), no con `grep` por línea.

## 6. Criterios de cierre (cumplidos)

`pytest tests/unit` 521 · `ui_snapshot.py check` 91 · `pytest tests/e2e -m e2e` 94 · `style_snapshot.py check` 0
diferencias · `test_ui_query_count.py` estricto · `test_routers_no_orm.py` · `git grep -n "db\.\(get\|scalar\|scalars\|execute\|query\|add\|delete\)(" app/routers/ui_*.py` vacío.

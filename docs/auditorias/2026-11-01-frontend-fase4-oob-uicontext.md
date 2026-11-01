# Auditoría — Frontend fase 4: plantillas OOB, `UIContext` y sidebar data-driven

**Fecha:** 2026-11-01 · **Alcance:** `develop...feature/frontend-fase4` (96 ficheros, +8464/−523; release 0.5.0, sin
bump) · **Specs:** `specs/frontend-fase4-oob-uicontext/` · **Responsable:** Claude Code (Opus 5.5)

**Score: 4.25 / 5.00 🟢** — maduro para su fase. Ningún hallazgo crítico introducido por la rama. Dos hallazgos
**altos previos** a la rama (en producción) que esta auditoría destapa y conviene corregir ya.

---

## 1. Contexto y método

- **Qué:** la fase 4 del frontend modular (REQ-1..12): arnés H-303, fragmentos OOB a plantillas en
  `templates/features/<dominio>/`, macros `qty_label`/`number_input`/`text_input`, `UIContext` (llamada directa
  `ctx = ui_context(request, db)`, commit `5cb168e`) en 20 routers UI, sidebar `NAV_ITEMS`, guarda
  `test_routers_no_html.py`.
- **Insumos:** `/code-review high` sobre el mismo diff (10 hallazgos, todos re-verificados aquí, ver §5.4) y la
  verificación propia descrita abajo.
- **Herramientas:**
  - **Serena:** proyecto activado, memorias leídas (`known_bug_patterns`). Referencias y cuerpos de símbolos.
  - **codebase-memory:** repo re-indexado (`home-liandy-PyCharmMiscProject-gestion_inventario`) para el mapa de
    capas e impacto.
  - **AST de Python** (script ad hoc) para las comprobaciones exhaustivas que piden "todos los sitios": orden de
    comprobaciones por handler `develop` vs rama (117 handlers) y handlers sin guardia de rol (lista completa).
  - **ruff** (`F401/F811/F841/F821`) sobre las versiones `develop` y rama de cada fichero, para separar avisos
    nuevos de previos.
  - **Ejecución real:** suite unit (483), snapshots (89), e2e (91), dos mutaciones del arnés (REQ-5) y un test
    temporal que reproduce el `NameError` de H-709.
- **Fuera de cobertura (fallback `grep`/`sed`):** plantillas Jinja2 (`templates/features/`, `dashboard.html`,
  `components/forms.html`), `CHANGELOG.md`, `CLAUDE.md` y `tasks.md` — el índice y el LSP solo cubren Python.
- **Limitación:** no se probó en staging (la rama no está publicada); la equivalencia visual la sostienen el arnés
  (89 claves) y los e2e.

## 2. Checklist por dimensión

| # | Dimensión | Estado | Evidencia |
|---|---|---|---|
| 1 | SoC / SRP | ✅ | `ui_context.py` (contexto), `ui_nav.py` (navegación), fragmentos en plantillas; los routers ya no arman HTML (guarda `test_routers_no_html.py` verde). Auxiliares con resolución directa (`_render_profit_items_tab`, `_render_error`, `_tab_error`, `_sales_doc_context`) quedan por diseño para la fase 5. |
| 2 | Cohesión / Acoplamiento | ⚠️ | 26 imports sin usar introducidos (20× `UIContext`, `NAV_ITEMS` en `ui_home.py:32`, `html` en `ui_products.py:6`/`ui_sales.py:4`/`ui_sales_documents.py:6`, `get_current_user_from_session` en `ui_sales.py:18`, `Request` en `ui_common.py:12`); `_ALL_ROLES` muerto (`ui_nav.py:6`); `ui_context()` conserva `db = Depends(session_dep)` aunque nunca se usa como dependencia (`ui_context.py:47`). |
| 3 | Capas / Límites | ✅ | `security.py`, `deps.py`, `ui_auth.py` sin tocar (0 ficheros en el diff). `ui_context` depende de `app.security`, nunca al revés. La plantilla del sidebar no decide roles: recibe `nav_items` filtrados. |
| 4 | Seguridad | ⚠️ | Autoescape en todos los fragmentos (sin `|safe`, guarda verde); XSS tests verdes. **Un cambio de RBAC** en una release de equivalencia: `tab_customers` admin → admin/owner (`87ea9fb`), justificado pero no registrado (H-701). Previo: 28 handlers sin guardia de rol (H-710). |
| 5 | Robustez | ⚠️ | `UIContext` no captura excepciones (mismos 403/409). Regresión de formato `float` cazada por el arnés y corregida en el lote J. Previo, en fichero tocado: `NameError` de `timedelta` (H-709). |
| 6 | Consistencia transaccional | ✅ | Sin escrituras nuevas ni cambios de commit/rollback. Solo `ui_auth.py` escribe la sesión y queda fuera, así que la caché de `ctx.business_id` no se desfasa. |
| 7 | Observabilidad | ✅ | Los `_log.exception` de los handlers de fragmentos se conservan (p. ej. `ui_stock.py` `ui_supplier_return_lots`). Sin logs nuevos (no hacían falta). |
| 8 | Configuración | ✅ | `visible_nav_items` recibe `app_mode_label()`, el mismo valor que antes usaba la plantilla. |
| 9 | Pruebas | ⚠️ | 483 unit, 89 snapshots, 91 e2e. REQ-5 probado (§4). Huecos: el e2e de Documentos no comprueba el autorrelleno de cliente pese a su nombre (H-702); `_login` del e2e sin espera de htmx (H-706); guarda por subcadena (H-707). |
| 10 | Escalabilidad | ✅ | Menos consultas de usuario por petición (de 3-4 a 2). La documentación dice "una" (H-703). |

## 3. Scorecard ponderado

| # | Dimensión | Peso | Score | Ponderado |
|---|---|---|---|---|
| 1 | SoC / SRP | 10 | 4 | 40 |
| 2 | Cohesión / Acoplamiento | 10 | 4 | 40 |
| 3 | Capas / Límites | 10 | 5 | 50 |
| 4 | Seguridad | 15 | 4 | 60 |
| 5 | Robustez | 15 | 4 | 60 |
| 6 | Consistencia transaccional | 10 | 5 | 50 |
| 7 | Observabilidad | 10 | 4 | 40 |
| 8 | Configuración | 5 | 5 | 25 |
| 9 | Pruebas | 10 | 4 | 40 |
| 10 | Escalabilidad | 5 | 4 | 20 |
| | **Total** | **100** | | **425 → 4.25 🟢** |

Los hallazgos previos (H-709..711) no puntúan contra la rama: no los introduce. Si se puntuara el estado del
código en producción, Seguridad y Robustez bajarían a 3.

## 4. Verificaciones exhaustivas

- **REQ-7 (mismo orden de comprobaciones):** el AST de los 117 handlers UI de `develop` y de la rama, normalizando
  `get_current_user_from_session`→`user`, `require_active_business_id`→`bid`, `ensure_*`→rol. **116 idénticos**; el
  único distinto es `ui_customers.py::tab_customers` (H-701).
- **REQ-5 (el arnés detecta cambios):** mutación 1 — quitar `oob=True` del input de precio en
  `features/sales/_defaults_oob.html` → `FALLO`, solo `frag_defaults_sale`. Mutación 2 — cambiar el texto
  "La cantidad debe ser mayor que 0" en `ui_purchases.py` → `FALLO`, solo `post_purchase_qty0`. Árbol restaurado.
- **Cobertura H-303 de la baseline:** 9 `frag_*`, 7 `post_*`, 14 `owner_*`, 2 `operator_*`, 17 `empty_tab_*`
  (REQ-1..4).
- **Invariantes:** `app/security.py`, `app/deps.py`, `app/routers/ui_auth.py` sin cambios.

## 5. Hallazgos

### 5.1 Introducidos por la rama

| ID | Sev. | Dim. | Título | Evidencia | Riesgo | Recomendación |
|---|---|---|---|---|---|---|
| H-701 | Medio | 4 | Cambio de RBAC en `tab_customers` sin registrar | `ui_customers.py:272` (`ctx.ensure_admin_or_owner()`; en `develop`, `ensure_admin`), commit `87ea9fb` | Release declarada "sin cambios de comportamiento" que amplía permisos sin constar en CHANGELOG ni requirements | Confirmar con el usuario; añadir entrada `fix(ui)` al CHANGELOG 0.5.0 |
| H-702 | Medio | 9 | REQ-9 incompleto: el e2e de Documentos no prueba el cliente | `tests/e2e/test_fase4_characterization_e2e.py:62-68` (`..._and_client` solo comprueba `#sales-doc-unit-price`) | Una regresión en `_customer_defaults_oob.html` en navegador pasa en verde (el snapshot cubre el marcado, no el swap) | Ampliar el test: rellenar el cliente, `change` y comprobar `#sales-doc-client-id`/`-address` |
| H-703 | Bajo | 2 | Documentación inexacta | `CLAUDE.md:156,160` ("inyectada", "9 tests… inyección de `ctx`"); `CHANGELOG.md` y `CLAUDE.md` ("una resolución de usuario/negocio por petición"); `tasks.md` T40-T48 paso 4 (`ctx=UIContext`, 10 líneas) | Quien retome sigue instrucciones falsas (la inyección daría `TypeError`) | Corregir: llamada directa, 5 tests adaptados por retarget de `monkeypatch`, 2 consultas por petición; anotar T40-T48 como superado |
| H-704 | Bajo | 2 | Imports sin usar y código muerto | ruff: 26 F401 nuevos (ver §2 fila 2); `ui_nav.py:6` `_ALL_ROLES`; `ui_context.py:47` default `Depends` | Ruido y una firma engañosa (`ui_context(request)` sin `db` fallaría al primer acceso) | Quitar imports y `_ALL_ROLES`; `def ui_context(request: Request, db: Session)` |
| H-705 | Bajo | 2 | Comentario de `forms.html` contradice REQ-12 | `components/forms.html` (comentario "para que el OOB no pueda pisar el panel con atributos distintos") vs `_customer_defaults_oob.html:2` (`required=True`) | Invita a "arreglar" la divergencia aprobada | Mencionar la excepción REQ-12 en el comentario |
| H-706 | Bajo | 9 | `_login` del e2e no espera a htmx | `test_fase4_characterization_e2e.py:24-30` (el helper `login` importado sí espera) | Flake en CI lenta (mismo patrón que la carrera de `open_tab`) | Usar `login` de `test_ui_tour_csp_e2e.py` en los 4 tests |
| H-707 | Bajo | 9 | Guarda de HTML por subcadena | `tests/unit/test_routers_no_html.py:12-19`; ya dio un falso positivo con dos comentarios (`ui_purchases.py`, lote L) | Falsos positivos en comentarios/docstrings | Buscar los marcadores solo en literales de cadena (AST `ast.Constant`) |
| H-708 | Bajo | 2 | Normalización del rol duplicada e inconsistente | `ui_home.py:184` (`.lower()`), `ui_context.py` `ensure_admin` (`.lower()`) vs `ensure_admin_or_owner` (`.strip().lower()`) | Con `role='owner '` en BD, guardia y menú discrepan (comportamiento previo, conservado) | Helper único `role_of(user)` usado por `UIContext` y `dashboard` |

### 5.2 Previos a la rama (en producción) — destapados por esta revisión

| ID | Sev. | Dim. | Título | Evidencia | Riesgo | Recomendación |
|---|---|---|---|---|---|---|
| H-709 | **Alto** | 5 | `NameError: timedelta` al filtrar "Utilidades por artículo" | `ui_profit_items.py:72`; en `main` y `develop` (línea 73) desde `d4d339c` (2026-08-15). Reproducido con un test temporal | 500 al usar el filtro de fechas o al volver tras editar con fechas | `from datetime import datetime, timedelta, timezone` + test de regresión |
| H-710 | **Alto** | 4 | 28 handlers UI sin guardia de rol | Lista completa por AST (§6). Escrituras sin rol: `customer_create/update/delete`, `sales_doc_update/delete/issue/preview`, carrito de documentos | Un operador (o cualquier sesión) puede borrar/modificar clientes y documentos de venta por POST directo; ver formularios de gastos/ventas | Decisión de producto: qué puede el operador (ventas, sí; clientes/documentos, ¿?). Añadir `ctx.ensure_admin_or_owner()` donde no deba |
| H-711 | Bajo | 5 | `sales_doc_product_defaults` responde 404 con SKU inexistente | `ui_sales_documents.py:473` (sin `try`; los hermanos de `ui_products.py` devuelven `""`) | Error en consola y autorrelleno roto al teclear texto libre | `try/except HTTPException: return HTMLResponse("")` |

### 5.3 Descartados del `/code-review`

- **Auxiliares sin migrar** (`_render_profit_items_tab`, `_render_error`, `_tab_error`, `_sales_doc_context`):
  fuera de alcance por diseño (design.md §2.6, fase 5).
- **`required` en el fragmento de cliente:** divergencia aprobada (REQ-12). Solo se corrige el comentario (H-705).

### 5.4 Trazabilidad con el `/code-review`

| Review | Auditoría |
|---|---|
| 1 `timedelta` | H-709 (previo) |
| 2 `expense_edit_form`/`sale_edit_form` sin rol | H-710 (previo; no lo introdujo la migración) |
| 3 404 en `sales_doc_product_defaults` | H-711 (previo) |
| 4 "una resolución por petición" | H-703 |
| 5 auxiliares sin migrar | Descartado (§5.3) |
| 6 `required` | H-705 (solo comentario) |
| 7 `_ = ctx.user` / borrados sin rol | H-710 (previo; `_ = …` ya existía, conservado por equivalencia) |
| 8 `strip` del rol | H-708 |
| 9 imports / `Depends` | H-704 |
| 10 e2e de Documentos y `_login` | H-702, H-706 |

## 6. Handlers sin guardia de rol (H-710, lista completa)

Escritura: `POST /customer/create`, `/customer/{id}/update`, `/customer/{id}/delete`, `/sales-doc/preview`,
`/sales-doc/{id}/delete`, `/sales-doc/{id}/update`, `/sales-doc/cart/{add,update,remove,clear}`,
`/sales-doc/issue`, `/sale`, `/sale/barcode`, `/sync`.
Lectura: `GET /customers/table`, `/customer/{id}`, `/customer/{id}/edit`, `/expense/{id}/edit`,
`/movement/sale/{id}/edit`, `/tabs/sales`, `/sales-doc/{id}/edit`, `/sales-doc/{id}/pdf`,
`/sales-doc/product-defaults`, `/sales-doc/customer-defaults`, `/product-defaults/{purchase,sale}`,
`/sync/status`, `/`.
Legítimos para el operador según el sidebar: venta (`/sale*`, `/tabs/sales`, `/product-defaults/sale`, edición de
venta) y sync.

## 7. Matriz impacto / esfuerzo

| | Bajo esfuerzo | Alto esfuerzo |
|---|---|---|
| **Alto impacto** | 🔥 H-709, H-701, H-702, H-711 | 🎯 H-710 (requiere decisión de RBAC + tests por rol) |
| **Bajo impacto** | 📋 H-703, H-704, H-705, H-706, H-707, H-708 | — |

## 8. Backlog priorizado

1. H-709 — import de `timedelta` + test de regresión (afecta a producción).
2. H-701 — confirmar y registrar el cambio de RBAC en el CHANGELOG.
3. H-702 — completar el e2e de Documentos (cliente).
4. H-711 — `try/except` en `sales_doc_product_defaults`.
5. H-703..H-708 — limpieza (docs, imports, comentario, helper de rol, `_login`, guarda por AST).
6. H-710 — decidir permisos del operador y añadir guardias + tests por rol (puede ir en su propio cambio).

## 9. Plan 30/60/90

- **Ahora (antes de merge a `develop`):** 1-5 del backlog, en la rama, con evidencia (`cc_verify`/suite). Criterio:
  483+ unit, 89 snapshots, 91+ e2e verdes; ruff F401/F841 sin avisos nuevos en los ficheros de la fase.
- **30 días:** H-710 con decisión de RBAC, tests por rol para cada endpoint de escritura.
- **60-90 días:** fase 5 (auxiliares a `UIContext`/servicios, `partials/` → `features/`), cerrando la resolución
  única de usuario (pasar el usuario resuelto a `get_active_business_id`).

## 10. Correcciones aplicadas (2026-11-01, misma sesión)

Decisión del usuario sobre permisos (H-701/H-710): el owner ve Clientes; el operador vende, crea y edita clientes
(no los borra) y crea, edita y borra documentos de venta.

| ID | Estado | Cambio | Prueba |
|---|---|---|---|
| H-701 | ✅ | Confirmado por el usuario; registrado en CHANGELOG 0.5.0 | snapshots `owner_tab_customers` |
| H-702 | ✅ | `test_sales_doc_autofill_client` (cliente, dirección y `required`); el test de precio se renombra | e2e falla si se quita el `oob` del fragmento de cliente |
| H-703 | ✅ | `CLAUDE.md`, `.claude/CLAUDE.md`, CHANGELOG, REQ-7 y aviso de "superado" en T40-T48 de `tasks.md` | — |
| H-704 | ✅ | Imports sin usar fuera; `ui_context(request, db)` sin `Depends`; `_ALL_ROLES` pasa a usarse | ruff: 0 avisos nuevos frente a `develop` |
| H-705 | ✅ | Comentario de `forms.html` con la excepción REQ-12 | — |
| H-706 | ✅ | El e2e de caracterización usa `login`/`open_tab` compartidos | 8/8 |
| H-707 | ✅ | Guarda por AST (solo literales, sin docstrings) + test de la propia guarda | detecta el `ui_stock.py` de `develop` |
| H-708 | ✅ | `role_of()` en `ui_context.py`, usado por guardias y sidebar | `test_ui_context.py` |
| H-709 | ✅ | `timedelta` importado | `test_profit_items_date_filter.py` (falla sin el arreglo) |
| H-710 | ✅ | `customer_delete` admin/owner; `tab_customers`/`tab_documents` abiertas; Clientes/Documentos en el sidebar del operador; botón "Eliminar" de cliente oculto al operador; `expense_edit_form`/`sale_edit_form` admin/owner y "Edit" del historial oculto al operador | `test_operator_permissions.py` (4 fallan con el código anterior); `test_operator_permissions_e2e.py`; baseline: solo cambian `operator_dashboard`, `operator_tab_sales` y dos claves nuevas |
| H-711 | ✅ | `try/except` en `sales_doc_product_defaults` | `test_sales_doc_product_defaults.py` |
| **H-712** | ✅ | **Nuevo, destapado por el e2e del operador:** borrar un documento de venta daba 500 (`IntegrityError` por FK: sin `relationship`, el documento se borraba antes que sus líneas). En producción también. `flush()` tras borrar las líneas | test unitario falla sin el arreglo |

**Verificación final:** 496 unit · 91 snapshots · 94 e2e · estilos calculados idénticos en los dos temas · ruff sin
avisos nuevos. Con las correcciones, Seguridad y Robustez suben a 5 y Pruebas a 5: **score 4.65 🟢**
(SoC 4·10, Cohesión 4·10, Capas 5·10, Seguridad 5·15, Robustez 5·15, Transaccional 5·10, Observabilidad 4·10,
Config 5·5, Pruebas 5·10, Escalabilidad 4·5 = 465).

# Auditoría de Ingeniería — Frontend modular, fase 1: layouts + macros `modal`/`field` (PAE-016)

- **Fecha:** 2026-10-23
- **Alcance:** `276b411..2c4df16` (lotes A-G, rama `feature/frontend-modular`, release 0.5.0 sin publicar)
- **Score:** **4.35** (primera pasada) → 4.60 → 4.65 tras la re-revisión (§10) → **4.80 / 5.00 🟢** tras H-307/H-308 (§11)
- **Responsable:** Claude Code (Opus 5.5) — paso 5 del loop (`/code-review` + `auditoria-ingenieria`, una pasada)
- **Estado:** hallazgos presentados al usuario (gate del paso 5) y **corregidos el mismo día, directamente por
  Claude Code** (excepción puntual al loop — el usuario decidió no pasar por cc_local para 5 fixes acotados sobre
  el arnés). Ver §9.

## 1. Contexto y método

Se revisaron los 7 commits que cc_local implementó desde `specs/frontend-fase1-layouts-macros/`. Antes de empezar se
leyeron `design.md` (D1-D9, R1-R8) y la sección "Estado" de `docs/prototipos/fase1-layouts-macros/traspaso.md`.

Verificado al cerrar la implementación, no repetido aquí como hallazgo: el código es idéntico al worktree validado;
el arnés da 40 snapshots equivalentes; 409 unit + 47 e2e en verde; los conteos del DoD; VERSION intacto.

**Herramientas:**

| Herramienta | Uso |
|---|---|
| Serena | `initial_instructions` → `activate_project` (7 memorias, sin onboarding). `get_symbols_overview` de `tests/snapshots/ui_snapshot.py`. `find_symbol` de `dt_to_local_input`. `find_referencing_symbols` de los handlers de `ui_profit_items.py`: no tienen llamadores Python, solo la ruta. |
| codebase-memory | `index_status`: índice de 16:26Z, anterior solo al lote G, que únicamente toca el CHANGELOG. `detect_changes --since 276b411`: impacto Python limitado a los 4 handlers `profit_items_*_edit_form`. `parse_unusable` en `profit_items_edit_form.html`, `sale_edit_form.html` y `tab_transfers.html`. |
| `/code-review` (high, skill en segundo plano) | 0 hallazgos. Confirmó la equivalencia del unificado (`transfer_in`→`transfer-in`, `min`, datalists, lote, readonly) y del título con `{% set %}`, y que no quedan referencias a las 4 plantillas borradas. Nota: el subagente usó Read/grep/git, no Serena: el diff es casi todo Jinja. |
| **Fuera de cobertura → `grep`/`Read`/`git diff`** | Las 25 plantillas Jinja2 (ninguna de las dos herramientas las cubre), `app/settings.py` (`load_dotenv`), `.gitea/workflows/`, `CHANGELOG.md`. |
| Ejecución | Arnés con `APP_MODE=desktop`. Mutación `value|safe` en `field()` en un worktree desechable (`/tmp/wt-mut`, ya borrado): el arnés no la detecta y el unit test sí. Demo de inyección por clave de `attrs` con Jinja2 3.1.6. |

## 2. Respuesta a los 5 puntos del encargo

1. **XSS.** `modal()` y `field()` autoescapan `id`, `title`, `close_label`, `container_style`, `label`, `value` y los
   valores de `attrs`. Ninguna macro usa `|safe`, y el cuerpo llega por `caller()` como `Markup`. Los títulos
   concatenados (`'Editar Usuario: ' ~ user.username`) son `str` y se escapan. El de `sales_document_edit_form`
   (`{% set %}…{% endset %}`) conserva `<code>` y escapa `doc.code`. **Hueco:** las *claves* de `attrs` no se
   validan (H-304).
2. **Arnés.** La guarda de `DATABASE_URL` es correcta: el entorno se fija antes de importar, `load_dotenv()` no
   sobrescribe y no hay acceso a BD al importar. Problemas:
   - No es hermético (H-301).
   - No detecta regresiones de escape (H-302).
   - Solo cubre GET de admin en el camino feliz (H-303).

   La normalización de fecha/hora/mes es global: también tapa las fechas sembradas, que son fijas. Es aceptable en
   la fase 1, pero un cambio de formato de fecha pasaría sin aviso. `checked` no se siembra (venta sin
   transferencia); lo cubre `test_sale_payment_method_e2e.py:50`.
3. **Unificado de Utilidades y router.** Correcto. Cada `kind` reproduce su original: ids, URLs, el `min` de cantidad
   (sin `min` en ajuste), los datalists, el lote solo en compra y los readonly. Los 4 handlers pasan el `kind` literal.
4. **Decisiones que se apartan del traspaso.**
   - **D2 (arnés lado servidor):** correcta para la fase 1, que es un refactor de plantillas; el resultado es
     determinista. No basta como red para las fases 2-3 (JS/CSS), donde el riesgo es de ejecución, ni para la 4
     (H-303).
   - **D4 (`close_label=None`):** correcta; mantiene el HTML idéntico.
   - **D5 (`field()` solo en modales):** correcta. Las 18 filas `row` que quedan en paneles y pestañas son de dominio
     y van a la fase 5.
5. **e2e nuevos.**
   - `test_customer_edit_modal_renders_inputs_and_saves` detecta R1 (cuerpo escapado, validado por mutación en el
     diseño). **No** detecta la pérdida de escape en un `value`, porque `<b>` dentro de un atributo da el mismo valor.
   - `closes_with_x` fija el contrato `data-modal-close`.
   - El de Utilidades abre venta y compra pero no guarda, y no abre ajuste ni traspaso (H-305).

## 3. Checklist por dimensión

| # | Dimensión | Estado | Evidencia |
|---|---|---|---|
| 1 | SoC/SRP | ✅ | Las macros no conocen dominios; el unificado deriva todo de `kind` (`profit_items_edit_form.html:6-9`) |
| 2 | Cohesión/Acoplamiento | ✅ | El contrato JS `data-modal-close`/`data-modal-static` vive en `components/modal.html:8,13` |
| 3 | Capas | ✅ | página → layout → `base.html`; partial → componente. `base.html` no carga assets |
| 4 | Seguridad | ⚠️ | Autoescape íntegro; claves de `attrs` sin validar (`components/forms.html:5`, H-304) |
| 5 | Robustez | ⚠️ | El arnés falla de forma explícita (`ui_snapshot.py:239-240`); `kind`/`heading` desconocidos no fallan (H-308) |
| 6 | Consistencia | ✅ | Todo se verifica contra la baseline commiteada (D8) |
| 7 | Observabilidad | ✅ | No aplica; el arnés imprime un diff legible (`ui_snapshot.py:290-295`) |
| 8 | Configuración | ❌ | El arnés hereda `APP_MODE`, `BUSINESS_CONFIG_*`, `AGENT_LLM_*` y `DEV_ACTIONS_ENABLED` del `.env`/shell (H-301) |
| 9 | Pruebas | ⚠️ | +22 unit, +3 e2e, 40 páginas; huecos en H-302, H-303, H-305, H-307 |
| 10 | Escalabilidad | ✅ | No aplica; el arnés tarda unos 3 s |

## 4. Scorecard ponderado

| # | Dimensión | Peso | Score | Ponderado |
|---|---|---|---|---|
| 1 | SoC/SRP | 10 | 5 | 0.50 |
| 2 | Cohesión/Acoplamiento | 10 | 5 | 0.50 |
| 3 | Capas/Límites | 10 | 5 | 0.50 |
| 4 | Seguridad | 15 | 4 | 0.60 |
| 5 | Robustez | 15 | 4 | 0.60 |
| 6 | Consistencia | 10 | 5 | 0.50 |
| 7 | Observabilidad | 10 | 4 | 0.40 |
| 8 | Configuración | 5 | 3 | 0.15 |
| 9 | Pruebas | 10 | 3.5 | 0.35 |
| 10 | Escalabilidad | 5 | 5 | 0.25 |
| | **Total** | 100 | | **4.35 🟢** |

El código de producción de la fase está limpio: `/code-review` no encontró bugs. La deuda está en el arnés, que es
la red de seguridad de las fases 2-5.

## 5. Hallazgos

### 🔴 Críticos / 🟠 Altos

Ninguno.

### 🟡 Medios

**H-301 — El arnés no es hermético: depende del `.env` y del shell de la máquina** (dim. 8/9)
- **Evidencia:** `tests/snapshots/ui_snapshot.py:30-38` solo fija `DATABASE_URL`, `SESSION_*` y las credenciales.
  `app/settings.py:21` hace `load_dotenv()`, que rellena el resto: `APP_MODE` (`settings.py:70`, usado en
  `dashboard.html:42,52,147`), `BUSINESS_CONFIG_PATH[_<KEY>]`/`_DIR`, `DEV_ACTIONS_ENABLED` y `AGENT_LLM_*`.
  Reproducido: `APP_MODE=desktop … check` → `DIFF: dashboard`, `FALLO: 1 de 40`.
- **Riesgo:** la baseline depende de la máquina donde se generó. En otra máquina, en CI o tras tocar el `.env` da
  falsos positivos. Peor: una baseline capturada con otra configuración cubre otras ramas sin que nadie lo note.
- **Recomendación:** en `os.environ.update` fijar todas las variables de `settings.py` que afectan al render
  (`APP_MODE=server`, `BUSINESS_CONFIG_PATH=app/business_config.conf`, `BUSINESS_CONFIG_DIR=""`,
  `DEV_ACTIONS_ENABLED=1`, `AGENT_LLM_BASE_URL` a una URL inalcanzable) y borrar las `BUSINESS_CONFIG_PATH_*`
  heredadas. Test: el `check` da OK con `APP_MODE=desktop` exportado.

**H-302 — El arnés es ciego a regresiones de escape** (dim. 9/4)
- **Evidencia:** la siembra (`ui_snapshot.py:124-164`) no tiene caracteres `< > & " '`, y `html.parser` decodifica
  las entidades de los atributos. Mutación `value="{{ value|safe }}"` en `components/forms.html:5` → `OK: 40
  snapshots equivalentes`. Solo falla `test_field_escapes_value_and_renders_flags`. El e2e del cliente
  (`RAW_NAME`) no la detecta, porque `<b>` dentro de un atributo da el mismo valor.
- **Riesgo:** en las fases 2-5 se reescribirán plantillas que no pasan por `field()`, y un `|safe` o un
  `Markup(...)` mal puesto reabre el XSS que cerró la fase 0 sin que la red de seguridad lo vea.
- **Recomendación:** sembrar valores hostiles y visibles en texto y atributos: nombre de cliente y producto, nota,
  concepto, `doc.code`, dirección (`Cliente <b>"Snap"</b> & 'co'`). Un `"` sin escapar rompe el atributo y un
  `<b>` sin escapar en texto se convierte en etiqueta, así que el diff canónico lo detecta. Regenerar la baseline
  **desde `276b411`** (el código previo) para mantener la referencia.

**H-303 — El arnés solo cubre GET de admin en el camino feliz** (dim. 9)
- **Evidencia:** `ui_snapshot.py:248-255` hace login como `admin` y solo `GET`. No hay capturas de:
  - respuestas `POST`: OOB `hx-swap-oob`, los 409/422 que re-renderizan modales con `form_error`/`message`/`flash`;
  - roles `operator`/`owner`: sidebar, panel de ventas;
  - estados vacíos.
- **Riesgo:** la fase 4 (OOB en vez de HTML en Python, `UIContext`, sidebar data-driven) cambia justo eso. Sin
  ampliarlo, el arnés daría "OK" sin probar lo que se toca.
- **Recomendación:** antes del PAE de la fase 4 (puede ir en el propio PAE), añadir:
  - un segundo login como `operator` con las pestañas visibles;
  - un juego de `POST` inválidos (compra con cantidad 0, usuario duplicado, producto con SKU de otro negocio) y
    válidos con OOB;
  - una pasada sobre un negocio sin datos.

### 🟢 Bajos

**H-304 — Las claves de `attrs` en `field()` no se validan** (dim. 4)
- **Evidencia:** `components/forms.html:5`, `{% for k, v in attrs.items() %} {{ k }}="{{ v }}"`. El autoescape no
  toca espacios ni `=`: `field('L','n',attrs={'x onfocus=alert(1) autofocus data-a':'x'})` →
  `<input … x onfocus=alert(1) autofocus data-a="x" />` (comprobado).
- **Riesgo:** hoy es latente: las 65 llamadas usan claves literales. Se volvería explotable si una fase posterior
  construye `attrs` desde datos.
- **Recomendación:** `<input…{{ attrs|xmlattr }}…>`. Jinja2 3.1.6 rechaza claves con espacio, `/`, `>` o `=`
  ("Invalid character in attribute name") y escapa los valores. Añadir un test que lo fije y otro que haga grep de
  `|safe`/`Markup` en `app/templates/components/` (la prohibición de `design.md` §2.5 no tiene test).

**H-305 — El e2e de Utilidades no guarda el modal unificado** (dim. 9)
- **Evidencia:** `tests/e2e/test_modal_components_e2e.py:69-95` abre venta y compra, comprueba valores y cierra.
  No hace submit y no abre ajuste ni traspaso.
- **Riesgo:** el `hx-post` y los `readonly` los vigilan el unit test y el arnés, pero ningún test recorre el flujo
  completo "editar costo → guardar → la tabla cambia" con el modal nuevo, que pide el DoD de e2e.
- **Recomendación:** guardar un cambio de costo desde `#profit-items-purchase-edit-modal` (o ajuste) y comprobarlo
  en `#tab-content`.

**H-306 — El arnés deja directorios temporales** (dim. 5)
- **Evidencia:** `ui_snapshot.py:29` hace `tempfile.mkdtemp` sin borrarlo; hay 32 `/tmp/ui-snapshot-*` con su
  `snap.db`.
- **Recomendación:** `atexit.register(shutil.rmtree, _TMP, ignore_errors=True)`.

**H-307 — El arnés no corre en CI** (dim. 9)
- **Evidencia:** no aparece en `.gitea/workflows/`. D9 lo deja fuera de pytest, pero nada lo ejecuta de forma
  automática.
- **Riesgo:** entre fases, alguien cambia el HTML sin pasar el arnés y la baseline se queda vieja; la siguiente fase
  arranca con un diff que no es suyo.
- **Recomendación:** un paso `python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline` en el job `test`.
  Requiere H-301, o fallará en el runner.

**H-308 — Macros sin fallo explícito ante parámetros desconocidos** (dim. 5)
- **Evidencia:**
  - `profit_items_edit_form.html:7-10`: un `kind` fuera de los 4 da título vacío e id `profit-items--edit-modal`,
    sin error.
  - `components/modal.html:12`: cualquier `heading` distinto de `'h2'` cae en `h3`.
- **Riesgo:** bajo, porque `kind` es literal del router. Pero en las fases 4-5, cuando se muevan routers, un typo
  se manifestaría como un modal que no abre y no como un error.
- **Recomendación:** al principio del unificado, `{% if kind not in titles %}{{ none.kind_desconocido }}{% endif %}`
  (con `StrictUndefined` no hace falta) o validar `kind` en un helper del router. Es opcional; puede agruparse con
  H-304.

**Nota (no es hallazgo):** con `value=None`, `field()` omite el atributo, mientras que antes se renderizaba
`value="None"`. Ningún sitio migrado pasa `None`: los modelos son `NOT NULL` y los routers dan `""`
(`ui_purchases.py:545`, `ui_profit_items.py:507`). Es un comportamiento documentado en la macro.

## 6. Matriz impacto vs esfuerzo

| Cuadrante | Hallazgos |
|---|---|
| 🔥 Quick wins | H-301, H-302, H-304, H-306 |
| 🎯 Estratégicos | H-303 (antes de la fase 4) |
| 📋 Baja prioridad | H-305, H-307, H-308 |
| ❌ Evitar | — |

## 7. Backlog priorizado (paso 6, cc_local)

1. H-301: arnés hermético. Es condición previa para H-302 y H-307.
2. H-302: siembra hostil y baseline regenerada desde `276b411`, y después `check` contra `2c4df16` + correcciones.
3. H-304: `xmlattr` + test de claves + test "sin `|safe` en `components/`".
4. H-306: limpieza de `_TMP`.
5. H-305: e2e que guarda el modal unificado.
6. H-307: paso del arnés en CI.
7. H-308: opcional.
8. H-303: al PAE de la fase 4 (o a uno propio previo).

## 8. Plan y criterios de cierre

- **Paso 6 (fase 1):** H-301, H-302, H-304, H-305 y H-306 corregidos. ✅ (ver §9)
  - `check` da OK con `APP_MODE=desktop` exportado. ✅
  - La mutación `value|safe` hace fallar el arnés. ✅ (13 de 40)
  - `field(attrs={'a b': 'x'})` lanza error. ✅
  - El e2e de Utilidades guarda y verifica. ✅
  - 411 unit y 47 e2e en verde. ✅
- **Antes de la fase 2:** H-307 (arnés en CI). Pendiente.
- **Antes de la fase 4:** H-303 (POST, roles, estados vacíos). Pendiente.

## 9. Resultado de las correcciones (mismo día, Claude Code)

| ID | Resultado | Evidencia |
|---|---|---|
| H-301 | **fixed** | `tests/snapshots/ui_snapshot.py`: fija `APP_MODE`, `BUSINESS_CONFIG_PATH`/`_DIR`, `DEV_ACTIONS_ENABLED`, `AGENT_LLM_BASE_URL`/`_API_KEY` a los defaults reales de `settings.py` y borra `BUSINESS_CONFIG_PATH_*` heredadas. Verificado: `check` con `APP_MODE=desktop BUSINESS_CONFIG_PATH=/tmp/fake.conf DEV_ACTIONS_ENABLED=0` exportados → "OK: 40 equivalentes". |
| H-302 | **fixed** | `_hostile()` en `_seed()`: nombre de producto, cliente, dirección, 4 notas de movimiento, `doc.notes`, `SalesDocumentItem.description`. Baseline regenerada **desde `276b411`** (worktree desechable, arnés ya corregido) — no desde el código migrado, para no invalidar la comparación. Verificado: mutación `value\|safe` en `field()` → **13 de 40 snapshots fallan** (antes de este fix, 0). Confirmado aparte que el canónico distingue un atributo bien escapado (entidades decodificadas dentro de un solo token) de uno roto (la etiqueta se fragmenta en varios tokens). |
| H-304 | **fixed** | `components/forms.html`: el bucle manual de `attrs` → `{{ attrs\|xmlattr }}`. Verificado por tokens: HTML idéntico para los 65 usos reales. Clave con espacio → `ValueError: Invalid character in attribute name`. +2 unit tests: `test_field_rejects_invalid_attrs_key`, `test_components_never_use_safe_or_markup` (grep de `\|safe`/`Markup(` en `components/*.html`). |
| H-305 | **fixed** | `test_profit_items_unified_modals_open_for_sale_and_purchase`: tras los pasos existentes, cambia `unit_cost` a 9.99, guarda, y reabre el modal de compra para comprobar el round-trip (la tabla de Utilidades muestra `cogs` calculado, no el `unit_cost` literal, así que el round-trip vía el modal es la aserción correcta). |
| H-306 | **fixed** | `atexit.register(shutil.rmtree, _TMP, ignore_errors=True)` en `ui_snapshot.py`; borrados los 35 directorios `/tmp/ui-snapshot-*` acumulados. |
| H-303 | pendiente | Al PAE de la fase 4 (o a uno propio previo), según lo ya planeado en §8. |
| H-307 | pendiente | Antes de la fase 2, según lo ya planeado en §8. |
| H-308 | pendiente | Opcional, sin fecha fijada. |

**Verificación tras corregir:** 411 unit (+2) y 47 e2e en verde; arnés "OK: 40 snapshots equivalentes".

**Incidente durante la corrección, resuelto en el momento:** un comando encadenado con `;` en vez de `&&` hizo
que, tras fallar un `cp` (el directorio `tests/snapshots/` no existe en `276b411`, se crea en el lote A), el
`write` corriera en el repo real sobre el código **ya migrado** en vez del worktree pre-migración —
sobrescribiendo por un instante la baseline real con una comparación trivial (código contra sí mismo).
Detectado con `git status`/`git diff --stat` antes de seguir; revertido con `git checkout --
tests/snapshots/baseline/`; regenerado correctamente desde el worktree en `276b411`. Sin impacto en el
resultado final, documentado aquí por transparencia.

## 10. Re-revisión tras las correcciones (2026-10-23, `2c4df16..9ddf3d3`)

**Método:** `/code-review` (high, skill en segundo plano) sobre el commit de correcciones + revisión propia del
diff. Ejecución en un worktree desechable de `9ddf3d3` (`/tmp/wt-re`, ya borrado) con un `.env` de prueba
(el `.env` real no se tocó ni se leyó). Serena/codebase-memory no aportan aquí: el diff es Jinja, un script
fuera de `app/` y tests; `grep`/`Read`/`git diff` como en la primera pasada.

**Confirmado correcto:** `xmlattr` mantiene el HTML (arnés OK) y Jinja2 está fijado a 3.1.6 en
`requirements.txt` (la validación de claves existe desde 3.1.4). Su regex `[\s/>=]` admite `:`, así que
`hx-on::…` sigue siendo válido. La siembra hostil sí la detecta el canonicalizador. El e2e de H-305 es
independiente: `live_server` tiene `scope="function"` (BD nueva por test) y el modal cierra por
`htmx:afterRequest` (`app.js:703`).

### Hallazgos nuevos

**H-309 — H-301 quedó incompleto: el `.env` vuelve a cargar `BUSINESS_CONFIG_PATH_<CODE>`** (Bajo, dim. 8)
- **Evidencia:** `tests/snapshots/ui_snapshot.py:36-38` borra esas claves *antes* de importar la app, pero
  `app/settings.py:21` ejecuta `load_dotenv()` al importarse y añade las que faltan.
  `app/business_config.py:193` las usa para elegir el `.conf` del negocio.
- **Reproducido:** en el worktree, un `.env` con `BUSINESS_CONFIG_PATH_ROPA`/`_BAZAR` → `FALLO: 4 de 40`
  (`dashboard`, `tab_users`, `tab_businesses`, `modal_user`). Hoy no pasa porque el `.env` real no tiene esas
  claves (dato del `/code-review`).
- **Recomendación (probada en el worktree: OK con y sin el `.env` contaminado):** neutralizar
  `dotenv.load_dotenv` en el arnés antes de importar la app. Así ninguna variable no fijada entra desde el
  `.env`, y la lista de variables fijadas deja de tener que estar completa.

**H-310 — El test anti-`|safe` no detecta `| safe` con espacios** (Bajo, dim. 9)
- **Evidencia:** `tests/unit/test_ui_components.py:92` busca el literal `"|safe"`. Jinja también acepta
  `{{ x | safe }}`.
- **Recomendación:** `re.search(r"\|\s*safe\b|Markup\s*\(", text)`.

**H-311 — (previo, destapado por la siembra hostil) `/ui/transfers/print?movement_id=` deja `:` al final de la referencia** (Bajo, dim. 5)
- **Evidencia:** `app/routers/ui_transfers.py:715` extrae la ref con `raw.split("ref=", 1)[1].split()[0]`. Si el
  traspaso tiene nota, la nota es `…ref=TP-…: <nota>` y la ref queda `TP-…:`: lo muestran el título y el
  "Ref:" de la impresión, y entra así en el `ilike`. El servicio ya la extrae bien
  (`inventory_service.py:724`, `[^\s:;]+`). Diff de baseline: `print_transfer.txt` `TP-…-260115100000` →
  `TP-…-260115100000:`.
- **Alcance:** la UI no usa esa ruta (`tab_transfers.html:115,232,306` usan `?ref=`); solo la usa el arnés. Por
  eso el snapshot `print_transfer` fija como referencia una ruta que nadie usa y con un bug.
- **Recomendación:**
  1. En el arnés, `print_transfer` → `/ui/transfers/print?ref={transfer_ref}` (`ids` ya lo tiene).
  2. Aparte, fuera del alcance del frontend: en `ui_transfers.py:715` reutilizar `_transfer_codes_from_out_note`.

### Scorecard tras las correcciones

| # | Dimensión | Peso | Antes | Ahora | Motivo |
|---|---|---|---|---|---|
| 1 | SoC/SRP | 10 | 5 | 5 | — |
| 2 | Cohesión | 10 | 5 | 5 | — |
| 3 | Capas | 10 | 5 | 5 | — |
| 4 | Seguridad | 15 | 4 | 5 | Claves de `attrs` validadas (H-304) |
| 5 | Robustez | 15 | 4 | 4 | H-308 abierto; H-311 previo |
| 6 | Consistencia | 10 | 5 | 5 | — |
| 7 | Observabilidad | 10 | 4 | 4 | — |
| 8 | Configuración | 5 | 3 | 4 | Arnés casi hermético (falta H-309) |
| 9 | Pruebas | 10 | 3.5 | 4 | Escape cubierto (H-302) y guardado e2e (H-305); siguen abiertos H-303, H-307, H-310 |
| 10 | Escalabilidad | 5 | 5 | 5 | — |
| | **Total** | | **4.35** | **4.60 🟢** | |

### Corrección de H-309, H-310 y H-311 (mismo día, Claude Code)

El usuario decidió corregir los tres, incluido el router de traspasos, dentro del mismo commit de correcciones.

| ID | Resultado | Evidencia |
|---|---|---|
| H-309 | **fixed** | `ui_snapshot.py`: `dotenv.load_dotenv` neutralizado antes de importar la app. Verificado en un worktree con un `.env` que define `BUSINESS_CONFIG_PATH_ROPA`/`_BAZAR`, `AGENT_LLM_MODEL` y `LOG_LEVEL` → "OK: 40" (antes, 4 fallos). |
| H-310 | **fixed** | `test_components_never_use_safe_or_markup` usa `\|\s*safe\b\|Markup\s*\(`. Comprobado: detecta `\|safe`, `\| safe`, `\|  safe` y `Markup (`; no salta con `\|safety` ni `\|e`. |
| H-311 (arnés) | **fixed** | `print_transfer` → `/ui/transfers/print?ref={transfer_ref}`, la ruta que usa la UI. |
| H-311 (router) | **fixed** | `ui_transfers.py` (`transfer_print`): la ref se extrae con `re.search(r"\bref=([^\s:;]+)")`, el mismo patrón que `InventoryService._transfer_codes_from_out_note`. Test nuevo `tests/unit/test_transfer_print_ref.py`, validado por mutación: con el `split` anterior, falla. |

**Baseline:** regenerada otra vez desde `276b411` (worktree con `set -e` y comprobación del directorio, sin
encadenar con `;`). Solo cambia `print_transfer.txt`, que pierde los `:` finales; los otros 39 snapshots no
cambian. `check` sobre el código actual da "OK: 40".

**Verificación:** 412 unit (+1) y 47 e2e en verde; 0 directorios temporales.

**Score final:** Robustez 4 → 4 (sigue abierto H-308), Configuración 4 → 5 (arnés hermético), Pruebas 4 → 4
(siguen abiertos H-303 y H-307). Total **4.65 / 5.00 🟢**.

**Abiertos:** H-303 (antes de la fase 4), H-307 (antes de la fase 2) y H-308 (opcional).

## 11. Cierre de H-307 y H-308; H-303 diferido a la fase 4 (2026-10-23)

| ID | Resultado | Evidencia |
|---|---|---|
| H-307 | **fixed** | Paso `UI snapshots` en el job `test` de `.gitea/workflows/deploy.yml`, entre los unit y los e2e. Simulado en un worktree limpio (sin `.env`, con el env del job): "OK: 40". Con un cambio de HTML sin regenerar la baseline sale con código 1 (17 de 40 difieren), así que el job falla. `.github/workflows/test.yml` (CI heredado del mirror, sin tocar desde `2a943a9` y sin e2e) no se modifica. |
| H-308 | **fixed** | Global `template_error(msg)` en `app/routers/ui_common.py`, que lanza `TemplateRuntimeError`. `components/modal.html` la usa si `heading` no es `h2`/`h3`, y `profit_items_edit_form.html` si `kind` no es uno de los 4. +2 unit tests (`test_modal_rejects_unknown_heading`, `test_profit_items_edit_form_rejects_unknown_kind`). Arnés "OK: 40". |
| H-303 | **diferido (decisión del usuario)** | Escrito como prerrequisito del PAE de la fase 4 en `docs/auditorias/2026-10-23-frontend-arquitectura-diagnostico.md` §4 ("Nota para la fase 4"): roles, POST con error, POST con OOB, estados vacíos, la baseline desde el commit anterior y el criterio de cierre. |

**Verificación:** 414 unit (+2) y 47 e2e en verde; arnés "OK: 40"; YAML del workflow válido.

### Scorecard final

| # | Dimensión | Peso | Score | Ponderado |
|---|---|---|---|---|
| 1 | SoC/SRP | 10 | 5 | 0.50 |
| 2 | Cohesión/Acoplamiento | 10 | 5 | 0.50 |
| 3 | Capas/Límites | 10 | 5 | 0.50 |
| 4 | Seguridad | 15 | 5 | 0.75 |
| 5 | Robustez | 15 | 5 | 0.75 |
| 6 | Consistencia | 10 | 5 | 0.50 |
| 7 | Observabilidad | 10 | 4 | 0.40 |
| 8 | Configuración | 5 | 5 | 0.25 |
| 9 | Pruebas | 10 | 4 | 0.40 |
| 10 | Escalabilidad | 5 | 5 | 0.25 |
| | **Total** | 100 | | **4.80 🟢** |

**Qué falta para 5:**
- **Pruebas → 5:** H-303 (fase 4) lleva el total a 4.90.
- **Observabilidad:** se queda en 4 porque no aplica a un refactor de plantillas; subirla sin cambios reales
  sería inflar el score.

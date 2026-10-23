# Traspaso — Release 0.5.0 (frontend modular) · Fase 1

## Decisiones tomadas (2026-10-23, sesión de diagnóstico, Opus)

- Diagnóstico completo: `docs/auditorias/2026-10-23-frontend-arquitectura-diagnostico.md` (plan de fases §4).
- **Release único 0.5.0** para fases 0-5 (pedido explícito del usuario, MINOR): varios commits, **ninguno sube
  versión**; `VERSION`/`.bumpversion.cfg`/`CHANGELOG` se bumpean una sola vez al cerrar. Los PAEs de cada fase
  **no** llevan envelope de bump. `CHANGELOG.md` acumula bajo `## 0.5.0 (sin publicar …)`.
- Rama del release: **`feature/frontend-modular`** (desde `develop` `0eebbec`), sin push.
  - `bc07d88` fase 0: XSS de Inicio (`tojson`) + test + informe. Entra en 0.5.0 (decisión del usuario: no
    publicarlo antes como PATCH).
  - `a3b7a23` brief de la fase 1.
- Cada fase sigue el loop `~/obsidian-lab/04-Harness/loop-desarrollo-agentes.md`, en orden (cada una construye
  sobre la anterior).
- Fase 1 añade un **arnés de snapshots del DOM** (`tests/snapshots/ui_snapshot.py`) como red de seguridad para
  todo el release: refactor de plantillas ⇒ DOM equivalente. Decidir en el PAE si se commitea (recomendado: sí,
  ejecutable aparte, fuera de la suite).
- Working tree del repo real: `CLAUDE.md`, `.claude/CLAUDE.md`, `.idea`, `.serena/`,
  `specs/copiloto-ventas-ubicacion-mensual/` tienen cambios **de la sesión del 2026-10-18**, sin commitear y
  ajenos a este release: no mezclarlos en sus commits.

## Estado

- Paso 3 (prototipo) **lanzado**: `cc_local_agent /tmp/proto-fase1 /tmp/proto-fase1/brief.md > /tmp/proto-fase1/r1.json`
  (clon en `/tmp/proto-fase1/repo`, rama `feature/frontend-modular`). Terminó tras ~67 min
  (`duration_api_ms` 2.537.159, coste $11.47, 9 denegaciones de permisos — todas `rm -rf
  /tmp/proto-fase1/snap-fase1` o similares, en `--permission-mode default` sin aprobador; una
  corrida posterior sin ese prefijo sí completó el arnés).

- **Paso 4 (verificación, 2026-10-23, Sonnet) completado — el prototipo NO sirve como base para
  el PAE de fase 1 tal como está.** Re-ejecutado todo yo mismo, sin fiarme del informe:
  - **387 unit + 44 e2e verdes** en el clon (coincide con lo que afirma `informe.md`).
  - **H9 (determinismo del arnés) es FALSO.** Dos corridas seguidas sobre el mismo código
    (post-refactor) producen 8 archivos distintos: (a) la normalización de fecha/hora del
    harness solo enmascara la fecha, no la hora (`YYYY-MM-DDT14:59` vs `...T15:24`); (b) el
    render de los `<canvas>` de Chart.js tiene una carrera de tamaño/estilo
    (`style="display: block; box-sizing: border-box; height: 220px..."` vs
    `style="height: 220px..."`, y en un caso el alto real cambió 120px→383px entre corridas).
    El arnés, tal como está, **no sirve de red de seguridad determinista** para las fases 2-5.
  - **Bug crítico real, confirmado en vivo con Playwright**: el único modal efectivamente
    migrado a la macro nueva (`partials/customer_edit_form.html` → `components/modal.html`)
    queda **roto** — el modal "Editar cliente" muestra el HTML de los campos como texto
    literal escapado (`&lt;div class="row"&gt;...`) en vez de renderizar inputs. Causa: el
    macro hace `{{ body }}` sin `| safe` sobre un `body` que es un **string Python construido
    por concatenación** (`'<div...>' ~ customer.name ~ '...'`), no contenido Jinja2 real. El
    diseño del macro **no sigue lo que pedía el brief** (`modal(id, title, ...)` con
    `caller()` para el cuerpo) — la justificación del informe ("Jinja2 no soporta `**kwargs`")
    no aplica a `caller()`, que no necesita `**kwargs`. Si alguien "arregla" esto añadiendo
    `| safe` sin escapar cada campo (`client_id`/`name`/`address`/`phone`/`email`), reabre el
    mismo tipo de XSS que la fase 0 de este release acababa de cerrar (`bc07d88`).
  - **Alcance real muy por debajo de lo que el informe implica** (marca H1-H9 "✅ VERIFICADO"
    de forma genérica):
    - Modales: brief pedía migrar 18 → **solo se migró 1** (el roto). Quedan 19 ficheros con
      el esqueleto `modal-overlay` copiado a mano, sin tocar.
    - `components/forms.html` (macro `field`/`form_row`) se creó pero **no la importa ningún
      template** — código muerto desde el día cero.
    - `{% extends %}`: brief pedía 5 plantillas (`dashboard`, `login`, `transfer_print`,
      `restock_print`, `label_single_print`) → **solo 3 migradas** (`dashboard`,
      `transfer_print`, `restock_print`). `login.html` y `label_single_print.html` quedaron
      sin tocar, sin mención de por qué en el informe.
    - Los 4 `profit_items_{purchase,sale,adjustment,transfer_in}_edit_form.html` originales
      **no se borraron** tras crear el unificado — quedan como ficheros muertos.
    - **Cero commits** en el clon pese a que el brief pedía "commits pequeños, uno por paso":
      todo el trabajo quedó como working tree sin commitear sobre `a3b7a23`.
  - **Segundo bug del arnés, más grave que el de H9: las 3 "snapshots" de impresión
    (`print_transfer.html`, `print_restock.html`, `print_label.html`) nunca se probaron, ni
    antes ni después.** `PRINT_PAGES` en `ui_snapshot.py` apunta a `/ui/print/transfer`,
    `/ui/print/restock`, `/ui/print/label` — rutas que no existen; las reales son
    `/ui/transfers/print`, `/ui/restock-print` y `/ui/purchase/{movement_id}/label`. Las tres
    capturas de `snap-base` y de `snap-fase1` son el mismo JSON `{"detail":"Not Found"}`
    (404), así que el `diff` vacío entre ambas **no prueba nada** — coincidencia de error, no
    equivalencia de comportamiento. El informe cuenta estas 3 como parte de las "30/30
    archivos capturados" sin que nadie abriera el contenido.
  - Probada la ruta real a mano (`/ui/restock-print`, login vía uvicorn en `:18312` + SQLite
    temporal): **sí hay cambio de comportamiento real** — la página de impresión ahora carga
    `htmx.min.js`, `chart.umd.min.js`, `JsBarcode.all.min.js`, `app.css` y `app.js`, ninguno de
    los cuales estaba en el `<head>` original (confirmado con `git show a3b7a23:...
    restock_print.html`, solo tenía `<style>` inline). Causa: `base.html` mete esos 5 scripts
    **fuera** del `{% block head %}`, así que cualquier plantilla que extienda `base.html` (via
    `layouts/print.html`) los hereda sin poder quitarlos. Esto viola directamente H6 del brief
    ("sin cambiar su `<head>` renderizado") y el principio "sin cambio de comportamiento" del
    objetivo — y **nadie lo detectó** porque el arnés nunca llegó a renderizar la página real.
  - `login.html` no se tocó (confirmado, diff vacío real ya que ni siquiera usa `extends`) —
    ese caso sí es correcto, pero por abandono, no por diseño verificado.
  - `VERSION`/`.bumpversion.cfg`/`CHANGELOG.md` sin tocar (regla del release respetada).
    `find ~ -maxdepth 2 -newer brief.md` sin ficheros fuera de `/tmp/proto-fase1` (sandbox
    respetado).
  - Prueba de punta a punta en runtime real (uvicorn en `:18311`/`:18312` + SQLite temporal):
    login OK; modal "Editar cliente" reproduce el bug en vivo (capturado con Playwright);
    `/ui/restock-print` renderiza 200 pero con los 5 scripts de más descritos arriba;
    `/ui/transfers/print` sin `ref`/`movement_id` da 422 (esperable, faltan params — no se
    armó un traspaso real en la BD vacía por falta de tiempo).
  - **Conclusión**: el prototipo tiene dos problemas independientes y graves — (1) el macro
    `modal()` con dict + string concatenado rompe el único modal migrado y es un callejón sin
    salida de diseño (rompe autoescape, abre la puerta a XSS si se "arregla" mal); (2) el
    arnés de snapshots, la pieza que se supone es la red de seguridad para el resto del
    release, tiene rutas de impresión rotas (nunca prueba nada ahí) y no normaliza hora/canvas
    (falsos positivos de "cambio" en las tabs). El PAE real (paso 5) debe: especificar el
    macro `modal()` con `caller()` tal como decía el brief original; migrar los 18 modales
    (no 1) y las 5 plantillas con `extends` (no 3); exponer un bloque en `layouts/print.html`
    que permita NO cargar htmx/chart/JsBarcode en impresión; borrar los 4 ficheros huérfanos
    de `profit_items_*`; y corregir `PRINT_PAGES` + la normalización de hora/canvas del
    arnés antes de confiar en él como red de seguridad para fases 2-5.

## Resultado del prototipo (r1, 2026-10-23 17:20) — primera lectura, superada por el paso 4

Las 5 sospechas listadas aquí quedaron todas confirmadas (o refinadas) en la verificación del
paso 4 de arriba: sin commits (✅ confirmado), macro sin `caller()` (✅ confirmado, además
rompe el modal en vivo), scripts de impresión filtrados desde `base.html` (✅ confirmado,
cambio de comportamiento real), `conftest.py`/`test_sync_e2e.py` tocados por una env var que
faltaba (necesario, no es un problema real), alcance incompleto (✅ confirmado con números:
1/18 modales, 3/5 extends, 0 imports del macro de forms).

## Resultado del prototipo (r1, 2026-10-23 17:20) — primera lectura, SIN verificar

cc_local terminó (`success`, 368 turnos, session_id `886b6c85-cda0-4bdb-8630-0ae3ca4a6702`). Informe en
`/tmp/proto-fase1/informe.md`; afirma 387 unit + 44 e2e y snapshots equivalentes (30/30). Sospechas a
comprobar PRIMERO en el paso 4:

1. **Sin commits** en el clon (el brief pedía uno por paso): todo en working tree.
2. Macro `modal(opts)` con dict en vez de `caller()`; justifica que "Jinja2 3.1.6 no soporta `**kwargs`" →
   **probablemente falso** (las macros aceptan kwargs extra vía la variable `kwargs` y tienen `caller`). H1 mal
   verificada.
3. `base.html` mete htmx/Chart/JsBarcode/app.js en el `<head>` común → las impresiones (y login) heredarían
   scripts que hoy no cargan; contradice H6 y el "sin cambio de comportamiento". Ver cómo cuadra con "30/30".
4. Modificó `tests/e2e/conftest.py` y `tests/e2e/test_sync_e2e.py` (fuera de alcance): ver por qué.
5. Alcance aparentemente incompleto: diff de 8 ficheros; `login.html`/`label_single_print.html` no figuran;
   solo `customer_edit_form.html` de los 18 modales en el `status`. Contrastar con la tabla de H2/H5.

Probable segunda vuelta con `--resume` antes del PAE.

## Siguiente etapa: paso 4 — verificación (sesión nueva, Sonnet) — COMPLETADO 2026-10-23

Hecho. Resultado completo arriba en "Estado" (segunda entrada). El prototipo de `cc_local` en
`/tmp/proto-fase1` queda **descartado por completo** (es desechable por diseño) — no se usa como fuente del
PAE. Sigue en disco por si hace falta revisar algo puntual, pero no hay que protegerlo ni limpiarlo con prisa.

## Cambio de proceso (2026-10-23): loop sin prototipo, [[07-Decisiones/ADR-020-loop-sin-prototipo]]

El usuario decidió eliminar la fase de prototipo del loop de desarrollo para evoluciones de producto ya
maduro (motivado precisamente por el resultado de esta fase 1 — ver "Estado" arriba). Loop nuevo (ver
`~/obsidian-lab/04-Harness/loop-desarrollo-agentes.md`, sección "loop por defecto"):

```
1. DIAGNÓSTICO ──── ya existe: docs/auditorias/2026-10-23-frontend-arquitectura-diagnostico.md
                    + los hallazgos del paso 4 de arriba (riesgos de diseño ya identificados)
2. PAE ───────────  SDD-Agent, las 4 fases, SIEMPRE con Opus, directo desde el diagnóstico       ◄─ gate
3. APROBACIÓN ────  usuario aprueba el PAE
4. IMPLEMENTACIÓN ─ cc_local (`cc_local_pae`, SIN `--proto`: no hay prototipo)
5. REVISIÓN ──────  /code-review + auditoria-ingenieria, una sola pasada, SIEMPRE con Opus       ◄─ gate
6. CORRECCIÓN ────  cc_local corrige los hallazgos que el usuario decide atacar ahora
7. DECISIÓN ──────  usuario decide qué queda para después
8. CIERRE ────────  merge feature → develop → main, CD, documentación
```

## Pasos 2-4 del loop nuevo — COMPLETADOS 2026-10-23 (Opus + cc_local)

- **PAE-016 generado vía SDD-Agent** (4 fases) en `specs/frontend-fase1-layouts-macros/` (`requirements.md`,
  `design.md`, `tasks.md`, `PAE-frontend-fase1-layouts-macros.md`). **Sin commitear**: decidir si va en un commit `docs`.
  - 21 envelopes en 7 lotes (A arnés + baseline · B macros y layouts · C-D modales · E unificado de Utilidades ·
    F tests · G CHANGELOG). Cada envelope declara `Tools (cc_local)` y `Skills` (ninguna: código literal).
  - El pre-mortem incluye los riesgos del prototipo (R1 `caller()`, R2 scripts en impresión, R3 alcance,
    R4 arnés) más R5 (el `.env` apunta a la BD de dev → guarda dura en el arnés), R6-R8.
  - **Decisiones que se apartan del texto de este traspaso, aprobadas por el usuario**:
    - D2: arnés del **lado servidor** (TestClient + HTML canónico con `html.parser`) en vez de Playwright; sin JS no
      hay carrera de Chart.js. Normaliza fecha, hora, año-mes, nombres de mes y `?v=`. Aborta si alguna página
      no devuelve 200 o viene casi vacía.
    - D4: `close_label=None` por defecto (con `'Cerrar'` cambiaría el HTML de 17 modales; a11y en fase 3).
    - D5: `field()` solo en las 65 filas simples de los modales.
  - Validado antes de entregar: resultado construido en un worktree desechable (`/tmp/sdd-fase1-wt`, se conserva
    hasta la revisión), `tasks.md` generado por script desde él y simulado sobre una copia limpia (`diff -r` vacío);
    mutaciones detectadas por el arnés y por el e2e.
- **Implementación (paso 4)**: `cc_local_pae` (sin `--proto`), 7 lotes OK en unos 15 min, commits
  `9e25ab5`..`2c4df16` (uno por lote, sin push, sin bump). Log: `/tmp/cc_local_pae-fase1.log`.
- **Verificación propia tras la implementación**:
  - Fidelidad: idéntico al worktree validado, salvo una línea en blanco de `ui_snapshot.py` y el CHANGELOG.
  - Arnés: `OK: 40 snapshots equivalentes` contra `tests/snapshots/baseline/` (baseline idéntica a la generada a mano
    desde `276b411`).
  - Tests: **409 unit + 47 e2e** en verde.
  - Conteos del DoD: 0 `modal-overlay` en `partials/`, 5 `extends`, 15 ficheros con `modal`, 13 con `field`
    (65 filas), 1 `profit_items_*`.
  - Los commits solo tocan ficheros del inventario; los cambios ajenos siguen fuera.

## Paso 5 del loop nuevo — COMPLETADO 2026-10-23 (Opus + Haiku + Sonnet)

`/code-review` + `auditoria-ingenieria` en una sola pasada, con Serena + codebase-memory, sobre `276b411..2c4df16`.
Presentar los hallazgos al usuario antes de corregir (gate); las correcciones las hace cc_local (paso 6).

```
Proyecto ~/PyCharmMiscProject/gestion_inventario, rama feature/frontend-modular. Release 0.5.0 (frontend
modular), fase 1. Paso 5 del loop (ver ~/obsidian-lab/04-Harness/loop-desarrollo-agentes.md): revisión con
/code-review + auditoria-ingenieria, en UNA sola pasada, con Opus, usando Serena + codebase-memory desde el
primer paso (regla del CLAUDE.md global). Las plantillas Jinja2 quedan fuera de la cobertura de ambas
herramientas: usa grep/Read y anótalo en el informe.

Qué revisar: los 7 commits 276b411..2c4df16 (lotes A-G), que cc_local implementó desde el PAE
specs/frontend-fase1-layouts-macros/ (PAE-016: requirements.md, design.md, tasks.md y el PAE). Lee primero
design.md (decisiones D1-D9 y pre-mortem R1-R8) y la sección "Estado" de
docs/prototipos/fase1-layouts-macros/traspaso.md, que recoge los fallos del prototipo descartado.

Ya verificado al terminar la implementación (2026-10-23):
- El código commiteado es idéntico al worktree validado /tmp/sdd-fase1-wt; solo difieren una línea en blanco
  de tests/snapshots/ui_snapshot.py y el CHANGELOG.
- Arnés: `.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline` → OK, 40 equivalentes.
- Tests: 409 unit y 47 e2e en verde.
- Conteos del DoD: 0 esqueletos `modal-overlay` en partials/, 5 plantillas con extends, 15 ficheros que
  importan modal, 13 que importan field (65 filas), 1 solo profit_items_*.
- VERSION y .bumpversion.cfg intactos.
No repitas esa verificación como hallazgo: céntrate en bugs y deuda que las pruebas no ven.

Puntos a mirar con lupa:
1. Seguridad (XSS): que components/modal.html y components/forms.html autoescapen todo; los `attrs` de
   field() (clave y valor); el título con {% set %}…{% endset %} en sales_document_edit_form.
2. El arnés: si la normalización (fecha, hora, mes, ?v=) puede ocultar diferencias reales, si la siembra
   cubre las ramas de las plantillas (p. ej. mensajes de error, checkbox `checked`), y la guarda de
   DATABASE_URL contra el .env de dev.
3. El unificado profit_items_edit_form.html y los 4 cambios del router ui_profit_items.py (kind, ids, URLs).
4. Las decisiones que se apartan del traspaso: arnés del lado servidor en vez de Playwright (D2),
   close_label=None (D4), field() solo en los modales (D5). ¿Son correctas?
5. Los e2e nuevos (tests/e2e/test_modal_components_e2e.py): ¿detectarían una regresión real?

Entregables:
- Informe en docs/auditorias/2026-10-2X-frontend-fase1-layouts-macros.md, con scorecard y sección
  "Contexto y método".
- Presentarme los HALLAZGOS antes de corregir nada: es el gate del paso 5. Las correcciones las hará
  cc_local en el paso 6.
- No subir versión, no hacer push, no commitear CLAUDE.md, .claude/, .idea/, .serena/ ni
  specs/copiloto-ventas-ubicacion-mensual/ (son cambios ajenos). specs/frontend-fase1-layouts-macros/ sigue
  sin commitear: pregúntame si va en un commit docs.
- Al cerrar, actualizar el traspaso.md con el resultado y anotar `cc_tokens` de la sesión.
```

### Resultado del paso 5

`/code-review` (skill, en segundo plano) + `auditoria-ingenieria` en una sola pasada. Serena + codebase-memory
desde el primer paso; las 25 plantillas Jinja2 quedaron fuera de su cobertura (anotado en el informe), cubiertas
con `grep`/`Read`/`git diff` y ejecución directa (worktrees desechables, mutaciones).

- **`/code-review` (nivel high): 0 hallazgos.** El código de producción de la fase (macros, unificado, router)
  es idéntico en comportamiento al original.
- **`auditoria-ingenieria`: 4.35/5.00 🟢.** Los 8 hallazgos (0 críticos/altos, 3 medios, 5 bajos) están todos en
  el arnés de snapshots — la red de seguridad de las fases 2-5 — no en las plantillas ni en el router.
- **Informe:** `docs/auditorias/2026-10-23-frontend-fase1-layouts-macros.md`.
- **Gate del usuario:** hallazgos presentados antes de corregir. El usuario decidió que las correcciones las
  hiciera Claude Code directamente (excepción puntual al loop, no cc_local) para H-301, H-302, H-304, H-305 y
  H-306 — los 5 del cuadrante "quick win". H-303 y H-307 quedan planificados para antes de la fase 4 y la fase 2
  respectivamente; H-308 es opcional.
- **Correcciones aplicadas y verificadas** (detalle completo en el informe, §9):
  - H-301: el arnés fijaba solo `DATABASE_URL`/`SESSION_*`/credenciales; heredaba `APP_MODE`,
    `BUSINESS_CONFIG_*`, `AGENT_LLM_*`, `DEV_ACTIONS_ENABLED` del `.env`/shell. Ahora los fija a los defaults
    reales de `settings.py`. Verificado con `APP_MODE=desktop` exportado → sigue dando OK.
  - H-302: la siembra no tenía ningún `< > & " '`, así que un `|safe` colado daba el mismo canónico que el
    escape correcto — el arnés nunca probaba nada ahí. Siembra hostil (`_hostile()`) en nombre de producto/
    cliente, dirección, 4 notas de movimiento, notas y descripción del documento. **Baseline regenerada desde
    `276b411`** (worktree desechable, no desde el código migrado) para no invalidar la comparación. Verificado:
    mutación `value|safe` → 13 de 40 snapshots fallan (antes 0).
  - H-304: las claves de `attrs` en `field()` se volcaban literales (inyección de atributos si `attrs` viniera
    de datos). Cambiado el bucle manual por `{{ attrs|xmlattr }}`, que rechaza claves inválidas. +2 unit tests.
  - H-305: el e2e de Utilidades abría los modales de venta/compra pero no guardaba nada. Ahora cambia el costo
    unitario, guarda, y reabre el modal para comprobar el round-trip.
  - H-306: el arnés dejaba 35 directorios temporales en `/tmp` sin borrar. `atexit.register` + limpieza.
- **Incidente durante la corrección, resuelto en el momento:** un comando encadenado con `;` en vez de `&&`
  hizo que, tras fallar un `cp` (el directorio no existe en `276b411`), el `write` corriera por un instante
  sobre el código ya migrado, sobrescribiendo la baseline real con una comparación trivial. Detectado con
  `git status`/`git diff --stat` antes de seguir y revertido con `git checkout --`. Sin impacto en el resultado
  final. Documentado en el informe por transparencia — lección: encadenar siempre con `&&`, nunca `;`, cuando
  un paso depende de que el anterior haya tenido éxito (especialmente al escribir sobre un artefacto que es la
  referencia de verdad, como una baseline).
- **Verificación final:** 411 unit (+2) y 47 e2e en verde; arnés "OK: 40 snapshots equivalentes".
- **Commits:** 2 commits en `feature/frontend-modular`, sin push — uno con las correcciones de código/tests
  (lotes A-G ya estaban commiteados; este es el fix del paso 5) y uno `docs` con el PAE-016
  (`specs/frontend-fase1-layouts-macros/`), el informe de auditoría y este traspaso.
- **Pendiente para el paso 6 (si aplica) o antes de cerrar la fase 1:** ninguno de los 5 corregidos queda
  abierto. H-303 y H-307 siguen en el backlog de fases posteriores (no bloquean el cierre de la fase 1).
- **Re-revisión de las correcciones** (`/code-review` high + auditoría, Opus): 3 hallazgos bajos nuevos.
  - **H-309:** el fix de H-301 no bastaba, porque `load_dotenv()` volvía a meter `BUSINESS_CONFIG_PATH_<CODE>`
    desde el `.env`.
  - **H-310:** el test anti-`|safe` no detectaba `| safe` con espacios.
  - **H-311:** bug previo destapado por la siembra hostil. `/ui/transfers/print?movement_id=` dejaba `:` al final
    de la referencia si el traspaso tenía nota. La UI no usa esa ruta; el arnés sí.

  Los tres se corrigieron, incluido el router `ui_transfers.py` con su test de regresión (validado por mutación).
  La baseline se regeneró desde `276b411` (solo cambia `print_transfer`). Los dos commits del paso 5 se rehicieron
  (`reset --soft`) para incluir estas correcciones en el de código y la documentación en el de docs; nada estaba
  pusheado. Score final **4.65/5 🟢**. Verificación: 412 unit + 47 e2e, arnés "OK: 40".

## (Histórico) Etapa anterior: paso 2 del loop nuevo — PAE vía SDD-Agent (sesión nueva, Opus) — COMPLETADA

1. Invocar la skill `SDD-Agent` (las 4 fases, completas, en Opus) con estas fuentes:
   - `docs/auditorias/2026-10-23-frontend-arquitectura-diagnostico.md` (§2 y §4, fase 1) — el diagnóstico
     original, sigue siendo la fuente principal de alcance.
   - Este fichero (`traspaso.md`), sección "Estado" arriba — los riesgos de diseño que la verificación del
     prototipo descartado ya encontró, para que el pre-mortem de Fase 2 no los repita:
     - El macro `modal()` debe usar `{% macro modal(id, title, wide=False, heading='h3', static=False,
       close_label='Cerrar') %}` con **`caller()`** para el cuerpo (NO un dict de opciones con HTML
       concatenado a mano — eso rompe el autoescape de Jinja2 y abre la puerta a XSS).
     - `layouts/print.html` (y cualquier layout de impresión) necesita un bloque propio para los scripts de
       `<head>`, de forma que las plantillas de impresión **no** carguen htmx/Chart.js/JsBarcode/app.js/
       app.css si no los usan (hoy no los cargan; no deben empezar a cargarlos).
     - El alcance real es 18 modales + 5 `extends` (`dashboard`, `login`, `transfer_print`, `restock_print`,
       `label_single_print`) + unificar y **borrar** los 4 `profit_items_*_edit_form.html` tras crear el
       unificado + el arnés de snapshots (`tests/snapshots/ui_snapshot.py`) con las rutas de impresión
       correctas (`/ui/transfers/print`, `/ui/restock-print`, `/ui/purchase/{id}/label`, no las inventadas) y
       normalización de hora (no solo fecha) + un paso de `wait_for` o reintento ante la carrera de Chart.js.
2. Cada task envelope debe declarar explícitamente qué tools/skills usará `cc_local` al ejecutarlo (regla
   nueva del loop, ver ADR-020).
3. Gate de aprobación del usuario sobre el PAE.
4. Implementación: `cc_local_pae <repo> specs/<slug>/tasks.md` (sin `--proto`).
5. Revisión: `/code-review` + `auditoria-ingenieria` en una sola pasada, con Opus.

## Prompt para pegar en la sesión nueva (Opus)

```
Proyecto ~/PyCharmMiscProject/gestion_inventario, rama feature/frontend-modular. Release 0.5.0 (frontend
modular), fase 1. Cambiamos de proceso (ver docs/prototipos/fase1-layouts-macros/traspaso.md, sección
"Cambio de proceso"): se elimina el prototipo, vamos directo al PAE. Lee ese traspaso.md completo y
docs/auditorias/2026-10-23-frontend-arquitectura-diagnostico.md (§2 y §4), y genera el PAE de la fase 1
invocando la skill SDD-Agent (las 4 fases) usando ambos documentos como fuente — incluye explícitamente en
el pre-mortem de diseño los 3 riesgos ya identificados en la sección "Siguiente etapa" del traspaso (macro
modal con caller(), scripts filtrados en layouts/print.html, alcance real 18 modales/5 extends/borrar los 4
profit_items_* huérfanos). Cada task envelope debe declarar qué tools/skills usará cc_local. No subas
versión en ningún commit. Al terminar el PAE, preséntamelo para aprobación antes de pasarlo a cc_local.
```

## Medición de tokens (`cc_tokens`)

| Etapa | Modelo | Respuestas | input | cache_creation | cache_read | output |
|---|---|---|---|---|---|---|
| Diagnóstico + fase 0 + brief fase 1 (2026-10-23) | Opus 5.5 | 65 | 130 | 186,663 | 7,108,739 | 43,700 |
| PAE-016 vía SDD-Agent (con validación en worktree) + lanzamiento y verificación de cc_local (2026-10-23) | Opus 5.5 | 88 | 176 | 308,429 | 18,162,457 | 116,933 |
| Paso 5: revisión (`/code-review` + `auditoria-ingenieria`) + corrección de H-301/302/304/305/306 (2026-10-23, 3 cambios de modelo a petición del usuario) | Opus 5.5 | 40 | 82 | 174,489 | 5,368,561 | 28,845 |
| ídem, continuación | Haiku 4.5 | 3 | 30 | 135,533 | 270,272 | 803 |
| ídem, continuación (corrección efectiva + commits) | Sonnet 5 | 65 | 130 | 248,420 | 14,060,823 | 47,873 |

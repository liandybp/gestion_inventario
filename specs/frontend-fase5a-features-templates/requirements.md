# Requirements — Frontend fase 5a: plantillas de `partials/` a `features/<dominio>/`

**Release:** 0.5.0 (agrupado, sin bump por commit). **Rama base:** `develop` `bf38a2b`.
**Insumo:** `docs/auditorias/2026-10-23-frontend-arquitectura-diagnostico.md` §2 (principio 1, "por dominio, no por
tipo de vista") y §4 fila "5"; `specs/frontend-fase4-oob-uicontext/design.md` §2.6 (los fragmentos de la fase 4 ya
nacieron en `features/<dominio>/`).
**Partición de la fase 5 (decisión del usuario, 2026-11-02):** dos PAE seguidos dentro de 0.5.0. Este (5a) solo mueve
plantillas. El 5b (`specs/frontend-fase5b-queries-uicontext/`) saca las consultas de los routers y parte de este.
**Principio rector de la release:** el HTML que ve el usuario no cambia. El arnés `tests/snapshots/ui_snapshot.py`
lo demuestra contra la baseline de `bf38a2b` **sin regenerarla**.

## 1. Requisitos funcionales

### REQ-1: Cada plantilla viva de `partials/` pasa a `features/<dominio>/` con el mismo nombre
**Priority:** Must Have
WHEN termina la fase, THE SYSTEM SHALL tener las 54 plantillas vivas de `app/templates/partials/` en
`app/templates/features/<dominio>/<mismo nombre>.html`, según la tabla de mapeo de `design.md` §2.3, y el directorio
`app/templates/partials/` no existe.

```gherkin
Scenario: una plantilla movida conserva su nombre
  GIVEN "app/templates/partials/sale_panel.html" existe en bf38a2b
  WHEN termina la fase 5a
  THEN existe "app/templates/features/sales/sale_panel.html" con el mismo contenido salvo rutas de include
  AND no existe "app/templates/partials/"

Scenario: los fragmentos de la fase 4 no se tocan
  GIVEN "app/templates/features/sales/_defaults_oob.html" existe en bf38a2b
  WHEN termina la fase 5a
  THEN el fichero sigue en la misma ruta y "git diff bf38a2b -- app/templates/features/sales/_defaults_oob.html" está vacío
```

### REQ-2: Todas las referencias apuntan a la ruta nueva
**Priority:** Must Have
WHEN se renderiza cualquier plantilla, THE SYSTEM SHALL resolverla por su ruta nueva. Esto cubre las 154 líneas con
`partials/` de 21 routers (152 nombres de plantilla de `TemplateResponse`, con `name=` o posicionales, y 2
comentarios en `app/routers/ui_purchases.py:59,327`), los 18 `{% include %}` entre plantillas (incluidos
`dashboard.html:45` y `restock_print.html:22`), las 9 líneas de tests y los comentarios de
`app/static/js/components/{home-charts,widget-chart}.js`.

```gherkin
Scenario: el HTML es idéntico
  GIVEN la baseline "tests/snapshots/baseline" de bf38a2b (91 capturas)
  WHEN corre "python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline"
  THEN termina con código 0 sin regenerar la baseline

Scenario: los estilos calculados son idénticos
  GIVEN una baseline de "tests/snapshots/style_snapshot.py" escrita desde bf38a2b
  WHEN corre su "check" sobre el código de la fase 5a
  THEN reporta 0 diferencias
```

### REQ-3: Se borran las dos plantillas muertas
**Priority:** Must Have
WHEN termina la fase, THE SYSTEM SHALL no contener `partials/action_result.html` (ningún router ni plantilla la
renderiza) ni `partials/products_history.html` (sin consumidor desde 0.4.13). Los comentarios que las citan
(`tests/e2e/test_ui_tour_csp_e2e.py:90`) dejan de nombrarlas.

```gherkin
Scenario: no hay rastro de las plantillas muertas
  WHEN se busca "action_result" o "products_history" en app/ y tests/
  THEN no hay coincidencias
```

### REQ-4: Una guarda impide volver a `partials/` y detecta rutas rotas
**Priority:** Must Have
WHEN corre la suite unitaria, THE SYSTEM SHALL fallar si (a) existe `app/templates/partials/`, (b) aparece la
cadena `partials/` en `app/**/*.py`, `app/templates/**/*.html`, `app/static/js/**/*.js` o `tests/**/*.py` (salvo en
el propio fichero de la guarda), o (c) algún nombre de plantilla literal de esos ficheros no se puede cargar con
`templates.env.get_template`.

```gherkin
Scenario: una ruta mal escrita hace fallar la guarda
  GIVEN un router con name="features/sales/sale_pannel.html"
  WHEN corre "pytest tests/unit/test_templates_layout.py"
  THEN falla y lista "features/sales/sale_pannel.html" con su fichero y línea

Scenario: recrear partials/ hace fallar la guarda
  GIVEN se crea "app/templates/partials/x.html"
  WHEN corre la guarda
  THEN falla
```

### REQ-5: El test de plantillas antiguas de Utilidades sigue comprobando algo
**Priority:** Must Have
WHEN corre `tests/unit/test_ui_components.py::test_old_profit_items_templates_are_gone`, THE SYSTEM SHALL comprobar
que no existen `features/profit_items/profit_items_<kind>_edit_form.html` (la ruta donde reaparecerían), en vez de
la ruta `partials/`, que tras la fase ya no existe y haría pasar el test sin comprobar nada.

### REQ-6: La documentación refleja la estructura nueva
**Priority:** Should Have
WHEN termina la fase, THE SYSTEM SHALL tener actualizados: el árbol de `CLAUDE.md` (raíz) y de `.claude/CLAUDE.md`
(`templates/` = `layouts/`, `components/`, `features/<dominio>/`), la fila "5" del diagnóstico
(`docs/auditorias/2026-10-23-frontend-arquitectura-diagnostico.md` §4) con "5a ✅", y la entrada de 0.5.0 del
`CHANGELOG.md`.

### REQ-7: Renombrar las plantillas (quitar el prefijo de dominio)
**Priority:** Won't Have (esta iteración). Decisión del usuario, 2026-11-02: mismo nombre, solo cambia la carpeta.

## 2. Requisitos no funcionales

| Categoría | Respuesta |
|---|---|
| **Performance** | No aplica: Jinja2 resuelve igual una ruta que otra; la caché de plantillas no cambia. |
| **Security** | Sin fronteras nuevas. `test_routers_no_html.py` ya prohíbe `\|safe` y `Markup(` en `features/**`; las 54 plantillas que entran ahí no usan ninguno de los dos (verificado en `bf38a2b`), así que la guarda pasa a cubrirlas sin cambios. |
| **Scalability** | No aplica. |
| **Observability** | No aplica: sin logs nuevos. |
| **Reliability** | La red es el arnés (91 capturas, indexadas por ruta HTTP, no por nombre de plantilla) más la guarda de REQ-4, que cubre también las plantillas que el arnés no captura (modales de error, impresiones). |

## 3. Fuera de alcance

- Cualquier cambio de consultas, `UIContext` o `app/security.py` (es la 5b).
- Renombrar ficheros (REQ-7), cambiar su contenido más allá de las rutas de `{% include %}`.
- Mover `layouts/`, `components/`, `base.html`, `dashboard.html`, `login.html` ni las 4 plantillas de impresión de
  la raíz (`label_single_print.html`, `labels_print.html`, `restock_print.html`, `transfer_print.html`).
- Borrar las rutas de backend sin enlace desde la UI (`/ui/product/{sku}/edit|update|delete`,
  `/ui/product/from-invoice`): siguen con su plantilla (`product_edit_form.html` → `features/products/`).
- Dividir los routers de más de 900 líneas (diagnóstico §3).

## 4. Dependencias

- Ninguna externa. Parte de `develop` `bf38a2b` (fase 4 mergeada, árbol limpio).
- El 5b depende de este: sus envelopes citan las rutas nuevas.

## 5. Preguntas abiertas

Ninguna. Las cuatro decisiones de partición, nombres, `security.py` y alcance de BD se tomaron con el usuario el
2026-11-02 antes de escribir este fichero.

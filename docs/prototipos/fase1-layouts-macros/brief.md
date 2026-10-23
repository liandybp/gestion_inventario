# Brief de prototipo — gestion_inventario · Fase 1 (layouts + macros de componentes)

> Eres `cc_local`. Construye y verifica un **prototipo desechable**. No diseñas el PAE ni decides arquitectura:
> si algo no cuadra con este brief, **anótalo en el informe** en lugar de cambiar el enfoque.
> Contexto: `docs/auditorias/2026-10-23-frontend-arquitectura-diagnostico.md` (§2 y §4, fase 1).

## Directorio y reglas

- Trabaja **solo** en `/tmp/proto-fase1/` (repo clonado en `/tmp/proto-fase1/repo`, rama `feature/frontend-modular`).
- Prohibido: `git push`, `sudo`, `systemctl`, instalar nada fuera del directorio, tocar `~/PyCharmMiscProject/`.
- Usa el venv del repo real **solo para ejecutar** (`~/PyCharmMiscProject/gestion_inventario/.venv/bin/python`); no instales paquetes.
- Invariantes del proyecto (de `.claude/CLAUDE.md`): no tocar `docker-compose*.yml`, `.env`, `requirements*.txt`,
  `app/auth.py`, `app/db.py`, `app/deps.py`, `app/migrations.py`. **No cambies la versión** (`VERSION`,
  `.bumpversion.cfg`, `CHANGELOG.md`): fases 0-5 son un único release que se versiona al final.
- Tests unitarios: `SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: <python> -m pytest -o addopts="" -q -p no:warnings tests/unit`
- Tests e2e: `<python> -m pytest tests/e2e -m e2e --no-cov -o addopts="" -q -p no:warnings`
- Haz commits pequeños en el clon (uno por paso de "Qué construir"); mensajes en inglés.

## Objetivo

Refactor **sin cambio de comportamiento** de las plantillas Jinja2: introducir herencia de layouts y macros
reutilizables para modal y campo de formulario, y migrar los sitios que hoy copian ese markup. El HTML
renderizado debe ser **equivalente** antes y después (salvo diferencias listadas y justificadas en el informe).

**Alcance:** `base.html` + `layouts/print.html`; macros `modal` y `field`; migrar los 18 modales y las filas
de formulario simples; unificar los 4 `profit_items_*_edit_form.html`; borrar `labels_print.html` (vacío);
arnés de snapshots del DOM.
**Fuera:** CSS inline / colores (fase 3), sacar JS de plantillas o `onclick=` (fase 2 — el `<script>` inline de
`dashboard.html` se queda tal cual, solo se mueve de sitio si hace falta para el `extends`), HTML construido en
Python (fase 4), mover ficheros a `features/` (fase 5), cambios en routers salvo los imprescindibles para H8.

## Hipótesis a verificar

Cada una con veredicto + comando exacto + extracto literal de la salida en el informe.

| # | Hipótesis | Cómo verificarla |
|---|---|---|
| H1 | La versión de Jinja2 del venv soporta macros con `caller()` y con argumentos extra vía `kwargs` (para pasar atributos arbitrarios: `step`, `min`, `list`, `data-*`, `hx-*`) | `pip show jinja2` + script mínimo que renderice una macro con `**kwargs` y `caller` |
| H2 | Los 18 ficheros con `modal-overlay` siguen el mismo esqueleto (`overlay#id > modal-container[ modal-container-wide] > modal-content > modal-header(h3.modal-title \| h2 + button.modal-close[data-modal-close=id])`) | Tabla por fichero: id, wide sí/no, tipo de título, `aria-label` del botón, atributos extra del overlay (p. ej. `data-modal-static`), y **cualquier excepción** (`must_change_password_form`, `tab_profit_items`, `transfer_bulk_modal`, `user_reset_password_form`) |
| H3 | El comportamiento JS de los modales depende solo de `.modal-overlay`, `[data-modal-close]`, `data-modal-static`, el `id` del overlay y la cabecera `X-Modal-Keep` (`app.js:288-310` y `:703-725`) — una macro que preserve eso no cambia el comportamiento | `grep -n` en `app/static/js/app.js` y en `app/templates` de cada selector/atributo de modal |
| H4 | Los e2e solo dependen de ids de modal (`#sale-edit-modal`, `#transfer-bulk-modal`…) y de clases internas que la macro conserva | `grep -rn` de selectores en `tests/e2e/` con la lista completa |
| H5 | De las 86 filas `<div class="row"><label>X</label><input …/></div>`, una mayoría son "simples" (1 label + 1 input/textarea) y el resto (select, datalist, checkbox, varios controles, texto de ayuda) cabe en `{% call row(label) %}` | Script que clasifique las 86 por tipo; tabla con recuento y ejemplos de cada grupo |
| H6 | `login.html` y las 3 plantillas de impresión pueden extender un base/layout con bloques (`title`, `head_extra`, `body`) sin cambiar su `<head>` renderizado (login NO carga `app.css` ni htmx hoy; `label_single_print` carga JsBarcode) | Snapshot antes/después del `<head>` de cada una |
| H7 | `app/templates/labels_print.html` (0 bytes) no se referencia desde ningún sitio | `grep -rn "labels_print" app tests` |
| H8 | Los 4 `profit_items_{purchase,sale,adjustment,transfer_in}_edit_form.html` difieren solo en: segmento de URL, título, qué campos son editables/readonly/disabled y si hay datalist de producto — y pueden ser 1 plantilla con un dict de configuración pasado desde los 4 handlers de `routers/ui_profit_items.py` (líneas ~134, 205, 287, 477) | `diff` entre los 4 ficheros + lectura de los 4 handlers |
| H9 | El arnés de snapshots es determinista: dos ejecuciones seguidas sobre el mismo código producen diff vacío | Ejecutarlo 2 veces sobre la base y `diff -r` |

## Qué construir

1. **Arnés de snapshots del DOM** (`tests/snapshots/ui_snapshot.py`, ejecutable aparte, NO parte de la suite):
   reutiliza la infraestructura de `tests/e2e/conftest.py` (`live_server`, datos sembrados, login) con
   Playwright; visita **cada pestaña** del sidebar como admin (y Ventas como operador) y **abre cada modal de
   edición** alcanzable con los datos sembrados (siembra lo que falte: un gasto, un retiro, un cliente, una
   venta, una compra, un traspaso, un documento, un usuario, un negocio). Vuelca el HTML de `#tab-content` y
   `#modal-container`/modal abierto, y la página completa de login y de cada plantilla de impresión, a
   `<dir>/<nombre>.html` **normalizado** (espacios entre etiquetas colapsados, atributos ordenados, `?v=<hash>`
   y fechas/horas variables sustituidos por marcadores). Uso: `python tests/snapshots/ui_snapshot.py <dir_salida>`.
   Ejecútalo sobre la base **antes** de tocar plantillas → `/tmp/proto-fase1/snap-base/`.
2. `templates/base.html` (doctype, `<html lang="es">`, meta charset/viewport, bloques `title`, `head`, `body`)
   y `templates/layouts/print.html` (extiende base; bloque para el `<style>` propio de cada impresión).
   `dashboard.html`, `login.html`, `transfer_print.html`, `restock_print.html`, `label_single_print.html` pasan a
   `{% extends %}`.
3. `templates/components/modal.html`: `{% macro modal(id, title, wide=False, heading='h3', static=False, close_label='Cerrar') %}`
   con `caller()` para el cuerpo. Migrar los 18 modales (las excepciones de H2 se migran si encajan con un
   parámetro más; si no, se dejan y se explica por qué).
4. `templates/components/forms.html`: `{% macro field(label, name, type='text', value='', required=False, **attrs) %}`
   (o el equivalente que permita Jinja según H1) para filas simples; mover aquí `row` desde `components/ui.html`
   **manteniendo** el import antiguo funcionando (re-export o dejar `row` en ambos sitios — documenta cuál).
   Migrar las filas "simples" de H5 y las complejas con `{% call row(...) %}` donde sea limpio.
5. Unificar los 4 `profit_items_*_edit_form.html` en `partials/profit_items_edit_form.html` según H8.
6. Borrar `labels_print.html` si H7 se confirma.
7. Ejecutar el arnés después → `/tmp/proto-fase1/snap-fase1/` y `diff -r snap-base snap-fase1`.

## Criterios de "hecho"

- [ ] H1-H9 con veredicto y evidencia literal
- [ ] Unit (esperado 387) y e2e (esperado 44) en verde, con los números exactos
- [ ] `diff -r snap-base snap-fase1` vacío, o cada diferencia listada en el informe con su motivo
      (p. ej. "aria-label añadido donde faltaba" → NO lo hagas: cualquier mejora de markup queda fuera)
- [ ] Recuento final: ficheros con esqueleto de modal copiado (objetivo 0 salvo excepciones justificadas),
      filas `<div class="row"><label>` manuales restantes, `{% extends %}` en uso
- [ ] `informe.md` en `/tmp/proto-fase1/` según `~/obsidian-lab/Templates/template-informe-prototipo.md`,
      con `git -C repo log --oneline` y `git -C repo diff --stat feature/frontend-modular`
- [ ] Ningún fichero fuera de `/tmp/proto-fase1/`

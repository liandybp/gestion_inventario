# Re-auditoría — Frontend fase 3: CSS sin estilos inline y CSP `style-src 'self'` (PAE-018)

- **Fecha:** 2026-10-24
- **Alcance:** diff `b0f5dd8..HEAD` en `feature/frontend-modular`: el de la auditoría anterior (con `01d2c24`
  rehecho con `--amend` como `51665b6`) más el commit que cierra H-004, R-1 y los reportes de CSP.
- **Score:** **4.90 / 5.00 🟢** (antes 4.45 🟢, condicionado a H-002). Sin hallazgos abiertos.
- **Informe anterior:** `docs/auditorias/2026-10-24-frontend-fase3-css-csp.md`

## Contexto y método

Se verifican los cuatro hallazgos del informe anterior, el riesgo R-1 y el endpoint de reportes de CSP contra el código y los resultados de ejecución actuales.

- **codebase-memory:**
  - Reindexé el repo tras el `--amend`.
  - Con `search_code` (regex `attributesToSettle` sobre `app/` y `tests/`) saqué la lista completa de sitios: la
    configuración vive solo en `layouts/app_shell.html`. La protegen
    `test_frontend_css.py::test_htmx_config_does_not_inject_or_copy_styles` y
    `test_css_csp_e2e.py::test_reopening_home_does_not_settle_style_attributes`; la otra coincidencia es el propio
    `htmx.min.js`.
- **Serena:** solo hay servidor de lenguaje para Python; nada nuevo que revisar con él en este diff.
- **Lectura directa (fallback):** plantillas, CSS y la allowlist.
- **Ejecutado:**
  - `style_snapshot.py check`, con tres baselines generadas por separado desde `b0f5dd8`:
    - 1.ª: OK, 31 diferencias admitidas.
    - 2.ª: OK, 38 diferencias admitidas.
    - 3.ª, con la regla de R-1 y el CSS reorganizado de H-004: OK, 20 diferencias, solo las intencionadas.
  - Unit: 465 passed.
  - e2e completo: 82 passed.
  - `ui_snapshot.py check`: OK, 40 equivalentes.
  - `grep` de `style=` en `app/templates`: 0 resultados.

## Estado de los hallazgos

| ID | Severidad | Estado | Evidencia |
|----|-----------|--------|-----------|
| H-001 | Medio | ✅ Cerrado | `layouts/app_shell.html:8`: `"attributesToSettle": ["class"]`. El unit test fija el meta. El e2e de reabrir Inicio comprueba que `canvas.width/height` = `clientWidth/clientHeight`. **Límite:** a 60 Hz la carrera no se reproducía ni antes del arreglo, así que ese e2e es un smoke check; la guarda real es el unit test. |
| H-002 | Medio | ✅ Cerrado | `tests/snapshots/style_allowlist.txt` admite, con comentario, el `transform` de `svg.barcode` (14 claves) y el `display`/`height` de los tres canvas de Inicio y las alturas de sus contenedores, en los dos temas; las entradas de los canvas las sustituyó después la regla de R-1. El check pasa con tres baselines distintas de `b0f5dd8`, la prueba de que aguanta la falta de determinismo de la base. |
| H-003 | Medio | ✅ Cerrado | `51665b6` sustituye a `01d2c24`. El mensaje describe las dos verificaciones reales de T39, reconoce que la versión anterior las daba por buenas sin haberlas hecho, da las cifras medidas e incluye `Co-Authored-By`. |
| H-004 | Bajo | ✅ Cerrado | Los tokens (`:root` y `[data-theme="dark"]`) viven solo en `tokens.css`. Las 14 utilidades compartidas viven solo en `utilities.css`, que se carga después de la hoja de cada página. `print.css` y `label-print.css` no tienen hex; `login.css`, solo el fondo propio de `.error`. Lo protege `test_frontend_css.py::test_tokens_and_shared_utilities_live_in_one_sheet`. El check de estilos con las 74 capturas sale idéntico (20 diferencias, todas intencionadas): mover las utilidades no alteró la cascada. |
| R-1 | Bajo | ✅ Cerrado | `style_snapshot.py` ignora el alto y el `display` de los `<canvas>` y el alto de lo que los contiene (los fija Chart.js). La allowlist vuelve a tener solo cambios intencionados: badge (6) y códigos de barras (14). |
| Reportes CSP | Bajo | ✅ Cerrado | `report-uri /csp-report` → `app/routers/csp_report.py`, que registra un `WARNING csp_violation` con los campos truncados. Acepta el formato clásico y la Reporting API, rechaza cuerpos de más de 16 KB con un 413, tiene un tope de 60 por minuto y no registra el `script-sample`. 11 unit tests, validados con 3 mutantes, y un e2e en el que Chromium envía de verdad el reporte y aparece en el log del servidor. |

## Prueba de punta a punta con datos reales

- **Montaje:**
  - Copia desechable de la BD de dev (`inventario_fase3_e2e`: 681 productos, 3 negocios), borrada al terminar.
  - La app de la rama corriendo contra esa copia.
- **Recorrido con Playwright**, en los dos temas y los 3 negocios:
  - las 17 pestañas y los modales disponibles;
  - Inicio reabierto y filtrado (el re-swap de H-001);
  - cambio de tema sin recarga con el copiloto abierto;
  - las tres impresiones.
- **Resultado:** 108 comprobaciones.
  - 0 errores de consola o de página.
  - 0 respuestas HTTP con error.
  - 0 `csp_violation` o ERROR en el log del servidor.
  - Todos los gráficos con el bitmap igual a su caja.
  - Capturas revisadas a ojo: Inicio, etiqueta, traspaso y login.

## Scorecard ponderado

| # | Dimensión | Peso | Antes | Ahora | Ponderado | Motivo |
|---|-----------|------|-------|-------|-----------|--------|
| 1 | SoC / SRP | 10 | 5 | 5 | 0.50 | — |
| 2 | Cohesión / Acoplamiento | 10 | 4 | 5 | 0.50 | H-004 cerrado |
| 3 | Capas / Límites | 10 | 5 | 5 | 0.50 | — |
| 4 | Seguridad | 15 | 5 | 5 | 0.75 | — |
| 5 | Robustez | 15 | 4 | 5 | 0.75 | H-001 cerrado |
| 6 | Consistencia transaccional | 10 | 5 | 5 | 0.50 | N/A |
| 7 | Observabilidad | 10 | 4 | 5 | 0.50 | Las violaciones de CSP en producción llegan al log |
| 8 | Configuración | 5 | 5 | 5 | 0.25 | — |
| 9 | Pruebas | 10 | 3 | 4 | 0.40 | H-002, H-003 y R-1 cerrados. La red de estilos sigue fuera de CI por diseño: tarda unos 2 minutos y necesita el commit base |
| 10 | Escalabilidad | 5 | 5 | 5 | 0.25 | — |
| | **Total** | 100 | 4.45 | | **4.90 🟢** | |

## Pendiente

- **De código:** nada.
- **De proceso:** seguir el flujo del proyecto (`feature/frontend-modular` → `develop` → prueba en dev → `main`),
  con el push y el merge confirmados por el usuario.
- **Fuera de CI por diseño:** la red de estilos calculados, porque tarda unos 2 minutos y necesita el commit base.

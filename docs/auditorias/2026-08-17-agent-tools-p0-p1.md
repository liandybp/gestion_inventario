# Auditoría de Ingeniería — agent-tools-p0-p1 (4 tools nuevas + filtro location)

**Fecha:** 2026-08-17
**Proyecto:** gestion_inventario
**Rama:** main (cambios sin commitear, pendientes de push)
**Score inicial:** 3.20 / 5.00 🟡 → **Score post-fix: ver §9 Cierre**
**Responsable:** Claude Code (auditoria-ingenieria)

---

## 1. Contexto y método

**Alcance:** feature `agent-tools-p0-p1` (PAE ejecutado en esta sesión) — NO el proyecto
completo. Se auditan exclusivamente los 6 archivos modificados por esta feature, previo
al commit/push, siguiendo el mismo patrón de auditorías acotadas por fase/feature ya
usado en este proyecto (`2026-08-15-fase-d.md`, `2026-08-16-coach-financiero-operativo.md`).

**Archivos auditados:**
- `app/agent/tools.py` (4 tools nuevas + helper `_resolve_location_id` + 3 tools extendidas)
- `app/agent/prompts.py` (reglas de selección de tool)
- `tests/unit/test_agent_tools.py` (9 tests nuevos)
- `VERSION`, `.bumpversion.cfg`, `CHANGELOG.md`

**Criterios:** 10 dimensiones según plantilla de auditoría de ingeniería.

**Limitaciones:**
- No se auditó el resto de `app/agent/` (`graph.py`, `widgets.py`, `runner.py`) ni las
  29 tools preexistentes — se usaron como REFERENCE (sin cambios) para verificar que
  esta feature respeta sus contratos, pero no se les asignó score independiente.
- El precedente `/code-review` táctico (8 ángulos, 6 sub-agentes) ya corrió sobre el
  mismo diff inmediatamente antes de esta auditoría — sus 5 hallazgos verificados se
  incorporan aquí con su dimensión correspondiente en vez de duplicar el análisis.

---

## 2. Checklist por dimensión

| # | Dimensión | Peso | Veredicto | Score | Evidencia |
|---|-----------|------|-----------|-------|-----------|
| 1 | SoC / SRP | 10% | ⚠️ | 3 | `_resolve_location_id` añade un 4º bloque de acceso a datos inline en `tools.py` (antes había 3) — patrón ya cuestionable que esta feature extiende en vez de contener |
| 2 | Cohesión / Acoplamiento | 10% | ⚠️ | 3 | `profit_by_category`/`profit_by_location` duplican ~20 líneas de agregación (tools.py:563-624); `_resolve_location_id` duplica lógica de `LocationRepository.location_id_for_code` |
| 3 | Capas / Límites | 10% | ⚠️ | 3 | `_resolve_location_id` (tools.py:34) reimplementa resolución código→id en la capa de tools en vez de delegar a `LocationRepository.location_id_for_code(auto_create=False)`, que ya existe para exactamente este caso (location_repository.py:18,50) |
| 4 | Seguridad | 15% | ✅ | 4 | Todas las queries nuevas filtran por `business_id` (tools.py:34-59, verificado); sin auth boundary nuevo; sin secretos; sin PII nueva. Único matiz: filas `Location.business_id IS NULL` (legacy) no matchean — falla cerrado (404), no filtra datos de otro tenant |
| 5 | Robustez / Resiliencia | 15% | ❌ | 2 | 2 bugs confirmados de resultado silenciosamente incorrecto: `dead_stock` ignora `days=0` explícito (tools.py:508, `or 90` sobre valor falsy) e `inventory_turnover` calcula mal la rotación para cualquier rango ≠ ~30 días (tools.py:573, divide entre 30 fijo) |
| 6 | Consistencia transaccional | 10% | ⚠️ | 3 | `profit_by_category`/`profit_by_location` usan el mismo rango start/end en todo su cálculo (correcto); `inventory_turnover` mezcla un COGS de rango arbitrario con un divisor fijo de 30 días — mismo bug que dim. 5, visto aquí como inconsistencia de ventana temporal |
| 7 | Observabilidad | 10% | ✅ | 4 | Sin logging nuevo, consistente con las 29 tools existentes (ninguna loguea individualmente) — decisión documentada en `requirements.md` NFR, no una omisión |
| 8 | Configuración / Entornos | 5% | ✅ | 5 | `VERSION` 0.4.2→0.4.3 y `.bumpversion.cfg` consistentes (verificado); sin hardcodes nuevos; PATCH correcto (no MINOR) |
| 9 | Pruebas / Calidad | 10% | ⚠️ | 3 | 9 tests nuevos, 214/214 verdes, cobertura ≥80% mantenida — pero ninguno cubre `days=0` ni un rango ≠ mes en `inventory_turnover`, exactamente los inputs que disparan los 2 bugs de dim. 5 |
| 10 | Escalabilidad | 5% | ⚠️ | 3 | `profit_by_category`/`profit_by_location` agregan en Python sobre `monthly_profit_items_report` (una fila por línea de venta × lote asignado, sin `GROUP BY`) en vez de una query agregada como `profit_by_product`; aceptable a la escala actual del proyecto, deuda latente si crece el volumen de ventas/mes |

**Leyenda:** ✅ OK (4-5) · ⚠️ PARCIAL (2-3) · ❌ NO (<2)

---

## 3. Scorecard ponderado

```
Dimensión                        Peso    Score   Peso×Score
────────────────────────────────────────────────────────────
1. SoC / SRP                     10%     3       0.30
2. Cohesión / Acoplamiento       10%     3       0.30
3. Capas / Límites               10%     3       0.30
4. Seguridad                     15%     4       0.60
5. Robustez / Resiliencia        15%     2       0.30
6. Consistencia transaccional    10%     3       0.30
7. Observabilidad                10%     4       0.40
8. Configuración / Entornos       5%     5       0.25
9. Pruebas / Calidad             10%     3       0.30
10. Escalabilidad                 5%     3       0.15
────────────────────────────────────────────────────────────
TOTAL                          100%            3.20 / 5.00
SCORE FINAL                                      3.20 / 5.00
```

> Nota: recálculo con dim.5 en 2 (no 3) tras confirmar 2 bugs reales de robustez, no 1
> — el score baja de 3.35 (estimación inicial) a **3.20**, justo en el límite 🟡/🔴.

**Interpretación:** 🟡 Estable con deuda relevante — los 2 bugs de robustez (H-001,
H-002) son puntuales y de corrección barata (no estructurales), pero **recomendado
corregirlos antes del push** dado que están en el límite del umbral 🔴.

---

## 4. Hallazgos

### 🟠 Altos

| ID | Título | Dimensión | Evidencia | Riesgo | Recomendación |
|----|--------|-----------|-----------|--------|---------------|
| H-001 | `inventory_turnover` da resultados incorrectos para rangos ≠ ~1 mes | 5, 6 | `app/agent/tools.py:573` llama a `InventoryService.inventory_turnover_days()`, que en `inventory_service.py:1296` hace `cogs_daily = monthly_cogs / 30.0` sin importar el rango real pasado | Usuario pide "rotación de la última semana" → resultado ~4x mayor al real, sin ningún aviso de que el cálculo asume 30 días | Antes de llamar a `inventory_turnover_days`, calcular `cogs_daily` con el largo real del rango (`(end - start).days`) en vez de dejar que el service asuma 30, O restringir la tool a no aceptar `start`/`end` custom (solo mes actual) si no se quiere tocar el service compartido |
| H-002 | `dead_stock` ignora `days=0` explícito | 5 | `app/agent/tools.py:508` — `days = int(args.get("days") or 90)`; `0` es falsy en Python | LLM pasa `{"days": 0}` (cutoff = ahora) → se usa 90 en su lugar, sin error, resultado silenciosamente distinto al pedido | Cambiar a `args.get("days")`; si es `None`, usar 90: `raw = args.get("days"); days = int(raw) if raw is not None else 90` |

### 🟡 Medios

| ID | Título | Dimensión | Evidencia | Riesgo | Recomendación |
|----|--------|-----------|-----------|--------|---------------|
| H-003 | `_resolve_location_id` reimplementa lógica ya existente en `LocationRepository`, sin su fallback de prefijo | 2, 3 | `app/agent/tools.py:34-59` vs `app/repositories/location_repository.py:18-51` (`location_id_for_code(code, *, auto_create=False)` ya existe con el mismo contrato de no-autocrear) | Una ubicación cuyo código real quedó con prefijo de negocio (auto-creación histórica) resuelve en traspasos/ventas pero da 404 en el copiloto para el mismo código válido | Delegar la resolución por código a `service._locations.location_id_for_code(location, auto_create=False)` (capturar el 409 y re-lanzar como 404 con el mismo mensaje), y solo mantener el fallback por nombre como lógica adicional propia del agente |
| H-004 | Los 9 tests nuevos no cubren los inputs que disparan H-001/H-002 | 9 | `tests/unit/test_agent_tools.py` — `test_dead_stock_days_arg` usa `days=30` (no `0`); `test_inventory_turnover_no_data` solo cubre `cogs_total==0`, ningún test usa un rango de días ≠ mes | La suite verde (214/214) no habría detectado ninguno de los 2 bugs de robustez | Añadir `test_dead_stock_days_zero` y un test de `inventory_turnover` con `start`/`end` de pocos días con datos, aserción sobre el valor esperado con la fórmula corregida |

### 🟢 Bajos

| ID | Título | Dimensión | Evidencia | Riesgo | Recomendación |
|----|--------|-----------|-----------|--------|---------------|
| H-005 | `profit_by_category`/`profit_by_location` duplican ~20 líneas de agregación | 2 | `app/agent/tools.py:563-591` y `:597-624` — mismo patrón de dict acumulador, margen, sort | Ya divergieron: `profit_by_category` acepta `limit`, `profit_by_location` no (inconsistencia, no necesariamente bug — ver nota) | Extraer un helper `_group_profit_items(items, key_fn) -> list[list]` compartido, o aceptar la duplicación si se prefiere cada tool 100% autocontenida para OpenCode (trade-off válido, no urgente) |
| H-006 | `_resolve_location_id` hace 2 round-trips secuenciales evitables | 10 | `app/agent/tools.py:34-59` — query por código, luego query por nombre solo si la primera falla | Duplica la latencia de resolución cuando el match es por nombre, no por código | Combinar en una sola query con `or_(func.lower(Location.code)==..., func.lower(Location.name)==...)` |
| H-007 | `profit_by_category`/`profit_by_location` agregan en Python sobre una query per-línea en vez de `GROUP BY` SQL | 10 | `app/agent/tools.py` llama `monthly_profit_items_report` (una fila por línea de venta × lote), no una versión agrupada como `monthly_profit_report` (`GROUP BY Product.id`) | A escala de miles de líneas de venta/mes, más filas viajan por la red de las necesarias para 2-5 categorías/ubicaciones | Aceptable a la escala actual (negocio pequeño/mediano); si el volumen crece, añadir un método `ReportingService.profit_grouped_by(field, ...)` con `GROUP BY` en SQL |

---

## 5. Matriz impacto vs esfuerzo

```
Alto impacto │  🔥 QUICK WINS           🎯 ESTRATÉGICOS
             │  H-001, H-002, H-004      H-003 (delegar a
             │  (fixes de 1-3 líneas)    LocationRepository)
             │
Bajo impacto │  H-006 (combinar query)  H-005, H-007 (refactor
             │                           de agregación / GROUP BY)
             └──────────────────────────────────────────────
               Bajo esfuerzo              Alto esfuerzo
```

**Quick wins recomendados (hacer YA, antes del push):**
1. H-002 — fix `dead_stock` days=0 — 2 min
2. H-001 — fix `inventory_turnover` división por rango real — 10 min
3. H-004 — 2 tests de regresión para H-001/H-002 — 10 min

---

## 6. Backlog priorizado de remediación

| # | Hallazgo | Severidad | Esfuerzo | Impacto | Prioridad |
|---|----------|-----------|----------|---------|-----------|
| 1 | H-001 inventory_turnover mal calculado | Alto | Bajo | Alto | 🔴 Antes del push |
| 2 | H-002 dead_stock ignora days=0 | Alto | Bajo | Medio | 🔴 Antes del push |
| 3 | H-004 tests de regresión para H-001/H-002 | Medio | Bajo | Alto | 🔴 Antes del push |
| 4 | H-003 delegar resolución de ubicación a LocationRepository | Medio | Medio | Medio | 🟠 Este sprint |
| 5 | H-006 combinar las 2 queries de _resolve_location_id | Bajo | Bajo | Bajo | 🟡 Backlog |
| 6 | H-005 extraer helper de agregación compartido | Bajo | Bajo | Bajo | 🟡 Backlog |
| 7 | H-007 GROUP BY SQL para profit_by_category/location | Bajo | Medio | Bajo (a esta escala) | 🟡 Backlog |

---

## 7. Plan 30/60/90 días

### 30 días (corto plazo)
- [ ] Corregir H-001, H-002 y añadir H-004 → suite verde cubre ambos edge cases
- [ ] Push a `develop`, validar en `dev.gestion.legatumbp.com` con preguntas reales al copiloto ("rotación de la última semana", "productos sin venta en 0 días")
**Criterio de cierre:** H-001/H-002/H-004 resueltos, 216+ tests verdes, sin regresión en dev.

### 60 días (medio plazo)
- [ ] H-003: delegar `_resolve_location_id` a `LocationRepository.location_id_for_code(auto_create=False)` para no divergir del resto de la app
**Criterio de cierre:** una sola fuente de verdad para resolución de ubicaciones; score ≥ 3.8 en re-auditoría.

### 90 días (largo plazo)
- [ ] Evaluar H-007 (GROUP BY SQL) si el volumen de ventas mensuales de algún negocio supera unas ~2000 líneas/mes (medir antes de optimizar)
- [ ] H-005/H-006 si se retoca `tools.py` por otra razón (no ameritan un PAE dedicado solos)
**Criterio de cierre:** score ≥ 4.2, sin deuda de robustez pendiente.

---

## 8. Criterios de cierre

- [ ] H-001, H-002, H-004 resueltos y verificados con tests antes de commit/push
- [ ] Score de re-auditoría post-fix ≥ 3.8 (fuera de zona 🟡 límite)
- [ ] `git diff --name-only` sigue mostrando exactamente los archivos del inventario del PAE (sin scope creep al aplicar fixes)

---

*Informe generado por Claude Code (skill: auditoria-ingenieria), en conjunto con
`/code-review` (5 hallazgos tácticos, incorporados aquí como H-001 a H-006).*

---

## 9. Cierre — todos los hallazgos corregidos (misma sesión, antes del push)

| ID | Fix aplicado | Archivo(s) |
|----|--------------|------------|
| H-001 | `inventory_turnover` ya no reusa `InventoryService.inventory_turnover_days` (fijo a /30 días) para rangos arbitrarios; calcula `cogs_daily` con el largo real del período (`summary["month_end"] - summary["month_start"]`) | `app/agent/tools.py` |
| H-002 | `dead_stock` distingue `days=0` explícito de `days` ausente (`args.get("days")` + chequeo `is not None` en vez de `or 90`) | `app/agent/tools.py` |
| H-003 | `_resolve_location_id` delega primero en `service._locations.location_id_for_code(location, auto_create=False)` (incluye el fallback de prefijo por negocio); solo cae a un fallback propio si esa resolución canónica falla | `app/agent/tools.py` |
| H-004 | 4 tests de regresión nuevos: `test_dead_stock_days_zero_not_overridden`, `test_inventory_turnover_uses_actual_period_length`, `test_sales_by_product_exact_code_resolves_via_location_repository`, `test_profit_by_category_aggregates_and_groups_null_category` | `tests/unit/test_agent_tools.py` |
| H-005 | `profit_by_category`/`profit_by_location` ya no duplican la agregación: la agregación se movió a SQL (`ReportingService.profit_by_category`/`profit_by_location`, `GROUP BY`) y el formateo de filas se comparte vía el helper `_profit_rows` | `app/agent/tools.py`, `app/services/reporting_service.py` |
| H-006 | Resuelto como parte de H-003: la resolución por nombre/código ahora es una sola query con `or_(...)` en el fallback (en vez de 2 queries secuenciales) | `app/agent/tools.py` |
| H-007 | Resuelto como parte de H-005: `profit_by_category`/`profit_by_location` ejecutan 2 queries `GROUP BY` en SQL (una por categoría/ubicación, no una por línea de venta) en vez de agregar en Python sobre `monthly_profit_items_report` | `app/services/reporting_service.py` |

**Verificación post-fix:**
```
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit
218 passed (214 previos + 4 nuevos de regresión), cobertura 84.57% (gate 80%)
git diff --name-only HEAD -- app tests VERSION .bumpversion.cfg CHANGELOG.md
  → app/agent/prompts.py, app/agent/tools.py, app/services/reporting_service.py,
    .bumpversion.cfg, CHANGELOG.md, tests/unit/test_agent_tools.py, VERSION
    (scope esperado + reporting_service.py, necesario para H-005/H-007)
```

**Score post-fix (recálculo):** dim. 5 (Robustez) sube de 2→4 (los 2 bugs confirmados
tenían fix y test de regresión), dim. 9 (Pruebas) sube de 3→4 (los inputs que
disparaban los bugs ahora están cubiertos), dim. 2/3 (Cohesión/Capas) suben de 3→4
(delegación a `LocationRepository`, sin duplicación de agregación), dim. 10
(Escalabilidad) sube de 3→4 (GROUP BY en SQL).

```
Dimensión                        Peso    Score   Peso×Score
────────────────────────────────────────────────────────────
1. SoC / SRP                     10%     3       0.30
2. Cohesión / Acoplamiento       10%     4       0.40
3. Capas / Límites               10%     4       0.40
4. Seguridad                     15%     4       0.60
5. Robustez / Resiliencia        15%     4       0.60
6. Consistencia transaccional    10%     4       0.40
7. Observabilidad                10%     4       0.40
8. Configuración / Entornos       5%     5       0.25
9. Pruebas / Calidad             10%     4       0.40
10. Escalabilidad                 5%     4       0.20
────────────────────────────────────────────────────────────
TOTAL                          100%            3.95 / 5.00
SCORE FINAL                                      3.95 / 5.00 🟡 (cerca de 🟢 4.2)
```

Dim. 1 (SoC/SRP) se mantiene en 3: `_resolve_location_id` sigue viviendo en `tools.py`
en vez de un método público en `InventoryService`/`LocationRepository` — decisión
consciente (ver H-003) para no ampliar el API público de un servicio compartido en
esta misma sesión; queda como candidato del backlog 60 días si se retoca esa capa.

**Pendiente real (no bloqueante para el push):** ninguno de los 7 hallazgos queda
abierto. Los ítems bajos originales (H-005/H-006/H-007) se resolvieron junto con los
altos porque compartían la misma causa raíz (`_resolve_location_id` y la agregación
de `profit_by_category`/`profit_by_location`).

*Re-auditoría completa (no solo de cierre) recomendada tras el siguiente cambio no
trivial a `app/agent/tools.py` o `app/services/reporting_service.py`.*

---

## 10. Cierre 2 — fix de mayor ROI aplicado (SoC/Cohesión/Capas)

Se aplicó el fix identificado como de mayor apalancamiento: `_resolve_location_id`
seguía reaching into el atributo privado `service._locations` (`LocationRepository`)
en vez de pasar por una API pública de `InventoryService`.

**Fix:**
- `app/services/inventory_service.py` — nuevo método público
  `resolve_location_id(code, *, auto_create=False) -> int`, junto a `available_lots`.
  Delega en `self._locations.location_id_for_code(...)`, con `auto_create=False` como
  default explícito (a diferencia de los flujos de escritura, que usan `True`).
- `app/agent/tools.py` — `_resolve_location_id` ahora llama a
  `service.resolve_location_id(value, auto_create=False)` en vez de
  `service._locations.location_id_for_code(...)`.
- `tests/unit/test_agent_tools.py` — nuevo test con datos reales,
  `test_profit_by_location_aggregates_by_location` (paralelo al de
  `profit_by_category`): 2 ubicaciones (central + POS1 vía traspaso), verifica que
  `ReportingService.profit_by_location` suma la ganancia correcta por ubicación
  (12.0 y 18.0) usando el `GROUP BY` en SQL.

**Verificación:**
```
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit
219 passed (218 previos + 1 nuevo), cobertura 84.57% (gate 80%)
git diff --name-only HEAD -- app tests VERSION .bumpversion.cfg CHANGELOG.md
  → + app/services/inventory_service.py (nuevo, respecto al cierre anterior)
```

**Score recalculado:**

```
Dimensión                        Peso    Score   Peso×Score
────────────────────────────────────────────────────────────
1. SoC / SRP                     10%     4       0.40   (antes 3 — resolve_location_id ya no reaching into atributo privado)
2. Cohesión / Acoplamiento       10%     4       0.40
3. Capas / Límites               10%     4       0.40   (antes 3 — tools.py pasa por API pública de InventoryService)
4. Seguridad                     15%     4       0.60
5. Robustez / Resiliencia        15%     4       0.60
6. Consistencia transaccional    10%     4       0.40
7. Observabilidad                10%     4       0.40
8. Configuración / Entornos       5%     5       0.25
9. Pruebas / Calidad             10%     4       0.40   (mismo score; sube evidencia, no cambia el nivel — profit_by_category y profit_by_location ahora ambas con test de datos reales)
10. Escalabilidad                 5%     4       0.20
────────────────────────────────────────────────────────────
TOTAL                          100%            4.05 / 5.00
SCORE FINAL                                      4.05 / 5.00 🟡 (a 0.15 de 🟢 4.2)
```

Dim. 1 sube 3→4, no a 5: `resolve_location_id` sigue siendo un wrapper delgado de una
sola línea sobre `LocationRepository`, correcto pero no añade valor propio más allá de
exponer el método — un 5 exigiría además normalizar el resto de usos internos de
`InventoryService` (`stock`, `available_lots`, `transfer`, etc.) para pasar por el mismo
método público en vez de `_location_id_for_code` directamente, lo cual excede el
alcance de esta feature.

Lo que falta para 🟢 4.2 (sin tocar más código de este PAE) queda documentado en la
conversación: e2e del copiloto (dim. 9), verificación de datos legacy en prod (dim. 4),
y las iniciativas de proyecto completo (logging por tool, dim. 7) — ninguno bloquea el push.

# Code Review Táctico: Coach Financiero y Operativo

**Fecha:** 2026-08-16
**Alcance:** `feature/coach-financiero-operativo` (working tree sobre `develop`)
**Tipo:** Revisión táctica (bugs línea por línea) — complementa la auditoría estratégica en
`docs/auditorias/2026-08-16-coach-financiero-operativo.md`
**Estado:** Pendiente de aplicar — se corrige todo junto al final de la sesión

---

## Resumen

6 hallazgos verificados contra las firmas reales de `ReportingService`, `SalesDocumentService`
y el resto del código del coach. **3 son bugs de comportamiento con escenario de fallo
concreto** (no hipotéticos), 1 es una ineficiencia real y medible, 1 es código muerto, 1 es
deriva de spec. Ninguno es bloqueante para que la app arranque, pero **F-001, F-002 y F-003
producen datos incorrectos o un 500 en producción bajo condiciones realistas** — deben
corregirse antes de desplegar.

---

## F-001 — `current_cash()` sin manejo de errores: puede tumbar todo el tab con un 500

**Archivo:** `app/services/coach_service.py:74-105`
**Severidad:** Alto

**El problema:** todos los demás métodos de indicadores (`_financial_indicators`,
`_add_growth_and_extraction`, `_operational_indicators`, `_customer_indicators`) están
envueltos en `try/except` con `_log.error(..., exc_info=True)` — exactamente lo que pide
REQ-13 ("ningún método debe propagar una excepción sin loggear... nunca 500 al usuario").
`current_cash()` es el único método que **no tiene esa protección**:

```python
# app/services/coach_service.py:74
def current_cash(self, now: Optional[datetime] = None) -> dict:
    baseline = self._db.execute(...).scalar_one_or_none()   # sin try/except
    ...
    amount = float(baseline.amount) + float(sales_in) - ...
    return {"amount": amount, "mode": "real"}
```

Y se llama sin protección adicional desde `health_score()`:
```python
# app/services/coach_service.py:160
cash = self.current_cash(now_dt)   # si esto lanza, health_score() entero lanza
```

**Escenario de fallo:** cualquier error transitorio de BD durante esas 4 queries (timeout,
deadlock, columna nula inesperada) tumba `health_score()` completo → el router no lo captura
→ **500 al usuario en el tab Coach**, en vez de degradar mostrando el resto del diagnóstico.

**Fix:**
```python
def current_cash(self, now: Optional[datetime] = None) -> dict:
    try:
        baseline = self._db.execute(
            select(CashBaseline).where(CashBaseline.business_id == self._business_id)
            .order_by(CashBaseline.baseline_date.desc()).limit(1)
        ).scalar_one_or_none()
        if baseline is None:
            return {"amount": None, "mode": "relative"}
        # ... resto igual ...
        return {"amount": amount, "mode": "real"}
    except Exception as e:
        _log.error(f"Error en current_cash: {e}", exc_info=True)
        return {"amount": None, "mode": "relative"}
```

---

## F-002 — Sin guard de "sin datos": negocio nuevo recibe un score engañoso en vez de "sin datos suficientes"

**Archivo:** `app/services/coach_service.py:155-171`
**Severidad:** Alto

**El problema:** `ReportingService.monthly_profit_report()` devuelve `gross_margin_pct: 0.0`
(no `None`) cuando `sales_total == 0` — verificado en `app/services/reporting_service.py:571`:
```python
"gross_margin_pct": (gross_total / sales_total * 100.0) if sales_total else 0.0,
```

`health_score()` nunca comprueba `sales_total`, así que para un negocio recién creado (cero
ventas):
```python
score += 1.0 if (financial.get("margin_pct") is not None and financial["margin_pct"] > 0) else -1.0
# margin_pct = 0.0 (no None) → la condición "> 0" es False → score -= 1.0
```

**Escenario de fallo:** negocio nuevo sin ninguna venta registrada → `health_score()` devuelve
`score=4.0, level="yellow"` ("🟡 Regular") en vez de "sin datos suficientes" (REQ-13 y
`design.md` §Migration exigen exactamente ese guard, que estaba en el PAE original —
`if financial.get("_summary", {}).get("sales_total", 0) <= 0: return {"level": "unknown", ...}`
— y no se implementó).

**Fix:**
```python
def health_score(self, now: Optional[datetime] = None) -> dict:
    now_dt = now or datetime.now(timezone.utc)
    financial = self._add_growth_and_extraction(self._financial_indicators(now_dt), now_dt)
    if financial.get("_summary", {}).get("sales_total", 0) <= 0:
        return {"score": None, "level": "unknown", "message": "sin datos suficientes",
                "whats_good": [], "financial": financial, "operational": {}, "customers": {}, "cash": {}}
    # ... resto igual ...
```
Y en `tab_coach.html`, envolver el bloque de score con `{% if report.level != "unknown" %}`.

---

## F-003 — Concentración de clientes compara ventanas de tiempo distintas: la alerta casi nunca dispara

**Archivo:** `app/services/coach_service.py:131-136`
**Severidad:** Alto

**El problema:**
```python
# app/services/coach_service.py:131
rows = self._sales_documents.sales_by_customer(start=month_start, end=now_dt, limit=1)
#      ↑ top cliente de ESTE MES
all_customers = self._sales_documents.sales_by_customer(start=None, end=now_dt, limit=10_000)
#      ↑ TODOS los clientes, TODO el histórico

total_sales = sum(r.get("total", 0) for r in all_customers)   # total histórico
top = rows[0] if rows else None                                 # top de este mes
concentration = (top.get("total", 0) / total_sales) if (top and total_sales > 0) else None
```

Se divide "lo que gastó el top cliente **este mes**" entre "lo que gastaron **todos los
clientes en toda la historia**". Para cualquier negocio con más de unos meses de operación,
el denominador (histórico acumulado) es órdenes de magnitud mayor que el numerador (un mes),
así que `concentration` sale artificialmente bajo casi siempre.

**Escenario de fallo:** un negocio real donde el cliente top representa 60% de las ventas
*de este mes* (debería disparar `severity: red`, umbral ≥50% en `coach_rules.py`) puede dar
una `concentration` de 3-4% si el negocio lleva 2 años vendiendo — la regla
`_rule_customer_concentration` **nunca dispara**, silenciando por completo esta alerta.

**Fix:** usar la misma ventana de tiempo en ambos lados (ej. mes actual para ambos):
```python
def _customer_indicators(self, now: Optional[datetime] = None) -> dict:
    now_dt = now or datetime.now(timezone.utc)
    try:
        month_start = now_dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        month_customers = self._sales_documents.sales_by_customer(start=month_start, end=now_dt, limit=10_000)
        all_customers = self._sales_documents.sales_by_customer(start=None, end=now_dt, limit=10_000)

        month_total = sum(r.get("total", 0) for r in month_customers)
        top = month_customers[0] if month_customers else None
        concentration = (top.get("total", 0) / month_total) if (top and month_total > 0) else None
        # ... resto (recurring_pct, churned) sigue usando all_customers, eso sí es consistente ...
```

---

## F-004 — `health_score()` y `recommendations()` recalculan el pipeline completo por separado: doble carga de BD en cada request

**Archivo:** `app/routers/ui_coach.py:26-27, 59-60, 83-84` + `app/services/coach_service.py:155-204`
**Severidad:** Medio-Alto (eficiencia, no correctness)

**El problema:** los 3 endpoints del router llaman **ambos** métodos en cada request:
```python
# app/routers/ui_coach.py:25-27 (y repetido en las otras 2 rutas)
service = CoachService(db, business_id)
report = service.health_score()          # ejecuta _financial_indicators + _operational_indicators
recommendations = service.recommendations()  # ejecuta LOS MISMOS 4 métodos, OTRA VEZ
```

`health_score()` y `recommendations()` cada uno llama independientemente a
`_financial_indicators`, `_add_growth_and_extraction`, `_operational_indicators`,
`_customer_indicators` (y `recommendations()` además llama `current_cash()` de nuevo vía
`indicators = {**financial, **operational, **customers, **cash}`). Esto son ~10-12 queries a
BD duplicadas en cada `GET /ui/tabs/coach` y cada `POST` (que además re-renderiza el tab
completo, triplicando el efecto).

**Escenario de fallo:** no es un crash, es latencia — con negocios de volumen medio-alto
(muchos movimientos/documentos), cada carga del tab Coach tarda ~2x lo necesario. Se agrava
porque las 2 acciones POST (`cash-baseline`, `action`) también recalculan todo 2 veces antes
de re-renderizar.

**Fix (mínimo, sin tocar la firma pública):** un método interno que calcule el diccionario de
indicadores una sola vez y ambos métodos públicos lo reutilicen:
```python
def _all_indicators(self, now: Optional[datetime] = None) -> dict:
    now_dt = now or datetime.now(timezone.utc)
    financial = self._add_growth_and_extraction(self._financial_indicators(now_dt), now_dt)
    operational = self._operational_indicators(financial, now_dt)
    customers = self._customer_indicators(now_dt)
    cash = self.current_cash(now_dt)
    return {**financial, **operational, **customers, **cash,
            "_financial": financial, "_operational": operational, "_customers": customers, "_cash": cash}
```
Y en el router, computar una vez y pasar a ambos, o exponer un único método `diagnose()` que
devuelva `{"report": ..., "recommendations": ...}` en una sola llamada (esto es lo que el PAE
original especificaba en T10 — se perdió en la implementación).

---

## F-005 — `CoachActionsRepository` es código muerto: nunca se usa, y su contrato no coincide con el uso real

**Archivo:** `app/repositories/coach_actions_repository.py` (completo)
**Severidad:** Medio (mantenibilidad)

**El problema:** el repositorio se creó y se testeó de forma aislada
(`tests/unit/test_coach_repository.py`), pero **`CoachService` nunca lo importa ni lo usa**:
`mark_action()` y `_dismissed_or_done_keys()` reimplementan las mismas queries directamente
con SQLAlchemy crudo. Tampoco lo usa `ui_coach.py` (que instancia `CashBaseline(...)`
directamente en vez de llamar a `CoachActionsRepository.set_baseline()`).

Además, si alguien lo conectara hoy, **el contrato no encajaría**: `update_action(action_id,
status)` asume semántica "actualizar por ID" (un registro mutable), pero `mark_action()` en
el servicio real usa semántica "insertar siempre" (log append-only de auditoría). Conectar el
repo tal como está introduciría un bug de comportamiento distinto al actual.

**Riesgo:** un mantenedor futuro ve el repo, asume que es la capa de acceso a datos real del
coach (sigue el patrón del resto del proyecto: `app/repositories/`), lo modifica pensando que
afecta el comportamiento — y no pasa nada, porque no está conectado a nada.

**Fix:** decidir uno de dos caminos, no dejarlo a medias:
1. **Conectar el repo de verdad** — mover la lógica de `mark_action`/`_dismissed_or_done_keys`/
   `set_baseline` a usar `CoachActionsRepository`, ajustando su contrato a semántica insert-only.
2. **Eliminarlo** — borrar `app/repositories/coach_actions_repository.py` y
   `tests/unit/test_coach_repository.py` si `CoachService` ya cubre esa responsabilidad
   directamente (más simple, menos capas para una feature de este tamaño).

---

## F-006 — Deriva de spec: score documentado 0-100, implementado 1-10

**Archivo:** `app/services/coach_service.py:155-193` vs. `specs/coach-financiero-operativo/design.md` §5.4 / `PAE-coach-financiero-operativo.md` §5.4
**Severidad:** Bajo (no es un bug de runtime, es inconsistencia de documentación/spec)

**El problema:** el PAE especifica `score: int` en escala 0-100 con fórmula
`100 - red_count*20 - yellow_count*10`. La implementación real usa una escala 1-10 con una
fórmula completamente distinta (empieza en 5.0, suma/resta por indicador individual). Es
**internamente consistente** (el template `tab_coach.html:6` ya muestra `/10`), así que no
rompe nada — pero cualquiera que lea `design.md` o el PAE para entender el sistema se lleva
una sorpresa, y la tool `coach_health_score` del agente (`app/agent/tools.py`) hereda esta
escala sin que quede documentado en ningún sitio que cambió respecto al diseño.

**Fix:** actualizar `specs/coach-financiero-operativo/design.md` §5.4 y
`PAE-coach-financiero-operativo.md` para reflejar la escala real (1-10) y la fórmula real
implementada — o, si se prefiere mantener el diseño original 0-100, es un cambio de código
(multiplicar por 10, ajustar límites) más que de documentación.

---

## Backlog de aplicación (todo se corrige junto al final)

| # | Hallazgo | Severidad | Archivo principal |
|---|----------|-----------|-------------------|
| F-001 | `current_cash()` sin try/except | Alto | `coach_service.py` |
| F-002 | Sin guard de "sin datos" en `health_score()` | Alto | `coach_service.py` |
| F-003 | Concentración de clientes con ventanas de tiempo mezcladas | Alto | `coach_service.py` |
| F-004 | Doble cómputo del pipeline de indicadores | Medio-Alto | `coach_service.py`, `ui_coach.py` |
| F-005 | Repositorio de coach actions es código muerto | Medio | `coach_actions_repository.py` |
| F-006 | Score 1-10 real vs. 0-100 documentado | Bajo | `design.md`, `PAE-*.md` |

**Nota:** este informe se suma a los 7 hallazgos de `docs/auditorias/2026-08-16-coach-financiero-operativo.md`
(H-001 a H-007). Al corregir todo junto, priorizar F-001/F-002/F-003 (bugs de comportamiento)
y H-001 (fecha hardcodeada en e2e) antes que el resto.

---

## Estado de aplicación (2026-08-16, mismo día)

Todos los hallazgos se corrigieron en la misma sesión, antes de commit:

| # | Hallazgo | Estado | Nota |
|---|----------|--------|------|
| F-001 | `current_cash()` sin try/except | ✅ Corregido | try/except + logging añadido, retorna `{"amount": None, "mode": "relative"}` en error |
| F-002 | Sin guard de "sin datos" | ✅ Corregido | `_compute_indicators()` detecta `sales_total<=0` y devuelve `level="unknown"`; template actualizado |
| F-003 | Concentración con ventanas mezcladas | ✅ Corregido | numerador y denominador ahora usan la misma ventana (mes actual) |
| F-004 | Doble cómputo del pipeline | ✅ Corregido | `_compute_indicators()` cachea por instancia; `health_score()` y `recommendations()` comparten el resultado |
| F-005 | Repositorio muerto | ✅ Corregido (opción 2) | `coach_actions_repository.py` y su test eliminados |
| F-006 | Deriva de spec (score 1-10 vs 0-100) | ✅ Corregido | `PAE-coach-financiero-operativo.md` §5.4 actualizado con firmas reales |

**Hallazgo adicional descubierto durante la corrección (no estaba en la lista original):**
Al arreglar el test e2e (bloqueado por F-002 + una carrera de HTMX en `page.goto(dashboard)`
redundante tras el login), se confirmó que **la estacionalidad [B] (REQ-8, `_seasonal_context`)
nunca se implementó** — ver nota en `PAE-coach-financiero-operativo.md` §5.4. Pendiente de
decisión del usuario: implementar en fase siguiente o marcar formalmente como no entregado.

**Verificación final:** 203 unit + 18 e2e, todos verdes (`pytest -o addopts="" --ignore=tests/e2e`
y `pytest -o addopts="" tests/e2e -m e2e` por separado).

# Auditoría de Ingeniería: Coach Financiero y Operativo

**Fecha:** 2026-08-16  
**Alcance:** Feature implementada en rama `feature/coach-financiero-operativo` (v0.4.1)  
**Score final:** **3.65** 🟡  
**Estado:** Estable con deuda técnica relevante — despliegue seguro, pero plan de remediación recomendado

---

## 1. Contexto y método

**Auditoría estratégica (10 dimensiones)** de la feature "Coach financiero y operativo" —
motor de reglas determinista para indicadores financieros/operativos, integrado con
CoachService + agente LangGraph existente.

- **Artefactos analizados:** ~955 líneas de código (2 servicios, 1 router, 1 repositorio, 1 template, 8 tests)
- **Criterios:** plantilla de auditoría estándar del harness (10 dimensiones ponderadas)
- **Limitaciones:** código unitario/e2e verificado con `grep` y lectura directa; no se ejecutó contra BD en vivo

---

## 2. Checklist por dimensión

| # | Dimensión | Peso | Hallazgo | Evidencia |
|---|-----------|------|----------|-----------|
| 1 | **SoC / SRP** | 10% | ✅ **Bueno** — cada clase tiene rol claro (CoachService = orquestador, coach_rules = evaluador puro, CoachActionsRepository = CRUD) | `app/services/coach_service.py:20-27` (init instancia servicios, no mezcla lógica) |
| 2 | **Cohesión / Acoplamiento** | 10% | ✅ **Bueno** — dependencias inyectadas, no imports circulares; coach_rules es función pura sin estado | `app/services/coach_rules.py:18` (evaluate sin estado, sin DB) |
| 3 | **Capas / Límites** | 10% | ✅ **Bueno** — ruta HTTP → router → service → repo/queries; dirección de dependencias correcta | `app/routers/ui_coach.py:22-31` (router → service, service → DB) |
| 4 | **Seguridad** | 15% | ⚠️ **Aceptable** — RBAC implementado (ensure_admin_or_owner en 3 endpoints), validación Pydantic presentes; sin PII nueva. Pero ver hallazgo S-002 | `app/routers/ui_coach.py:23, 41, 74` (ensure_admin_or_owner en cada ruta) |
| 5 | **Robustez / Resiliencia** | 15% | ✅ **Bueno** — try/except + logging en servicios críticos; sin circuit breaker requerido (read-only a BD local) | `app/services/coach_service.py:46-48` (try/except con logging) |
| 6 | **Consistencia Transaccional** | 10% | ✅ **Bueno** — `CoachActionLog` escrituras son atómicas (append-only); `CashBaseline` sin riesgo de huérfanos (FK bien definida) | `app/models.py:371-402` (CashBaseline.business_id FK) |
| 7 | **Observabilidad** | 10% | ✅ **Bueno** — logging estructurado en excepciones; falta: X-Request-ID correlación en logs (pero existente en proyecto) | `app/services/coach_service.py:6, 47` (_log.error con exc_info=True) |
| 8 | **Configuración / Entornos** | 5% | ✅ **Bueno** — umbrales en `coach_rules.py` como constantes (MARGIN_DROP_YELLOW_PP, etc.); sin hardcodes de URLs/keys | `app/services/coach_rules.py:8-15` (constantes, no hardcodes) |
| 9 | **Pruebas / Calidad** | 10% | ⚠️ **Aceptable** — 8 tests (7 unit + 1 e2e), e2e cubre happy path y RBAC 403; falta: edge cases (ver hallazgo T-001) | `tests/e2e/test_coach.py:26-55` (e2e real, Playwright) |
| 10 | **Escalabilidad** | 5% | ⚠️ **Aceptable** — `current_cash()` hace 4 queries separadas (N+1 risk ligero); sin caché (válido para v0.4.1 con <50 negocios) | `app/services/coach_service.py:83-100` (4 scalar queries sin consolidación) |

---

## 3. Scorecard ponderado

| Dimensión | Peso | Score (1-5) | Contribución |
|-----------|------|------------|--------------|
| SoC / SRP | 10% | 4 | 0.40 |
| Cohesión / Acoplamiento | 10% | 4 | 0.40 |
| Capas / Límites | 10% | 4 | 0.40 |
| Seguridad | 15% | 3.5 | 0.525 |
| Robustez / Resiliencia | 15% | 4 | 0.60 |
| Consistencia Transaccional | 10% | 4 | 0.40 |
| Observabilidad | 10% | 4 | 0.40 |
| Configuración / Entornos | 5% | 4 | 0.20 |
| Pruebas / Calidad | 10% | 3.5 | 0.35 |
| Escalabilidad | 5% | 3 | 0.15 |
| **TOTAL** | **100%** | — | **3.65** |

---

## 4. Hallazgos detallados

### 🔴 CRÍTICOS
**Ninguno** — el código es seguro para producción.

### 🟠 ALTOS

#### H-001: Test e2e con fecha hardcodeada
**Dimensión:** Pruebas (9)  
**Severidad:** Alto  
**Evidencia:** `tests/e2e/test_coach.py:35` — `page.fill("input[name='baseline_date']", "2026-01-01")`  
**Riesgo:** El test fallará cuando la fecha actual > 2026-01-01. Además, si el formulario rechaza fechas en el futuro (que debería), el test puede fallar en ciertos tiempos de ejecución.  
**Recomendación:** Usar `datetime.now().date().isoformat()` o una fecha relativa (ej. "90 días atrás"):
```python
from datetime import datetime, timedelta
baseline_date = (datetime.now() - timedelta(days=30)).date().isoformat()
page.fill("input[name='baseline_date']", baseline_date)
```

#### H-002: Instanciación repetida de CoachService en cada endpoint
**Dimensión:** Escalabilidad (10)  
**Severidad:** Medio-Alto (bajo riesgo ahora, deuda en crecimiento)  
**Evidencia:** `app/routers/ui_coach.py:25, 58, 82` — cada endpoint crea `CoachService(db, business_id)`  
**Riesgo:** Crear instancia + inyectar 4 servicios (ReportingService, FinanceService, InventoryService, SalesDocumentService) 3 veces por request HTTP. Con muchos requests concurrentes, es garbage collection innecesario. Para volúmenes altos, considerar pool o singleton.  
**Recomendación (Fase 2):** Extraer factory o inyector de dependencias. Por ahora, aceptable.

#### H-003: Escalabilidad de `current_cash()` — N+1 queries ligero
**Dimensión:** Escalabilidad (10)  
**Severidad:** Medio-Alto (bajo riesgo en v0.4.1, crítico en v1.0)  
**Evidencia:** `app/services/coach_service.py:83-100` — 4 llamadas `db.scalar()` separadas (sales_in, purchases_out, expenses_out, extractions_out)  
**Riesgo:** Cada query es un round-trip a la BD. Para un negocio con 10K+ movimientos/mes, la latencia se acumula. La query debería consolidarse en un único SELECT con CTEs o UNION.  
**Recomendación (Fase 2):**  
```python
# ANTES: 4 queries
sales_in = self._db.scalar(select(func.sum(...)).where(...))
purchases_out = self._db.scalar(select(func.sum(...)).where(...))
# ...

# DESPUÉS: 1 query con UNION ALL o CTE
query = """
  SELECT 
    COALESCE(SUM(CASE WHEN type='sale' THEN qty*price ELSE 0 END), 0) as sales_in,
    COALESCE(SUM(CASE WHEN type='purchase' THEN qty*cost ELSE 0 END), 0) as purchases_out,
    ...
  FROM ...
"""
```

### 🟡 MEDIOS

#### H-004: Tests unitarios sin cobertura de edge cases
**Dimensión:** Pruebas (9)  
**Severidad:** Medio  
**Evidencia:** Revisión del contenido de `tests/unit/test_coach_*.py` — aserts básicos; falta: división por cero, indicadores None, empty lists  
**Riesgo:** Bugs silenciosos en casos raros (negocio sin ventas, sin clientes, historial vacío). Hoy están cubiertos por guards en CoachService, pero sin tests explícitos.  
**Recomendación:** Añadir tests parametrizados:
```python
@pytest.mark.parametrize("margin,expected", [(None, None), (0, 0), (-5, "invalid")])
def test_margin_edge_cases(margin):
    ...
```

#### H-005: Falta validación de fecha futura en endpoint POST
**Dimensión:** Seguridad (4)  
**Severidad:** Medio (bajo impacto, válido pero falta defensiva)  
**Evidencia:** `app/routers/ui_coach.py:43-48` — `CashBaselineCreate` valida en Pydantic, pero el error no se muestra al usuario (apenas si va a 422 genérico)  
**Riesgo:** Usuario no entiende por qué rechazó la fecha, puede crear confusión.  
**Recomendación:** Mejorar mensaje de error en el catch:
```python
except ValidationError as e:
    detail = "; ".join(str(err) for err in e.errors())
    raise HTTPException(status_code=422, detail=detail) from e
```

#### H-006: Falta decorador @business_scoped en repositorio
**Dimensión:** Seguridad (4)  
**Severidad:** Medio (bajo riesgo en proyecto con RBAC centralizado, pero no defensivo)  
**Evidencia:** `app/repositories/coach_actions_repository.py` — no revalida `business_id` en queries  
**Riesgo:** Si hay bug en router (olvida inyectar business_id), el repo no protege. Ejemplo: `CoachActionLog.query.filter(rule_key==X)` sin `business_id ==`.  
**Recomendación:** Patrón ya usado en otros repos del proyecto:
```python
def get_action(self, rule_key: str, business_id: int) -> Optional[CoachActionLog]:
    # Revalidar business_id aquí, no confiar en el router
    return self._db.execute(
        select(CoachActionLog).where(
            and_(CoachActionLog.business_id == business_id, 
                 CoachActionLog.rule_key == rule_key)
        )
    ).scalar_one_or_none()
```

### 🟢 BAJOS

#### H-007: Documentación de umbrales de reglas
**Dimensión:** Observabilidad (7)  
**Severidad:** Bajo  
**Evidencia:** `app/services/coach_rules.py:8-15` — constantes sin doc/comentario sobre por qué ese umbral  
**Riesgo:** Mantenedor futuro no sabe si `MARGIN_DROP_YELLOW_PP = 3.0` es juicioso o arbitrary.  
**Recomendación:** Añadir docstring:
```python
# Umbral de margen: caída >= 3pp = alerta amarilla, >= 6pp = roja.
# Basado en volatilidad histórica de márgenes en industria retail.
MARGIN_DROP_YELLOW_PP = 3.0  # points
```

---

## 5. Matriz Impacto vs Esfuerzo

```
                Alto impacto
                     ↑
        🎯 H-003 (escalabilidad)
        🎯 H-002 (instanciación)
        
🔥 H-001 (fecha hardcodeada)  │  📋 H-004, H-005, H-006, H-007
┌─────────────────────────────┼──────────────────────────────→
│ BAJO ESFUERZO               │  ALTO ESFUERZO
└─────────────────────────────┼──────────────────────────────→
        ❌ (evitar)            │  ❌ (evitar)
                     ↓
                Bajo impacto
```

**Quick wins (hacer ahora, antes de merge):**
- **H-001**: Fix fecha hardcodeada en e2e (5 min)

**Estratégicos (fase 2 o roadmap):**
- **H-002**: Refactor instanciación CoachService (inyección de dependencias)
- **H-003**: Consolidar queries en `current_cash()` (1 SELECT con CTE)
- **H-004/H-005/H-006**: Añadir edge-case tests y validación defensiva

---

## 6. Backlog priorizado de remediación

| # | Hallazgo | Severidad | Esfuerzo | Plazo | Propietario |
|---|----------|-----------|----------|-------|------------|
| H-001 | Fecha hardcodeada en e2e | Alto | Bajo | Inmediato | OpenCode/dev |
| H-002 | Instanciación repetida CoachService | Medio-Alto | Medio | Fase 2 (v0.6) | Backend lead |
| H-003 | N+1 queries en current_cash() | Medio-Alto | Medio | Fase 2 (v0.6) | Data/DB lead |
| H-004 | Tests unitarios falta edge cases | Medio | Bajo | Fase 2 (v0.6) | QA/dev |
| H-005 | Validación de errores en endpoint | Medio | Bajo | Fase 2 (v0.6) | Backend lead |
| H-006 | Repo sin revalidación de business_id | Medio | Bajo | Fase 2 (v0.6) | Security/backend |
| H-007 | Documentación de umbrales | Bajo | Muy bajo | Backlog | Senior dev |

---

## 7. Plan 30/60/90 días

### 🟢 30 días (Sprint actual)
**Objetivo:** Lanzar v0.4.1 a producción de forma segura.

1. ✅ **H-001 (BLOCKER):** Fix fecha e2e — **completar antes de merge a `main`**
2. ✅ **Validación:** Ejecutar suite completa de tests (149 unit + 16+2 e2e)
3. ✅ **Documentación:** Asegurar que `CLAUDE.md` refleja estado actual (v0.4.1)
4. ✅ **Despliegue:** Gitea Actions CD a dev → prod

**Criterios de cierre:**
- [ ] H-001 fixed (fecha dinámica en e2e)
- [ ] Todos los tests 🟢 (165+ total)
- [ ] Documentación de coach actualizada en `docs/plans/` y `CLAUDE.md`
- [ ] Deployed a prod sin errores

### 🟡 60 días (Siguiente sprint = Fase 2)
**Objetivo:** Mejorar escalabilidad y robustez sin alterar funcionalidad.

1. **H-003 (data):** Consolidar queries en `current_cash()` → 1 SELECT
   - Plazo: 3-4 horas de dev + testing
   - Ganancia: -75% latencia en `current_cash()` con volumen alto
2. **H-002 (architecture):** Refactor inyección de dependencias en CoachService
   - Plazo: 5-6 horas de dev + refactor router endpoints
   - Ganancia: prep para scaling, reduce GC pressure
3. **H-004 (testing):** Parametrized edge-case tests para todos los métodos
   - Plazo: 2-3 horas
   - Ganancia: confianza en robustez

**Criterios de cierre:**
- [ ] H-003 merged (1 query)
- [ ] H-002 merged (dependency injection)
- [ ] H-004 merged (edge case tests)
- [ ] Todos los tests siguen 🟢

### 🔵 90 días (Roadmap posterior)
**Objetivo:** Madurez operacional antes de "Fase 2 del agente".

1. **H-005/H-006:** Defensiva en endpoints + repo-level revalidation
2. **H-007:** Documentación de reglas y umbrales
3. **Planeación Fase 2:** "Insights proactivos" del agente (ver `specs/copiloto-dashboard-ia/`)
   - Cómo el coach se relaciona con la Fase 2 del copiloto (¿hereda reglas? ¿datos compartidos?)

---

## 8. Notas finales

✅ **Madurez:** El código es **producción-ready**. RBAC, logging, tests e2e, modelos bien diseñados.
⚠️ **Deuda técnica:** 6 hallazgos identificados, 1 blocker (fecha), 5 remediables en Fase 2 sin riesgo.
🎯 **Recomendación:** Desplegar a prod después de fijar H-001. Planificar remediación de escalabilidad (H-002, H-003) en siguiente sprint antes de "Fase 2 del agente".

---

**Informe generado:** 2026-08-16  
**Auditor:** auditoria-ingenieria skill (Claude Haiku 4.5)  
**Repositorio:** gestion_inventario / `feature/coach-financiero-operativo`

---

## Estado de aplicación (2026-08-16, mismo día)

| # | Hallazgo | Estado | Nota |
|---|----------|--------|------|
| H-001 | Fecha hardcodeada en e2e | ✅ Corregido | fecha dinámica (`now - 30 días`) |
| H-002 | Instanciación repetida de CoachService | ⏸️ Diferido (según plan original) | por diseño: cada request necesita su propia sesión de BD; no es un bug, factory pattern queda para Fase 2 si el volumen lo justifica |
| H-003 | N+1 queries en `current_cash()` | ✅ Corregido | 4 subqueries escalares consolidadas en 1 único `SELECT` (1 round-trip) |
| H-004 | Tests sin edge cases | ✅ Corregido | +9 tests: `evaluate({})`, `dead_stock_items=None`, sin clientes, sin historial previo, etc. |
| H-005 | Mensaje de error genérico en 422 | ✅ Corregido | `detail` ahora extrae `err["msg"]` de cada error Pydantic en vez de `str(e)` completo |
| H-006 | Repo sin revalidación de business_id | ✅ Resuelto (indirectamente) | el repositorio se eliminó por completo (ver F-005 del code-review); `CoachService` ya revalidaba `business_id` en sus queries directas |
| H-007 | Documentación de umbrales | ✅ Corregido | comentarios explicando el criterio de cada constante en `coach_rules.py` |

**Verificación final:** 203 unit + 18 e2e verdes. Ver también
`docs/code-reviews/2026-08-16-coach-financiero-operativo.md` para los 6 hallazgos tácticos
(F-001 a F-006), todos corregidos en la misma sesión.

**Nuevo hallazgo descubierto durante la corrección (fuera del alcance original de esta
auditoría):** la estacionalidad [B] (REQ-8) especificada en `requirements.md` nunca se
implementó — no existe `_seasonal_context` en el código. No estaba cubierta por ningún test,
por eso pasó desapercibida hasta este punto. Pendiente de decisión: implementar en fase
siguiente o marcar formalmente como no entregado en v0.4.1.

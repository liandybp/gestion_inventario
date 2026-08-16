# Phase 1 — Requirements: Coach financiero y operativo

- **Slug:** `coach-financiero-operativo`
- **Fecha:** 2026-08-16
- **Fuente:** brainstorming aprobado en `docs/plans/2026-08-16-coach-financiero-operativo-design.md`

---

## 1.1 Functional Requirements (EARS + Gherkin)

### REQ-1: Tab "Coach" accesible solo para admin/owner
**Priority:** Must Have
**WHEN** un usuario admin u owner navega al tab "Coach",
**THE SYSTEM SHALL** mostrar score de salud, alertas priorizadas, recomendaciones accionables
y "lo que va bien" del negocio activo.

```gherkin
**Scenario:** Admin ve el tab Coach
  GIVEN un usuario autenticado con rol admin y negocio activo con ventas este mes
  WHEN el usuario hace GET /ui/tabs/coach
  THEN la respuesta es 200
  AND contiene el score de salud (0-100) y el nivel (verde/amarillo/rojo)

**Scenario:** Operator no ve el tab
  GIVEN un usuario autenticado con rol operator
  WHEN el usuario hace GET /ui/tabs/coach
  THEN la respuesta es 403
  AND el botón "Coach" no aparece en el sidebar
```

### REQ-2: Indicadores financieros calculados sin LLM
**Priority:** Must Have
**WHEN** se solicita el diagnóstico del negocio,
**THE SYSTEM SHALL** calcular de forma determinista (sin llamada a LLM): margen bruto % vs.
promedio 6 meses, ratio gastos/ingresos, ratio extracciones/utilidad, crecimiento de ventas.

```gherkin
**Scenario:** Margen calculado desde datos reales
  GIVEN un negocio con ventas y costos registrados este mes
  WHEN CoachService.diagnose() se ejecuta
  THEN el indicador margin_pct coincide con gross_margin_pct de monthly_profit_report()
  AND no se realiza ninguna llamada al LLM para este cálculo
```

### REQ-3: Indicadores operativos calculados sin LLM
**Priority:** Must Have
**WHEN** se solicita el diagnóstico del negocio,
**THE SYSTEM SHALL** calcular: riesgo de quiebre de stock (% productos bajo ROP), días de
rotación de inventario, valor de stock muerto (sin venta en 90 días), concentración de clientes.

```gherkin
**Scenario:** Stock muerto excluye productos vendidos recientemente
  GIVEN un lote con qty_remaining > 0 recibido hace 120 días
  AND ese producto tiene una venta hace 10 días
  WHEN se calcula dead_stock()
  THEN ese producto NO aparece en dead_stock_items
```

### REQ-4: Salud de clientes — recurrencia y fuga
**Priority:** Should Have
**WHEN** se solicita el diagnóstico del negocio,
**THE SYSTEM SHALL** calcular el % de clientes recurrentes (≥2 compras) y detectar clientes
que compraban con regularidad y llevan más de 60 días sin volver.

```gherkin
**Scenario:** Cliente en fuga detectado
  GIVEN un cliente con 3 compras históricas, la última hace 90 días
  WHEN se calcula _customer_indicators()
  THEN el cliente aparece en churned_customers
```

### REQ-5: Motor de reglas genera recomendaciones accionables
**Priority:** Must Have
**WHEN** un indicador cruza su umbral amarillo o rojo,
**THE SYSTEM SHALL** generar una recomendación con síntoma, causa operativa, acción concreta
e impacto estimado en dinero (cuando sea calculable).

```gherkin
**Scenario:** Margen bajo dispara recomendación roja
  GIVEN margin_pct = 12.0 y margin_avg_6m = 18.0 (caída de 6 puntos)
  WHEN coach_rules.evaluate() se ejecuta
  THEN se genera una recomendación con rule_key="margin_drop" y severity="red"

**Scenario:** Sin cruce de umbral, sin recomendación
  GIVEN margin_pct = 17.5 y margin_avg_6m = 18.0 (caída de 0.5 puntos)
  WHEN coach_rules.evaluate() se ejecuta
  THEN no se genera recomendación de tipo margin_drop
```

### REQ-6: Score de salud 0-100
**Priority:** Must Have
**WHEN** el diagnóstico se completa,
**THE SYSTEM SHALL** calcular un score 0-100 (100 - 20×alertas_rojas - 10×alertas_amarillas,
piso 0) y un nivel (`red` si hay alguna roja, `yellow` si hay alguna amarilla, `green` si no
hay ninguna), y ordenar las recomendaciones por severidad y luego por impacto estimado.

### REQ-7: Caja real desde saldo inicial declarado
**Priority:** Must Have
**WHEN** existe al menos un `CashBaseline` para el negocio,
**THE SYSTEM SHALL** calcular el saldo de caja actual como `baseline.amount + ventas - compras
- gastos - extracciones` desde `baseline_date`, usando el `CashBaseline` más reciente.
**WHEN** no existe ningún `CashBaseline`,
**THE SYSTEM SHALL** devolver `{"amount": None, "mode": "relative"}` sin romper el diagnóstico.

```gherkin
**Scenario:** Caja real con baseline y sin movimientos posteriores
  GIVEN un CashBaseline de $500 con baseline_date = ahora
  AND ningún movimiento posterior a esa fecha
  WHEN se llama current_cash()
  THEN el resultado es {"amount": 500.0, "mode": "real"}

**Scenario:** Sin baseline, modo relativo
  GIVEN un negocio sin ningún CashBaseline
  WHEN se llama current_cash()
  THEN el resultado es {"amount": None, "mode": "relative"}
```

### REQ-8: Estacionalidad atenúa falsas alarmas
**Priority:** Should Have
**WHEN** las ventas caen respecto al promedio móvil de 3 meses,
**THE SYSTEM SHALL** comparar contra el mismo mes del año anterior (requiere ≥13 meses de
historial); si la caída es consistente con el patrón histórico (diferencia ≤5 puntos
porcentuales), añadir una nota de contexto sin suprimir la recomendación.

```gherkin
**Scenario:** Sin historial suficiente, sin nota estacional
  GIVEN un negocio con menos de 13 meses de historial de ventas
  WHEN se llama _seasonal_context(-15.0)
  THEN el resultado es None
```

### REQ-9: Círculo cerrado de recomendaciones
**Priority:** Must Have
**WHEN** un admin/owner marca una recomendación como "hecha" o "no aplica" (`dismissed`),
**THE SYSTEM SHALL** persistir el estado en `CoachActionLog` y excluir esa `rule_key` de
futuros diagnósticos hasta que la condición vuelva a evaluarse desde cero.

```gherkin
**Scenario:** Recomendación descartada se suprime
  GIVEN una recomendación con rule_key="margin_drop" activa
  WHEN el admin hace POST /ui/coach/action con rule_key="margin_drop", status="dismissed"
  THEN diagnose() ya no incluye esa recomendación
```

### REQ-10: Burbuja de chat reutiliza el agente existente
**Priority:** Should Have
**WHEN** el usuario interactúa con la burbuja de chat del tab Coach,
**THE SYSTEM SHALL** exponer 2 tools nuevas read-only al agente LangGraph existente
(`coach_health_score`, `coach_recommendations`), scoped por `business_id`, para que el LLM
explique el diagnóstico determinista sin inventar cifras.

### REQ-11: RBAC — operator sin acceso
**Priority:** Must Have
**WHEN** un usuario con rol `operator` solicita cualquier endpoint del coach (`GET
/tabs/coach`, `POST /coach/cash-baseline`, `POST /coach/action`),
**THE SYSTEM SHALL** responder 403 usando `ensure_admin_or_owner`, igual que el copiloto.

### REQ-12: Validación de inputs
**Priority:** Must Have
**WHEN** se envía un `CashBaseline` con `amount < 0` o `baseline_date` futura, o un
`CoachActionUpdate` con `status` fuera de `("done", "dismissed")`,
**THE SYSTEM SHALL** rechazar con 422 vía validación Pydantic.

### REQ-13: Degradación elegante
**Priority:** Must Have
**WHEN** el LLM del copiloto no está disponible,
**THE SYSTEM SHALL** seguir mostrando score/alertas/recomendaciones (deterministas), solo la
burbuja de chat degrada.
**WHEN** el negocio no tiene ventas registradas este mes,
**THE SYSTEM SHALL** devolver `{"level": "unknown", "message": "sin datos suficientes"}` en
vez de calcular ratios sobre cero.
**WHEN** cualquier método de `CoachService` o `coach_rules.evaluate()` lanza una excepción,
**THE SYSTEM SHALL** loggearla (`logging.error(..., exc_info=True)`) y devolver un resultado
seguro por defecto, nunca propagar un 500 al usuario.

---

## 1.2 MoSCoW Prioritization

| Priority | REQs | % |
|----------|------|---|
| **Must Have** | REQ-1, REQ-2, REQ-3, REQ-5, REQ-6, REQ-7, REQ-9, REQ-11, REQ-12, REQ-13 | ~65% |
| **Should Have** | REQ-4 (salud de clientes), REQ-8 (estacionalidad), REQ-10 (tools del chat) | ~25% |
| **Could Have** | reordenar recomendaciones manualmente, exportar diagnóstico a PDF | ~5% |
| **Won't Have** (esta iteración) | indicadores de deuda/liquidez (sin dato de origen), metas con acompañamiento, proyección a 30 días, matriz impacto×esfuerzo | ~5% |

## 1.3 Non-Functional Requirements (NFRs)

| Category | Question | Answer |
|----------|----------|--------|
| **Performance** | Tiempo de respuesta esperado? | `diagnose()` ejecuta ~6-8 queries agregadas; aceptable <2s para el volumen actual (<50 negocios). Sin caché en esta iteración. |
| **Security** | Nuevo límite de auth? Datos sensibles? | Sí — admin/owner only (`ensure_admin_or_owner`); sin PII nueva (caja/utilidad ya son datos del sistema) |
| **Scalability** | Volumen esperado? | Cálculo on-demand sin caché; válido para la escala actual del proyecto. Si el número de negocios crece significativamente, evaluar caché — **fuera de alcance de este PAE** |
| **Observability** | Logs nuevos? | Sí — `try/except` con `logging.error(..., exc_info=True)` en cada método de `CoachService` y en `coach_rules.evaluate()` |
| **Reliability** | Comportamiento ante error? | Guards explícitos: sin ventas → "unknown"; excepción interna → resultado seguro por defecto, nunca 500; LLM caído → solo el chat degrada |

## 1.4 Out of Scope

- Indicadores de deuda/pasivos y liquidez clásica (no hay dato de origen en el sistema; ver REQ-7 para el sustituto de caja).
- Metas con acompañamiento, proyección a 30 días, matriz impacto×esfuerzo (D/E/F del brainstorming) — fase siguiente.
- Chat embebido dentro del mismo tab Coach — la burbuja navega al tab del copiloto existente (`/ui/tabs/agent`), no se duplica el chat.
- Caché de indicadores / recálculo programado — cálculo on-demand únicamente (D4 del design doc).
- Edición de umbrales desde la UI — los umbrales viven en constantes de `coach_rules.py`.

## 1.5 Dependencies

- Depende del **Copiloto IA** ya implementado (`specs/copiloto-dashboard-ia/`) para la burbuja
  de chat — reutiliza `app/agent/` (LangGraph, runner, `/ui/agent/chat`) sin modificarlo.
- Depende de servicios existentes de solo lectura: `ReportingService`, `FinanceService`,
  `InventoryService`, `SalesDocumentService` — ninguno se modifica en su lógica existente,
  solo se añaden métodos aditivos (`dead_stock`, `inventory_turnover_days`).
- No depende de ninguna migración de schema fuera de `Base.metadata.create_all` (tablas nuevas
  únicamente, sin `ALTER` sobre tablas existentes).

## 1.6 Open Questions

Ninguna — todos los requisitos fueron validados con el usuario en la sesión de brainstorming
previa (`docs/plans/2026-08-16-coach-financiero-operativo-design.md`). Proceeding to Phase 2.

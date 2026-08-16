# Diseño — Coach financiero y operativo

- **Fecha:** 2026-08-16
- **Proyecto:** gestion_inventario
- **Rama base:** `develop`
- **Versión objetivo:** v0.4.1
- **Estado:** diseño aprobado (brainstorming) → pendiente PAE

---

## 1. Objetivo

Añadir a la app un **coach financiero y operativo** que, usando los datos reales del negocio
activo (ventas, compras, inventario FIFO, gastos, extracciones, documentos, clientes), calcule
**indicadores financieros y operativos que hoy no se calculan** y genere **recomendaciones
accionables** para usuarios **sin conocimientos de finanzas ni operaciones**, con el fin de
mejorar el rendimiento y el crecimiento del negocio.

**Idea rectora:** los indicadores financieros son el *resultado* (lo que ya pasó); las acciones
operativas son las *palancas* que lo mueven. El coach no da consejos genéricos: **rastrea cada
síntoma financiero hasta su causa operativa en los datos y recomienda una acción concreta,
anclada al producto/proveedor/monto real, con un impacto estimado en dinero.**

**Qué NO se toca:** los servicios existentes solo se leen/extienden (`InventoryService`,
`ReportingService`, `FinanceService`), `security.py`, `deps.py`, `db.py`, `auth.py`, los routers
UI existentes y los modelos existentes (solo se añaden tablas nuevas).

---

## 2. Decisiones de diseño

| # | Decisión | Justificación | Alternativa descartada |
|---|----------|---------------|------------------------|
| D1 | **Enfoque híbrido:** núcleo determinista (reglas) + chat vía agente LLM existente | Una recomendación financiera que el usuario seguirá a ciegas no puede depender de que el LLM "adivine bien"; el diagnóstico debe ser predecible y testeable | Todo vía LLM (riesgo de alucinación, no testeable, cae con el LLM) |
| D2 | Diagnóstico, score y alertas los calcula `CoachService` (Python puro, patrón de los servicios existentes) | Rápido, 100% testeable con pytest, funciona aunque el LLM esté caído | — |
| D3 | La burbuja de chat reutiliza el agente LangGraph (`app/agent/`) con tools nuevas read-only | El LLM **explica** el diagnóstico determinista, nunca lo inventa | Chat separado |
| D4 | Cálculo **on-demand** al abrir el tab (sin jobs en background) | Simple, sin scheduler nuevo; datos siempre frescos | Job programado (más infraestructura) |
| D5 | **Caja real** vía tabla `CashBaseline` (saldo inicial cargado una vez) + cálculo en vivo | El modelo no tiene balance; con un baseline puntual se deriva el saldo real desde los movimientos ya registrados | Columnas en `Business` (sin historial de recalibración) |
| D6 | Deuda/pasivos **fuera de alcance** | No hay ningún dato en el sistema del que derivarla; pedirla sería fricción sin fuente confiable | Formulario de deuda manual |
| D7 | Recomendación = síntoma + causa operativa + acción + impacto en $ | El usuario no experto nunca queda con "¿y ahora qué hago?" | Consejo genérico por indicador |
| D8 | Reglas, umbrales, textos y estimadores de impacto en `coach_rules.py` (declarativo) | Tunear sin tocar lógica; P5 (configurabilidad) | Umbrales hardcodeados en la lógica |
| D9 | RBAC: **solo admin/owner**; operator sin acceso (tab oculto + 403) | El coach maneja diagnóstico financiero sensible del negocio | Operator solo-lectura |
| D10 | Incorporar **A (círculo cerrado), B (estacionalidad), C (salud de clientes)** al núcleo | Blindan confianza, evitan falsas alarmas y atacan satisfacción del cliente + crecimiento | D/E/F (metas, proyección, impacto×esfuerzo) → fase siguiente |

---

## 3. Arquitectura

```
Tab "Coach" (GET /ui/tabs/coach)  ── admin/owner only
  │
  ├─► CoachService (app/services/coach_service.py)   ◄── NÚCLEO DETERMINISTA (sin LLM)
  │     ├─ reúne indicadores vía ReportingService / FinanceService / InventoryService
  │     │  (+ métricas nuevas: caja real, stock muerto, rotación, retención/fuga de clientes)
  │     ├─ evalúa reglas contra umbrales (coach_rules.py)
  │     ├─ ajusta por estacionalidad (compara contra historial del propio negocio)  [B]
  │     ├─ estima impacto en $ de cada acción
  │     ├─ cruza con CoachActionLog (suprime descartadas, verifica hechas)          [A]
  │     └─ produce: score 0-100 + lista priorizada de {síntoma, causa, acción, impacto, severidad}
  │
  ├─► render: partials/tab_coach.html  (score + alertas + recomendaciones + "lo que va bien" + chat)
  └─► badge en sidebar: nº de alertas rojas

Burbuja de chat  →  agente LangGraph existente (app/agent/) + tools nuevas del coach
```

El coach vive en el mismo proceso que la API y llama a los servicios **directamente**.
`business_id` lo inyecta el router (`require_active_business_id`); ni el LLM ni el cliente lo deciden.

---

## 4. Modelo de datos (tablas nuevas)

`Base.metadata.create_all` crea las tablas en startup (dev y prod). Sin `ALTER`, sin bump de
`schema_version`, sin backfill. Rollback = `DROP TABLE` (sin pérdida de datos de negocio).

### 4.1 CashBaseline — saldo de caja inicial

| Campo | Tipo | Notas |
|-------|------|-------|
| id | Integer PK | |
| business_id | FK businesses.id, index | aislamiento multi-negocio |
| baseline_date | DateTime(tz) | fecha del saldo declarado |
| amount | Numeric(14,4) | monto ≥ 0 |
| created_by | FK users.id, nullable | |
| created_at | DateTime(tz) server_default now | |

Se usa la fila **más reciente** por negocio (permite recalibrar). Saldo actual:

```
caja_actual = baseline.amount
            + Σ ventas (unit_price·qty)   desde baseline_date
            − Σ compras (unit_cost·qty)   desde baseline_date
            − Σ gastos operativos         desde baseline_date
            − Σ extracciones de socios    desde baseline_date
```

Sin baseline → indicadores de caja en modo **flujo relativo** + sugerencia de cargar el saldo.

### 4.2 CoachActionLog — círculo cerrado de acciones [A]

| Campo | Tipo | Notas |
|-------|------|-------|
| id | Integer PK | |
| business_id | FK businesses.id, index | |
| rule_key | String(128), index | identidad estable de la recomendación (ej. `supplier_cost_increase:product_id=42`) |
| status | String(16) | `open` \| `done` \| `dismissed` |
| impact_estimate | Numeric(14,4), nullable | impacto estimado al momento de marcar |
| marked_by | FK users.id, nullable | |
| marked_at | DateTime(tz) | |
| verified_result | Numeric(14,4), nullable | resultado real medido después |
| verified_at | DateTime(tz), nullable | |

En cada visita, `CoachService` genera recomendaciones y las cruza por `rule_key`:
`dismissed` → se suprime; `done` → se verifica si el indicador mejoró y se muestra el resultado.

---

## 5. Indicadores y motor de reglas

### 5.1 Financieros

| Indicador | Fórmula (datos reales) | Detecta |
|---|---|---|
| Margen bruto % + tendencia | `(ventas − COGS)/ventas` vs promedio 6m (`monthly_profit_report`) | Caída de precios o subida de costos |
| Ratio gastos/ingresos | `total_expenses / ventas` del mes | Gastos creciendo más rápido que ventas |
| Ratio extracciones/utilidad | `extracciones / utilidad_neta` (`dividends_report`) | Socios drenando caja |
| Caja — saldo y tendencia | baseline + Σ movimientos (§4.1) | Caja bajando sostenidamente |
| Crecimiento de ventas | mes vs promedio móvil 3m (`monthly_overview`) | Estancamiento/caída |

### 5.2 Operativos

| Indicador | Fórmula | Detecta |
|---|---|---|
| Riesgo de quiebre de stock | # bajo ROP / activos (`replenishment_table`) | Productos por agotarse |
| Rotación de inventario (días) | `inventario_promedio / COGS_diario` | Sobre-stock (dinero inmovilizado) |
| Stock muerto | lotes sin venta en N días (`InventoryLot × InventoryMovement`) | Capital atrapado |
| Concentración de clientes | % ventas top-1 cliente (`sales_by_customer`) | Dependencia riesgosa |
| Ticket promedio + tendencia | `ventas / nº documentos` (`sales_documents_summary`) | Valor por venta subiendo/bajando |

### 5.3 Salud de clientes [C]

| Indicador | Fórmula | Detecta |
|---|---|---|
| Recurrentes vs nuevos | clientes con ≥2 compras vs 1 (histórico de documentos) | Base leal vs dependencia de nuevos |
| Clientes en fuga | compraban con regularidad y llevan > N días sin volver | Oportunidad de recontacto (satisfacción + crecimiento) |

### 5.4 Vínculos causa → acción → impacto

Cada regla tiene la forma `indicador_financiero_malo + causa_operativa_en_datos → acción_operativa_anclada + impacto_estimado`.

| Síntoma financiero | Causa operativa (dato) | Acción recomendada | Estimación de impacto |
|---|---|---|---|
| Margen ↓ | `unit_cost` de producto X subió | Renegociar/cambiar proveedor de X | `(costo_nuevo−costo_base)·volumen` = $/mes de margen |
| Caja ↓ | sobre-stock / extracciones > utilidad | Pausar reposición de baja rotación | capital liberado |
| Rotación lenta / stock muerto | productos sin venta N días | Promoción/liquidación de esos productos | `Σ lote·costo` = caja recuperable |
| Gastos/ingresos ↑ | concepto de gasto creció (`top_expense_concept`) | Revisar ese concepto | exceso sobre promedio = ahorro/mes |
| Ticket ↓ | ventas concentradas en bajo valor | Vender complementos del producto estrella | — |
| Cliente en fuga | recurrente sin volver N días | Contactar al cliente | ticket histórico del cliente |

### 5.5 Estacionalidad [B]

Antes de emitir una alerta de caída (ventas, caja, ticket), el coach compara contra el **mismo
período de meses/años anteriores del propio negocio**. Si la caída es consistente con el patrón
histórico, la alerta se **atenúa con contexto** (*"es normal en agosto según tu historial"*) en
vez de gritar una falsa alarma. Requiere ≥ 1 año de datos comparables; si no, se omite el ajuste.

### 5.6 Scoring

Score de salud **0–100** = promedio ponderado de los indicadores (más peso a financieros).
Recomendaciones ordenadas por **impacto × severidad** (rojo primero, mayor $ primero).
Cada indicador declara su **mínimo de datos**; sin datos suficientes → "aún no hay datos para
diagnosticar" (nunca división por cero ni números sin sentido).

---

## 6. RBAC y seguridad

| Acción | admin | owner | operator |
|---|---|---|---|
| Ver tab Coach / usar chat / editar caja | ✅ | ✅ | ❌ (sin acceso: tab oculto + 403) |

- Todo scoped por `business_id` inyectado por el router (`require_active_business_id`).
- Tools del agente **read-only** y scoped, igual que las 27 actuales — cero mutación por chat.
- Única escritura del feature: `CashBaseline` y `CoachActionLog` (validadas por Pydantic:
  monto ≥ 0, fecha no futura, `status` ∈ whitelist).
- Reutiliza rate limiting + circuit breaker del agente para la burbuja de chat.
- Sin PII nueva (caja/utilidad ya son datos del sistema).

---

## 7. UX (usuario no experto)

```
SALUD DEL NEGOCIO           72 / 100   🟡 Regular   ▲ +5

⚠️ NECESITA TU ATENCIÓN (ordenado por impacto $)
 🔴 Tu margen bajó a 12% (tu normal es 18%)
     → Causa: el costo de "Filtro aceite X" subió 20%
     → Acción: renegocia con el proveedor o busca otro
     💰 Recuperarías ~$340/mes                         [Ver más]
 🟡 Tienes $1.200 en stock parado hace 90 días
     → Acción: haz promoción de "Bujía Y" y "Correa Z"
     💰 Recuperarías ~$1.200 de caja                   [Ver más]

✅ LO QUE VA BIEN
 • Tus ventas crecieron 8% este mes
 • Ningún cliente concentra más del 30% de tus ventas

💬 [Pregúntale a tu coach...]  ← burbuja de chat (HTMX)
```

- **Lenguaje cotidiano, no jerga** ("stock parado", no "rotación"); el término técnico vive en "Ver más" (educativo, opcional).
- **Semáforo universal** 🔴🟡🟢 + score único: se entiende sin saber finanzas.
- **Cada alerta = síntoma + causa + acción + impacto en $.**
- **Círculo cerrado** [A]: botones "Hecho" / "No aplica" en cada recomendación; el coach luego confirma si funcionó.
- La **burbuja de chat** permite preguntar libre y el agente responde grounded en el diagnóstico real.

---

## 8. Testing y manejo de errores

**Unit (pytest, SQLite en memoria):** cada indicador con datos sintéticos → valor exacto; cada
umbral verde/amarillo/rojo → dispara/no la recomendación; estimadores de impacto → monto exacto;
caja desde baseline (y sin baseline → modo relativo); score → valor esperado; estacionalidad →
atenúa alarma con historial; círculo cerrado → suprime descartadas y verifica hechas; RBAC
operator → 403; caja monto negativo/fecha futura → 422.

**E2E (Playwright, `tests/e2e/`, `@pytest.mark.e2e`):** admin → cargar caja → abrir Coach → score
+ recomendación → marcar "Hecho" → usar chat → respuesta → assert. Operator → tab ausente / 403.

**Errores (degradación elegante):**
- LLM caído → chat muestra "coach no disponible", pero **diagnóstico/score/alertas siguen
  funcionando** (deterministas) — beneficio clave del enfoque híbrido.
- Sin baseline → caja en modo relativo + sugerencia de cargar saldo.
- Negocio sin datos → "aún no hay datos suficientes para diagnosticar".
- Ventas=0 en un ratio → guard explícito → indicador "N/A", no crashea.

---

## 9. Alcance por fases

- **Fase 1 (este PAE):** núcleo determinista + tab + chat + A/B/C + tests. Target v0.4.1.
- **Fase siguiente (fuera de alcance):** D (metas con acompañamiento), E (proyección 30 días),
  F (matriz impacto×esfuerzo).

---

## Handoff

```yaml
phase: design
produced_by: brainstorming
consumed_by: PAE (SDD-Agent / writing-plans)
open_questions: []
```

→ PAE de ejecución: `specs/coach-financiero-operativo/PAE-coach-financiero-operativo.md`

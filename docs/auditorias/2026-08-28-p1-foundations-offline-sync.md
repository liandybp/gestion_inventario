# Auditoría de Ingeniería — offline-desktop-sync / Slice P1 (Fundamentos)

**Fecha:** 2026-08-28
**Proyecto:** gestion_inventario
**Rama:** develop (cambios sin commitear, implementados por OpenCode a partir de `specs/offline-desktop-sync/p1-foundations/PAE-p1-foundations.md`)
**Score:** 2.90 / 5.00 🔴
**Responsable:** Claude Code (auditoria-ingenieria), tras `/code-review` (medium, 8 hallazgos verificados)

---

## 1. Contexto y método

**Alcance:** slice P1 del épico `offline-desktop-sync` — outbox de sincronización
(`app/sync/outbox.py`, modelo `SyncEvent`) y numeración de SKU por rango de negocio
(`app/services/sku_range.py`). Es infraestructura pura, sin endpoints HTTP nuevos ni UI.

**Archivos auditados:** 11 archivos (3 nuevos de producción, 2 modificados de producción,
2 modificados de config/versión, 4 de test) en `app/services/`, `app/sync/`, `app/routers/`,
`app/models.py`, `app/migrations.py`, `VERSION`, `tests/unit/`.

**Criterios:** 10 dimensiones de la plantilla de auditoría de ingeniería del proyecto.

**Insumo previo:** `/code-review` (medium effort, 8 ángulos, 1-vote verify) — 8 hallazgos
verificados (5 CONFIRMED, 1 PLAUSIBLE, 2 cleanup CONFIRMED). Esta auditoría los integra y
añade los hallazgos estructurales que un diff no muestra (dimensiones 5, 7, 9 en particular).

**Limitaciones:** no se auditó el resto del repo (fuera de scope de P1); dimensiones 4
(seguridad) y 8 (config) tienen superficie mínima porque P1 no expone endpoints ni añade
configuración nueva — se evalúan igual, con esa salvedad explícita.

**Evidencia de ejecución real:** `python -m pytest tests/unit -q` (con `.venv` activado) →
**233 tests, 100% verdes**, cobertura 84.60% (≥80% requerido). Confirmado con exit code 0,
no solo reportado por OpenCode.

---

## 2. Checklist por dimensión

| # | Dimensión | Peso | Veredicto | Score | Evidencia |
|---|-----------|------|-----------|-------|-----------|
| 1 | SoC / SRP | 10% | ⚠️ | 3 | `sku_range.py` es un módulo puro ejemplar; `outbox.py` mezcla cálculo de hash + acceso a datos en la misma función (`append_event`) |
| 2 | Cohesión / Acoplamiento | 10% | ✅ | 4 | Bajo acoplamiento, sin imports circulares; `next_sku_in_range` es puro (no depende de sesión) |
| 3 | Capas / Límites | 10% | ⚠️ | 3 | `outbox.py:33-38` hace `select()` ORM directo, sin pasar por un repositorio — se aparta del patrón de Fase B (`app/repositories/`) que el propio proyecto adoptó tras auditoría previa (score 3.85, CLAUDE.md). Fue un trade-off documentado (design.md P1 §T2), no un olvido, pero el costo es real |
| 4 | Seguridad | 15% | ✅ | 4 | Sin endpoints/auth nuevos en P1; `payload: dict` sin schema/validación todavía — aceptable porque no hay superficie HTTP en este slice, pero es deuda latente para cuando P2/P3 lo conecten a input de red |
| 5 | Robustez / Resiliencia | 15% | ❌ | 2 | **Cero try/except y cero logging en todo `app/sync/`**, pese a que `design.md` P1 §2.9 (pre-mortem, dimensión 5) exige explícitamente ese patrón para todo método público que toque BD. `append_event`/`verify_chain` no capturan ninguna excepción. Además: boundary bug crítico (H-001) y race condition (H-004) |
| 6 | Consistencia transaccional | 10% | ❌ | 2 | H-001 (colisión permanente de SKU) y H-004 (seq duplicado bajo concurrencia) son ambos fallos de consistencia — la migración DDL sí es transaccional (`with engine.begin()`), pero la lógica de dominio nueva no lo es |
| 7 | Observabilidad | 10% | ❌ | 2 | Ningún `_log.error(..., exc_info=True)` en el código nuevo, a diferencia del patrón ya establecido en el resto del proyecto (`coach_service.py` lo usa 5+ veces). Si `verify_chain` detecta manipulación o `append_event` falla, no queda rastro en logs |
| 8 | Configuración / Entornos | 5% | ✅ | 4 | `SKU_RANGE_SIZE=10000` es una constante de dominio (no específica de entorno), consistente con el resto del código; sin hardcodes de URL/secretos |
| 9 | Pruebas / Calidad | 10% | ⚠️ | 3 | 233/233 tests verdes, cobertura 84.6%; pero **cero test cubre el caso de saturación de rango** (el bug crítico H-001), cero test de concurrencia (H-004), cero test del mismatch de case-sensitivity (H-006) — los tests existentes prueban el happy path y el tamper-detection básico, no los bordes que realmente rompieron el código |
| 10 | Escalabilidad | 5% | ❌ | 2 | `verify_chain` trae y recorre TODO el historial de eventos del negocio en cada llamada, sin checkpoint — el propio diseño anticipa nodos con "días offline" (miles de eventos acumulados) como caso normal, no un edge case |

**Leyenda:** ✅ OK (4-5) · ⚠️ PARCIAL (2-3) · ❌ NO (<2)

---

## 3. Scorecard ponderado

```
Dimensión                        Peso    Score   Peso×Score
────────────────────────────────────────────────────────────
1. SoC / SRP                     10%     3       3.0
2. Cohesión / Acoplamiento       10%     4       4.0
3. Capas / Límites               10%     3       3.0
4. Seguridad                     15%     4       6.0
5. Robustez / Resiliencia        15%     2       3.0
6. Consistencia transaccional    10%     2       2.0
7. Observabilidad                10%     2       2.0
8. Configuración / Entornos       5%     4       2.0
9. Pruebas / Calidad             10%     3       3.0
10. Escalabilidad                 5%     2       1.0
────────────────────────────────────────────────────────────
TOTAL                           100%             29.0 / 100
SCORE FINAL                                      2.90 / 5.00 🔴
```

**Interpretación:** 🔴 < 3.2 — Alto riesgo. **No recomendado mergear a `develop` tal cual.**
El bug crítico (H-001) reproduce la misma clase de incidente de producción que este propio
slice fue diseñado para prevenir (v0.4.6, SKU global vs. scoped). La ausencia total de
logging/manejo de errores (H-003) es una regresión respecto al patrón ya establecido en el
resto del proyecto, no una carencia nueva a justificar por ser "solo infraestructura".

---

## 4. Hallazgos

### 🔴 Críticos

| ID | Título | Dimensión | Evidencia | Riesgo | Recomendación |
|----|--------|-----------|-----------|--------|---------------|
| H-001 | Boundary bug en `next_sku_in_range`: colisión permanente de SKU | 6 | `app/services/sku_range.py:31` — `if base <= n < ceiling` (estricto) deja `ceiling` fuera del conteo tras insertarse | Al SKU #10.000 generado para un negocio, la función vuelve a devolver el mismo candidato para siempre → 409 permanente en compras/altas de producto, requiere intervención manual en BD. Reproduce el incidente v0.4.6 que este slice debía prevenir | Cambiar a `n <= ceiling` en el chequeo y `ceiling` inclusivo en el "candidato ocupa espacio", o reservar el rango como `[base+1, base+SKU_RANGE_SIZE]` cerrado en ambos extremos con test explícito del borde |

### 🟠 Altos

| ID | Título | Dimensión | Evidencia | Riesgo | Recomendación |
|----|--------|-----------|-----------|--------|---------------|
| H-002 | Hash del outbox no cubre `business_id`/`location_id`/`seq`/`status` | 5 | `app/sync/outbox.py:18` (`compute_event_hash`) | Editar esos campos directamente en el `.db` local no rompe `verify_chain` — el mecanismo de integridad que REQ-9 pide queda incompleto justo para el escenario que dice defender (edición offline del archivo) | Incluir los 4 campos en la cadena canónica del hash; añadir test que edite cada campo por separado y confirme que `verify_chain` lo detecta |
| H-003 | Cero logging / manejo de errores en `app/sync/` | 5, 7 | `app/sync/outbox.py` completo — sin `try/except`, sin `_log.error` | Sin observabilidad ante fallos de `append_event`/`verify_chain` en producción; contradice el propio pre-mortem del diseño (`p1-foundations/design.md` §2.9, dimensión 5) que exigía este patrón explícitamente | Envolver ambas funciones públicas en try/except con `_log.error(..., exc_info=True)`, siguiendo el patrón de `coach_service.py` |
| H-004 | Race condition (TOCTOU) en `append_event`: `seq` puede duplicarse | 6 | `app/sync/outbox.py:33-38`; solo índice no-único en `models.py`/`migrations.py` | Dos escrituras concurrentes al mismo `business_id` (esperable cuando P2/P3 conecten esto a requests web) generan `seq` duplicado y cadenas de hash divergentes, silenciosamente | Añadir constraint `UNIQUE(business_id, seq)` a nivel de BD (falla explícita en vez de corrupción silenciosa) antes de que P2/P3 conecten callers reales |

### 🟡 Medios

| ID | Título | Dimensión | Evidencia | Riesgo | Recomendación |
|----|--------|-----------|-----------|--------|---------------|
| H-005 | `verify_chain` sin checkpoint escala mal | 10 | `app/sync/outbox.py:71-77` | Con miles de eventos acumulados (días offline, el caso de uso normal del diseño), cada verificación recalcula sha256 desde `seq=1` — costo creciente sin cota | Guardar un checkpoint "verificado hasta seq X" y verificar solo el delta |
| H-006 | Filtro case-sensitive (`startswith`) no coincide con queries case-insensitive (`ilike`) | 6, 9 | `app/services/sku_range.py:26` vs. `product_service.py`/`purchase_repository.py` | Un SKU manual en minúsculas dentro del rango de un negocio queda invisible para `next_sku_in_range`, subestimando `max_n` | Normalizar a mayúsculas en el filtro (`s.upper().startswith(prefix.upper())`) o forzar `.upper()` en el SKU manual al guardar |
| H-011 | Sin test de los casos borde que causaron H-001/H-004/H-006 | 9 | `tests/unit/test_sku_range.py`, `tests/unit/test_sync_outbox.py` | Los 11 tests nuevos cubren happy-path y un caso de tamper básico, pero ninguno satura un rango, ninguno concurre dos `append_event`, ninguno prueba minúsculas | Añadir `test_range_saturation_does_not_repeat_ceiling`, `test_concurrent_append_event_seq`, `test_lowercase_sku_counted` antes de mergear |

### 🟢 Bajos

| ID | Título | Dimensión | Evidencia | Riesgo | Recomendación |
|----|--------|-----------|-----------|--------|---------------|
| H-007 | `zfill(6)` no trunca; SKU excede ancho fijo si `business_id ≥ 100` | 6 | `app/services/sku_range.py:33` | Formato "SKU"+6 dígitos roto para negocio #100+; sin overflow real (columna `String(64)`), baja probabilidad al volumen actual del proyecto (~5-10 negocios) | Documentar el límite o usar `width` dinámico basado en `SKU_RANGE_SIZE` |
| H-008 | Query de listado de SKUs duplicada en dos capas | 1, 2 | `product_service.py:26` vs. `purchase_repository.py::list_sku_prefixes` | Cambiar la regla de "qué SKUs cuentan" exige tocar 2 sitios sin garantía de sincronía | Extraer a un único punto (repo o helper) que ambos callers usen |
| H-009 | Import muerto `func` en `outbox.py` | 1 | `app/sync/outbox.py:8` | Ruido de linter, sin impacto funcional | Quitar el import |
| H-010 | `outbox.py` sin capa de repositorio (acceso ORM directo) | 3 | `app/sync/outbox.py:33-38` | Se aparta del patrón Fase B; fue trade-off documentado (design.md P1 §T2) pero acumula deuda si `app/sync/` crece | Si P2/P3 añaden más funciones a `app/sync/`, extraer un `SyncEventRepository` antes de que crezca más |

---

## 5. Matriz impacto vs esfuerzo

```
Alto impacto │  🔥 QUICK WINS                    🎯 ESTRATÉGICOS
             │  H-001 (fix boundary, ~15 min)     H-004 (constraint único + fix
             │  H-002 (ampliar hash, ~10 min)     concurrencia, requiere decidir
             │  H-003 (try/except+logging,        estrategia de lock)
             │  ~20 min, patrón ya existe)
             │  H-006 (normalizar case, ~10 min)
             │
Bajo impacto │  📋 BAJA PRIORIDAD                ❌ EVITAR
             │  H-007, H-008, H-009 (cosmético/   H-010 (fue trade-off consciente;
             │  bajo riesgo actual)               no vale la pena revertir ahora)
             │  H-005 (checkpoint, no urge en P1
             │  sin callers reales todavía)
             └──────────────────────────────────────────────────────
               Bajo esfuerzo                       Alto esfuerzo
```

**Quick wins recomendados (hacer YA, antes de dar P1 por cerrado):**
1. H-001 — cambiar el chequeo de rango a inclusivo + test de saturación (~20 min)
2. H-003 — try/except + logging en las 2 funciones públicas de `outbox.py` (~20 min)
3. H-002 — incluir `business_id`/`location_id`/`seq`/`status` en el hash (~15 min, requiere migrar el formato del hash — coordinar si ya hay eventos creados en dev)
4. H-006 — normalizar mayúsculas en el filtro de SKU (~10 min)

---

## 6. Backlog priorizado de remediación

| # | Hallazgo | Severidad | Esfuerzo | Impacto | Prioridad |
|---|----------|-----------|----------|---------|-----------|
| 1 | H-001 Boundary bug SKU | Crítico | Bajo | Alto | 🔴 Inmediata — bloquea cierre de P1 |
| 2 | H-003 Sin logging/try-except | Alto | Bajo | Alto | 🔴 Inmediata |
| 3 | H-002 Hash incompleto | Alto | Bajo | Alto | 🟠 Antes de que P2 dependa del outbox |
| 4 | H-004 Race condition seq | Alto | Medio | Medio (latente, sin callers hoy) | 🟠 Antes de que P3 conecte callers concurrentes |
| 5 | H-006 Case-sensitivity | Medio | Bajo | Medio | 🟠 Este ciclo |
| 6 | H-011 Tests de bordes faltantes | Medio | Bajo | Alto (previene reincidencia) | 🟠 Este ciclo, junto con los fixes de arriba |
| 7 | H-005 verify_chain sin checkpoint | Medio | Medio | Bajo (sin callers hoy) | 🟡 Antes de P5 (e2e con volumen real) |
| 8 | H-007 zfill ancho fijo | Bajo | Bajo | Bajo | 🟡 Backlog |
| 9 | H-008 Duplicación de query SKU | Bajo | Bajo | Bajo | 🟡 Backlog |
| 10 | H-009 Import muerto | Bajo | Trivial | Bajo | 🟡 Backlog |
| 11 | H-010 Sin repo en app/sync/ | Bajo | Medio | Bajo (hoy) | 🟡 Revisar al crecer P2/P3 |

---

## 7. Plan 30/60/90 días

### 30 días (antes de avanzar a P2)
- [ ] Corregir H-001, H-002, H-003, H-006 → re-ejecutar `pytest tests/unit` verde
- [ ] Añadir los 3 tests de H-011 (saturación, concurrencia, minúsculas)
- [ ] Re-auditar P1 con los fixes aplicados
**Criterio de cierre:** score de re-auditoría ≥ 3.8 antes de generar el PAE de P2 sobre esta base.

### 60 días (durante P2/P3)
- [ ] H-004 resuelto con constraint `UNIQUE(business_id, seq)` antes de que P3 conecte los hooks de venta/cliente al outbox
- [ ] H-005 (checkpoint de `verify_chain`) si el volumen de eventos en dev lo justifica
**Criterio de cierre:** `append_event` seguro bajo concurrencia real (test de carrera verde).

### 90 días (cierre del épico, con P5)
- [ ] H-010 revisado — decidir si `app/sync/` necesita capa de repositorio propia según cuánto haya crecido
- [ ] Backlog de bajos (H-007, H-008, H-009) limpiado
**Criterio de cierre:** score ≥ 4.2 en la auditoría de cierre del épico completo (P1-P5).

---

## 8. Criterios de cierre

- [ ] H-001, H-002, H-003 resueltos (críticos/altos con impacto directo en correctness/observabilidad)
- [ ] H-006, H-011 resueltos (correctness + cobertura de test)
- [ ] Score de re-auditoría ≥ 3.8
- [ ] Suite unit 100% verde incluyendo los 3 tests nuevos de borde
- [ ] `git diff --name-only develop` sigue mostrando exactamente el scope de 3.2 del PAE-P1 (sin regresión de scope al aplicar los fixes)

---

## 9. Remediación aplicada (2026-08-28, misma sesión)

Fixes aplicados sobre el mismo árbol (aún sin commitear), verificados con
`pytest tests/unit` → **241 passed** (233 previos + 8 tests de borde nuevos),
cobertura 84.5% (≥80%).

| Hallazgo | Acción | Estado |
|----------|--------|--------|
| H-001 | `next_sku_in_range` pasa a rango cerrado `(base, base+SKU_RANGE_SIZE]` + `SKURangeExhausted` mapeado a 409 en `product_service`/`ui_purchases` | ✅ Resuelto |
| H-002 | `compute_event_hash` incluye `business_id`, `location_id`, `seq`, `status`; `verify_chain` además detecta huecos de seq | ✅ Resuelto |
| H-003 | `append_event`/`verify_chain` con try/except + `_log.error(..., exc_info=True)` (patrón `get_logger`) + warnings en integridad rota | ✅ Resuelto |
| H-004 | Constraint `UNIQUE(business_id, seq)` en `models.py` + índice único en migraciones SQLite/PG (adelantado de los 60 días: la tabla está vacía, es gratis y da backstop determinista para el test de H-011) | ✅ Resuelto |
| H-006 | Filtro normaliza a mayúsculas (`s.upper().startswith(prefix.upper())`) para casar con las queries `ilike` | ✅ Resuelto |
| H-007 | Límite de `zfill(6)` (SKU más ancho si `business_id ≥ 100`) documentado en docstring | ✅ Resuelto |
| H-009 | Import muerto `func` eliminado de `outbox.py` | ✅ Resuelto |
| H-011 | 8 tests nuevos: saturación de rango, `ceiling` inclusivo, SKU en minúsculas, tamper de `business_id`/`location_id`/`seq`/`status`, seq duplicado rechazado por constraint | ✅ Resuelto |
| H-005 | Checkpoint de `verify_chain` | ⏸️ Deferido a P5 (sin callers reales hoy) |
| H-008 | Query de SKU duplicada en dos capas (`product_service`/`purchase_repository`) | ⏸️ Backlog |
| H-010 | Capa de repositorio en `app/sync/` | ⏸️ Revisar al crecer P2/P3 |

**Nota de diseño (H-002):** `status` está en la cadena canónica del hash, así que la
transición legítima `pending → synced` (P3) exige recalcular el hash del evento cambiado
y de los que le siguen — ya documentado en el docstring de `compute_event_hash`.

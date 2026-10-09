# Auditoría de Ingeniería (Re-auditoría) — offline-desktop-sync / Slice P1 (Fundamentos)

**Fecha:** 2026-08-28
**Proyecto:** gestion_inventario
**Rama:** develop (cambios sin commitear, con la remediación de la auditoría previa aplicada)
**Score:** 3.80 / 5.00 🟡 (delta +0.90 vs 2.90 🔴)
**Responsable:** Claude Code (auditoria-ingenieria)

---

## 1. Contexto y método

**Alcance:** re-auditoría del slice P1 del épico `offline-desktop-sync` — outbox
(`app/sync/outbox.py`, modelo `SyncEvent`) y numeración de SKU por rango
(`app/services/sku_range.py`), tras aplicar la remediación de
`docs/auditorias/2026-08-28-p1-foundations-offline-sync.md` (§9).

**Archivos auditados:** 8 archivos de producción/test del slice P1
(`app/services/sku_range.py`, `app/sync/outbox.py`, `app/models.py`,
`app/migrations.py`, `app/services/product_service.py`, `app/routers/ui_purchases.py`,
`tests/unit/test_sku_range.py`, `tests/unit/test_sync_outbox.py`).

**Criterios:** 10 dimensiones de la plantilla de auditoría de ingeniería del proyecto.

**Evidencia de ejecución real:** `python -m pytest tests/unit` (con `.venv` activado) →
**241 tests, 100% verdes**, cobertura 84.51% (≥80% requerido). Confirmado con exit code 0.

**Limitaciones:** mismo alcance que la auditoría previa — P1 es infraestructura pura sin
endpoints HTTP nuevos; dimensiones 4 (seguridad) y 8 (config) tienen superficie mínima y se
evalúan con esa salvedad. No se auditó el resto del repo.

---

## 2. Checklist por dimensión

| # | Dimensión | Peso | Veredicto | Score | Evidencia |
|---|-----------|------|-----------|-------|-----------|
| 1 | SoC / SRP | 10% | ✅ | 4 | `sku_range.py` módulo puro (una responsabilidad); `outbox.py` cohesivo por dominio con `compute_event_hash` separada como función pura (`outbox.py:17`) |
| 2 | Cohesión / Acoplamiento | 10% | ✅ | 4 | Sin imports circulares; `next_sku_in_range` puro (no depende de sesión); módulos pequeños y autocontenidos |
| 3 | Capas / Límites | 10% | ⚠️ | 3 | `outbox.py:60` hace `select()` ORM directo sin repositorio (H-010) — trade-off documentado (design.md P1 §T2); la dirección de dependencias es correcta (sync → models) |
| 4 | Seguridad | 15% | ✅ | 4 | Sin endpoints/auth nuevos en P1; `payload: dict` sin schema/validación todavía — deuda latente para cuando P3 conecte input de red |
| 5 | Robustez / Resiliencia | 15% | ✅ | 4 | `append_event`/`verify_chain` con try/except + `_log.error(exc_info=True)` (H-003 resuelto, `outbox.py:59,96,111,147`); saturación de rango lanza `SKURangeExhausted` en vez de colisión (H-001, `sku_range.py:52`); carrera de seq con backstop de constraint (H-004) |
| 6 | Consistencia transaccional | 10% | ✅ | 4 | H-001 resuelto (rango cerrado `(base, base+SKU_RANGE_SIZE]`, `sku_range.py:50`); `UNIQUE(business_id, seq)` en `models.py` + migraciones SQLite/PG impide seq duplicado silencioso; append lee `max(seq)` y escribe en la misma transacción (sin commit propio) |
| 7 | Observabilidad | 10% | ✅ | 4 | `_log.error(..., exc_info=True)` en ambos métodos públicos + `_log.warning` cuando `verify_chain` detecta integridad rota (`outbox.py:123,139`); patrón `get_logger` (JSON + request_id, H-043) |
| 8 | Configuración / Entornos | 5% | ✅ | 4 | `SKU_RANGE_SIZE=10000` constante de dominio; sin hardcodes de URL/secretos |
| 9 | Pruebas / Calidad | 10% | ✅ | 4 | 241/241 verdes (233 + 8 nuevos), cobertura 84.5%; los 3 bordes que rompieron el código (saturación H-001, seq duplicado H-004, minúsculas H-006) ahora tienen test explícito |
| 10 | Escalabilidad | 5% | ❌ | 2 | `verify_chain` trae y recorre TODO el historial del negocio en cada llamada sin checkpoint (H-005) — caso "días offline" (miles de eventos) sin cota |

**Leyenda:** ✅ OK (4-5) · ⚠️ PARCIAL (2-3) · ❌ NO (<2)

---

## 3. Scorecard ponderado

```
Dimensión                        Peso    Score   Peso×Score
────────────────────────────────────────────────────────────
1. SoC / SRP                     10%     4       4.0
2. Cohesión / Acoplamiento       10%     4       4.0
3. Capas / Límites               10%     3       3.0
4. Seguridad                     15%     4       6.0
5. Robustez / Resiliencia        15%     4       6.0
6. Consistencia transaccional    10%     4       4.0
7. Observabilidad                10%     4       4.0
8. Configuración / Entornos       5%     4       2.0
9. Pruebas / Calidad             10%     4       4.0
10. Escalabilidad                 5%     2       1.0
────────────────────────────────────────────────────────────
TOTAL                           100%             38.0 / 100
SCORE FINAL                                      3.80 / 5.00 🟡
```

**Interpretación:** 🟡 3.2–4.19 — **Estable con deuda relevante.** Salida de la zona roja
(delta +0.90), P1 habilitado para avanzar a P2. La deuda restante es estructural
(H-005 checkpoint, H-010 capa de repo) con mitigación ya documentada y sin callers reales hoy.

**Evolución:** 2.90 🔴 (2026-08-28, previa) → **3.80 🟡** (esta re-auditoría).

---

## 4. Hallazgos

### 🔴 Críticos

Ninguno. H-001 (boundary SKU), H-002 (hash incompleto), H-003 (sin logging) y H-004 (race seq)
quedaron resueltos en la remediación (§9 de la auditoría previa).

### 🟠 Altos

Ninguno.

### 🟡 Medios

| ID | Título | Dimensión | Evidencia | Riesgo | Recomendación |
|----|--------|-----------|-----------|--------|---------------|
| H-005 | `verify_chain` sin checkpoint escala mal | 10 | `app/sync/outbox.py:112-118` | Con miles de eventos acumulados (días offline — el caso de uso normal del diseño), cada verificación recalcula sha256 desde `seq=1` — costo creciente sin cota | Guardar checkpoint "verificado hasta seq X" y verificar solo el delta (deferido a P5, sin callers reales hoy) |

### 🟢 Bajos

| ID | Título | Dimensión | Evidencia | Riesgo | Recomendación |
|----|--------|-----------|-----------|--------|---------------|
| H-008 | Query de listado de SKUs duplicada en dos capas | 1, 2 | `product_service.py:27` vs. `purchase_repository.py:35` | Cambiar la regla de "qué SKUs cuentan" exige tocar 2 sitios | Extraer a un único punto (repo o helper) que ambos callers usen — backlog |
| H-010 | `outbox.py` sin capa de repositorio | 3 | `app/sync/outbox.py:60` | Acceso ORM directo en la capa de dominio — trade-off documentado (T2) | Si P2/P3 añaden más funciones a `app/sync/`, extraer `SyncEventRepository` antes de que crezca |
| H-012 | `except Exception` amplio en `append_event`/`verify_chain` | 5 | `app/sync/outbox.py:96,147` | No distingue IntegrityError de otros errores de BD — hoy es correcto porque log + re-raise | Al conectar callers en P2/P3, diferenciar `IntegrityError` (esperable por el constraint) del resto |

**Resueltos en esta remediación** (ver §9 de la auditoría previa): H-001, H-002, H-003,
H-004, H-006, H-007, H-009, H-011.

**Nota de implementación de H-011:** el test pedido `test_concurrent_append_event_seq` se
implementó como `test_duplicate_seq_same_business_rejected` (`tests/unit/test_sync_outbox.py`)
— prueba el backstop determinista del constraint `UNIQUE(business_id, seq)` simulando la
escritura duplicada. Un test de threads sobre SQLite en memoria es no determinista/flaky;
probar el constraint es la guarda de regresión significativa.

---

## 5. Matriz impacto vs esfuerzo

```
Alto impacto │  🔥 QUICK WINS                    🎯 ESTRATÉGICOS
             │  (ninguno crítico pendiente)      H-005 (checkpoint verify_chain,
             │                                    ~medio esfuerzo, antes de P5)
             │
Bajo impacto │  📋 BAJA PRIORIDAD                ❌ EVITAR
             │  H-007 (documentado ✓)            H-010 (trade-off consciente;
             │  H-008 (dedupe query SKU)          no revertir ahora)
             │  H-012 (except específico,
             │   solo al conectar P2/P3)
             └──────────────────────────────────────────────────────
               Bajo esfuerzo                       Alto esfuerzo
```

**Quick wins recomendados:** ninguno bloquea el cierre de P1. Los cuatro hallazgos de la
prioridad inmediata (H-001/H-002/H-003/H-006) ya están resueltos.

---

## 6. Backlog priorizado de remediación

| # | Hallazgo | Severidad | Esfuerzo | Impacto | Prioridad |
|---|----------|-----------|----------|---------|-----------|
| 1 | H-005 verify_chain sin checkpoint | Medio | Medio | Bajo (sin callers hoy) | 🟡 Antes de P5 (e2e con volumen real) |
| 2 | H-008 Duplicación de query SKU | Bajo | Bajo | Bajo | 🟡 Backlog |
| 3 | H-012 except Exception amplio | Bajo | Trivial | Bajo (hoy correcto) | 🟡 Al conectar P2/P3 |
| 4 | H-010 Sin repo en app/sync/ | Bajo | Medio | Bajo (hoy) | 🟡 Revisar al crecer P2/P3 |

---

## 7. Plan 30/60/90 días

### 30 días (avanzar a P2)
- [x] H-001, H-002, H-003, H-004, H-006 corregidos → `pytest tests/unit` verde (241)
- [x] Tests de bordes de H-011 añadidos (saturación, seq duplicado, minúsculas, tamper por campo)
- [ ] Generar el PAE de P2 sobre esta base (criterio de cierre de P1 cumplido: ≥ 3.8)
**Criterio de cierre:** re-auditoría ≥ 3.8 ✅ (3.80)

### 60 días (durante P2/P3)
- [ ] Conectar los callers reales (venta/cliente) al outbox; si hay concurrencia real en el
      servidor, evaluar retry de `append_event` sobre `IntegrityError` (H-012 como guía)
- [ ] H-008: unificar la query de SKUs en un único punto
**Criterio de cierre:** `append_event` seguro bajo concurrencia real (el constraint ya lo garantiza)

### 90 días (cierre del épico, con P5)
- [ ] H-005: checkpoint de `verify_chain` si el volumen de eventos en dev lo justifica
- [ ] H-010: decidir si `app/sync/` necesita capa de repositorio según cuánto haya crecido
**Criterio de cierre:** score ≥ 4.2 en la auditoría de cierre del épico completo (P1-P5)

---

## 8. Criterios de cierre

- [x] H-001, H-002, H-003 resueltos (críticos/altos con impacto directo en correctness/observabilidad)
- [x] H-006, H-011 resueltos (correctness + cobertura de test)
- [x] Score de re-auditoría ≥ 3.8 → **3.80** ✅
- [x] Suite unit 100% verde incluyendo los tests nuevos de borde (241 passed)
- [x] `git diff --name-only develop` respeta el scope del PAE-P1 (sin regresión de scope)

**P1 queda habilitado para mergear a `develop`** (los fixes están en el mismo árbol sin
commitear; falta el commit del slice completo).

---

*Informe generado por Claude Code (skill: auditoria-ingenieria).*
*Re-auditoría recomendada: en el cierre del épico P1-P5 o tras P5 (con volumen real).*

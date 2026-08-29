# Auditoría de Ingeniería (Re-auditoría) — offline-desktop-sync / Slice P2 (Backend de sincronización)

**Fecha:** 2026-08-28
**Proyecto:** gestion_inventario
**Rama:** develop (remediación aplicada, sin commitear)
**Score:** 3.70 / 5.00 🟡 (delta +1.20 vs 2.50 🔴)
**Responsable:** Claude Code (auditoria-ingenieria), remediación aplicada directamente (no vía OpenCode)

---

## 1. Contexto y método

**Alcance:** re-auditoría del slice P2 tras aplicar la remediación completa de
`docs/auditorias/2026-08-28-p2-sync-backend-offline-sync.md` (8 hallazgos, 3 críticos + 3
altos + 2 medios/bajos abordados).

**Proceso de verificación:** segunda ronda de `/code-review` (4 ángulos, incluyendo
**mutation-testing real** por dos de los agentes — revertir cada fix y confirmar que el test
correspondiente falla) sobre el código post-remediación, no solo lectura estática.

**Evidencia de ejecución real:** `python -m pytest tests/unit -q` (con `.venv` activado) →
**267 tests, 100% verdes**, cobertura 84.51%. Confirmado con exit code 0. Migración v6
(con el nuevo índice único compuesto) verificada dos veces contra una BD SQLite en archivo
real (idempotencia confirmada, no solo en memoria).

**Hallazgo del propio proceso de remediación:** la corrección inicial de H-001..H-013
introdujo una **regresión nueva** — al "scopear por negocio" el chequeo de colisión de
`client_id` en `_apply_customer`, se rompió la detección de colisión real, porque
`Customer.client_id` sigue siendo `unique=True` **global** (mismo patrón que
`products.sku`/ADR-012). La segunda ronda de `/code-review` la detectó antes de cerrar la
sesión y se corrigió en el mismo pase. Se documenta explícitamente porque es la tercera vez
en este proyecto que "scopear por negocio algo que la BD trata como global" causa un bug
(SKU en v0.4.6, este mismo patrón en P2 dos veces: el hallazgo original H-003/H-004 y esta
regresión de remediación).

---

## 2. Checklist por dimensión

| # | Dimensión | Peso | Veredicto | Score | Evidencia |
|---|-----------|------|-----------|-------|-----------|
| 1 | SoC / SRP | 10% | ⚠️ | 3 | Sin cambio — `_apply_customer` sigue con política de negocio embebida en `SyncService`, no un `CustomerService` (deuda aceptada, no bloqueante) |
| 2 | Cohesión / Acoplamiento | 10% | ✅ | 4 | Mejorado: `_dump_columns()` centraliza la lógica de exclusión de columnas sensibles, reutilizada por los 3 métodos de dump (antes duplicada) |
| 3 | Capas / Límites | 10% | ✅ | 4 | Mejorado: `register_device` ahora usa `LocationRepository.location_id_for_code` en vez de reimplementar la resolución (H-010 resuelto) |
| 4 | **Seguridad** | 15% | ✅ | **4** | Los 3 críticos (IDOR, password_hash, fuga cross-tenant) y 2 de los 3 altos (event_id global, sin location scoping) verificados CORREGIDOS por mutation-testing independiente. Rate limit + expiración de token añadidos. Resta: `X-Forwarded-For` sin validar contra proxy de confianza (debilidad heredada de `ui_auth.py`, no nueva, no bloqueante) |
| 5 | Robustez / Resiliencia | 15% | ✅ | 4 | Atomicidad `mark_applied→sale()→rollback` verificada correcta; `sale_stock` ahora distingue causa real (H-006); el location_code se resuelve ANTES de `sale()` en vez de dejar que auto-cree una ubicación fantasma (endurecido tras el re-review, cierra el mismo patrón del incidente v0.4.5) |
| 6 | Consistencia transaccional | 10% | ✅ | 4 | `AppliedSyncEvent` con `UniqueConstraint(business_id, event_id)` verificado (H-004); regresión de `client_id` scope detectada y corregida en el mismo ciclo — el chequeo de colisión vuelve a ser global, coherente con el constraint real de BD |
| 7 | Observabilidad | 10% | ⚠️ | 3 | Sin cambios — logging sigue mínimo (un solo `_log.error` en el path genérico), no hay logging específico por tipo de conflicto |
| 8 | Configuración / Entornos | 5% | ✅ | 4 | `DEVICE_RATE_LIMIT`/`DEVICE_RATE_WINDOW`/`DEVICE_TOKEN_MAX_INACTIVITY_DAYS` añadidos siguiendo el patrón centralizado existente (`app/settings.py`) |
| 9 | Pruebas / Calidad | 10% | ✅ | 4 | 267/267 verdes; los tests multi-tenant nuevos (H-013) **verificados por mutation-testing real** (dos agentes revirtieron cada fix y confirmaron que el test correspondiente falla) — no son tests que pasarían igual con el código viejo |
| 10 | Escalabilidad | 5% | ❌ | 2 | Sin cambio — H-008 (`build_pull` sin filtro incremental) sigue como deuda aceptada, documentada, sin callers de producción todavía |

**Leyenda:** ✅ OK (4-5) · ⚠️ PARCIAL (2-3) · ❌ NO (<2)

---

## 3. Scorecard ponderado

```
Dimensión                        Peso    Score   Peso×Score
────────────────────────────────────────────────────────────
1. SoC / SRP                     10%     3       3.0
2. Cohesión / Acoplamiento       10%     4       4.0
3. Capas / Límites               10%     4       4.0
4. Seguridad                     15%     4       6.0
5. Robustez / Resiliencia        15%     4       6.0
6. Consistencia transaccional    10%     4       4.0
7. Observabilidad                10%     3       3.0
8. Configuración / Entornos       5%     4       2.0
9. Pruebas / Calidad             10%     4       4.0
10. Escalabilidad                 5%     2       1.0
────────────────────────────────────────────────────────────
TOTAL                           100%             37.0 / 100
SCORE FINAL                                      3.70 / 5.00 🟡
```

**Interpretación:** 🟡 3.2–4.19 — Estable con deuda relevante. Sale de la zona roja
(delta +1.20). **Habilitado para commitear y avanzar a P3**, con deuda residual documentada
(observabilidad, escalabilidad de `build_pull`, `X-Forwarded-For` sin endurecer) que no
bloquea pero debe planificarse.

---

## 4. Hallazgos residuales (no bloqueantes)

### 🟡 Medios

| ID | Título | Dimensión | Evidencia | Riesgo | Recomendación |
|----|--------|-----------|-----------|--------|---------------|
| H-014 | `X-Forwarded-For` sin validar contra proxy de confianza | 4 | `app/sync/auth.py::_real_ip` | Un atacante puede rotar el header en cada intento y anular el rate limit del token de dispositivo | Validar XFF solo si la request viene de una IP de proxy conocida (mismo fix aplicaría a `ui_auth.py`, es deuda compartida) |
| H-015 | `_device_attempts` en memoria de proceso, no compartido entre workers | 4 | `app/sync/auth.py` | Con `--workers N>1` el límite efectivo se multiplica por N | Mover a almacenamiento compartido (Redis) solo si se despliega multi-worker — hoy el despliegue documentado es proceso único |
| H-016 | `DeviceToken.business_id` sin `ForeignKey` explícita | 3, 6 | `app/models.py` (`DeviceToken`) | Sin enforcement a nivel de BD contra un `business_id` inexistente (hoy mitigado indirectamente por la validación de `location_code`) | Añadir `ForeignKey("businesses.id")` en un fix de bajo riesgo, no urgente |

### 🟢 Bajos

| ID | Título | Dimensión | Evidencia | Riesgo | Recomendación |
|----|--------|-----------|-----------|--------|---------------|
| H-008 | `build_pull` sin filtro incremental | 10 | `app/sync/service.py::build_pull` | Deuda aceptada desde el diseño original (design.md P2 §Trade-off T4); sin callers de producción todavía | Diferir a P5 con volumen real |

---

## 5. Plan 30/60/90 días

### 30 días (ya cumplido en esta sesión)
- [x] H-001, H-002, H-003, H-004, H-005, H-006, H-007, H-013 corregidos y verificados por mutation-testing
- [x] Regresión propia (client_id scope) detectada y corregida en el mismo ciclo
- [x] 267 tests verdes, migración v6 validada contra BD real

### 60 días (durante P3)
- [ ] H-014 (XFF trust) — evaluar junto con el mismo fix pendiente en `ui_auth.py`
- [ ] H-016 (FK en DeviceToken) — fix de bajo riesgo, agregar cuando se toque `models.py` de nuevo

### 90 días (cierre del épico, con P5)
- [ ] H-008 (delta incremental en `build_pull`) si el volumen real de P5 lo justifica
- [ ] H-015 (rate limiter compartido) solo si se decide desplegar multi-worker
**Criterio de cierre:** score ≥ 4.2 en la auditoría de cierre del épico completo (P1-P5).

---

## 6. Criterios de cierre

- [x] H-001, H-002, H-003 resueltos y verificados (críticos)
- [x] H-004, H-005, H-007 resueltos y verificados (altos)
- [x] H-013 resuelto — cobertura multi-tenant verificada por mutation-testing
- [x] Regresión de remediación (client_id scope) detectada y corregida
- [x] Score de re-auditoría ≥ 3.2 (umbral mínimo para salir de 🔴) → **3.70** ✅
- [x] Suite unit 100% verde (267 tests) incluyendo los 14 tests nuevos/ajustados
- [x] Migración v6 verificada idempotente contra BD real (no solo in-memory)

**P2 queda habilitado para commitear y avanzar a P3** (backend de sync central), con la
deuda residual (H-014, H-015, H-016, H-008) documentada para los próximos ciclos.

---

*Informe generado por Claude Code (skill: auditoria-ingenieria), tras remediación aplicada
directamente y verificada por `/code-review` con mutation-testing independiente.*

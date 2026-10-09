# Auditoría de Ingeniería — offline-desktop-sync / Slice P2 (Backend de sincronización)

**Fecha:** 2026-08-28
**Proyecto:** gestion_inventario
**Rama:** develop (cambios sin commitear, implementados por OpenCode a partir de `specs/offline-desktop-sync/p2-sync-backend/PAE-p2-sync-backend.md`)
**Score:** 2.50 / 5.00 🔴
**Responsable:** Claude Code (auditoria-ingenieria), tras `/code-review` (medium, 8 hallazgos, 8/8 CONFIRMED)

---

## 1. Contexto y método

**Alcance:** slice P2 del épico `offline-desktop-sync` — backend de sincronización central:
`DeviceToken`/`AppliedSyncEvent`/`SyncConflict` (modelos + migración v6), `SyncService`
(idempotencia, replay vía `InventoryService.sale`, dedup de clientes, `build_pull`), 3
endpoints (`/sync/register`, `/sync/push`, `/sync/conflicts`).

**Archivos auditados:** 10 archivos (7 de producción, 3 de test) en `app/models.py`,
`app/migrations.py`, `app/sync/`, `app/routers/sync.py`, `app/repositories/customer_repository.py`,
`app/main.py`, `tests/unit/`.

**Criterios:** 10 dimensiones de la plantilla de auditoría de ingeniería del proyecto.

**Insumo previo:** `/code-review` (medium effort, 8 ángulos, 1-vote verify) — **8 hallazgos,
8/8 CONFIRMED** por un verificador independiente que leyó el código real línea por línea.
Ratio de confirmación inusualmente alto: no hubo PLAUSIBLE ni REFUTED en esta ronda.

**Evidencia de ejecución real:** `python -m pytest tests/unit -q` (con `.venv` activado) →
**251 tests, 100% verdes**, cobertura 84.51%. Confirmado con exit code 0.

**Limitaciones:** no se auditó el resto del repo (fuera de scope de P2). El alcance de
seguridad de esta auditoría es más profundo que en P1 porque P2 **sí** expone endpoints HTTP
nuevos con autenticación propia — la dimensión 4 no tiene la salvedad "superficie mínima" de
la auditoría de P1.

---

## 2. Checklist por dimensión

| # | Dimensión | Peso | Veredicto | Score | Evidencia |
|---|-----------|------|-----------|-------|-----------|
| 1 | SoC / SRP | 10% | ⚠️ | 3 | El router es delgado (correcto); pero `_apply_customer` implementa una política de negocio completa (dedup, resolución de colisión de `client_id`) dentro de `SyncService` en vez de un `CustomerService` — no existe ese service en el proyecto, la regla queda enterrada en el módulo de sync |
| 2 | Cohesión / Acoplamiento | 10% | ⚠️ | 3 | `_dump()` reimplementa a mano la serialización ORM→dict que el proyecto ya resuelve con Pydantic `from_attributes=True` (`ProductRead`, `MovementRead`); dos mecanismos de serialización divergentes para las mismas tablas |
| 3 | Capas / Límites | 10% | ⚠️ | 3 | `register_device` reimplementa la resolución de ubicación por código en vez de usar `LocationRepository` ya existente — mismo patrón de duplicación que causó bugs de ubicaciones huérfanas en sesiones anteriores (CLAUDE.md, 08-18) |
| 4 | **Seguridad** | 15% | ❌ | **1** | **4 hallazgos críticos/altos de aislamiento multi-tenant confirmados**: H-001 IDOR en `register_device` (sin validar pertenencia del `business_id`), H-002 `password_hash` viaja sin cifrar en el pull, H-003 `movement_allocations`/`sales_document_items` sin filtro de negocio (fuga cross-tenant), H-007 token de dispositivo sin rate limiting ni expiración |
| 5 | Robustez / Resiliencia | 15% | ⚠️ | 3 | La atomicidad `mark_applied → sale() → rollback` está correctamente implementada (verificado); pero H-006 etiqueta todo fallo de `sale()` (404/422/409) como `"sale_stock"`, perdiendo la causa real del error |
| 6 | Consistencia transaccional | 10% | ❌ | 2 | H-004: `AppliedSyncEvent.event_id` único GLOBAL (no compuesto con `business_id`, pese a que la columna existe) permite que un negocio bloquee silenciosamente la sincronización de otro por colisión de `event_id` |
| 7 | Observabilidad | 10% | ⚠️ | 3 | Hay `_log.error(..., exc_info=True)` en el path de error genérico de `push()`, pero no hay logging específico cuando se registra un `SyncConflict` (sale_stock/customer_client_id) ni contexto de `business_id`/`device_id` en los mensajes |
| 8 | Configuración / Entornos | 5% | ✅ | 4 | `MIN_APP_SCHEMA_VERSION=6` es constante de dominio; sin hardcodes de URL/secretos nuevos |
| 9 | Pruebas / Calidad | 10% | ❌ | 2 | 251/251 verdes, pero **`grep` confirma que ningún test de `test_sync_service.py`/`test_sync_router.py` usa una segunda `business_id`** — el sistema es multi-tenant por diseño y su test suite nunca ejercita dos negocios interactuando, exactamente la clase de escenario que hubiera detectado H-001/H-003/H-004 antes de esta auditoría |
| 10 | Escalabilidad | 5% | ❌ | 2 | H-008: `build_pull()` trae la foto completa de 13 tablas en CADA `/sync/push`, sin filtro incremental (`updated_at`/cursor) — mismo patrón de deuda que `verify_chain` en P1 |

**Leyenda:** ✅ OK (4-5) · ⚠️ PARCIAL (2-3) · ❌ NO (<2)

---

## 3. Scorecard ponderado

```
Dimensión                        Peso    Score   Peso×Score
────────────────────────────────────────────────────────────
1. SoC / SRP                     10%     3       3.0
2. Cohesión / Acoplamiento       10%     3       3.0
3. Capas / Límites               10%     3       3.0
4. Seguridad                     15%     1       1.5
5. Robustez / Resiliencia        15%     3       4.5
6. Consistencia transaccional    10%     2       2.0
7. Observabilidad                10%     3       3.0
8. Configuración / Entornos       5%     4       2.0
9. Pruebas / Calidad             10%     2       2.0
10. Escalabilidad                 5%     2       1.0
────────────────────────────────────────────────────────────
TOTAL                           100%             25.0 / 100
SCORE FINAL                                      2.50 / 5.00 🔴
```

**Interpretación:** 🔴 < 3.2 — Alto riesgo, **peor que P1 (2.90)**. **No mergear a `develop`
tal cual.** La causa no es dispersión de deuda menor como en P1 (SoC/observabilidad) — es una
concentración de **4 defectos de aislamiento multi-tenant en el corazón de lo que P2 existe
para hacer** (sincronizar datos de negocios que no deben verse entre sí). Un backend de sync
que filtra datos de un negocio a otro es, en este proyecto, la misma clase de incidente que ya
costó dos sesiones completas de debugging con el bug de SKU (v0.4.6) — pero aquí el radio de
impacto es mayor: no es un 409 molesto, es PII y datos comerciales cruzando el límite de
tenant.

---

## 4. Hallazgos

### 🔴 Críticos

| ID | Título | Dimensión | Evidencia | Riesgo | Recomendación |
|----|--------|-----------|-----------|--------|---------------|
| H-001 | IDOR en `register_device`: no valida pertenencia del `business_id` | 4 | `app/routers/sync.py:27` | Cualquier admin/owner autenticado puede registrar un token de sync para el negocio de OTRO usuario (pasando su `business_id` en el body) y sincronizar datos/ventas contra él indefinidamente | Validar `business_id` contra `get_user_business_ids(user)` (owner) o el negocio activo de sesión (admin), igual que hace `require_active_business_id` |
| H-002 | `password_hash` viaja sin cifrar en el JSON del pull | 4 | `app/sync/service.py:136` (`_dump`) | Los hashes PBKDF2 de todos los usuarios del negocio quedan en el snapshot que recibe cualquier dispositivo — material para ataques offline de fuerza bruta si el equipo se pierde | Excluir explícitamente `password_hash` (y cualquier columna sensible) del `_dump(User, ...)`; nunca depender de "está cifrado en P4" para no filtrar el dato en primer lugar |
| H-003 | `movement_allocations`/`sales_document_items` sin filtro de negocio | 4 | `app/sync/service.py:152` | Fuga de datos comerciales (SKU, cantidades, precios, asignaciones FIFO) de TODOS los negocios del sistema en cada pull, sin importar qué negocio sincroniza | Filtrar vía join con su tabla padre (`SalesDocument.business_id` / `InventoryMovement.business_id`) antes de volcar |

### 🟠 Altos

| ID | Título | Dimensión | Evidencia | Riesgo | Recomendación |
|----|--------|-----------|-----------|--------|---------------|
| H-004 | `event_id` único global permite bloqueo cruzado entre negocios | 6 | `app/models.py:472` | Colisión de `event_id` entre dos negocios distintos bloquea silenciosamente el evento legítimo del segundo como conflicto `apply_error` | Constraint/índice único compuesto `(business_id, event_id)` en vez de `event_id` global |
| H-005 | `device.location_id` nunca restringe qué ubicación puede vender | 3, 4 | `app/sync/service.py:96` | Un token de un POS puede reportar ventas que afectan el stock de CENTRAL u otra ubicación del mismo negocio — el scoping por ubicación que sugiere el modelo no se aplica | Validar `location_code` del evento contra `self._device.location_id` en `_apply_sale` |
| H-007 | Token de dispositivo sin rate limiting ni expiración | 4 | `app/sync/auth.py:13` | Token filtrado (robo de equipo) válido indefinidamente, sin throttling ante intentos — a diferencia del login de usuario que sí tiene ese control | Añadir rate limiting a `/sync/push` (mismo patrón que `ui_auth.py`) y `expires_at`/rotación al `DeviceToken` |

### 🟡 Medios

| ID | Título | Dimensión | Evidencia | Riesgo | Recomendación |
|----|--------|-----------|-----------|--------|---------------|
| H-006 | `_apply_sale` etiqueta todo fallo como `"sale_stock"` | 5 | `app/sync/service.py:90` | Un 404 (SKU no encontrado) o 422 (validación) se confunde con falta de stock, engañando cualquier UI de resolución de conflictos | Inspeccionar `e.status_code`/`detail` y asignar `kind` específico por causa |
| H-008 | `build_pull` trae la foto completa en cada push, sin delta | 10 | `app/sync/service.py:148` | Con volumen real y varios dispositivos sincronizando seguido, cada push paga el costo de serializar todo el histórico del negocio | Filtro incremental (`updated_at`/cursor) — puede diferirse a P5 si no hay callers reales todavía |
| H-013 | Cero test ejercita un segundo negocio (multi-tenant) | 9 | `tests/unit/test_sync_service.py`, `test_sync_router.py` | Toda la clase de bugs de esta auditoría (H-001, H-003, H-004) es invisible a la suite actual por construcción — un sistema multi-tenant sin un solo test de dos tenants interactuando | Añadir `test_register_device_rejects_other_business`, `test_pull_does_not_leak_other_business_data`, `test_duplicate_event_id_across_businesses` antes de mergear |

### 🟢 Bajos

| ID | Título | Dimensión | Evidencia | Riesgo | Recomendación |
|----|--------|-----------|-----------|--------|---------------|
| H-009 | `_dump` reimplementa serialización ORM ya resuelta con Pydantic | 2 | `app/sync/service.py:136` | Dos mecanismos de serialización divergentes para las mismas tablas | Evaluar reusar `from_attributes=True` de los schemas existentes donde aplique |
| H-010 | `register_device` reimplementa resolución de ubicación | 3 | `app/routers/sync.py:34` | Duplica lógica que `LocationRepository` ya centraliza — mismo patrón que causó bugs de ubicaciones huérfanas antes | Usar `LocationRepository.location_id_for_code` |
| H-011 | `_apply_customer` repite 3x el bloque create+mark+commit | 1 | `app/sync/service.py:104-125` | Cambios futuros al flujo de cliente requieren tocar 3 lugares casi idénticos | Extraer un helper interno `_create_and_mark(...)` |
| H-012 | `require_device` hace un commit extra por request solo para `last_seen_at` | 5 | `app/sync/auth.py:20` | `last_seen_at` queda actualizado aunque el push falle después (409 por versión), ensuciando cualquier monitoreo que confíe en ese campo | Mover el update de `last_seen_at` al final del handler, tras confirmar éxito |

---

## 5. Matriz impacto vs esfuerzo

```
Alto impacto │  🔥 QUICK WINS                    🎯 ESTRATÉGICOS
             │  H-001 (validar pertenencia,      H-013 (tests multi-tenant,
             │   ~20 min)                         ~1-2h, pero es la red de
             │  H-002 (excluir password_hash,     seguridad que faltó)
             │   ~10 min)
             │  H-003 (filtrar 2 tablas por
             │   join, ~30 min)
             │  H-004 (constraint compuesto,
             │   ~20 min + migración)
             │  H-005 (validar location_id,
             │   ~15 min)
             │
Bajo impacto │  📋 BAJA PRIORIDAD                ❌ EVITAR
             │  H-006, H-009, H-010, H-011,      (ninguno — todo lo
             │  H-012 (cosmético/bajo riesgo)     encontrado vale la pena)
             │  H-008 (diferible a P5 si no
             │   hay volumen real todavía)
             └──────────────────────────────────────────────────────
               Bajo esfuerzo                       Alto esfuerzo
```

**Quick wins recomendados (hacer YA, bloquean el cierre de P2):**
1. H-001 — validar pertenencia del `business_id` en `register_device` (~20 min)
2. H-002 — excluir `password_hash` del `_dump(User, ...)` (~10 min)
3. H-003 — filtrar `movement_allocations`/`sales_document_items` por join a business_id (~30 min)
4. H-004 — constraint único compuesto `(business_id, event_id)` + migración (~20 min)
5. H-005 — validar `location_code` contra `device.location_id` en `_apply_sale` (~15 min)
6. H-007 — rate limiting + expiración de token (~30-45 min, sigue el patrón de `ui_auth.py`)

Total estimado de los 6 quick wins críticos/altos: **~2 horas**.

---

## 6. Backlog priorizado de remediación

| # | Hallazgo | Severidad | Esfuerzo | Impacto | Prioridad |
|---|----------|-----------|----------|---------|-----------|
| 1 | H-001 IDOR register_device | Crítico | Bajo | Alto | 🔴 Inmediata — bloquea cierre de P2 |
| 2 | H-002 password_hash en pull | Crítico | Bajo | Alto | 🔴 Inmediata |
| 3 | H-003 Fuga cross-tenant (2 tablas) | Crítico | Bajo | Alto | 🔴 Inmediata |
| 4 | H-004 event_id global | Alto | Bajo | Alto | 🔴 Inmediata |
| 5 | H-005 location_id sin usar | Alto | Bajo | Medio-Alto | 🟠 Antes de P3 |
| 6 | H-007 Sin rate limit/expiración | Alto | Medio | Medio | 🟠 Antes de P3 |
| 7 | H-013 Tests multi-tenant faltantes | Medio | Medio | Alto (previene reincidencia) | 🟠 Junto con los fixes 1-4 |
| 8 | H-006 sale_stock mal etiquetado | Medio | Bajo | Bajo-Medio | 🟡 Este ciclo |
| 9 | H-008 build_pull sin delta | Medio | Medio | Bajo (sin callers hoy) | 🟡 Antes de P5 |
| 10 | H-009/H-010/H-011/H-012 | Bajo | Bajo | Bajo | 🟡 Backlog |

---

## 7. Plan 30/60/90 días

### 30 días (antes de avanzar a P3)
- [ ] Corregir H-001, H-002, H-003, H-004 (críticos/altos de aislamiento — ~1.5h)
- [ ] Corregir H-005, H-007 (~45 min)
- [ ] Añadir los 3 tests de H-013 (multi-tenant) → re-ejecutar suite verde
- [ ] Re-auditar P2 (objetivo ≥3.8, mismo umbral que se usó para cerrar P1)
**Criterio de cierre:** score de re-auditoría ≥ 3.8 antes de generar el detalle de implementación de P3.

### 60 días (durante P3/P4)
- [ ] H-006 (etiquetado de conflictos) y H-012 (last_seen_at) resueltos
- [ ] H-008 evaluado con volumen real de dev/staging antes de P5
**Criterio de cierre:** `build_pull` no es el cuello de botella dominante en pruebas de carga informales.

### 90 días (cierre del épico, con P5)
- [ ] H-009, H-010, H-011 (limpieza) resueltos si el tiempo lo permite
- [ ] Revisar si el patrón de aislamiento multi-tenant roto en P2 (4 hallazgos) amerita una
      regla nueva institucionalizada en el harness (ej. "todo endpoint/service multi-tenant
      nuevo requiere al menos un test con 2 tenants antes de auditar")
**Criterio de cierre:** score ≥ 4.2 en la auditoría de cierre del épico completo (P1-P5).

---

## 8. Criterios de cierre

- [ ] H-001, H-002, H-003, H-004 resueltos (críticos/altos de aislamiento multi-tenant)
- [ ] H-005, H-007 resueltos (scoping de ubicación + hardening de auth de dispositivo)
- [ ] H-013 resuelto (cobertura de test multi-tenant)
- [ ] Score de re-auditoría ≥ 3.8
- [ ] Suite unit 100% verde incluyendo los tests nuevos multi-tenant
- [ ] `git diff --name-only develop` sigue mostrando exactamente el scope de 3.2 del PAE-P2

---

## Nota de proceso

Este es el segundo slice consecutivo (tras P1) donde `/code-review` + `auditoria-ingenieria`
encuentran hallazgos que bloquean el merge — y en ambos casos el patrón de fondo es el mismo:
**el PAE especificaba fielmente lo que pedía, pero no pedía verificar el borde que rompió el
código** (en P1: saturación del rango de SKU; en P2: aislamiento entre negocios en cada punto
donde el código toca datos de más de un tenant). Vale la pena que el pre-mortem de Fase 2 del
SDD-Agent (§2.9, dimensiones 1,2,3,5,6) incorpore explícitamente una pregunta de multi-tenancy
("¿todo endpoint/query nuevo que toca datos compartidos está scoped por `business_id` en el
punto de lectura Y de escritura, no solo en uno de los dos?") para los PAEs de P3/P4/P5 que
faltan.

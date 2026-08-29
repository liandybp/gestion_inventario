# Auditoría de Ingeniería — offline-desktop-sync P3 (Cliente de sync + escritura offline)

**Fecha:** 2026-08-29
**Proyecto:** gestion_inventario
**Rama:** `develop` (working tree, **sin commitear**)
**Score:** 2.35 / 5.00 🔴
**Responsable:** Claude Code (auditoria-ingenieria + /code-review previo)

---

## 1. Contexto y método

**Alcance:** slice P3 del épico `offline-desktop-sync` (`specs/offline-desktop-sync/p3-sync-client/`),
implementado por OpenCode sobre el PAE generado por SDD-Agent. Cubre: modo `APP_MODE=desktop`,
hooks de outbox en venta/alta de cliente, cliente de sincronización (`app/sync/client.py`, push +
`apply_pull`), router `ui_sync` + partial de banner, gating de pestañas offline en el dashboard.

**Archivos auditados (12, inventario exacto del PAE):** `app/settings.py`, `app/models.py`,
`app/migrations.py`, `app/routers/ui_sales.py`, `app/routers/ui_customers.py`, `app/sync/client.py`,
`app/routers/ui_sync.py`, `app/templates/partials/sync_status.html`, `app/main.py`,
`app/routers/ui_home.py`, `app/templates/dashboard.html`, `tests/unit/test_sync_client.py`.
Revisados además para verificar interoperabilidad (no modificados en este diff, ya en `develop`):
`app/sync/outbox.py` (P1), `app/sync/service.py` (P2), `app/schemas.py::SaleCreate`,
`app/services/inventory_service.py::sale`.

**Criterios:** 10 dimensiones ponderadas de la plantilla de auditoría de ingeniería.
**Método:** `/code-review` táctico previo (8 ángulos independientes vía subagentes + verificación),
más reproducción empírica directa de los hallazgos de mayor severidad (ejecución de
`apply_pull`/`sync`/`verify_chain` contra una BD SQLite real con `PRAGMA foreign_keys=ON`) antes de
puntuar cada dimensión.
**Limitaciones:** el scope es el diff de P3, no el épico completo (P1/P2 ya auditados y
committeados, ver `docs/auditorias/2026-08-28-*` y `2026-08-28-p2-*`). No se auditó el flujo
multi-nodo (P5, fuera de scope de P3 por diseño).

---

## 2. Checklist por dimensión

| # | Dimensión | Peso | Veredicto | Score | Evidencia |
|---|-----------|------|-----------|-------|-----------|
| 1 | SoC / SRP | 10% | ✅ | 4 | Hooks de outbox son una sola llamada a `append_event`; transporte vive en `client.py`; router solo delega. Duplicación menor: `dashboard.html` reimplementa el banner en vez de `{% include %}` `sync_status.html`. |
| 2 | Cohesión / Acoplamiento | 10% | ⚠️ | 2 | `_PULL_MODELS` (client.py:20) hardcodea el mismo set de tablas que `SyncService.build_pull` (service.py) sin fuente compartida; el orden de borrado no modela el grafo de FKs real del esquema — acoplamiento implícito y no verificado a la estructura de `app/models.py`. |
| 3 | Capas / Límites | 10% | ❌ | 2 | El límite "compra/traspaso/copiloto = online-only" declarado en el diseño (REQ-19) no se aplica en la capa correcta (router/servicio); solo se intenta ocultar en la plantilla, y ni eso funciona del todo (H-005). |
| 4 | Seguridad | 15% | ⚠️ | 3 | `/ui/sync` reutiliza la sesión existente (correcto); pero el límite declarado de escritura offline (solo venta/cliente) no está enforced server-side en compras/traspasos (H-006) — el server confía en que el cliente desktop se comporte, no en un guard propio. |
| 5 | Robustez / Resiliencia | 15% | ❌ | 1 | `apply_pull` falla de forma determinística y reproducible en cualquier sync real (orden de borrado viola FKs bajo `PRAGMA foreign_keys=ON`, y aunque se corrigiera el orden, reinserta `User` sin `password_hash` violando NOT NULL) — H-001/H-002, verificado empíricamente. El router `do_sync` no envuelve `run_sync()` en try/except: cualquier excepción no capturada por `client.py` llega como 500 crudo sin romper el swap de HTMX de forma controlada. |
| 6 | Consistencia transaccional | 10% | ⚠️ | 2 | `apply_pull` sí hace rollback correcto ante error (no dejamedias-tablas) — punto a favor. Pero: (a) marcar `synced` sin recalcular `event_hash` rompe la cadena de integridad de P1 (H-004); (b) el commit de la venta/cliente y el `append_event` posterior no son atómicos (H-007). |
| 7 | Observabilidad | 10% | ✅ | 3 | `client.py` loguea con `exc_info=True` de forma consistente (mismo patrón que P1/P2). Falta try/except en el router `ui_sync.py::do_sync` (la excepción no queda contextualizada a nivel HTTP) y no hay métrica de tasa de éxito/fallo de sync. |
| 8 | Configuración / Entornos | 5% | ✅ | 4 | `APP_MODE` vía `env()` existente, sin hardcodes, migración aditiva (`IF NOT EXISTS`), consistente con el resto de `settings.py`. |
| 9 | Pruebas / Calidad | 10% | ❌ | 1 | 4 tests superficiales (`test_sync_client.py`), ninguno cubre `is_desktop()==True` en los hooks, ni `apply_pull`/`sync()` con un snapshot realista (con `users` o con tablas FK-dependientes) — exactamente donde viven H-001/H-002. `tests/unit/test_offline_hooks.py` prometido en `design.md` §2.1 nunca se creó. Cero e2e pese a la regla dura del `CLAUDE.md` raíz ("Toda feature nueva lleva tests e2e... no se da por terminada sin ellos"). |
| 10 | Escalabilidad | 5% | ⚠️ | 3 | `apply_pull` hace `DELETE` + reinsert completo de las 13 tablas en cada sync (no incremental) — aceptado como trade-off v1 en `design.md` T3, pero no escala con el historial acumulado. Doble commit por venta (servicio + hook) duplica fsyncs. |

**Leyenda:** ✅ OK (4-5) · ⚠️ PARCIAL (2-3) · ❌ NO (<2)

---

## 3. Scorecard ponderado

```
Dimensión                        Peso    Score   Peso×Score
────────────────────────────────────────────────────────────
1. SoC / SRP                     10%     4       0.40
2. Cohesión / Acoplamiento       10%     2       0.20
3. Capas / Límites               10%     2       0.20
4. Seguridad                     15%     3       0.45
5. Robustez / Resiliencia        15%     1       0.15
6. Consistencia transaccional    10%     2       0.20
7. Observabilidad                10%     3       0.30
8. Configuración / Entornos       5%     4       0.20
9. Pruebas / Calidad             10%     1       0.10
10. Escalabilidad                 5%     3       0.15
────────────────────────────────────────────────────────────
TOTAL                          100%             2.35 / 5.00
SCORE FINAL                                      2.35 / 5.00 🔴
```

**Interpretación:** 🔴 Alto riesgo — acción inmediata requerida. **No mergear ni desplegar en este
estado**: el flujo principal de sincronización (`apply_pull`) falla de forma garantizada y
reproducible en cualquier sync real, no solo en casos borde. La suite unitaria reporta "265 verdes"
porque ningún test ejercita ese camino — el verde es un falso negativo, no evidencia de que la
feature funcione.

---

## 4. Hallazgos

### 🔴 Críticos

| ID | Título | Dimensión | Evidencia | Riesgo | Recomendación |
|----|--------|-----------|-----------|--------|---------------|
| H-001 | `apply_pull` borra tablas en orden que viola FKs | 5, 6 | `app/sync/client.py:62` (`db.query(Business).delete()` corre antes que sus hijas) | Con `PRAGMA foreign_keys=ON` (invariante del proyecto), el primer `DELETE FROM businesses` con datos locales no vacíos lanza `IntegrityError: FOREIGN KEY constraint failed` — verificado por reproducción directa. Todo sync después del primero (o de cualquier provisión con datos) falla. | Ordenar `_PULL_MODELS` por dependencia inversa de FK (hijas antes que padres) o desactivar temporalmente `PRAGMA foreign_keys` dentro de la transacción de `apply_pull` y reactivarlo al final. |
| H-002 | `apply_pull` reinserta `User` sin `password_hash` (NOT NULL) | 4, 5 | `app/sync/client.py:65` (`_PULL_MODELS["users"] = User`) vs `app/sync/service.py:41` (`_SENSITIVE_COLUMNS = {User: {"password_hash"}}`) | Incluso arreglado H-001, cualquier pull con `users` (siempre, P2 lo dumpea completo) lanza `IntegrityError: NOT NULL constraint failed: users.password_hash` — verificado por reproducción directa. El sync nunca puede completar la tabla `users`. | Excluir `users` de `_PULL_MODELS` (no se necesita reemplazar usuarios completos en el nodo desktop — solo login local) o hacer un upsert por `username` que preserve `password_hash` existente en vez de delete+insert ciego. |
| H-003 | Venta offline sin fecha explícita queda irrecuperable | 5, 6 | `app/routers/ui_sales.py:209,298` (`"movement_date": str(movement_date)`) | `str(None) == "None"` (string literal) viaja en el payload del outbox; al reintentar en el server, `SaleCreate(movement_date="None")` lanza `ValidationError` de Pydantic (verificado), no capturada como `HTTPException` en `_apply_sale` — la venta queda para siempre en conflicto `apply_error`, sin reintento posible (payload determinístico). Es el caso más común (venta rápida sin fecha manual). | Enviar el valor crudo del form (`movement_date`, puede ser `None`) sin `str()`, tal como especificaba el envelope T4 original del PAE — `None` serializa a `null` en JSON y Pydantic lo acepta como campo opcional ausente. |

### 🟠 Altos

| ID | Título | Dimensión | Evidencia | Riesgo | Recomendación |
|----|--------|-----------|-----------|--------|---------------|
| H-004 | Marcar evento `synced` rompe la cadena de hash de P1 | 6 | `app/sync/client.py:95` (`e.status = "synced"` sin recalcular `event_hash`) | `compute_event_hash` incluye `status` en el hash (documentado explícitamente en `app/sync/outbox.py`, que advierte que P3 debe recalcular). Verificado: tras marcar `synced`, `verify_chain()` reporta el evento como manipulado permanentemente, aunque nada fue alterado — invalida la garantía de tamper-evidence construida en P1. | Al marcar `synced`, recalcular `event_hash` del evento (y de los que le siguen, ya que cada hash incluye el `prev_hash` del anterior) con el nuevo `status`, o mover `status` fuera del material hasheado (rediseño menor de `compute_event_hash`). |
| H-005 | Gating de pestañas offline incompleto (REQ-19) | 3, 4 | `app/templates/dashboard.html:62-78` (solo se quita `hx-get`, el botón sigue renderizado) y `:176` (FAB del copiloto sin ningún gate de `app_mode`) | En modo desktop, Compras/Traspasos/Copiloto se ven como botones normales pero no hacen nada al click (UX rota, parece un bug) en vez de ocultarse como pide el PAE (T11: envolver en `{% if app_mode != 'desktop' %}...{% endif %}`); el burbuja flotante del copiloto queda **totalmente funcional** para admin/owner en desktop, sin ningún gate. | Envolver los 3 botones del sidebar y el FAB del copiloto (no solo su `hx-get`) en `{% if app_mode != 'desktop' %}`, exactamente como especifica el envelope T11. |
| H-006 | Compra/traspaso sin guard server-side en modo desktop | 3, 4 | `app/routers/ui_purchases.py`, `app/routers/ui_transfers.py` — cero referencias a `is_desktop()` | El límite "online-only" es puramente cosmético (y roto, ver H-005): un POST directo a `/ui/purchase` o `/ui/transfer` desde un nodo desktop se ejecuta completo contra SQLite local, sin evento en el outbox — invisible para sync, se pierde silenciosamente en el próximo `apply_pull` (una vez arreglado) o diverge del server para siempre. | Añadir un guard explícito (`if is_desktop(): raise HTTPException(404)`) al inicio de los endpoints de escritura de compra/traspaso, igual que ya hace `ui_sync.py::do_sync` para lo contrario. |

### 🟡 Medios

| ID | Título | Dimensión | Evidencia | Riesgo | Recomendación |
|----|--------|-----------|-----------|--------|---------------|
| H-007 | Commit de venta/cliente y `append_event` no son atómicos | 6 | `app/routers/ui_sales.py:204-212,293-301`; `app/routers/ui_customers.py:76-87` — `InventoryService.sale()` ya commitea en `inventory_service.py:1765` antes del hook | Si `append_event`/su `db.commit()` falla (no es `HTTPException`, no lo captura el handler del router → 500 crudo), la venta/cliente ya quedó commiteada pero sin evento en el outbox: pérdida silenciosa e irreversible de esa operación para efectos de sync. | Mover el `append_event` (con su propio manejo de error, capturado y logueado sin abortar la respuesta 200 al usuario) o aceptar explícitamente el riesgo residual con un job de reconciliación que detecte movimientos sin evento correspondiente. |
| H-008 | No existe ningún camino para aprovisionar `SyncState` | 1, 9 | `app/migrations.py` (v7 solo `CREATE TABLE IF NOT EXISTS`, sin `INSERT`); ningún archivo del diff llama a `POST /sync/register` (P2) | La decisión de diseño cerrada con el usuario dice explícitamente que P3 "consume y guarda" la respuesta del registro de P2, pero no hay ningún formulario, CLI o hook que lo haga — `sync()` siempre devuelve `"Nodo no aprovisionado"`. La feature es inalcanzable de fábrica. | Definir explícitamente si el bootstrap va en P3 (falta un task envelope) o se mueve a P4 (empaquetado); documentar la decisión en el PAE en vez de dejarla implícita. |
| H-009 | Cobertura de tests no llega al camino real de fallo | 9 | `tests/unit/test_sync_client.py` (4 tests, ninguno con `users` ni tablas FK-dependientes); `tests/unit/test_offline_hooks.py` prometido en `design.md` nunca creado; 0 e2e | Los 265 tests verdes no dicen nada sobre si el sync funciona — los dos bugs críticos (H-001, H-002) viven exactamente en el hueco de cobertura. Viola además la regla dura del `CLAUDE.md` raíz de e2e obligatorio en toda feature nueva. | Añadir un test de `apply_pull` con un snapshot que incluya `users` + al menos una tabla FK-dependiente de `businesses`, y los hooks de `is_desktop()==True` en `test_offline_hooks.py` como estaba planeado. |

### 🟢 Bajos

| ID | Título | Dimensión | Evidencia | Riesgo | Recomendación |
|----|--------|-----------|-----------|--------|---------------|
| H-010 | `apply_pull` no incremental + doble commit por venta | 10 | `app/sync/client.py:62` (delete+reinsert completo de 13 tablas cada sync); `ui_sales.py:204-212` (segundo commit tras el del servicio) | Con historial grande, cada "Sincronizar" reescribe todo el dataset local en vez de solo el delta — costo crece con el total acumulado, no con el cambio real. Aceptado como trade-off v1 en el diseño, pero sin fecha de revisión. | Documentar el límite de filas/tamaño a partir del cual se vuelve un problema; considerar upsert incremental por `updated_at`/`id` en P4 o una fase posterior si el volumen lo justifica. |
| H-011 | Duplicación de banner/tablas/imports | 1, 2 | `dashboard.html:152-161` reimplementa el markup de `partials/sync_status.html` en vez de `{% include %}`; `client.py` importa `get_logger` dos veces; `_PULL_MODELS` duplica la lista de tablas de `service.py::build_pull` sin fuente compartida | Cambios futuros al banner o al set de tablas sincronizadas requieren editar 2-3 lugares a mano; fácil que diverjan silenciosamente. | `dashboard.html` debería incluir el partial en vez de reimplementarlo; extraer el set de nombres de tabla a una constante compartida entre `service.py` y `client.py`. |

---

## 5. Matriz impacto vs esfuerzo

```
Alto impacto │  🔥 QUICK WINS              🎯 ESTRATÉGICOS
             │  H-001, H-002, H-003,        H-006 (guard server-side
             │  H-004, H-005 (fixes         en 2 routers + tests)
             │  puntuales, <1 día c/u)
             │
Bajo impacto │  📋 BAJA PRIORIDAD          ❌ EVITAR
             │  H-010, H-011                 —
             └──────────────────────────────────────────────
               Bajo esfuerzo              Alto esfuerzo
```

**Quick wins recomendados (hacer YA, antes de cualquier re-auditoría):**
1. H-001 — reordenar/deshabilitar FK check en `apply_pull` — ~30 min
2. H-002 — excluir `users` de `_PULL_MODELS` o upsert por username — ~1 h
3. H-003 — quitar el `str()` sobre `movement_date` — ~10 min
4. H-004 — recalcular hash al marcar `synced` — ~1-2 h (incluye rehash de la cola)
5. H-005 — envolver botones + FAB del copiloto en el gate de `app_mode` — ~30 min

---

## 6. Backlog priorizado de remediación

| # | Hallazgo | Severidad | Esfuerzo | Impacto | Prioridad |
|---|----------|-----------|----------|---------|-----------|
| 1 | H-001 apply_pull viola FKs | Crítico | Bajo | Alto | 🔴 Inmediata |
| 2 | H-002 apply_pull viola NOT NULL en users | Crítico | Bajo | Alto | 🔴 Inmediata |
| 3 | H-003 movement_date="None" | Crítico | Bajo | Alto | 🔴 Inmediata |
| 4 | H-004 hash chain roto tras sync | Alto | Medio | Alto | 🟠 Este sprint |
| 5 | H-005 gating de pestañas incompleto | Alto | Bajo | Medio | 🟠 Este sprint |
| 6 | H-006 compra/traspaso sin guard server-side | Alto | Medio | Alto | 🟠 Este sprint |
| 7 | H-007 commit no atómico venta/outbox | Medio | Medio | Medio | 🟡 Próximo ciclo |
| 8 | H-008 sin bootstrap de SyncState | Medio | Medio | Alto (bloquea uso real) | 🟡 Próximo ciclo |
| 9 | H-009 cobertura de tests insuficiente | Medio | Medio | Alto (préviene regresión de 1-3) | 🟡 Próximo ciclo |
| 10 | H-010 apply_pull no incremental | Bajo | Alto | Bajo (v1 aceptable) | 📋 Backlog |
| 11 | H-011 duplicación banner/imports/tablas | Bajo | Bajo | Bajo | 📋 Backlog |

---

## 7. Plan 30/60/90 días

### 30 días (corto plazo)
- [ ] Corregir H-001 a H-005 (los 5 quick wins) → sync completa un ciclo real sin crashear
- [ ] Añadir tests que cubran exactamente los 2 caminos de fallo (`users` en el pull, FK-dependientes) para que no puedan regresar en silencio
- [ ] Re-`/code-review` + re-auditoría sobre el mismo diff corregido
**Criterio de cierre:** los 3 hallazgos críticos resueltos y verificados por reproducción (no solo por lectura), score de re-auditoría >= 3.5.

### 60 días (medio plazo)
- [ ] H-006 (guard server-side en compras/traspasos) + H-007 (atomicidad venta/outbox) + H-008 (decidir y construir el bootstrap de `SyncState`, aunque sea manual/admin-only)
- [ ] `tests/e2e/` con el flujo mínimo: venta offline → outbox → sync (mock del servidor) → banner actualizado, para cumplir la regla dura de e2e del `CLAUDE.md`
**Criterio de cierre:** score >= 4.0, P3 mergeado a `develop` y validado en dev real (no solo unit).

### 90 días (largo plazo)
- [ ] H-010/H-011 si el volumen de datos de los primeros nodos desktop reales lo justifica
- [ ] Iniciar P4 (empaquetado desktop + cifrado) solo después de que P3 esté verde en producción-dev, tal como exige el propio PAE ("No empezar P4 antes de que P3 esté verde")
**Criterio de cierre:** score >= 4.2, al menos un nodo desktop real sincronizando en dev sin intervención manual.

---

## 8. Criterios de cierre

- [ ] Los 3 hallazgos críticos (H-001, H-002, H-003) resueltos y con test de regresión que reproduzca el fallo original
- [ ] Score de re-auditoría >= 3.5 antes de mergear a `main`/desplegar
- [ ] Al menos un ciclo de sync real (push + pull con datos no triviales, incluyendo `users`) ejecutado manualmente en dev y verificado sin excepciones

---

*Informe generado por Claude Code (skill: auditoria-ingenieria, precedido de `/code-review` táctico
con 8 ángulos independientes + verificación).*
*Re-auditoría recomendada: tras aplicar el backlog de 30 días (H-001 a H-005 + tests de regresión).*

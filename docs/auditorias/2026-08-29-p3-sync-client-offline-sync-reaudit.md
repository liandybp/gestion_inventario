# Re-auditoría de Ingeniería — offline-desktop-sync P3 (Cliente de sync + escritura offline)

**Fecha:** 2026-08-29
**Proyecto:** gestion_inventario
**Rama:** `develop` (working tree, **sin commitear**)
**Score:** 3.95 / 5.00 🟡 (antes: 2.35 / 5.00 🔴, ver `2026-08-29-p3-sync-client-offline-sync.md`)
**Responsable:** Claude Code (auditoria-ingenieria)

---

## 1. Contexto y método

Re-auditoría tras aplicar directamente (sin pasar por OpenCode, a pedido del usuario) los 11
hallazgos del informe original. Mismo alcance de archivos que la auditoría inicial, más
`app/sync/outbox.py` (nueva función `rehash_chain`), `app/routers/ui_purchases.py` y
`app/routers/ui_transfers.py` (guards nuevos), `tests/unit/test_offline_hooks.py` (nuevo).

**Verificación:** cada fix crítico se confirmó por reproducción directa (no solo lectura) antes y
después del cambio — `apply_pull`/`sync`/`verify_chain` ejecutados contra SQLite real con
`PRAGMA foreign_keys=ON`, más un smoke test end-to-end vía `TestClient` (login → dashboard en
modo server y desktop → aprovisionamiento → banner). 274 unit tests verdes (265 + 9 nuevos:
5 en `test_offline_hooks.py`, 4 en `test_sync_client.py`).

---

## 2. Checklist por dimensión (post-fix)

| # | Dimensión | Peso | Veredicto | Score (antes → ahora) | Evidencia |
|---|-----------|------|-----------|------------------------|-----------|
| 1 | SoC / SRP | 10% | ✅ | 4 → 5 | `dashboard.html` ahora `{% include %}` el partial en vez de duplicar el banner (H-011) |
| 2 | Cohesión / Acoplamiento | 10% | ✅ | 2 → 4 | `_PULL_ORDER` es una única lista ordenada que dirige tanto el delete como el insert; `get_sync_state`/`app_mode_label` centralizan lo que antes eran 3 y 2 copias respectivamente |
| 3 | Capas / Límites | 10% | ✅ | 2 → 4 | Compra/traspaso ahora bloqueados server-side (`is_desktop()` guard en 10 endpoints de escritura, H-006); gating de pestañas ahora oculta el botón completo, no solo el atributo (H-005) |
| 4 | Seguridad | 15% | ✅ | 3 → 4 | Aprovisionamiento (`/ui/sync/provision`) admin-only vía `ensure_admin`; `password_hash` local nunca se pierde en un pull (H-002) |
| 5 | Robustez / Resiliencia | 15% | ✅ | 1 → 4 | `apply_pull` ya no falla de forma determinística (H-001/H-002 verificados por reproducción); `do_sync` con try/except (ya no 500 crudo); hooks de outbox degradan con log en vez de 500 tras un commit ya exitoso (H-007) |
| 6 | Consistencia transaccional | 10% | ✅ | 2 → 4 | `rehash_chain` mantiene `verify_chain()` íntegro tras marcar eventos `synced` (H-004, verificado por reproducción) |
| 7 | Observabilidad | 10% | ✅ | 3 → 4 | `do_sync` loguea con `exc_info=True` antes de degradar a un error controlado |
| 8 | Configuración / Entornos | 5% | ✅ | 4 → 4 | Sin cambios — ya estaba bien |
| 9 | Pruebas / Calidad | 10% | ⚠️ | 1 → 3 | +9 tests que cubren exactamente los 2 caminos de fallo crítico (FK order, password_hash, hash chain, hooks `is_desktop()`); **sigue sin e2e Playwright** — no llega a ✅ hasta cerrar esa brecha |
| 10 | Escalabilidad | 5% | ⚠️ | 3 → 3 | Sin cambios — `apply_pull` sigue siendo full-replace no incremental (trade-off v1 documentado, H-010 no era crítico) |

**Leyenda:** ✅ OK (4-5) · ⚠️ PARCIAL (2-3) · ❌ NO (<2)

---

## 3. Scorecard ponderado

```
Dimensión                        Peso    Score   Peso×Score
────────────────────────────────────────────────────────────
1. SoC / SRP                     10%     5       0.50
2. Cohesión / Acoplamiento       10%     4       0.40
3. Capas / Límites               10%     4       0.40
4. Seguridad                     15%     4       0.60
5. Robustez / Resiliencia        15%     4       0.60
6. Consistencia transaccional    10%     4       0.40
7. Observabilidad                10%     4       0.40
8. Configuración / Entornos       5%     4       0.20
9. Pruebas / Calidad             10%     3       0.30
10. Escalabilidad                 5%     3       0.15
────────────────────────────────────────────────────────────
TOTAL                          100%             3.95 / 5.00
SCORE FINAL                                      3.95 / 5.00 🟡
```

**Interpretación:** 🟡 Estable con deuda relevante — ya no bloquea por riesgo de crash
garantizado (los 3 críticos de la primera pasada están resueltos y verificados por
reproducción), pero **falta e2e Playwright** (regla dura del `CLAUDE.md` raíz) para llegar a
🟢. Recomendado cerrar esa brecha antes de mergear a `main`/desplegar, no antes de continuar con
P4.

---

## 4. Hallazgos originales — estado

| ID | Título | Severidad original | Estado |
|----|--------|---------------------|--------|
| H-001 | apply_pull borra tablas en orden que viola FKs | 🔴 Crítico | ✅ Corregido — `_PULL_ORDER` topológico, verificado por reproducción (2 syncs consecutivos sin IntegrityError) |
| H-002 | apply_pull reinserta User sin password_hash | 🔴 Crítico | ✅ Corregido — hash local preservado por username; usuario nuevo recibe hash aleatorio inutilizable (documentado) |
| H-003 | Venta offline sin fecha → `"None"` literal | 🔴 Crítico | ✅ Corregido — se envía el valor crudo del form, no `str()` |
| H-004 | Marcar `synced` rompe la cadena de hash | 🟠 Alto | ✅ Corregido — `rehash_chain` nueva en `outbox.py`, invocada tras cada sync exitoso |
| H-005 | Gating de pestañas offline incompleto | 🟠 Alto | ✅ Corregido — botones envueltos completos + FAB del copiloto gateado |
| H-006 | Compra/traspaso sin guard server-side | 🟠 Alto | ✅ Corregido — `is_desktop()` guard en los 5 endpoints de escritura de cada router (10 total) |
| H-007 | Commit de venta/cliente y outbox no atómicos | 🟡 Medio | ⚠️ Mitigado, no resuelto de raíz — el hook ahora degrada con log en vez de 500 crudo; la venta sigue commiteada aunque el evento falle (riesgo residual documentado, ver `design.md` T3) |
| H-008 | Sin camino para aprovisionar SyncState | 🟡 Medio | ✅ Cerrado (MVP) — `POST /ui/sync/provision` (admin-only) + formulario en el banner; el registro remoto automático (`POST /sync/register` desde el nodo) sigue sin existir, ver nota abajo |
| H-009 | Cobertura de tests insuficiente | 🟡 Medio | ⚠️ Mejorado, no cerrado — 9 tests nuevos cubren los 4 caminos de fallo crítico; falta e2e Playwright |
| H-010 | apply_pull no incremental + doble commit | 🟢 Bajo | Sin cambios — trade-off v1 aceptado, no crítico |
| H-011 | Duplicación banner/tablas/imports | 🟢 Bajo | ✅ Corregido — `{% include %}`, `get_sync_state`/`app_mode_label` compartidos, imports duplicados eliminados |

**Nota sobre H-008:** el MVP implementado requiere que un admin obtenga el `device_token` por
fuera de este nodo (ej. llamando `POST /sync/register` manualmente en el servidor central) y lo
pegue en el formulario de aprovisionamiento. No es el flujo automático que `design.md` describía
("P3 consume y guarda la respuesta del registro"), pero cierra el hueco real (antes: ningún
camino posible) sin inventar un handshake de autenticación cross-servidor no especificado. Un
flujo de registro remoto automático queda como candidato explícito para P4 (empaquetado
desktop), donde ya hay un asistente de primer arranque previsto.

---

## 5. Criterios de cierre

- [x] Los 3 hallazgos críticos (H-001, H-002, H-003) resueltos y con test de regresión
- [ ] Cobertura e2e del flujo de sync (pendiente — recomendado antes de mergear a `main`)
- [x] Al menos un ciclo de sync real (push + pull con `users`) ejecutado y verificado sin excepciones (smoke test `TestClient` + reproducción directa de `sync()`)

---

*Informe generado por Claude Code (skill: auditoria-ingenieria).*
*Re-auditoría recomendada: tras añadir cobertura e2e (H-009) — candidato para cerrar antes de mergear P3 a `main`.*

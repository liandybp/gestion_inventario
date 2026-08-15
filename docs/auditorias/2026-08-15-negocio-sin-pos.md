# Auditoría de Ingeniería — Negocio sin POS + Quick Wins (H-029/034/036/053)

**Fecha:** 2026-08-15
**Proyecto:** gestion_inventario
**Rama:** develop (commit `906fa0e` + fixes de code-review)
**Score:** 2.95 / 5.00 🔴
**Responsable:** Claude Code (auditoria-ingenieria)
**Referencias:** previa `docs/auditorias/2026-08-15-general.md` (score 2.70)

---

## 1. Contexto y método

**Alcance:** auditoría de seguimiento enfocada en los subsistemas tocados en la
sesión (config de negocio/ubicaciones con soporte "negocio sin POS", migraciones
e índices H-053, seguridad XSS H-029/H-034 y credenciales H-036), con puntuación
de las 10 dimensiones sobre el estado actual del proyecto.

**Archivos auditados:** 19 archivos en `app/` (routers, services, repositories,
business_config, migrations, models) + `tests/` + 2 docker-compose.

**Criterios:** 10 dimensiones según plantilla de auditoría de ingeniería
(pesos: Seguridad 15%, Robustez 15%, SoC/Capas/Cohesión 10% c/u,
Transaccional/Observabilidad/Testing 10% c/u, Config/Escalabilidad 5% c/u).

**Limitaciones:** auditoría de seguimiento sobre el delta de la sesión, no un
barrido completo re-evaluado desde cero; los scores de las dimensiones estructurales
(god router/service) arrastran el histórico ya conocido.

---

## 2. Checklist por dimensión

| # | Dimensión | Peso | Veredicto | Score | Evidencia |
|---|-----------|------|-----------|-------|-----------|
| 1 | SoC / SRP | 10% | ❌ | 2 | `ui_tabs.py` god router (2437 ln, 36 handlers); `inventory_service.py` god service (3066 ln, 66 métodos) |
| 2 | Cohesión / Acoplamiento | 10% | ⚠️ | 3 | Triple reimplementación de `_location_id_for_code` (service vs 2 routers); `LocationsConfig` construido en 2 sitios |
| 3 | Capas / Límites | 10% | ❌ | 2 | 13/18 routers con ORM directo (H-028); `ui_businesses.py` CRUD de negocio inline |
| 4 | Seguridad | 15% | ✅ | 4 | XSS escapado (ui_sales_documents/ui_products); seed sin credenciales triviales; auth universal + RBAC UI + PBKDF2 + CSRF |
| 5 | Robustez / Resiliencia | 15% | ⚠️ | 3 | Sin circuit breaker LLM (H-030); ~40 `except Exception` sin log (H-039); timeout LLM 60s |
| 6 | Consistencia transaccional | 10% | ⚠️ | 3 | Core multi-tabla atómico; `_location_id_for_code` commitea anidado (H-040); audit desacoplado (H-041) |
| 7 | Observabilidad | 10% | ❌ | 2 | Sin structured logging ni correlation_id (H-043); health check solo `SELECT 1` (H-065) |
| 8 | Configuración / Entornos | 5% | ⚠️ | 3 | `settings.py` parcial (solo AGENT_LLM_*); `os.getenv` en 9+ archivos (H-045); paridad compose incompleta (H-046) |
| 9 | Pruebas / Calidad | 10% | ✅ | 4 | 138 unit + 5 e2e; CI test-gate; e2e no corren en CI (gap); feature sin POS ya con e2e |
| 10 | Escalabilidad | 5% | ⚠️ | 3 | Índices compuestos H-053 aplicados; stock_list/SS-ROP full-scan (H-033); FIFO replay O(historial) |

**Leyenda:** ✅ OK (4-5) · ⚠️ PARCIAL (2-3) · ❌ NO (<2)

---

## 3. Scorecard ponderado

```
Dimensión                        Peso    Score   Peso×Score
────────────────────────────────────────────────────────────
1. SoC / SRP                     10%     2       0.20
2. Cohesión / Acoplamiento       10%     3       0.30
3. Capas / Límites               10%     2       0.20
4. Seguridad                     15%     4       0.60
5. Robustez / Resiliencia        15%     3       0.45
6. Consistencia transaccional    10%     3       0.30
7. Observabilidad                10%     2       0.20
8. Configuración / Entornos       5%     3       0.15
9. Pruebas / Calidad             10%     4       0.40
10. Escalabilidad                 5%     3       0.15
────────────────────────────────────────────────────────────
TOTAL                                  2.95 / 5.00 🔴
```

**Delta vs 2026-08-15-general (2.70):** +0.25 — los quick wins subieron Seguridad
(4) y Testing (4); la deuda estructural (capas, observabilidad) mantiene el score
en rojo.

---

## 4. Hallazgos

### 🔴 Críticos / Altos (del code-review de la sesión — corregidos)

| ID | Título | Dim | Evidencia | Riesgo | Estado |
|----|--------|-----|-----------|--------|--------|
| H-067 | Dropdown de ubicación vacío en Documentos para negocio sin POS | 2/3 | `ui_sales_documents.py` pos_locations solo de `config.locations.pos` (sin central) | El form de emisión no muestra la ubicación (solo funciona por fallback server-side) | ✅ Corregido: `LocationsConfig.all_locations()` (central + POS) en todos los renderers |
| H-068 | Traspasos inutilizables para negocio sin POS (from=to=central → 422) | 3 | `tab_transfers.html` + `default_sale_code()` | Cada traspaso falla con 422 sin forma de corregirlo desde la UI | ✅ Corregido: el form de traspaso se oculta cuando `all_locations|length <= 1` (aviso al usuario) |
| H-069 | Edit form renderiza literal `"None"` en "POS por defecto" | 3 | `business_edit_form.html:243` | Al guardar crea `{CODE}_None` como default | ✅ Corregido: `default_pos or ''` |
| H-070 | `_prefix_location_code("")` → `""` crea `Location(code="")` (colisión UNIQUE) | 6 | `ui_businesses.py` `_parse_loc_list` | Spec `:Tienda` crea Location vacía → UNIQUE global roto en el 2º negocio (409 engañoso) | ✅ Corregido: specs con código vacío se descartan en create y reconcile |
| H-071 | Form de crear pre-rellena `POS1` contradiciendo "vacío = sin POS" | 1 | `tab_businesses.html` + `Form("POS1...")` | Un admin que deja el default crea un negocio con POS no deseado | ✅ Corregido: default vacío en Form y template |

### 🟠 Medios (corregidos)

| ID | Título | Dim | Evidencia | Estado |
|----|--------|-----|-----------|--------|
| H-072 | Seed H-036 genera contraseña aleatoria solo en log (lockout si se pierde) | 4 | `migrations.py` | ✅ Corregido: fail-fast — aborta el seed si falta la env password (dev/prod ya la tienen) |
| H-073 | `default_pos` legacy sobrevive cuando `pos` vacío en `.conf` | 8 | `business_config.py` parser | ✅ Corregido: `if not pos_list: default_pos = ""` |
| H-074 | `or 'POS1'` muerto en `issue_form.html` | 2 | `issue_form.html:5` | ✅ Corregido: eliminado |
| H-075 | DDL de índices compuestos duplicado ×2 (sqlite + pg) | 2 | `migrations.py` | ✅ Corregido: constante única `_COMPOSITE_INDEX_SQL` |
| H-076 | `import html as _html` duplicado en 2 funciones ×2 routers | 2 | `ui_products.py`, `ui_sales_documents.py` | ✅ Corregido: `import html` a nivel de módulo |
| H-077 | Feature sin POS sin e2e (viola DoD) | 9 | `tests/e2e/` | ✅ Corregido: `test_business_no_pos_e2e.py` (crear negocio sin POS + Documentos con central) |

### 🟡 Deuda pre-existente confirmada (pendiente de remediación)

| ID | Título | Dim | Severidad | Evidencia | Recomendación |
|----|--------|-----|-----------|-----------|---------------|
| H-030 | Sin circuit breaker para el LLM | 5 | Medio | `graph.py:74-99` | Circuit breaker OPEN/CLOSED/HALF-OPEN con cooldown |
| H-039 | `except Exception` silenciosos sin log (~40) | 5 | Medio | routers UI | `_log.exception()` en operaciones de negocio |
| H-040 | Commit anidado en `_location_id_for_code` | 6 | Alto | `inventory_service.py:108-109` | `flush()` en vez de `commit()` |
| H-041 | Audit desacoplado de la operación | 6 | Medio | `audit.py:32` | Mismo commit que la operación |
| H-043 | Sin correlation_id / structured logging | 7 | Medio | `logger.py`, `main.py` | Middleware X-Request-ID + JSON |
| H-045 | `os.getenv` disperso en 9+ archivos | 8 | Medio | `db.py`, `main.py`, ... | Mover todo a `settings.py` |
| H-046 | Paridad env incompleta entre composes + defaults débiles (`SESSION_SECRET=change-me`, `ADMIN_PASSWORD=change-me`) | 8 | Alto | `docker-compose.yml:21-27` | `${VAR:?must-be-set}` como homelab; ⚠️ requiere confirmación para tocar compose |
| H-026/H-023 | God router `ui_tabs.py` + god service `inventory_service.py` | 1 | Alto | — | Dividir por tab / extraer FinanceService + ReportingService |
| H-028 | 13/18 routers con ORM directo | 3 | Alto | — | Consolidar en repos/services |
| H-049 | Triple `_location_id_for_code` divergente | 2 | Medio | — | `LocationRepository` único |
| H-033 | stock_list/SS-ROP full-scan sin paginación | 10 | Alto | `inventory_repository.py:108`, `inventory_service.py:2819` | LIMIT + filtros SQL + cache SS/ROP |
| RBAC API | API JSON sin role check para operador | 4 | Medio | `inventory.py:113`, `products.py:21` | `require_admin_api` en adjustment/product |
| H-036 legacy | Prod/Dev ya tienen `admin/admin` (seed histórico) | 4 | Medio | migraciones pre-v2 | Rotación manual de credenciales en prod (ops) |

---

## 5. Matriz impacto vs esfuerzo

```
Alto impacto │  🔥 QUICK WINS                         🎯 ESTRATÉGICOS
             │  H-030 circuit breaker LLM            H-026 dividir ui_tabs
             │  H-039 logs en except Exception       H-023 dividir InventoryService
             │  H-040 flush() en _location_id_for_code H-028 consolidar repos
             │  H-046 compose env obligatoria        H-033 paginación + cache SS/ROP
             │  RBAC API JSON                        H-043 correlation_id + structlog
             │  H-036 rotación creds prod
             │
Bajo impacto │  📋 BAJA PRIORIDAD                    ❌ EVITAR
             │  H-041 audit atómico                  (ninguno claramente evitable)
             │  H-049 dedup _location_id_for_code
             │  H-045 consolidar os.getenv
             │  e2e en CI (Playwright)
             └────────────────────────────────────────────────────────────
               Bajo esfuerzo                           Alto esfuerzo
```

---

## 6. Backlog priorizado de remediación

1. **H-046** compose env obligatoria (quick win de seguridad, alto) — requiere confirmación para tocar `docker-compose.yml`.
2. **H-036 legacy** — rotar `admin/admin` en prod (ops manual).
3. **H-030** circuit breaker LLM (evita bloqueos de 3×60s por chat).
4. **H-040** `flush()` en `_location_id_for_code` (consistencia transaccional).
5. **H-039** logging en `except Exception` de routers.
6. **RBAC API JSON** — `require_admin_api` en adjustment/product/transfer.
7. **e2e en CI** — instalar Playwright + correr `tests/e2e` en el test-gate.
8. **H-049** `LocationRepository` único (reduce la triple resolución).
9. Estructurales (H-026/H-023/H-028/H-033/H-043) → roadmap.

---

## 7. Plan 30/60/90 días

- **30 días:** H-046 (env obligatoria), H-030 (circuit breaker), H-040 (flush), H-039 (logging), RBAC API. Criterio de cierre: ningún `except Exception` sin log en routers; ajustes de inventario solo admin/owner.
- **60 días:** e2e en CI (Playwright en test-gate), H-049 (LocationRepository), H-036 (rotación de creds). Criterio: el gate CI falla si los e2e no corren; resolución de ubicación en un solo método.
- **90 días:** H-026/H-023 (dividir ui_tabs + InventoryService), H-028 (repos), H-033 (paginación + cache SS/ROP), H-043 (correlation_id + structlog). Criterio: `ui_tabs.py` < 1200 ln; dashboard con datos >5K SKUs sin degradación.

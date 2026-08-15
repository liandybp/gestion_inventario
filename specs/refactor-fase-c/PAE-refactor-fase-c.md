# PAE-refactor-fase-c: RBAC API + Escalabilidad + Capas + e2e en CI

<!-- ═══════════════════════════════════════════════════════════════
     INSTRUCCIONES PARA OPENCODE — LEER ANTES DE CUALQUIER ACCIÓN
     1. Leer CLAUDE.md del proyecto. Si no existe, detenerse y reportar.
     2. Leer design.md §4 (firmas reales) y §5 (inventario) ANTES de tocar archivos.
     3. EJECUTAR VERIFICACIÓN PRE-IMPLEMENTACIÓN (sección 7.0) — sin excepción.
     4. Fases en orden: C1 (T1-2) → C2/C3 (T3-4) → C4 (T5-9) → C5 (T10) → T11 verificación.
     5. Si algo no está especificado: no se inventa, se reporta.
     6. Archivos fuera del inventario (design.md §5): no se tocan.
     7. CADA tarea incluye verificación. Si falla: corregir, no avanzar.
     8. Al terminar: script de verificación completa (sección 7.2).
     9. docker-compose*.yml NO se toca (invariante).
     ═══════════════════════════════════════════════════════════════ -->

## 1. Metadatos

| Campo | Valor |
|-------|-------|
| **Proyecto** | gestion_inventario |
| **Rama base** | `develop` |
| **Rama de trabajo** | `feature/refactor-fase-c` |
| **Fecha** | 2026-08-15 |
| **Versión actual** | v0.4.0 |
| **Versión objetivo** | v0.4.1 |
| **PAE anterior** | `refactor-fase-b` |
| **Fase** | ejecución (OpenCode + `qwen36_35b_coder_mtp` local) |
| **Test runner** | `SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -v` |

## 2. Documentos fuente

| Fase SDD | Output | Contenido |
|----------|--------|-----------|
| Phase 1 | `specs/refactor-fase-c/requirements.md` | REQ-1..8 + NFRs + W |
| Phase 2 | `specs/refactor-fase-c/design.md` | Decisiones C1-C5, firmas, inventario |
| Phase 3 | `specs/refactor-fase-c/tasks.md` | **envelopes T1..T11** |

## 3. Objetivo

Acercar el score de auditoría **3.85 → ~4.0+** con 4 frentes:

| Frente | Cambio | Impacto |
|--------|--------|---------|
| **C1 Seguridad** | RBAC en API JSON: operador no ajusta/transfiere/compra/crea producto | dim4 (seguridad) |
| **C2 Escalabilidad** | Paginación en stock list + cache de SS/ROP con invalidación | dim10 3→4 (H-033) |
| **C3 Capas** | Migrar ORM de los 5 routers top → repos/services | dim3 3→4 (H-028) |
| **C4 Testing** | e2e en CI (Playwright en test-gate) | dim9 |

**Qué NO se toca:** `docker-compose*.yml`, `app/auth.py`, `app/deps.py`, `app/models.py`,
`app/schemas.py`, `app/templates/*`, `app/agent/*`, `app/routers/ui_auth.py`.

## 4. Decisiones de diseño (cerradas — ver design.md)

| # | Decisión |
|---|----------|
| C1 | `require_admin_or_owner_api` nueva; adjustment/transfer/purchase/return/product-create lo usan; sale queda operator-OK |
| C2 | `stock_list(limit, offset)`; callers sin limit reciben todo (no-regresión); stock-table pasa limit=200 |
| C3 | Cache dict módulo con TTL (INVENTORY_METRICS_TTL=300) + invalidación en todas las mutaciones de stock |
| C4 | 3 repos nuevos (User/Customer/Purchase) + ampliar BusinessRepository y SalesDocumentService; migrar los 5 routers top |
| C5 | Playwright en el test-gate; fallar si los e2e no corren |

## 5. Fases de implementación (Task Envelopes)

> Envelopes completos en `specs/refactor-fase-c/tasks.md`. Orden C1→C2→C3→C4→C5.

| Envelope | Título | Files |
|----------|--------|-------|
| T1 | `require_admin_or_owner_api` + test | security.py, test_security.py |
| T2 | RBAC en inventory.py + products.py | 2 routers |
| T3 | `limit`/`offset` en stock_list + paginación stock-table | inventory_service.py, ui_stock.py |
| T4 | Cache SS/ROP (TTL + invalidación) | inventory_service.py, settings.py |
| T5 | UserRepository + migrar ui_users | CREATE + ui_users.py |
| T6 | CustomerRepository + migrar ui_customers | CREATE + ui_customers.py |
| T7 | PurchaseRepository + migrar ui_purchases | CREATE + ui_purchases.py |
| T8 | Ampliar SalesDocumentService + migrar ui_sales_documents | 2 files |
| T9 | Ampliar BusinessRepository + migrar ui_businesses | 2 files |
| T10 | Playwright + e2e en CI test-gate | deploy.yml |
| T11 | Verificación completa | (verificación) |

## 6. Riesgos y mitigaciones

| # | Riesgo | Prob. | Impacto | Mitigación |
|---|--------|-------|---------|------------|
| R1 | T4 (cache) da valores stale tras mutación | Media | Alto | Invalidación en TODAS las mutaciones de stock (T4 Step 3); tests de stock verifican frescura |
| R2 | T5-T9 (migración ORM) rompe comportamiento | Media | Alto | Mover VERBATIM; tests unit por router; protocolo de fallo |
| R3 | T10 (Playwright en CI) rompe el gate si no instala | Media | Medio | `install chromium` sin `--with-deps` como fallback documentado; el gate debe fallar (no skip) |
| R4 | T2 (RBAC) bloquea a un operator que legitima ajusta | Baja | Medio | Sale queda operator-OK (cajero); adjustment/transfer/purchase son admin/owner en la UI |

**Rollback:** cada hito es un commit atómico; `git revert` del hito.

## 7. Plan de tests y verificación

### 7.0 Pre-implementación (OBLIGATORIO)

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -m "not e2e" --no-cov 2>&1 | tail -2
# Expected: 143 passed
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/e2e -m e2e --no-cov 2>&1 | tail -2
# Expected: 11 passed
```

### 7.2 Verificación completa

```bash
#!/bin/bash
set -e
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -m "not e2e" --no-cov 2>&1 | tail -2
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/e2e -m e2e --no-cov 2>&1 | tail -2
echo "=== RBAC aplicado ==="
grep -c "require_admin_or_owner_api" app/routers/inventory.py app/routers/products.py
echo "=== ORM migrado en routers top ==="
grep -c "select(User)\|select(Business)\|select(Customer)\|select(SalesDocument)" app/routers/ui_users.py app/routers/ui_customers.py app/routers/ui_sales_documents.py || true
echo "=== Scope ==="
git diff --name-only develop
echo "=== ALL CHECKS PASSED ==="
```

### 7.3 Protocolo de fallo

1. Leer error. 2. `git diff`. 3. Corregir código. 4. Re-ejecutar. 5. Tras 3 intentos: reportar, NO avanzar.

## 8. Definición de hecho (DoD)

### 8.1 Checklist pre-commit

- [ ] **Pre-check:** 143 unit + 11 e2e pasaban antes
- [ ] **RBAC:** adjustment/transfer/purchase/return/product-create exigen admin_or_owner; sale operator-OK
- [ ] **Cache:** SS/ROP cacheado con TTL; invalidación en todas las mutaciones de stock
- [ ] **Paginación:** stock-table usa limit; callers sin limit intactos
- [ ] **Repos:** UserRepository/CustomerRepository/PurchaseRepository creados; ui_users/ui_customers/ui_purchases/ui_sales_documents/ui_businesses delegan
- [ ] **CI:** deploy.yml instala Playwright y corre e2e en el gate
- [ ] **Tests:** suite unit + e2e pasan
- [ ] **Invariantes:** no se tocó compose/auth/deps/models/templates
- [ ] **Scope:** `git diff --name-only develop` = solo design.md §5
- [ ] **Limpieza:** sin temporales

### 8.2 Commits esperados

```
feat(security): RBAC en API JSON (operador no gestiona inventario)
perf(inventory): paginacion en stock + cache SS/ROP con invalidacion (H-033)
refactor(layers): migrar ORM de routers top a repositories (H-028)
ci: e2e Playwright en el test-gate
docs: actualizar CLAUDE.md estado + session notes
```

---

> Fuente: `Templates/template-pae.md` · Generado por SDD Lead (Phase 4 — PAE Assembly)
> Skill: `SDD-Agent` · Firma del SDD Lead: Claude Code (DeepSeek API)

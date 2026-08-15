# Fase D — Requirements (SDD Phase 1)

> Empujón final para cruzar a **verde (≥4.2)**. Score actual: 4.00 (`docs/auditorias/2026-08-15-fase-c.md`).
> Ruta al verde: dim9 Testing 4→5 (+0.10) + dim5 Robustez 4→5 (+0.15) = **4.25**. dim4 Seguridad 4→5 como bonus.

## REQs (MoSCoW)

### Must (M)

| ID | Requisito | Criterio de aceptación (Gherkin) |
|----|-----------|-----------------------------------|
| REQ-1 | **API contract tests para TODOS los endpoints JSON** | Given el TestClient/live server, When se llama cada endpoint API (`/products`, `/movements/*`, `/stock*`), Then se aserta status code esperado + schema de respuesta (campos correctos). |
| REQ-2 | **RBAC cubierto por contract tests** | Given un operador autenticado, When llama a adjustment/transfer/purchase/return/product-create, Then 403; cuando llama a sale/GET, Then NO 403. Given admin, When llama, Then NO 403. |
| REQ-3 | **Extender e2e a más flujos** | Given la suite e2e, When se añaden flujos, Then cubre al menos clientes, compras y documentos (además de venta/traspaso/gasto/extracción existentes). |
| REQ-4 | **LLM retry con backoff exponencial + jitter** | Given el copiloto, When el planner falla (parse/network), Then los reintentos usan backoff exponencial con jitter (no inmediatos). |
| REQ-5 | **Rate limiter de login keyed por (IP real + username)** | Given login, When se intenta, Then el bucket se keyea por `X-Forwarded-For` (IP real detrás del proxy) + username, no por la IP del proxy (bucket global). Un atacante no bloquea el login de todos. |

### Should (S)

| ID | Requisito | Criterio de aceptación |
|----|-----------|------------------------|
| REQ-6 | **Error-path tests en la API** | Given payloads inválidos (SKU inexistente, cantidad 0, stock insuficiente), When se llama la API, Then se devuelve 4xx con mensaje claro (no 5xx). |
| REQ-7 | **Cobertura de contratos contada en el informe** | Given la suite, When corre, Then `pytest --cov` reporta ≥85% y el informe de la Fase D documenta la cobertura por endpoint. |

### Won't (W)

| ID | Excluido | Razón |
|----|----------|-------|
| W-1 | Refactor CSP completo (quitar `unsafe-inline`) | Requiere migrar TODOS los onclick inline de templates a JS externo — refactor grande y arriesgado; se documenta el trade-off. |
| W-2 | Dividir más `inventory_service.py` | Ya < 2000 ln; no mueve el score. |
| W-3 | Redis | Cache en memoria suficiente. |
| W-4 | `docker-compose*.yml` (H-046) | Invariante, requiere decisión. |

## NFRs

- **No-regresión:** 145 unit + 14 e2e pasan; las respuestas API no cambian (solo status codes de RBAC, ya en prod de dev).
- **Seguridad:** `X-Forwarded-For` se usa SOLO como pista de IP real (primer valor); el rate limiter sigue funcionando si no hay header (fallback a `request.client.host`).
- **Robustez:** el backoff/jitter del LLM es de ≤5s por retry (no bloquea el chat perceptiblemente); timeout global del turno se mantiene.
- **Testing:** los contract tests corren contra el live server e2e (SQLite por archivo, compartido) — evitan el problema del in-memory.

## Fuentes de verdad

- Firmas reales: `specs/refactor-fase-d/design.md` §4.
- Envelopes: `specs/refactor-fase-d/tasks.md`.

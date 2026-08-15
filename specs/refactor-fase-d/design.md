# Fase D — Design (SDD Phase 2)

> Decisiones cerradas. OpenCode NO decide nada de esta sección.

## D1 — API contract tests (dim9 Testing → 5)

- Nuevo `tests/e2e/test_api_contracts_e2e.py` usando el `live_server` existente (SQLite por archivo) + `page.request` (lleva la sesión).
- Cobertura por endpoint:
  - `GET /products`, `POST /products`
  - `POST /movements/purchase`, `/movements/sale`, `/movements/transfer`, `/movements/adjustment`, `/movements/return-supplier-lot`
  - `GET /stock/{sku}`, `GET /stock`
- Para cada endpoint: asertar status code (RBAC) + campos del schema de respuesta.
- Dos usuarios: `admin` (todo OK) y `operator` (403 en sensitive, OK en sale/GET).
- Error paths: payload inválido → 4xx (no 5xx).

## D2 — LLM retry con backoff/jitter (dim5 Robustez → 5)

- En `app/agent/graph.py` `_plan_node`, el loop `for _ in range(config.MAX_RETRIES + 1)`:
  - Antes de cada reintento (excepto el primero), `time.sleep(backoff)` donde
    `backoff = min(base * 2**attempt, max_backoff) + random.uniform(0, jitter)`.
  - Config en settings: `AGENT_RETRY_BASE_S=1.0`, `AGENT_RETRY_MAX_S=5.0`, `AGENT_RETRY_JITTER=0.5`.
- Solo aplica a retries (no al primer intento ni al degrade). El breaker (Fase A) sigue siendo la primera línea de defensa.

## D3 — Rate limiter login keyed por (IP real + username) (dim4 Seguridad → 5)

- En `app/routers/ui_auth.py`:
  - Resolver IP real: `X-Forwarded-For` (primer valor, strip) si presente, si no `request.client.host`.
  - Clave del bucket: `(real_ip, username)` en vez de solo `real_ip`.
  - Límites: mismos `LOGIN_RATE_LIMIT`/`LOGIN_RATE_WINDOW` por (ip, usuario).
  - `_check_login_rate(key)` recibe la clave compuesta.
- Efecto: un atacante no agota el bucket global de TODOS (solo el suyo por IP+user); brute-force por usuario se frena.

## Fuera de scope (W)

- CSP completo (migrar onclick inline → JS externo) — W-1.
- `docker-compose*.yml` — invariante.
- Redis, dividir inventory_service — W-2/W-3.

---

## 4. Firmas reales del repo (NO modificar salvo la tarea que lo indica)

```python
# app/agent/graph.py — EXISTE
# En _plan_node:
    for _ in range(config.MAX_RETRIES + 1):
        try:
            plan = _PLANNER.invoke(messages)
            ...  # success → breaker.record_success()
        except (OutputParserException, ValidationError) as exc:
            error = str(exc); messages.append(HumanMessage(...feedback...))
        except Exception as exc:
            breaker.record_failure(); state["error"] = ...; return state

# app/routers/ui_auth.py — EXISTE
_client_ip = request.client.host if request.client else "unknown"
if not _check_login_rate(_client_ip): ...
def _check_login_rate(client_ip: str) -> bool:  # keyea _login_attempts por client_ip

# app/settings.py — EXISTE
LOGIN_RATE_LIMIT = int(env("LOGIN_RATE_LIMIT", "10"))
LOGIN_RATE_WINDOW = int(env("LOGIN_RATE_WINDOW", "300"))
```

## 5. Inventario de archivos (scope duro)

### CREATE
- `tests/e2e/test_api_contracts_e2e.py`

### MODIFY
- `app/agent/graph.py` (backoff/jitter en retries)
- `app/settings.py` (AGENT_RETRY_*)
- `app/routers/ui_auth.py` (rate limiter por IP+usuario)
- `tests/e2e/test_flows_e2e.py` (añadir flujos: cliente, compra, documento)

### NO SE TOCAN
`docker-compose*.yml`, `app/auth.py` (hash), `app/deps.py`, `app/models.py`,
`app/schemas.py`, `app/templates/*`, `app/agent/llm.py`, `app/agent/runner.py`.

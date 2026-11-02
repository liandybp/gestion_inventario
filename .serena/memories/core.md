# gestion_inventario — core

Multi-business/multi-location inventory management app (FastAPI + HTMX + Jinja2). Full
context lives in root `CLAUDE.md` (architecture, deploy topology, session history) and
`.claude/CLAUDE.md` (OpenCode-facing patterns/invariants) — read those first for anything not
covered here. This memory set covers only what those miss or what changes fast enough to need
a terser, code-search-oriented index.

- `mem:tech_stack` — stack + version-pin gotchas.
- `mem:suggested_commands` — exact dev/test invocations (env vars matter, see below).
- `mem:conventions` — code patterns specific to this repo, not generic Python/FastAPI style.
- `mem:task_completion` — what "done" means before reporting a task finished.
- `mem:known_bug_patterns` — a recurring bug family that has hit this codebase 3+ times;
  check this before touching sync/pull code or anything crossing an HTTP JSON boundary.

## Source map (non-obvious parts only)

- `app/models.py` — ALL ORM models in one file (13+ models). `products.sku` and
  `inventory_lots.lot_code` are `unique=True` GLOBALLY, not composite by `business_id`,
  despite what older docs said — see `mem:known_bug_patterns`.
- `app/routers/` — two kinds: `products.py`/`inventory.py`/`health.py`/`sync.py` are JSON
  API; everything prefixed `ui_*.py` is HTMX-serving UI, mounted under `/ui`.
  `ui_tabs_common.py` and `ui_common.py` hold shared helpers (auth checks, templates object).
- `app/sync/` — the offline-desktop-sync epic (5 slices, P1-P5, all committed as of
  2026-08-29). `client.py` = desktop-node side (`sync()`/`apply_pull()`), `service.py` =
  central-server side (`SyncService.push()`/`build_pull()`), `outbox.py` = local event log
  with a hash chain, `tables.py` = single source of truth for which tables sync and in what
  FK order (`SYNC_TABLE_ORDER` — both client and server assert against it, so drift fails
  loud, not silent).
- `app/desktop/` — desktop-mode entry point (`launcher.py`, pywebview+uvicorn) and SQLCipher
  key management (`keyring.py`, DPAPI on Windows / base64 fallback elsewhere). Gated by
  `app.settings.is_desktop()` (env `APP_MODE=desktop` vs `server`).
- `app/business_config.py` — legacy `.conf`-file config parser. Business config is now
  DB-backed (`Business` model has 22+ config columns); `.conf` files are dead/inert, kept
  only for a one-time backfill path.
- `tests/e2e/conftest.py` — `live_server` fixture (isolated temp SQLite, dynamic port) is the
  ONLY safe pattern for booting a real server in a test. Never point a test fixture at
  `app.settings.DATABASE_URL` or a fixed port — that's pointed at real dev infra and has
  caused a real incident (see `mem:known_bug_patterns`).

## Hard invariants (do not violate)

- `app/db.py`: all models inherit `Base` from here; no alternate declarative bases.
- `app/auth.py`: PBKDF2-SHA256, 200k iterations — don't touch hashing logic without
  explicit instruction.
- `app/deps.py`: `require_user_api`/`require_admin_api`/`require_active_business_id` are the
  centralized access-control chokepoints — don't bypass or duplicate.
- Migrations: only ever append new migration steps in `app/migrations.py`; never edit an
  existing one.
- Versioning: every bump is PATCH (`x.y.Z`→`x.y.Z+1`), never MINOR/MAJOR, unless the user
  explicitly asks for a release milestone.

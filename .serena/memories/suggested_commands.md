# suggested_commands

## Run the dev server
```
source .venv/bin/activate
uvicorn app.main:app --host 0.0.0.0 --port 10000 --reload
```
Dev DB is PostgreSQL via `.env` (`postgresql+psycopg://liandy:devpassword@localhost:5433/inventario`),
NOT SQLite, despite tests defaulting to SQLite. Check `app.settings.DATABASE_URL` before
assuming which backend is live — a real dev server is commonly already running on :10000;
never start a second process on that same port (checked via `ps aux | grep uvicorn` first).

## Unit tests (always override DATABASE_URL to in-memory SQLite, else it hits real dev PG)
```
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest --no-cov
```
Drop `--no-cov` only when you want the coverage report; the default `pytest.ini` addopts
already include a coverage gate scoped to specific files, so partial-file runs with coverage
on can fail spuriously — prefer `--no-cov` for anything but a full-suite run.

## e2e tests (real subprocess servers, isolated temp SQLite + dynamic ports — see `mem:core`)
```
SESSION_SECRET=test python3 -m pytest tests/e2e -m e2e --no-cov
```
Needs `python3 -m playwright install chromium` once per environment. Never needs
`DATABASE_URL` set manually — `tests/e2e/conftest.py` fixtures manage their own isolated DBs.

## Epic-specific full verification (offline-desktop-sync)
```
bash specs/offline-desktop-sync/verify.sh
```
Runs unit + the sync-specific e2e tests + structural checks, `set -e` as the real gate (exit
!=0 on any failure). Historically this script silently didn't verify what it claimed to (see
`mem:known_bug_patterns`) — trust its exit code now, but if editing it again, read every line
of each embedded `python3 -c "..."` heredoc for bash/Python syntax mixups before trusting it.

## Git remote
Push to `gitea` (192.168.1.21), never directly to GitHub (Gitea mirrors automatically).
Changes always land on `develop` first; `main` is prod, merged only after dev validation.

# tech_stack

- Python 3.12 target (dev venv observed running 3.14 locally — code must stay 3.12-compatible
  per `requirements.txt`, don't rely on 3.13+-only stdlib features).
- FastAPI 0.115.6, SQLAlchemy 2.0.36 (declarative + `Mapped[]` typed columns throughout —
  match this style in new models, don't fall back to legacy `Column()` style),
  Pydantic 2.10.3, Jinja2 3.1.4, uvicorn 0.32.1, itsdangerous 2.2.0 (sessions).
- DB: SQLite (dev/test, `sqlite+pysqlite:///`) or PostgreSQL via `psycopg` (prod/dev-real,
  driver name is literally `postgresql+psycopg://`, not `postgresql://`). `app/db.py` picks
  connect args/pool kwargs based on which URL scheme is active — don't hardcode either.
- Desktop-only extra deps (`requirements-desktop.txt`, NOT in main `requirements.txt`,
  Windows-only, not installed in this dev environment): `pywebview`, `pyinstaller`,
  `sqlcipher3-binary`, `pywin32`. Code importing these must be guarded/optional — see
  `app/desktop/keyring.py`'s `try/except` around `win32crypt`.
- Frontend: vanilla CSS + HTMX + Chart.js + JsBarcode, no build step, no npm — JS/CSS assets
  vendored directly under `app/static/`, cache-busted via a content-hash query param computed
  in `app/routers/ui_common.py`.
- Frontend rules since release 0.5.0 (enforced by CSP `script-src 'self'; style-src 'self'` and by
  `tests/unit/test_frontend_js_modules.py` / `tests/unit/test_frontend_css.py`): JS only as ES modules under
  `app/static/js/` (no inline `<script>`, no `on*=`); CSS only in `app/static/css/` (no `style=`, no `<style>`,
  no hex colors in templates). Colors are tokens in `tokens.css` (light `:root` + `[data-theme="dark"]`);
  utilities shared by dashboard, login and print pages live in `utilities.css`. Charts read colors via
  `components/chart-palette.js` and redraw on the `gi:themechange` event. htmx is configured by
  `<meta name="htmx-config">` in `layouts/app_shell.html` (`attributesToSettle: ["class"]`). CSP violations
  are logged by `POST /csp-report` (`app/routers/csp_report.py`).
- Testing: pytest, `pytest-cov` (default `addopts` in `pytest.ini` runs with coverage gate
  `--cov-fail-under=80` scoped to a specific file allowlist — use `--no-cov` when running a
  subset, otherwise the coverage gate can spuriously fail on files outside that subset).
  Playwright for e2e (`pytest-playwright`), marker `e2e` excluded by default
  (`addopts = -q -m "not e2e"` in `pytest.ini`).

# known_bug_patterns

Two recurring bug families in this codebase, each hit multiple times across different
features — check for these specifically when touching related code.

## 1. Global-unique-but-treated-as-per-business

`products.sku`, `inventory_lots.lot_code`, `Customer.client_id` are all `unique=True`
GLOBALLY in the schema, not composite by `business_id`, despite composite
`(business_id, ...)` indexes existing for query performance (easy to misread as implying
compound uniqueness — it doesn't). Any "generate the next SKU/ID for this business" logic
that scopes its uniqueness check to `business_id` alone will eventually collide with another
business's row. Hit in: SKU auto-generation (prod incident, reverted to global-scoped
lookup), sync `apply_pull` customer dedup (introduced the same bug while fixing something
else, caught by mutation-testing review), sync `apply_pull` user replacement (same shape,
password-hash preservation had to be keyed by username not by scoped ID). Before writing any
"find next available X" or dedup-by-Y logic touching these three columns, check the real
constraint in `app/models.py`, don't infer it from an index name.

## 2. Code that was written but never executed

A pattern specific to the `offline-desktop-sync` epic (P1-P5, all slices): OpenCode-generated
implementation work that reads as plausible but crashes on the very first real invocation,
because nothing in the loop ever actually ran it. Concrete instances, worst first:
- `app/desktop/keyring.py::get_db_key()` called a `pathlib.Path.as_bytes()` method that
  doesn't exist — the SQLCipher key-derivation function had never generated a real key, ever.
- `app/desktop/launcher.py::main()` called `__import__("app.logger").get_logger(...)` —
  `__import__("app.logger")` returns the `app` package, not the submodule; crashed on the
  first log line, meaning the desktop entry point had never opened a window successfully.
- `tests/e2e/test_sync_multinode_e2e.py` (P5) had two independent `NameError`s (missing
  `sqlalchemy.select` import; `client_id-a` parsed as subtraction of undefined names instead
  of the variable `client_id_a`) — the test had literally never completed a single run.
- `packaging/desktop.spec` / `packaging/installer.iss` used fabricated PyInstaller/Inno Setup
  API that doesn't exist in either tool's real surface — would not have compiled.
- A test fixture (`central_server` in `tests/e2e/conftest.py`, P5) pointed directly at
  `app.settings.DATABASE_URL` and a hardcoded port 10000 — the real dev Postgres DB and the
  same port the actual dev server runs on — instead of an isolated temp DB + dynamic port.
- `specs/offline-desktop-sync/verify.sh` claimed to validate the epic's e2e tests but never
  actually invoked `pytest tests/e2e` — only checked file existence, always printed success.

All fixed as of 2026-08-29 (`docs/auditorias/2026-08-29-offline-desktop-sync-epic-full-audit{,-reaudit}.md`
has full detail), but the pattern is worth remembering: when reviewing or resuming any
OpenCode-authored PAE work, especially anything claiming a slice is "verde"/done in
`CLAUDE.md`, verify by actually running the thing — don't trust the claim, and don't trust a
"verify" script's exit code without reading what it actually executes line by line.

## Snapshot/test determinism and environment identity (2026-11-01)

- **Date-dependent snapshot baseline.** `tests/snapshots/ui_snapshot.py` broke CI on the 1st of the month: the home
  tab renders the current month (one point per day) and the last 12 months from `datetime.now()`. Fixed by freezing
  `app.routers.ui_home.datetime` (`_FrozenDatetime`, now = 2026-10-24). Rule: any harness must pin every
  non-deterministic input that reaches the HTML (current date, not only seeded dates). New harness captures of
  other date-driven views need the same freeze on the module that calls `datetime.now()`.
- **"Tested in dev" can be prod.** From 2026-09-07 to 2026-11-01 `dev.gestion.legatumbp.com` hit production (proxy
  default server). Before trusting a staging check, confirm identity: `/static/js/main.js` returns 200 and
  `/health` matches `curl localhost:10000/health` on ai-lab-home.
- **Theme tokens.** A color token that only exists in `:root` keeps its light value in dark mode. Chart.js axis
  text/lines come from `Chart.defaults` unless `applyChartDefaults()` sets them from `--chart-text`/`--chart-border`.

## Refactors executed by cc_local: partial lots and silent signature errors (2026-11-02, phase 5b)

- `cc_local_pae` marks a lot "OK" when tests pass and nothing outside its files changed. It does **not** check that
  every envelope in the lot was applied: in phase 5b one lot shipped only the new repository methods while the
  routers kept the old duplicated helpers, another lot deleted 147 lines of `ui_transfers.py`. Review each lot's diff
  against its envelopes.
- UI handlers are not all covered by tests: a call with the wrong signature (`_location_id_for_code(code, ctx)`,
  `_render_profit_items_tab(request=..., db=...)`) passed the full suite. `pyflakes` catches undefined names only.
- Removing a name from a module breaks every test that `monkeypatch.setattr`s it there (also multi-line and
  string-path forms). Before removing, scan tests with AST, not line `grep`. Pattern used: patch
  `ui_context.get_active_business_id` with `lambda _db, _req, **_kw: bid`; keep the old patch with `raising=False`
  while the router still imports it.
- New repository/service methods must not `commit()`; the router owns the transaction (cc_local moved a commit into
  `CoachService.add_cash_baseline`).
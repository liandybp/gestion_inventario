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

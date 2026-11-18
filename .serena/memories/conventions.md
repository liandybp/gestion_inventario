# conventions

- Repos live under `app/repositories/`, business logic under `app/services/` — routers
  (`app/routers/*.py`) should stay thin, delegating to a service/repo, not embedding query
  logic or multi-step business rules directly in a route handler. This split was retrofitted
  in a "Fase B" refactor; new code should follow the post-refactor pattern, not the old
  ui_tabs.py-does-everything style (that file no longer exists, was split into
  `ui_stock.py`/`ui_profit.py`/`ui_activity.py`/`ui_history.py`/`ui_dividends.py`/
  `ui_home.py`/`ui_tabs_common.py`).
- Logging: always `from app.logger import get_logger; _log = get_logger(__name__)` once at
  module scope. Never inline `import logging as _log_moduleN` per call site — this exact
  anti-pattern was found and removed from `app/desktop/keyring.py` (7 separate ad-hoc
  loggers) during a 2026-08-29 audit.
- Config: always import from `app.settings`, never read `os.getenv(...)` directly in
  application code outside of `app/settings.py` itself. A direct `os.getenv("DATABASE_URL")`
  bypass in `app/desktop/keyring.py` caused a real bug (fixed 2026-08-29) — same class of
  mistake as a test fixture once pointing at real infra instead of settings-mediated config.
- Money/Decimal: `Numeric(14, 4, asdecimal=False)` is the standard column type for currency
  amounts in `app/models.py` — match it for new money-related columns rather than `Float`.
- Audit trail: any action that should be traceable calls `app.audit.log_event(...)`, writing
  an `AuditLog` row. Note `AuditLog.user_id` is a nullable FK to `users.id` — code that
  bulk-deletes/replaces `User` rows (sync's `apply_pull`, or manual user deletion in
  `ui_users.py`) must null out `AuditLog.user_id` (and similarly for `DashboardWidget`,
  `ChatMessage`, `CoachActionLog`) *before* the delete, or it violates the FK under
  `PRAGMA foreign_keys=ON`.
- Multi-tenancy footgun: several tables have both a `unique=True` GLOBAL constraint
  (`products.sku`, `inventory_lots.lot_code`, `Customer.client_id`) and separate
  `(business_id, ...)` composite indexes for query performance — do not assume the composite
  index implies compound uniqueness. Any code deriving new IDs/SKUs/client_ids by scoping
  a "next number" query to `business_id` alone will eventually collide across businesses.
  See `mem:known_bug_patterns` — this has caused 3 separate production incidents.
- Routers UI: los routers de `app/routers/ui_*.py` no usan el ORM directamente (`select(`, `db.get(`,
  `db.scalar(`, `db.scalars(`, `db.execute(`, `db.query(`, `db.add(`, `db.delete(`) ni resuelven el
  negocio por su cuenta (`get_active_business_config/code/id`, `require_active_business_id`,
  `get_current_user_from_session` fuera de `ui_context.py`/`ui_auth.py`): la guarda es
  `tests/unit/test_routers_no_orm.py`. El contexto de la petición es `UIContext` (`ctx.config`,
  `ctx.business_code`, `ctx.active_business_id`, `ctx.user`), creado en el cuerpo del handler con
  `ctx = ui_context(request, db)`. Los métodos de repositorio/servicio no hacen `commit()`.
- Reglas únicas de documentos de venta (0.5.2): plazo de validez `parse_validity_days` (`app/business_config.py`,
  0-365, defecto 15); fecha por defecto `default_valid_until` y validación `resolve_valid_until`
  (`app/sales_documents_format.py`, funciones puras sin fpdf/ORM); longitudes de columnas de `businesses` con
  `_fit_column` en `ui_businesses.py`. No duplicar estas reglas en routers o plantillas.
- Errores que HTMX debe mostrar: 422 con `TemplateResponse` HTML (htmx solo hace swap de 4xx `text/html`), no
  `HTTPException` (JSON).
- Tests que parchean la resolución del negocio: `monkeypatch.setattr(ui_context, "get_active_business_id",
  lambda _db, _req, **_kw: bid)` (acepta `user=`); `require_active_business_id` ya no existe en `ui_context`.

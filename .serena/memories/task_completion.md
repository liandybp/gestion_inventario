# task_completion

A coding task on this repo is not done until:

1. `SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest --no-cov`
   passes (whole unit suite, not just the file you touched — cross-module regressions are
   common here, especially anything touching `app/models.py`, `app/db.py`, or `app/sync/`).
2. If the change touches a user-facing flow (new feature, not a pure bugfix), a Playwright
   e2e test exists under `tests/e2e/` and passes:
   `SESSION_SECRET=test python3 -m pytest tests/e2e -m e2e --no-cov`.
   This project's `CLAUDE.md` treats this as a hard requirement, not optional — "toda feature
   nueva se da por terminada solo con tests e2e".
3. No leftover `uvicorn` subprocesses or temp SQLite files — e2e fixtures should clean up in
   `finally`; verify with `ps aux | grep uvicorn` after a real-server e2e run if you wrote a
   new fixture.
4. If you touched anything under `app/sync/` or `app/desktop/`, also run
   `bash specs/offline-desktop-sync/verify.sh` (exit code must be 0).
5. Version bump (if any) is PATCH only, and only if explicitly requested — see `mem:core`.
6. Don't self-declare a change "verde"/complete/audited in `CLAUDE.md` without having actually
   run the thing you're claiming passes — this repo has a documented history (2026-08-29) of
   exactly that mistake causing multi-session-long false confidence in broken code.

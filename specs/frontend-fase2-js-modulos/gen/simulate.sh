#!/usr/bin/env bash
# Simula el PAE-017: aplica todos los envelopes a una copia limpia de b7a9881 y compara con el
# worktree validado. Uso: simulate.sh <target_worktree>
set -euo pipefail
REPO="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
TARGET="$1"
SIM="$(mktemp -d)/sim"
git -C "$REPO" worktree add --detach "$SIM" b7a9881 >/dev/null
ln -s "$REPO/.venv" "$SIM/.venv"
python3 "$(dirname "$0")/gen_tasks.py" apply "$TARGET" "$REPO" "$SIM"
echo "== diff -r (vacío = idéntico)"
diff -r -q -x .git -x .venv -x __pycache__ -x .pytest_cache -x '*.pyc' "$TARGET" "$SIM" && echo "IDENTICO"
echo "== unit + arnés en la simulación"
cd "$SIM"
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest -p no:warnings --no-cov -rfE tests/unit 2>&1 | tail -1
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline 2>/dev/null | tail -1
echo "SIM=$SIM"

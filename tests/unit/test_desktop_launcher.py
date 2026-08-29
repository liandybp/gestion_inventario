"""Tests para app/desktop/launcher.py.

`pywebview` no está instalado en este entorno (dependencia solo-Windows de
requirements-desktop.txt) — se inyecta un módulo falso en sys.modules ANTES
de importar app.desktop.launcher, técnica estándar para probar código que
depende de una librería opcional/específica de plataforma sin instalarla.

Cubre el code-review 2026-08-29 (docs/auditorias/2026-08-29-offline-desktop-sync-epic-full-audit.md,
H-P4-003 + hallazgos de la ronda de fixes): antes de esta suite, launcher.py
nunca había sido ejecutado ni probado — su bug de import (`__import__("app.logger")`)
solo se descubrió por lectura + ejecución manual, no por un test.
"""
from __future__ import annotations

import sys
import threading
import time
import types
from unittest.mock import MagicMock

import pytest

if "pywebview" not in sys.modules:
    sys.modules["pywebview"] = types.SimpleNamespace(
        create_window=MagicMock(), start=MagicMock()
    )

from app.desktop import launcher  # noqa: E402


def test_module_imports_without_error():
    """H-P4-003: el bug __import__("app.logger").get_logger(...) crasheaba
    con AttributeError en cuanto main() intentaba loguear. Con el fix
    (`from app.logger import get_logger`), el módulo importa limpio y
    `_log` es un logger real."""
    assert launcher._log is not None
    assert hasattr(launcher._log, "info")


def test_wait_ready_fails_fast_when_thread_dies(monkeypatch: pytest.MonkeyPatch):
    """Si el hilo de uvicorn muere (ej. puerto ocupado), _wait_ready debe
    fallar de inmediato con el motivo real, no agotar todo el timeout."""
    start_errors = [OSError("[Errno 98] Address already in use")]
    dead_thread = threading.Thread(target=lambda: None)
    dead_thread.start()
    dead_thread.join()  # ya terminó -> is_alive() es False

    start = time.time()
    with pytest.raises(RuntimeError, match="Address already in use"):
        launcher._wait_ready("http://127.0.0.1:1", dead_thread, start_errors, timeout=15.0)
    assert time.time() - start < 2.0  # falló rápido, no esperó los 15s


def test_wait_ready_succeeds_when_health_responds(monkeypatch: pytest.MonkeyPatch):
    """Camino feliz: /health responde 200 -> _wait_ready retorna sin excepción."""
    alive_thread = threading.Thread(target=lambda: time.sleep(5), daemon=True)
    alive_thread.start()
    try:
        monkeypatch.setattr(
            launcher.urllib.request, "urlopen", lambda *a, **k: MagicMock(read=lambda: b"ok")
        )
        launcher._wait_ready("http://127.0.0.1:1", alive_thread, [], timeout=5.0)
    finally:
        alive_thread.join(timeout=0.1)


def test_wait_ready_surfaces_http_error_detail_on_timeout(monkeypatch: pytest.MonkeyPatch):
    """Si /health responde con un error HTTP (servidor arriba pero degradado),
    el detalle debe aparecer en el RuntimeError final en vez de perderse
    detrás de un mensaje genérico de timeout."""
    import urllib.error

    alive_thread = threading.Thread(target=lambda: time.sleep(5), daemon=True)
    alive_thread.start()

    def _raise_http_error(*a, **k):
        raise urllib.error.HTTPError(
            "http://x/health", 503, "Service Unavailable",
            hdrs=None, fp=MagicMock(read=lambda: b'{"detail": "db down"}'),
        )

    try:
        monkeypatch.setattr(launcher.urllib.request, "urlopen", _raise_http_error)
        with pytest.raises(RuntimeError, match="HTTP 503"):
            launcher._wait_ready("http://127.0.0.1:1", alive_thread, [], timeout=0.5)
    finally:
        alive_thread.join(timeout=0.1)

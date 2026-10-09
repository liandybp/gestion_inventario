from __future__ import annotations

import asyncio
import threading
import time
import urllib.error
import urllib.request

import pywebview
import uvicorn

from app.logger import get_logger
from app.main import app

_log = get_logger(__name__)


def run_uvicorn(host: str = "127.0.0.1", port: int = 10000) -> tuple[uvicorn.Server, threading.Thread, list]:
    """Iniciar el servidor uvicorn en un hilo independiente.

    Devuelve también el hilo y una lista donde se apila cualquier excepción de
    arranque (ej. puerto ya en uso) — sin esto, `start()` moría en silencio
    (el hilo es daemon, sin excepthook propio) y `_wait_ready` agotaba todo su
    timeout sin dar ninguna pista del motivo real (code-review 2026-08-29).
    """
    config = uvicorn.Config(app=app, host=host, port=port, log_level="info")
    server = uvicorn.Server(config)
    start_errors: list[BaseException] = []

    def start() -> None:
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(server.run())
        except BaseException as e:  # noqa: BLE001
            _log.error("uvicorn no pudo arrancar: %s", e, exc_info=True)
            start_errors.append(e)

    thread = threading.Thread(target=start, daemon=True)
    thread.start()
    return server, thread, start_errors


def _wait_ready(url: str, thread: threading.Thread, start_errors: list, timeout: float = 15.0) -> None:
    """Poll /health hasta que uvicorn responda, en vez de un sleep fijo.

    H-P4 (auditoría 2026-08-29): un time.sleep(1) fijo es frágil — en una
    máquina lenta o con arranque de la BD más pesado, la ventana podía abrirse
    antes de que uvicorn estuviera listo. Mismo patrón que ya usan los
    fixtures e2e (tests/e2e/conftest.py::_wait_ready), con dos mejoras que
    surgieron en code-review: detecta si el hilo de uvicorn murió (falla
    rápido con el motivo real en vez de agotar el timeout completo) y
    conserva el último cuerpo de /health devuelto por un error HTTP (la BD
    puede tardar en levantar y responder 503 legítimamente antes de estar
    lista del todo — eso no es motivo para fallar rápido, pero si al final
    del timeout seguimos sin 200, ese detalle es la pista real).
    """
    deadline = time.time() + timeout
    last_detail = ""
    while time.time() < deadline:
        if not thread.is_alive():
            reason = start_errors[-1] if start_errors else "motivo desconocido"
            raise RuntimeError(f"uvicorn terminó antes de quedar listo: {reason}")
        try:
            urllib.request.urlopen(f"{url}/health", timeout=1).read()
            return
        except urllib.error.HTTPError as e:
            last_detail = f"HTTP {e.code}: {e.read().decode(errors='replace')[:300]}"
            time.sleep(0.2)
        except Exception:
            time.sleep(0.2)
    detail = f" — última respuesta: {last_detail}" if last_detail else ""
    raise RuntimeError(f"El servidor local no quedó listo en {timeout}s ({url}/health){detail}")


def main(
    host: str = "127.0.0.1",
    port: int = 10000,
    title: str = "Inventario - Escritorio",
    width: int = 1200,
    height: int = 800,
) -> None:
    """Punto de entrada para la aplicación desktop.

    1. Inicia uvicorn embebido en hilo separado
    2. Abre una ventana pywebview contra la URL local
    3. Mantiene el proceso vivo hasta que se cierra la ventana
    """
    _server, thread, start_errors = run_uvicorn(host=host, port=port)

    url = f"http://{host}:{port}"
    _wait_ready(url, thread, start_errors)
    _log.info("Abriendo ventana desktop en %s", url)

    try:
        pywebview.create_window(
            title=title,
            url=url,
            width=width,
            height=height,
            resizable=True,
            text_encoding="utf-8",
        )
        pywebview.start()
    except KeyboardInterrupt:
        pass
    finally:
        _log.info("Cerrando aplicación desktop")


if __name__ == "__main__":
    main()

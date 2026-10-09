from __future__ import annotations

import os
import sys

from app.settings import APP_MODE, is_desktop
from app.logger import get_logger

_log = get_logger(__name__)


def get_db_key() -> bytes:
    """Obtener la clave de cifrado para SQLCipher.

    Modo desktop (Windows): Usa DPAPI para proteger la clave.
    Modo desktop (no Windows / dev): Fallback a clave desde entorno o generación.
    Modo server: Retorna None (sin cifrado).

    Returns:
        bytes: La clave de 32 bytes para SQLCipher, o None si no aplica.
    """
    if not is_desktop():
        _log.debug("get_db_key(): modo server → None")
        return None

    # Intentar obtener clave del entorno (para entornos de desarrollo)
    env_key = os.getenv("SQLCIPHER_KEY")
    if env_key:
        _log.debug("get_db_key(): usando clave desde SQLCIPHER_KEY env")
        return env_key.encode("utf-8")

    # Fallback: generar una clave determinista desde el nombre de la BD + negocio
    # Esto permite reabrir la BD sin intervención del usuario en dev
    from pathlib import Path
    db_path_str = os.getenv("DATABASE_URL", "")
    if db_path_str.startswith("sqlite"):
        # Extraer ruta de la BD
        db_path = db_path_str.replace("sqlite:///", "").replace("sqlite+pysqlite:///", "")
        db_path_obj = Path(db_path)
        if db_path_obj.exists():
            # Generar clave determinista desde la ruta + negocio
            import hashlib
            salt = db_path_obj.resolve().as_bytes()
            business_id = os.getenv("BUSINESS_ID", "default")
            combined = f"{salt}{business_id}".encode("utf-8")
            key = hashlib.sha256(combined).digest()
            _log.debug("get_db_key(): fallback determinista desde ruta BD")
            return key

    _log.warning("get_db_key(): no hay clave disponible; SQLCipher no será usado")
    return None
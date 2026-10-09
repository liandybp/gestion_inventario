from __future__ import annotations

import base64
import hashlib
import sys
from pathlib import Path
from typing import Optional

from app.logger import get_logger
from app.settings import is_desktop

try:
    from win32crypt import CryptUnprotectData  # pywin32 DPAPI
    _HAS_DPAPI = True
except Exception:  # pywin32 no disponible (Linux/macOS)
    _HAS_DPAPI = False

_log = get_logger(__name__)

# Prefijo de 1 byte que marca qué codificación protegió la clave, para que
# unprotect_key() nunca tenga que ADIVINAR (encontrado en code-review 2026-08-29:
# si DPAPI fallaba al desproteger — ej. perfil de Windows migrado a otra máquina —
# el código caía a base64.b64decode() sobre el mismo blob cifrado por DPAPI, que
# en modo no estricto puede "tener éxito" silenciosamente y devolver una clave
# incorrecta en vez de fallar de forma clara).
_MARK_DPAPI = b"D"
_MARK_BASE64 = b"B"


def _dpapi_encrypt(data: bytes) -> bytes:
    """Encriptar datos usando Windows DPAPI."""
    if not _HAS_DPAPI:
        raise RuntimeError("DPAPI solo disponible en Windows con pywin32 instalado")
    encrypted = CryptUnprotectData(data, None)[1]
    return encrypted


def _dpapi_decrypt(encrypted: bytes) -> bytes:
    """Desencriptar datos usando Windows DPAPI."""
    if not _HAS_DPAPI:
        raise RuntimeError("DPAPI solo disponible en Windows con pywin32 instalado")
    decrypted = CryptUnprotectData(encrypted, None)[1]
    return decrypted


def protect_key_for_desktop(key: bytes) -> Optional[bytes]:
    """Proteger una clave de SQLCipher para el entorno desktop.

    Windows: Usa DPAPI para encriptar la clave atada al usuario/equipo.
    No Windows (dev): Retorna la clave codificada en base64 (fallback).
    El resultado lleva un prefijo de 1 byte (_MARK_DPAPI/_MARK_BASE64) que
    identifica la codificación usada, para que unprotect_key() nunca necesite
    adivinar cuál aplicar.

    Args:
        key: Clave de 32 bytes para SQLCipher.

    Returns:
        Optional[bytes]: Clave protegida (con prefijo de marca), o None si no
        aplica (modo server).
    """
    if not is_desktop():
        return None

    if sys.platform == "win32" and _HAS_DPAPI:
        try:
            protected = _dpapi_encrypt(key)
            _log.info("Clave de SQLCipher protegida con DPAPI")
            return _MARK_DPAPI + protected
        except Exception as e:  # noqa: BLE001
            _log.warning("DPAPI falló, usando fallback: %s", e)

    # Fallback: base64 encode para entornos no Windows / dev
    _log.info("Usando fallback base64 para protección de clave SQLCipher")
    return _MARK_BASE64 + base64.b64encode(key)


def unprotect_key(protected: Optional[bytes]) -> Optional[bytes]:
    """Recuperar la clave original desde la versión protegida.

    Despacha por el prefijo de marca de `protect_key_for_desktop()` en vez de
    adivinar por plataforma — evita que un fallo de DPAPI caiga silenciosamente
    a interpretar el mismo blob cifrado como base64 (code-review 2026-08-29:
    `base64.b64decode` en modo no estricto puede "tener éxito" sobre bytes
    binarios arbitrarios y devolver una clave incorrecta en vez de fallar).

    Args:
        protected: Clave protegida desde protect_key_for_desktop(), o None.

    Returns:
        Optional[bytes]: Clave original de 32 bytes para SQLCipher, o None si
        no se pudo recuperar.
    """
    if protected is None or len(protected) < 2:
        return None

    mark, payload = protected[:1], protected[1:]

    if mark == _MARK_DPAPI:
        if not (sys.platform == "win32" and _HAS_DPAPI):
            _log.error("Clave protegida con DPAPI pero DPAPI no está disponible en este equipo")
            return None
        try:
            key = _dpapi_decrypt(payload)
            _log.info("Clave de SQLCipher recuperada via DPAPI")
            return key
        except Exception as e:  # noqa: BLE001
            _log.error("DPAPI falló al recuperar la clave (perfil/equipo distinto?): %s", e)
            return None

    if mark == _MARK_BASE64:
        try:
            key = base64.b64decode(payload, validate=True)
            _log.info("Clave de SQLCipher recuperada via fallback base64")
            return key
        except Exception as e:  # noqa: BLE001
            _log.error("No se pudo decodificar la clave: %s", e)
            return None

    _log.error("Clave protegida con un formato desconocido (prefijo %r)", mark)
    return None


def get_db_key() -> Optional[bytes]:
    """Obtener la clave de cifrado para SQLCipher.

    Modo desktop: intenta `SQLCIPHER_KEY` del entorno; si no está, deriva una
    clave determinista (SHA-256) de la ruta absoluta del archivo SQLite, para
    poder reabrir la BD sin intervención del usuario en dev/primera corrida.
    Modo server: siempre None (sin cifrado).

    Returns:
        Optional[bytes]: la clave de 32 bytes para SQLCipher, o None si no aplica.
    """
    import os

    if not is_desktop():
        _log.debug("get_db_key(): modo server -> None")
        return None

    env_key = os.getenv("SQLCIPHER_KEY")
    if env_key:
        # SHA-256 normaliza cualquier passphrase a exactamente 32 bytes — el
        # raw-key hex de SQLCipher (PRAGMA key = "x'...'", app/db.py) requiere
        # una longitud fija; una passphrase humana de longitud arbitraria
        # rompía ese contrato (code-review 2026-08-29).
        _log.debug("get_db_key(): usando clave desde SQLCIPHER_KEY env")
        return hashlib.sha256(env_key.encode("utf-8")).digest()

    # Fallback: clave determinista desde la ruta absoluta del archivo SQLite.
    # H-P4-001/H-P4-002 (auditoría 2026-08-29): antes leía DATABASE_URL vía
    # os.getenv en vez de app.settings (bypass de configuración centralizada,
    # ver regla de app/settings.py del proyecto) y llamaba a un método
    # `Path.as_bytes()` que no existe en pathlib — la función nunca había
    # generado una clave real en ningún escenario.
    from app.settings import DATABASE_URL

    if not DATABASE_URL.startswith("sqlite"):
        _log.warning("get_db_key(): DATABASE_URL no es SQLite; SQLCipher no será usado")
        return None

    db_path_str = DATABASE_URL.replace("sqlite+pysqlite:///", "").replace("sqlite:///", "")
    db_path = Path(db_path_str)
    if not db_path.exists():
        _log.warning("get_db_key(): %s no existe; SQLCipher no será usado", db_path)
        return None

    salt = str(db_path.resolve()).encode("utf-8")
    business_id = os.getenv("BUSINESS_ID", "default").encode("utf-8")
    key = hashlib.sha256(salt + business_id).digest()
    _log.debug("get_db_key(): clave determinista derivada de %s", db_path)
    return key

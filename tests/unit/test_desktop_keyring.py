from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

from app.desktop.keyring import get_db_key, protect_key_for_desktop, unprotect_key


@pytest.fixture(autouse=True)
def _reset_app_settings_after_test():
    """Recarga app.settings tras cada test de este archivo.

    Encontrado en code-review 2026-08-29: monkeypatch.setenv revierte
    os.environ al terminar el test, pero NO vuelve a ejecutar app/settings.py
    — los globals de módulo (APP_MODE, DATABASE_URL) quedaban con el último
    valor que un test haya reload-eado (aquí, 'desktop'), filtrando ese modo
    hacia cualquier test posterior en el mismo proceso pytest que dependa de
    is_desktop()==False (p.ej. app/db.py evaluando si debe intentar la
    PRAGMA key de SQLCipher en cada conexión SQLite nueva).
    """
    yield
    import importlib
    import app.settings
    importlib.reload(app.settings)


class TestGetDbKey:
    """Tests para get_db_key() - cobertura unit en Linux/CI."""

    def test_returns_none_in_server_mode(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """En modo server, get_db_key debe retornar None."""
        from app.settings import is_desktop

        # Asegurar modo server
        monkeypatch.setenv("APP_MODE", "server")
        import importlib
        import app.settings
        importlib.reload(app.settings)
        assert not is_desktop()

        key = get_db_key()
        assert key is None

    def test_fallback_env_key(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Fallback: clave desde entorno SQLCIPHER_KEY."""
        from app.settings import is_desktop

        # Modo desktop
        monkeypatch.setenv("APP_MODE", "desktop")
        import importlib
        import app.settings
        importlib.reload(app.settings)
        assert is_desktop()

        # Proporcionar clave via entorno — se normaliza a 32 bytes vía SHA-256
        # (una passphrase arbitraria no es necesariamente de la longitud que
        # SQLCipher espera para su sintaxis de clave raw en hex, ver
        # app/db.py::_set_sqlite_pragma).
        import hashlib
        monkeypatch.setenv("SQLCIPHER_KEY", "un-passphrase-cualquiera")
        key = get_db_key()
        assert key is not None
        assert len(key) == 32
        assert key == hashlib.sha256(b"un-passphrase-cualquiera").digest()

    def test_fallback_deterministic_from_db_path(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Fallback determinista: clave generada desde la ruta de la BD.

        H-P4-002 (auditoría 2026-08-29): la versión original de este test nunca
        seteaba DATABASE_URL hacia el archivo temporal que crea, así que
        get_db_key() leía cualquier DATABASE_URL ambiental del proceso (":memory:"
        bajo la invocación estándar de la suite, que no existe como path) y
        nunca alcanzaba la rama del salt determinista.
        """
        from app.settings import is_desktop

        # Crear una BD temporal para que exista
        db_path = Path("/tmp/test_desktop_keyring.db")
        db_path.unlink(missing_ok=True)

        # Usar SQLAlchemy create_engine para crear la BD
        from sqlalchemy import create_engine
        from app.models import Base
        engine = create_engine(f"sqlite:///{db_path}")
        Base.metadata.create_all(engine)
        engine.dispose()

        # Modo desktop + DATABASE_URL apuntando al archivo real que acabamos de crear
        monkeypatch.setenv("APP_MODE", "desktop")
        monkeypatch.setenv("DATABASE_URL", f"sqlite+pysqlite:///{db_path}")
        import importlib
        import app.settings
        importlib.reload(app.settings)
        assert is_desktop()

        try:
            key = get_db_key()
            # Debería generar una clave determinista (no None)
            assert key is not None
            assert len(key) == 32  # SHA-256 produce 32 bytes

            # Determinismo: la misma ruta produce la misma clave.
            assert get_db_key() == key
        finally:
            db_path.unlink(missing_ok=True)


class TestProtectUnprotectKey:
    """Tests para protect_key_for_desktop / unprotect_key."""

    def test_protect_then_unprotect_with_fallback(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Proteger y recuperar clave usando fallback base64 (no Windows)."""
        from app.settings import is_desktop

        # Modo desktop
        monkeypatch.setenv("APP_MODE", "desktop")
        import importlib
        import app.settings
        importlib.reload(app.settings)
        assert is_desktop()

        # Generar una clave de prueba
        test_key = b"\x01" * 32  # 32 bytes para SQLCipher

        # Proteger
        protected = protect_key_for_desktop(test_key)
        assert protected is not None

        # Desproteger (fallback base64)
        recovered = unprotect_key(protected)
        assert recovered is not None
        assert recovered == test_key

    def test_protect_none_in_server_mode(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """En modo server, proteger debe retornar None."""
        from app.settings import is_desktop

        # Asegurar modo server
        monkeypatch.setenv("APP_MODE", "server")
        import importlib
        import app.settings
        importlib.reload(app.settings)
        assert not is_desktop()

        test_key = b"\x01" * 32
        protected = protect_key_for_desktop(test_key)
        assert protected is None
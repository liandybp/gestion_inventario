from __future__ import annotations

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from app.logger import get_logger
from app.settings import DATABASE_URL, DB_MAX_OVERFLOW, DB_POOL_SIZE

_log = get_logger(__name__)


@event.listens_for(Engine, "connect")
def _set_sqlite_pragma(dbapi_connection, connection_record):
    """Enable foreign key enforcement on SQLite connections.

    Also set SQLCipher PRAGMA key condicionalmente en modo desktop cuando
    haya una clave disponible (DPAPI protegida o fallback dev).
    """
    if DATABASE_URL.startswith("sqlite"):
        cursor = dbapi_connection.cursor()

        # PRAGMA key DEBE ser la primera operación en la conexión según la
        # documentación de SQLCipher — por eso corre antes que foreign_keys
        # (code-review 2026-08-29; orden invertido previamente).
        from app.settings import is_desktop
        if is_desktop():
            # Importo perezoso para evitar circular imports
            from app.desktop.keyring import get_db_key
            key = get_db_key()
            if key is not None:
                try:
                    # H-P4 (auditoría 2026-08-29): `key` es un digest binario
                    # crudo (SHA-256), no texto — key.decode() lanzaba
                    # UnicodeDecodeError casi siempre (silenciado por el except
                    # de abajo), así que SQLCipher nunca recibía la clave.
                    # SQLCipher espera una clave raw en hex vía `x'...'`.
                    cursor.execute(f"PRAGMA key = \"x'{key.hex()}'\"")
                    _log.debug("PRAGMA key establecido en conexión SQLite")
                except Exception as e:  # noqa: BLE001
                    _log.warning("No se pudo establecer PRAGMA key: %s", e)

        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

_pool_kwargs: dict = {}
if not DATABASE_URL.startswith("sqlite"):
    _pool_kwargs = {
        "pool_size": DB_POOL_SIZE,
        "max_overflow": DB_MAX_OVERFLOW,
    }

_connect_args: dict = {}
if DATABASE_URL.startswith("sqlite"):
    _connect_args = {"check_same_thread": False}

if _connect_args:
    engine = create_engine(
        DATABASE_URL,
        connect_args=_connect_args,
        pool_pre_ping=True,
    )
else:
    engine = create_engine(
        DATABASE_URL,
        **_pool_kwargs,
        pool_pre_ping=True,
    )

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


class Base(DeclarativeBase):
    pass


def get_session() -> Session:
    return SessionLocal()

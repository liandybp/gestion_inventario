from __future__ import annotations

import os

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+pysqlite:///./inventario.db")

_pool_kwargs: dict = {}
if not DATABASE_URL.startswith("sqlite"):
    _pool_kwargs = {
        "pool_size": int(os.getenv("DB_POOL_SIZE", "10")),
        "max_overflow": int(os.getenv("DB_MAX_OVERFLOW", "20")),
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

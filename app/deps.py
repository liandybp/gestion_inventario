from __future__ import annotations

from collections.abc import Generator

from sqlalchemy.orm import Session

from app.db import get_session


def session_dep() -> Generator[Session, None, None]:
    db = get_session()
    try:
        yield db
    finally:
        db.close()

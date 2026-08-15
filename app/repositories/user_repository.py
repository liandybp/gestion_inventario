"""Acceso a datos de usuarios (extraído de ui_users, H-028)."""
from __future__ import annotations
from typing import Optional
from sqlalchemy import select
from sqlalchemy.orm import Session
from app.models import Business, User, UserBusiness


class UserRepository:
    def __init__(self, db: Session):
        self._db = db

    def list_users(self) -> list[User]:
        return list(self._db.scalars(select(User).order_by(User.username.asc())))

    def list_businesses(self) -> list[Business]:
        return list(self._db.scalars(select(Business).order_by(Business.name.asc())))

    def get_by_username(self, username: str) -> Optional[User]:
        return self._db.scalar(select(User).where(User.username == username))

    def get(self, user_id: int) -> Optional[User]:
        return self._db.get(User, int(user_id))

    def add(self, user: User) -> None:
        self._db.add(user)

    def delete(self, user: User) -> None:
        self._db.delete(user)

    def set_user_businesses(self, user_id: int, business_ids: list[int]) -> None:
        self._db.query(UserBusiness).filter(UserBusiness.user_id == int(user_id)).delete()
        for bid in business_ids:
            self._db.add(UserBusiness(user_id=int(user_id), business_id=int(bid)))
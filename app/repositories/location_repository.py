"""Resolución de ubicaciones: código → id (única fuente de verdad, H-049)."""
from __future__ import annotations
from typing import Optional
from fastapi import HTTPException
from sqlalchemy import and_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from app.models import Business, Location
from app.repositories.business_repository import get_business_config


class LocationRepository:
    def __init__(self, db: Session, business_id: int, business_code: Optional[str] = None):
        self._db = db
        self._business_id = int(business_id)
        self._business_code = business_code or ""

    def location_id_for_code(self, code: str, *, auto_create: bool = True) -> int:
        """Resuelve código→id. Si no existe y auto_create, lo crea (flush)."""
        c = (code or "").strip()
        if not c:
            raise HTTPException(status_code=409, detail="location_code is required")
        loc_id = self._db.scalar(
            select(Location.id).where(
                and_(
                    Location.code == c,
                    Location.business_id == self._business_id,
                )
            )
        )
        if loc_id is not None:
            return int(loc_id)

        cfg = get_business_config(self._db, self._business_id)
        business_code = (self._business_code or "").strip()
        if not business_code:
            business_code = str(
                self._db.scalar(select(Business.code).where(Business.id == self._business_id)) or ""
            ).strip()
        prefix = "".join(ch if ch.isalnum() else "_" for ch in (business_code.upper() or "BUSINESS"))
        alt_code = c if c.startswith(f"{prefix}_") else f"{prefix}_{c}"

        loc_id = self._db.scalar(
            select(Location.id).where(
                and_(
                    Location.code == alt_code,
                    Location.business_id == self._business_id,
                )
            )
        )
        if loc_id is not None:
            return int(loc_id)

        if not auto_create:
            raise HTTPException(status_code=409, detail=f"Unknown location_code for this business: {c}")

        name = c
        try:
            cfg_central = str(getattr(cfg.locations.central, "code", "") or "").strip()
            if c == cfg_central or c.upper() == "CENTRAL":
                name = str(getattr(cfg.locations.central, "name", "") or "").strip() or c
            else:
                for p in (getattr(cfg.locations, "pos", None) or []):
                    p_code = str(getattr(p, "code", "") or "").strip()
                    if p_code == c:
                        name = str(getattr(p, "name", "") or "").strip() or c
                        break

            self._db.add(Location(business_id=int(self._business_id), code=alt_code, name=name))
            self._db.flush()
        except IntegrityError:
            self._db.rollback()

        loc_id = self._db.scalar(
            select(Location.id).where(
                and_(
                    Location.code == alt_code,
                    Location.business_id == self._business_id,
                )
            )
        )
        if loc_id is None:
            raise HTTPException(status_code=409, detail=f"Unknown location_code for this business: {c}")
        return int(loc_id)

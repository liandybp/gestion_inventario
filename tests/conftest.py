from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.db import Base
from app.models import Business, Product


@pytest.fixture
def db_session() -> Session:
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    TestingSessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

    Base.metadata.create_all(bind=engine)
    db = TestingSessionLocal()
    try:
        biz = Business(code="test", name="Test Business")
        db.add(biz)
        db.commit()
        yield db
    finally:
        db.close()
        Base.metadata.drop_all(bind=engine)
        engine.dispose()


@pytest.fixture
def business_id(db_session: Session) -> int:
    bid = db_session.query(Business.id).filter(Business.code == "test").scalar()
    assert bid is not None
    return int(bid)


@pytest.fixture
def sample_product(db_session: Session, business_id: int) -> Product:
    product = Product(
        business_id=business_id,
        sku="SKU100001",
        name="Sample Product",
        min_stock=2,
        unit_of_measure="ud",
        default_purchase_cost=1.0,
        default_sale_price=2.0,
        lead_time_days=0,
    )
    db_session.add(product)
    db_session.commit()
    db_session.refresh(product)
    return product


@pytest.fixture
def now_utc() -> datetime:
    return datetime.now(timezone.utc)

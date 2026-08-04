from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import Business, Location, User
from app.routers.ui_businesses import (
    business_create,
    business_delete,
    business_edit_form,
    business_update,
)


def _make_admin_request() -> MagicMock:
    """Build a mock Request with an admin session."""
    req = MagicMock()
    req.session = {"username": "admin", "active_business_id": 1}
    req.url.path = "/ui/tabs/businesses"
    return req


@pytest.fixture
def admin_request() -> MagicMock:
    return _make_admin_request()


@pytest.fixture
def admin_user(db_session: Session) -> User:
    user = User(
        username="admin",
        password_hash="pbkdf2_sha256$...",
        role="admin",
        is_active=True,
        business_id=1,
    )
    db_session.add(user)
    db_session.commit()
    return user


class TestBusinessCreate:
    def test_creates_business_and_central_location(self, admin_request, db_session, admin_user):
        result = business_create(
            request=admin_request,
            code="nuevo_negocio",
            name="Nuevo Negocio",
            db=db_session,
        )

        assert result.status_code == 200
        # Verify business was created
        biz = db_session.scalar(select(Business).where(Business.code == "nuevo_negocio"))
        assert biz is not None
        assert biz.name == "Nuevo Negocio"

        # Verify location was auto-created
        loc = db_session.scalar(
            select(Location).where(
                Location.business_id == biz.id,
                Location.code == "nuevo_negocio_CENTRAL",
            )
        )
        assert loc is not None
        assert loc.name == "Almacén Central"

    def test_duplicate_code_returns_409(self, admin_request, db_session, admin_user):
        # First creation
        business_create(request=admin_request, code="dup", name="First", db=db_session)

        # Second creation with same code
        result = business_create(request=admin_request, code="dup", name="Second", db=db_session)

        assert result.status_code == 409
        assert result.context["message_class"] == "error"

    def test_empty_code_returns_422(self, admin_request, db_session, admin_user):
        with pytest.raises(Exception) as exc:
            business_create(request=admin_request, code="", name="Test", db=db_session)
        assert exc.value.status_code == 422

    def test_empty_name_returns_422(self, admin_request, db_session, admin_user):
        with pytest.raises(Exception) as exc:
            business_create(request=admin_request, code="test", name="", db=db_session)
        assert exc.value.status_code == 422


class TestBusinessEdit:
    def test_edit_form_loads_business(self, admin_request, db_session, admin_user):
        biz = Business(code="edit_me", name="Edit Me")
        db_session.add(biz)
        db_session.commit()

        result = business_edit_form(request=admin_request, business_id=biz.id, db=db_session)

        assert result.status_code == 200
        assert result.context["business"].id == biz.id

    def test_edit_form_404_for_missing(self, admin_request, db_session, admin_user):
        with pytest.raises(Exception) as exc:
            business_edit_form(request=admin_request, business_id=9999, db=db_session)
        assert exc.value.status_code == 404


class TestBusinessUpdate:
    def test_updates_business(self, admin_request, db_session, admin_user):
        biz = Business(code="update_me", name="Update Me")
        db_session.add(biz)
        db_session.commit()

        result = business_update(
            request=admin_request,
            business_id=biz.id,
            code="updated_code",
            name="Updated Name",
            db=db_session,
        )

        assert result.status_code == 200
        db_session.refresh(biz)
        assert biz.code == "updated_code"
        assert biz.name == "Updated Name"

    def test_duplicate_code_returns_409(self, admin_request, db_session, admin_user):
        biz1 = Business(code="code_a", name="A")
        biz2 = Business(code="code_b", name="B")
        db_session.add_all([biz1, biz2])
        db_session.commit()

        result = business_update(
            request=admin_request,
            business_id=biz1.id,
            code="code_b",  # conflicts with biz2
            name="A Updated",
            db=db_session,
        )

        assert result.status_code == 409
        assert "X-Modal-Keep" in result.headers


class TestBusinessDelete:
    def test_deletes_empty_business(self, admin_request, db_session, admin_user):
        biz = Business(code="delete_me", name="Delete Me")
        db_session.add(biz)
        db_session.commit()
        ctx_bid = biz.id

        result = business_delete(request=admin_request, business_id=ctx_bid, db=db_session)

        assert result.status_code == 200
        assert db_session.get(Business, ctx_bid) is None

    def test_cannot_delete_last_business(self, admin_request, db_session, admin_user):
        # Only one business exists (from conftest "test" business)
        # Delete all except one
        # Actually db_session already has "test" business from conftest
        # Let's remove any extra businesses first
        for b in db_session.scalars(select(Business)).all():
            if b.code != "test":
                db_session.delete(b)
        db_session.commit()

        businesses = db_session.scalars(select(Business)).all()
        assert len(businesses) >= 1

        if len(businesses) == 1:
            result = business_delete(
                request=admin_request,
                business_id=businesses[0].id,
                db=db_session,
            )
            assert result.status_code == 400

    def test_cannot_delete_business_with_users(self, admin_request, db_session, admin_user):
        biz = Business(code="has_users", name="Has Users")
        db_session.add(biz)
        db_session.commit()

        user = User(
            username="someuser",
            password_hash="x",
            role="operator",
            business_id=biz.id,
        )
        db_session.add(user)
        db_session.commit()

        result = business_delete(request=admin_request, business_id=biz.id, db=db_session)

        assert result.status_code == 409
        assert db_session.get(Business, biz.id) is not None

    def test_cannot_delete_business_with_products(self, admin_request, db_session, admin_user):
        from app.models import Product

        biz = Business(code="has_products", name="Has Products")
        db_session.add(biz)
        db_session.commit()

        product = Product(
            business_id=biz.id,
            sku="SKU-DEL-001",
            name="Test Product",
            min_stock=0,
            unit_of_measure="ud",
        )
        db_session.add(product)
        db_session.commit()

        result = business_delete(request=admin_request, business_id=biz.id, db=db_session)

        assert result.status_code == 409
        assert db_session.get(Business, biz.id) is not None

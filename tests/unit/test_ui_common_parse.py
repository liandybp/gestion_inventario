from __future__ import annotations

import pytest

from app.routers.ui_common import parse_optional_float


@pytest.mark.parametrize("raw", ["nan", "NaN", "inf", "-inf", "Infinity", "1e999"])
def test_parse_optional_float_rejects_non_finite(raw: str) -> None:
    assert parse_optional_float(raw) is None


@pytest.mark.parametrize(("raw", "expected"), [("1,5", 1.5), ("1.234,5", 1234.5), (" 2 ", 2.0), ("", None), ("abc", None)])
def test_parse_optional_float_keeps_existing_behaviour(raw: str, expected) -> None:
    assert parse_optional_float(raw) == expected


@pytest.mark.parametrize("raw", ["nan", "inf", "abc"])
def test_parse_typed_float_rejects_typed_invalid(raw: str) -> None:
    """H-204: un precio tecleado inválido es un 422, no un campo vacío (que usaría el precio por defecto)."""
    from fastapi import HTTPException

    from app.routers.ui_common import parse_typed_float

    with pytest.raises(HTTPException) as exc:
        parse_typed_float(raw, "El precio debe ser un número")
    assert exc.value.status_code == 422
    assert exc.value.detail == "El precio debe ser un número"


@pytest.mark.parametrize(("raw", "expected"), [("", None), ("  ", None), (None, None), ("2,5", 2.5)])
def test_parse_typed_float_empty_or_valid(raw, expected) -> None:
    from app.routers.ui_common import parse_typed_float

    assert parse_typed_float(raw, "x") == expected


def test_delete_uploaded_image_only_touches_upload_dir(tmp_path, monkeypatch) -> None:
    from app.routers import ui_common

    monkeypatch.setattr(ui_common, "_UPLOAD_DIR", tmp_path)
    img = tmp_path / "abc.png"
    img.write_bytes(b"x")
    outside = tmp_path.parent / "keep.png"
    outside.write_bytes(b"x")

    ui_common.delete_uploaded_image("/static/img/keep.png")  # no es una subida: se ignora
    ui_common.delete_uploaded_image(None)
    ui_common.delete_uploaded_image("/static/uploads/../keep.png")  # solo el nombre: no sale del dir
    ui_common.delete_uploaded_image("/static/uploads/abc.png")

    assert not img.exists()
    assert outside.exists()

from __future__ import annotations

import pytest

from app.services.sku_range import (
    SKURangeExhausted,
    SKU_RANGE_SIZE,
    next_sku_in_range,
    sku_range_base,
)


def test_base_is_business_times_range_size():
    assert sku_range_base(1) == 10000
    assert sku_range_base(2) == 20000


def test_empty_list_starts_at_base_plus_one():
    assert next_sku_in_range([], 2) == "SKU020001"


def test_continues_from_max_in_range():
    assert next_sku_in_range(["SKU020001", "SKU020005"], 2) == "SKU020006"


def test_ignores_skus_of_other_business_ranges():
    # SKUs del negocio 1 no afectan la numeración del negocio 2
    assert next_sku_in_range(["SKU010500", "SKU000509"], 2) == "SKU020001"


def test_ignores_non_numeric_suffix():
    assert next_sku_in_range(["SKUABC", "SKU020003"], 2) == "SKU020004"


def test_ranges_are_disjoint_between_businesses():
    a = next_sku_in_range(["SKU010001"], 1)
    b = next_sku_in_range(["SKU020001"], 2)
    assert a.startswith("SKU01") and b.startswith("SKU02")
    assert a != b


def test_lowercase_sku_counted_in_range():
    # H-006: las queries de SKU usan ilike (case-insensitive); un SKU manual en
    # minúsculas dentro del rango debe contar para max_n.
    assert next_sku_in_range(["sku020001", "SKU020005"], 2) == "SKU020006"


def test_ceiling_is_inclusive_in_range():
    # El último número válido del rango es base + SKU_RANGE_SIZE (inclusivo).
    base = sku_range_base(2)
    ceiling = base + SKU_RANGE_SIZE
    # Todos los números salvo el último: el siguiente disponible es `ceiling`.
    existing = [f"SKU{n:06d}" for n in range(base + 1, ceiling)]
    assert next_sku_in_range(existing, 2) == f"SKU{ceiling:06d}"


def test_range_saturation_raises_instead_of_repeating_ceiling():
    # H-001: al agotar los SKU_RANGE_SIZE números del negocio, el generador debe
    # fallar explícitamente en vez de devolver el mismo candidato para siempre
    # (el bug previo dejaba `ceiling` fuera del escaneo -> 409 permanente).
    base = sku_range_base(2)
    ceiling = base + SKU_RANGE_SIZE
    saturated = [f"SKU{n:06d}" for n in range(base + 1, ceiling + 1)]
    with pytest.raises(SKURangeExhausted):
        next_sku_in_range(saturated, 2)

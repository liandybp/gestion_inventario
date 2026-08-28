from __future__ import annotations

SKU_RANGE_SIZE = 10000


class SKURangeExhausted(Exception):
    """El rango de SKU reservado a un negocio está agotado (base+1 .. base+SKU_RANGE_SIZE)."""


def sku_range_base(business_id: int) -> int:
    """Base del rango de SKU reservado a un negocio.

    El último número válido del rango es base + SKU_RANGE_SIZE (el rango son
    exactamente SKU_RANGE_SIZE números: base+1 .. base+SKU_RANGE_SIZE).
    """
    return int(business_id) * SKU_RANGE_SIZE


def next_sku_in_range(
    existing_skus: list[str],
    business_id: int,
    prefix: str = "SKU",
    width: int = 6,
) -> str:
    """Siguiente SKU autogenerado dentro del rango (base, base + SKU_RANGE_SIZE].

    El rango es cerrado en el extremo superior (base+SKU_RANGE_SIZE incluido) para
    que el enésimo SKU generado siga contando al escanear: con el antiguo chequeo
    estricto `n < ceiling`, el último candidato quedaba fuera del escaneo y se
    devolvía el mismo número para siempre (colisión permanente, incidente v0.4.6).

    El filtro normaliza a mayúsculas porque las queries de SKU usan `ilike`
    (case-insensitive): un SKU manual en minúsculas dentro del rango debe contar
    para `max_n`. Sufijos no numéricos y números fuera del rango se ignoran.
    Rango vacío -> base + 1. Rango agotado -> SKURangeExhausted.

    `zfill(width)` solo rellena, no trunca: para business_id >= 100 el sufijo
    excede `width` dígitos y el SKU es más ancho (columna String(64), sin overflow).
    """
    base = sku_range_base(business_id)
    ceiling = base + SKU_RANGE_SIZE
    max_n = base
    for sku in existing_skus:
        s = str(sku or "").upper()
        if not s.startswith(prefix.upper()):
            continue
        suffix = s[len(prefix):]
        if suffix.isdigit():
            n = int(suffix)
            if base < n <= ceiling:
                max_n = max(max_n, n)
    if max_n >= ceiling:
        raise SKURangeExhausted(
            f"Rango de SKU agotado para business_id={business_id} "
            f"(límite {prefix}{str(ceiling).zfill(width)})"
        )
    return f"{prefix}{str(max_n + 1).zfill(width)}"

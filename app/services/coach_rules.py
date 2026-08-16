from __future__ import annotations

import logging
from typing import Optional

_log = logging.getLogger(__name__)

# Margen bruto: caída vs. promedio de 6 meses. >=3pp = alerta amarilla, >=6pp = roja.
MARGIN_DROP_YELLOW_PP = 3.0
MARGIN_DROP_RED_PP = 6.0

# Gastos operativos / ventas. >=30% empieza a ser alto para un negocio minorista típico,
# >=45% indica que los gastos se están comiendo la mayor parte del margen.
EXPENSE_RATIO_YELLOW = 0.30
EXPENSE_RATIO_RED = 0.45

# Extracciones / utilidad neta. >=80% deja poco margen de reinversión; >=100% significa
# que se está extrayendo más de lo que el negocio genera (drena caja).
EXTRACTION_RATIO_YELLOW = 0.80
EXTRACTION_RATIO_RED = 1.0

# % de ventas del mes que representa el cliente principal. >=30% es dependencia relevante,
# >=50% es riesgo alto si ese cliente se va.
CUSTOMER_CONCENTRATION_YELLOW = 0.30
CUSTOMER_CONCENTRATION_RED = 0.50


def evaluate(indicators: dict) -> list[dict]:
    """Evalúa indicadores contra reglas. Log errores, nunca alucina."""
    try:
        checks = [_rule_margin, _rule_expense_ratio, _rule_extraction_ratio,
                  _rule_dead_stock, _rule_customer_concentration]
        recs = [r(indicators) for r in checks]
        return [r for r in recs if r is not None]
    except Exception as e:
        _log.error(f"Error en evaluate: {e}", exc_info=True)
        return []


def _rule_margin(ind: dict) -> Optional[dict]:
    current, avg6 = ind.get("margin_pct"), ind.get("margin_avg_6m")
    if current is None or avg6 is None:
        return None
    drop = avg6 - current
    if drop < MARGIN_DROP_YELLOW_PP:
        return None
    severity = "red" if drop >= MARGIN_DROP_RED_PP else "yellow"
    return {"rule_key": "margin_drop", "severity": severity,
            "symptom": f"Tu margen bajó a {current:.1f}%", "cause": "revisa costos de compra",
            "action": "renegocia con proveedor", "impact_amount": None, "impact_text": "revisa lista de precios"}


def _rule_expense_ratio(ind: dict) -> Optional[dict]:
    ratio = ind.get("expense_ratio")
    if ratio is None or ratio < EXPENSE_RATIO_YELLOW:
        return None
    severity = "red" if ratio >= EXPENSE_RATIO_RED else "yellow"
    return {"rule_key": "expense_ratio_high", "severity": severity,
            "symptom": f"Gastos {ratio * 100:.0f}% de ventas", "cause": "gasto creciendo rápido",
            "action": "revisa tu gasto más alto", "impact_amount": None, "impact_text": "audit de gastos"}


def _rule_extraction_ratio(ind: dict) -> Optional[dict]:
    ratio = ind.get("extraction_ratio")
    if ratio is None or ratio < EXTRACTION_RATIO_YELLOW:
        return None
    severity = "red" if ratio >= EXTRACTION_RATIO_RED else "yellow"
    return {"rule_key": "extraction_ratio_high", "severity": severity,
            "symptom": f"Extracciones {ratio * 100:.0f}%", "cause": "drena caja",
            "action": "reduce extracciones", "impact_amount": None, "impact_text": "compare vs utilidad"}


def _rule_dead_stock(ind: dict) -> Optional[dict]:
    value = ind.get("dead_stock_value") or 0.0
    items = ind.get("dead_stock_items") or []
    if value <= 0 or not items:
        return None
    names = ", ".join(i["name"] for i in items[:3])
    return {"rule_key": "dead_stock", "severity": "yellow",
            "symptom": f"${value:,.0f} sin vender 90 días", "cause": f"stock parado en: {names}",
            "action": "promo o liquidación", "impact_amount": value, "impact_text": f"Recuperarías ~${value:,.0f}"}


def _rule_customer_concentration(ind: dict) -> Optional[dict]:
    share = ind.get("customer_concentration")
    if share is None or share < CUSTOMER_CONCENTRATION_YELLOW:
        return None
    severity = "red" if share >= CUSTOMER_CONCENTRATION_RED else "yellow"
    name = ind.get("top_customer_name") or "cliente principal"
    return {"rule_key": "customer_concentration", "severity": severity,
            "symptom": f"{name} {share * 100:.0f}% ventas", "cause": "dependencia riesgosa",
            "action": "diversifica clientes", "impact_amount": None, "impact_text": "riesgo si cliente se va"}
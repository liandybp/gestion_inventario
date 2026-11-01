from __future__ import annotations

import json
import time

import pytest

pytest.importorskip("playwright")
pytest.importorskip("pytest_playwright")

from playwright.sync_api import Page
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.models import Business, DashboardWidget, User

pytestmark = pytest.mark.e2e

MONTHS = ["2026-05", "2026-06", "2026-07", "2026-08", "2026-09", "2026-10"]
MULTI = {"labels": MONTHS, "datasets": [
    {"label": "Punto de venta Playa", "data": [2690.5, 1292.5, 2741.9, 2001.0, 873.0, 2834.0]},
    {"label": "Taller Boyeros", "data": [980.5, 90.0, 545.4, 1453.5, 791.2, 152.5]},
    {"label": "Taller Cerro", "data": [967.5, 976.0, 971.0, 1181.7, 413.2, 0.0]}]}
SINGLE = {"labels": ["A", "B"], "values": [1.0, 2.0]}


def _seed_widget(db_path: str, payload: dict) -> int:
    engine = create_engine(f"sqlite+pysqlite:///{db_path}")
    db = sessionmaker(bind=engine)()
    try:
        admin = db.scalar(select(User).where(User.username == "admin"))
        bid = admin.business_id if admin is not None and admin.business_id is not None else \
            db.scalar(select(Business.id).where(Business.code == "recambios"))
        w = DashboardWidget(business_id=bid, type="bar", title="Ventas por punto de venta", position=1,
                            params=json.dumps({"tool": "sales_by_location_monthly", "tool_args": {"months": 6},
                                               "payload": payload, "unit": None}))
        db.add(w)
        db.commit()
        return int(w.id)
    finally:
        db.close()
        engine.dispose()


def _chart_info(page: Page, base_url: str, wid: int) -> dict:
    page.goto(f"{base_url}/ui/login")
    page.fill('input[name="username"]', "admin")
    page.fill('input[name="password"]', "admin")
    page.click('button[type="submit"]')
    page.wait_for_url(f"{base_url}/ui/dashboard**")
    page.wait_for_function("typeof window.htmx !== 'undefined'")
    page.locator("#tab-agent").click()
    page.locator(f"#chart-{wid}").wait_for(state="attached", timeout=10000)
    # Polling con page.evaluate (NO wait_for_function): la CSP de la app no permite
    # 'unsafe-eval' y wait_for_function con reintentos evalúa strings en la página.
    js = (f"() => {{ const el = document.getElementById('chart-{wid}');"
          " const c = (el && window.Chart) ? Chart.getChart(el) : null;"
          " return c ? {n: c.data.datasets.length, labels: c.data.labels.length,"
          " legend: c.options.plugins.legend.display, names: c.data.datasets.map(d => d.label)} : null; }")
    deadline = time.time() + 10
    info = page.evaluate(js)
    while info is None and time.time() < deadline:
        time.sleep(0.2)
        info = page.evaluate(js)
    assert info is not None, "el gráfico del widget no se inicializó"
    return info


def test_multiseries_bar_chart_one_dataset_per_location(central_server, page: Page) -> None:
    base_url, db_path = central_server
    wid = _seed_widget(db_path, MULTI)
    info = _chart_info(page, base_url, wid)
    assert info == {"n": 3, "labels": 6, "legend": True,
                    "names": ["Punto de venta Playa", "Taller Boyeros", "Taller Cerro"]}


def test_single_series_bar_chart_unchanged(central_server, page: Page) -> None:
    base_url, db_path = central_server
    wid = _seed_widget(db_path, SINGLE)
    info = _chart_info(page, base_url, wid)
    assert info["n"] == 1 and info["labels"] == 2 and info["legend"] is False

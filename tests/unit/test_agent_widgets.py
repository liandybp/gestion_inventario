from app.agent.widgets import (
    add_widget, delete_widget, latest_widget_meta, list_widgets, widgets_with_payload,
)


def test_add_and_list_widget(db_session, business_id):
    w = add_widget(db_session, business_id, type="kpi", title="Valor", tool="inventory_value_total",
                   tool_args={}, payload={"value": 10.0, "label": None}, unit=None, created_by=None)
    assert w.id is not None
    widgets = widgets_with_payload(db_session, business_id)
    assert len(widgets) == 1
    assert widgets[0]["title"] == "Valor"
    assert widgets[0]["payload"] == {"value": 10.0, "label": None}


def test_delete_widget_scoped_to_business(db_session, business_id):
    w = add_widget(db_session, business_id, type="kpi", title="X", tool="stock", tool_args={},
                   payload={"value": 1.0, "label": None}, unit=None, created_by=None)
    assert delete_widget(db_session, business_id, w.id) is True
    assert delete_widget(db_session, business_id, 999999) is False
    assert list_widgets(db_session, business_id) == []


def test_latest_widget_meta_returns_none_without_widgets(db_session, business_id):
    assert latest_widget_meta(db_session, business_id) is None


def test_latest_widget_meta_returns_last_tool_and_args(db_session, business_id):
    add_widget(db_session, business_id, type="kpi", title="A", tool="total_sales",
              tool_args={"start": "2026-01-01"}, payload={}, unit=None, created_by=None)
    add_widget(db_session, business_id, type="bar", title="B", tool="sales_by_product",
              tool_args={"limit": 5}, payload={}, unit=None, created_by=None)
    meta = latest_widget_meta(db_session, business_id)
    assert meta == {"tool": "sales_by_product", "tool_args": {"limit": 5}, "type": "bar"}

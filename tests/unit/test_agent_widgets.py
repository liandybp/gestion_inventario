from app.agent.widgets import add_widget, delete_widget, list_widgets, widgets_with_payload


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

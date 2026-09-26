"""Smoke tests that the example app serves /api/v1alpha3."""

from fastapi.testclient import TestClient


def test_v1alpha3_graph_from_example_app():
    from app import app

    client = TestClient(app)
    res = client.get("/api/v1alpha3/graph")
    assert res.status_code == 200
    body = res.json()
    assert "events" in body["catalog"]


def test_v1alpha3_capabilities_from_example_app():
    from app import app

    client = TestClient(app)
    res = client.get("/api/v1alpha3/capabilities")
    assert res.status_code == 200
    body = res.json()
    assert "addons" in body
    assert "ml_metrics" not in body

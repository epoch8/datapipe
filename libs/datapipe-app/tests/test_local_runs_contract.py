"""Contract smoke for local in-memory runs + capabilities flags."""

from __future__ import annotations

from fastapi.testclient import TestClient

from datapipe_app.datapipe_api import DatapipeAPI


def test_capabilities_advertise_run_flags(app: DatapipeAPI):
    client = TestClient(app)
    body = client.get("/api/v1alpha3/capabilities").json()
    for key in (
        "graph",
        "table_data",
        "run_history",
        "run_start",
        "run_stop",
        "run_logs",
        "transform_run",
        "transform_reset",
    ):
        assert key in body


def test_stop_run_endpoint_exists(app: DatapipeAPI):
    client = TestClient(app)
    started = client.post("/api/v1alpha3/runs", json={"labels": [], "background": True})
    assert started.status_code == 200
    run_id = started.json()["run_id"]
    stopped = client.post(f"/api/v1alpha3/runs/{run_id}/stop")
    # Endpoint is registered; body depends on race with background completion.
    assert stopped.status_code in (200, 409)

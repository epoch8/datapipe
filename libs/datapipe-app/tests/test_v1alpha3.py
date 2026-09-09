import typing as t

import pandas as pd
import pytest
from datapipe.compute import run_steps
from fastapi.testclient import TestClient

from datapipe_app.datapipe_api import DatapipeAPI
from datapipe_app.models import AddonCapability


def test_graph_works(app):
    client = TestClient(app)
    res = client.get("/api/v1alpha3/graph")
    assert res.status_code == 200


@pytest.fixture
def test_client(app: DatapipeAPI) -> t.Iterator[TestClient]:
    events_table = app.ds.get_table("events")
    events_table.store_chunk(
        pd.DataFrame.from_records(
            [
                {
                    "user_id": 1,
                    "event_id": 1,
                    "event": {"event_type": "click", "offer_id": 1},
                }
            ]
        )
    )
    run_steps(ds=app.ds, steps=app.steps)
    yield TestClient(app)


def get_request_data() -> t.Iterator[t.Dict[str, t.Any]]:
    yield dict(
        url="/api/v1alpha3/get-table-data",
        json={
            "table": "events",
            "filters": {
                "user_id": 1,
            },
        },
        result={
            "user_id": 1,
            "event_id": 1,
            "event": {"event_type": "click", "offer_id": 1},
        },
    )
    yield dict(
        url="/api/v1alpha3/get-table-data",
        json={
            "table": "user_profile",
            "filters": {
                "user_id": 1,
            },
        },
        result={
            "user_id": 1,
            "offer_clicks": [1],
            "events_count": 1,
            "active": True,
        },
    )


@pytest.mark.parametrize("request_data", get_request_data())
def test_get_table_data(test_client: TestClient, request_data: t.Dict[str, t.Any]):
    response = test_client.post(url=request_data["url"], json=request_data["json"])
    assert response.status_code == 200
    assert response.json()["data"][0] == request_data["result"]


def test_capabilities(app):
    client = TestClient(app)
    res = client.get("/api/v1alpha3/capabilities")
    assert res.status_code == 200
    body = res.json()
    assert body["graph"] is True
    assert body["table_data"] is True
    assert body["table_meta"] is True
    assert body["transform_meta"] is True
    assert body["run_history"] is True
    assert body["run_start"] is True
    assert body["run_stop"] is True
    assert body["run_logs"] is True
    assert body["transform_run"] is True
    assert body["transform_reset"] is True
    assert "addons" in body
    assert body.get("run_logs_configured") is False


def test_capabilities_endpoint_with_addons(app):
    from datapipe_app import api_v1alpha3

    addon = AddonCapability(name="demo-addon", features={"widgets": True})
    mounted = api_v1alpha3.make_app(
        app.ds,
        app.catalog,
        app.pipeline,
        app.steps,
        addons=[addon],
    )
    client = TestClient(mounted)
    res = client.get("/capabilities")
    assert res.status_code == 200
    body = res.json()
    assert body["addons"] == [{"name": "demo-addon", "features": {"widgets": True}}]
    assert body["run_start"] is True
    assert body["graph"] is True


def test_settings(app):
    client = TestClient(app)
    res = client.get("/api/v1alpha3/settings")
    assert res.status_code == 200


def test_pipeline_overview(app):
    client = TestClient(app)
    res = client.get("/api/v1alpha3/pipeline")
    assert res.status_code == 200
    body = res.json()
    assert "stages" in body
    assert "label_graph" in body
    assert "available_label_keys" in body
    assert "recent_runs" in body
    assert body.get("pipeline_id") == "local"

def test_graph_includes_schema_and_stages(app):
    client = TestClient(app)
    res = client.get("/api/v1alpha3/graph")
    assert res.status_code == 200
    body = res.json()
    assert "stages" in body
    assert body["catalog"]
    first_table = next(iter(body["catalog"].values()))
    assert "schema" in first_table
    assert first_table["size"] is None


def test_table_size(test_client: TestClient):
    res = test_client.get("/api/v1alpha3/tables/events/size")
    assert res.status_code == 200
    assert res.json() == {"table": "events", "size": 1}


def test_reset_metadata(test_client: TestClient, app: DatapipeAPI):
    step_name = app.steps[0].name
    res = test_client.post(f"/api/v1alpha3/transforms/{step_name}/reset-metadata")
    assert res.status_code == 200
    assert res.json() == {"transform_name": step_name, "status": "ok"}


def test_reset_metadata_unknown_transform(app):
    client = TestClient(app)
    res = client.post("/api/v1alpha3/transforms/does-not-exist/reset-metadata")
    assert res.status_code == 404


def test_runs_list_empty(app):
    client = TestClient(app)
    res = client.get("/api/v1alpha3/runs")
    assert res.status_code == 200
    body = res.json()
    assert body["rows"] == []
    assert body["total"] == 0


def test_start_run_and_logs(test_client: TestClient):
    start = test_client.post("/api/v1alpha3/runs", json={"labels": [], "background": False})
    assert start.status_code == 200
    started = start.json()
    assert "run_id" in started
    assert started["status"] in ("succeeded", "failed", "running")

    run_id = started["run_id"]
    detail = test_client.get(f"/api/v1alpha3/runs/{run_id}")
    assert detail.status_code == 200
    assert detail.json()["run_id"] == run_id

    logs = test_client.get(f"/api/v1alpha3/runs/{run_id}/logs")
    assert logs.status_code == 200
    body = logs.json()
    assert body["run_id"] == run_id
    assert isinstance(body["lines"], list)

    listed = test_client.get("/api/v1alpha3/runs")
    assert listed.status_code == 200
    assert listed.json()["total"] >= 1

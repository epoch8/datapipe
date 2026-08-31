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
    body = res.json()
    assert "events" in body["catalog"]
    assert "user_profile" in body["catalog"]
    assert len(body["pipeline"]) >= 1


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


def test_capabilities_has_no_ml_fields(app, monkeypatch):
    monkeypatch.setenv("DATAPIPE_APP_PIPELINE_ID", "example_pipeline")
    from datapipe_app import settings

    settings.API_SETTINGS = settings.APISettings()

    client = TestClient(app)
    res = client.get("/api/v1alpha3/capabilities")
    assert res.status_code == 200
    body = res.json()
    assert "ml_metrics" not in body
    assert "ml_training" not in body
    assert body["pipeline_id"] == "example_pipeline"
    assert body["addons"] == []


def test_capabilities_includes_injected_addons():
    from datapipe_app.capabilities import collect_addon_capabilities

    extra = [AddonCapability(name="demo-addon", features={"widgets": True, "quota": 10})]
    addons = collect_addon_capabilities(extra=extra)
    assert any(a.name == "demo-addon" and a.features["widgets"] is True for a in addons)


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
    assert res.json()["addons"] == [{"name": "demo-addon", "features": {"widgets": True}}]


def test_settings(app, monkeypatch):
    monkeypatch.setenv("DATAPIPE_APP_PIPELINE_ID", "example_pipeline")
    from datapipe_app import settings

    settings.API_SETTINGS = settings.APISettings()

    client = TestClient(app)
    res = client.get("/api/v1alpha3/settings")
    assert res.status_code == 200
    body = res.json()
    assert body["pipeline_id"] == "example_pipeline"
    assert isinstance(body["version"], str)
    assert "run_logs_configured" not in body
    assert "observability_db_connected" not in body


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

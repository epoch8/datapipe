import typing as t

import pandas as pd
import pytest
from datapipe.compute import run_steps
from fastapi.testclient import TestClient

from datapipe_app.datapipe_api import DatapipeAPI


def test_graph_works(app):
    client = TestClient(app)
    res = client.get("/api/v1alpha2/graph")
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
        url="/api/v1alpha2/get-table-data",
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
        url="/api/v1alpha2/get-table-data",
        json={
            "table": "events",
            "filters": {
                "user_id": "1",
            },
        },
        result={
            "user_id": 1,
            "event_id": 1,
            "event": {"event_type": "click", "offer_id": 1},
        },
    )
    yield dict(
        url="/api/v1alpha2/get-table-data",
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
def test_update_data(test_client: TestClient, request_data: t.Dict[str, t.Any]):
    response = test_client.post(url=request_data["url"], json=request_data["json"])
    assert response.status_code == 200
    print(response.json())
    assert response.json()["data"][0] == request_data["result"]

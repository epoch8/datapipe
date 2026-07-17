import time

import pytest
from fastapi.testclient import TestClient


def test_graph_works(app):
    client = TestClient(app)
    res = client.get("/api/v1alpha1/graph")
    assert res.status_code == 200


@pytest.mark.parametrize(
    "background",
    [True, False],
)
def test_update_data(app, background):
    client = TestClient(app)

    res = client.post(
        "/api/v1alpha1/update-data",
        json={
            "table_name": "events",
            "upsert": [
                {
                    "user_id": 1,
                    "event_id": 1,
                    "event": {"event_type": "click", "offer_id": 1},
                },
            ],
            "background": background,
        },
    )
    assert res.status_code == 200

    time.sleep(0.5)

    res = client.post(
        "/api/v1alpha1/get-data",
        json={
            "table": "events",
            "filters": {
                "user_id": 1,
            },
        },
    )
    assert res.status_code == 200
    assert res.json()["data"][0] == {
        "user_id": 1,
        "event_id": 1,
        "event": {"event_type": "click", "offer_id": 1},
    }

    res = client.post(
        "/api/v1alpha1/get-data",
        json={
            "table": "user_profile",
            "filters": {
                "user_id": 1,
            },
        },
    )
    assert res.status_code == 200
    assert res.json()["data"][0] == {
        "user_id": 1,
        "offer_clicks": [1],
        "events_count": 1,
        "active": True,
    }

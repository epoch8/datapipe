from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from datapipe_app_ml_ops.ops.training_experiments_routes import (
    register_training_experiments_routes,
)

BASE = "/pipelines/detection/ops-specs/detection/training"
API_BASE = "/api/v1alpha3" + BASE


@pytest.fixture
def client(env):
    app = FastAPI()
    register_training_experiments_routes(
        app,
        registry=env.registry,
        ds=env.ds,
        catalog=env.catalog,
        run_steps=env.run_steps,
    )
    return TestClient(app)


def test_list_experiments_route_empty(client):
    res = client.get(f"{BASE}/experiments")
    assert res.status_code == 200
    payload = res.json()
    assert payload["total"] == 0
    assert payload["rows"] == []


def test_create_and_get_experiment_route(client):
    res = client.post(
        f"{BASE}/experiments",
        json={"display_name": "R1", "params": {"model": "yolov8m.pt", "epochs": 12}},
    )
    assert res.status_code == 201, res.text
    exp = res.json()["experiment"]
    assert exp["source"] == "custom"
    assert exp["display_name"] == "R1"
    assert exp["active"] is True
    assert exp["archived"] is False

    got = client.get(f"{BASE}/experiments/{exp['id']}")
    assert got.status_code == 200
    assert got.json()["experiment"]["id"] == exp["id"]


def test_get_experiment_404_error_envelope(client):
    res = client.get(f"{BASE}/experiments/nope")
    assert res.status_code == 404
    body = res.json()
    assert body["error"]["code"] == "experiment_not_found"
    assert "message" in body["error"]
    assert "details" in body["error"]


def test_create_experiment_validation_error(client):
    res = client.post(
        f"{BASE}/experiments", json={"display_name": "Bad", "params": {"bogus": 1}}
    )
    assert res.status_code == 422
    assert res.json()["error"]["code"] == "train_config_validation_failed"


def test_config_schema_route(client):
    res = client.get(f"{BASE}/config-schema")
    assert res.status_code == 200
    payload = res.json()
    assert payload["config_type"] == "yolov8_detection"
    assert "properties" in payload["json_schema"]


def test_delete_experiment_route(client):
    created = client.post(
        f"{BASE}/experiments", json={"display_name": "D1", "params": {"epochs": 3}}
    ).json()
    config_id = created["experiment"]["id"]
    res = client.delete(f"{BASE}/experiments/{config_id}")
    assert res.status_code == 200
    assert client.get(f"{BASE}/experiments").json()["total"] == 0


def test_archive_route(client):
    created = client.post(
        f"{BASE}/experiments", json={"display_name": "A1", "params": {"epochs": 3}}
    ).json()
    config_id = created["experiment"]["id"]
    res = client.post(f"{BASE}/experiments/{config_id}/archive")
    assert res.status_code == 200
    exp = res.json()["experiment"]
    assert exp["active"] is False
    assert exp["archived"] is True


def test_update_locked_experiment_returns_409(env, client):
    env.write_frozen_dataset("fd-1")
    created = client.post(
        f"{BASE}/experiments", json={"display_name": "L1", "params": {"epochs": 3}}
    ).json()
    config_id = created["experiment"]["id"]
    client.post(
        f"{BASE}/requests",
        json={
            "frozen_dataset_id": "fd-1",
            "train_config_id": config_id,
            "client_request_id": "cli-lock",
            "launch": False,
        },
    )
    res = client.patch(
        f"{BASE}/experiments/{config_id}",
        json={"display_name": "New", "params": {"epochs": 3}, "expected_revision": 1},
    )
    assert res.status_code == 409
    assert res.json()["error"]["code"] == "experiment_locked"


def test_create_request_route_with_train_config_id_in_body(env, client):
    env.write_frozen_dataset("fd-1")
    created = client.post(
        f"{BASE}/experiments", json={"display_name": "R1", "params": {"epochs": 3}}
    ).json()
    config_id = created["experiment"]["id"]
    res = client.post(
        f"{BASE}/requests",
        json={
            "frozen_dataset_id": "fd-1",
            "train_config_id": config_id,
            "client_request_id": "cli-req-1",
            "launch": True,
        },
    )
    assert res.status_code == 201, res.text
    body = res.json()
    assert body["request"]["train_config_id"] == config_id
    assert body["launch"]["started"] is True
    assert body["launch"]["run_id"] == "run-123"

    listed = client.get(f"{BASE}/requests")
    assert listed.status_code == 200, listed.text
    payload = listed.json()
    assert payload["total"] >= 1
    match = next(row for row in payload["rows"] if row["id"] == body["request"]["id"])
    assert match["train_config_id"] == config_id
    assert match["frozen_dataset_id"] == "fd-1"
    assert match["can_delete"] is True

    deleted = client.delete(f"{BASE}/requests/{body['request']['id']}")
    assert deleted.status_code == 200, deleted.text
    assert deleted.json()["deleted"] == body["request"]["id"]
    after = client.get(f"{BASE}/requests").json()
    assert all(row["id"] != body["request"]["id"] for row in after["rows"])


def test_launch_route_invokes_run_steps(env, client):
    env.write_frozen_dataset("fd-1")
    created = client.post(
        f"{BASE}/experiments", json={"display_name": "L1", "params": {"epochs": 3}}
    ).json()
    config_id = created["experiment"]["id"]
    req = client.post(
        f"{BASE}/requests",
        json={
            "frozen_dataset_id": "fd-1",
            "train_config_id": config_id,
            "client_request_id": "cli-launch-1",
            "launch": False,
        },
    ).json()["request"]

    res = client.post(f"{BASE}/requests/{req['id']}/launch")
    assert res.status_code == 200
    payload = res.json()
    assert payload["started"] is True
    assert payload["run_id"] == "run-123"

    assert len(env.run_steps.calls) == 1
    _, _, filters = env.run_steps.calls[0]
    assert filters["training_request_id"] == [req["id"]]


def test_routes_mounted_through_datapipe_api(full_api_env):
    """Full-stack wiring (spec §20): routes are reachable through the v1alpha3
    entry-point extension and ``run_steps`` is passed without error."""
    client = full_api_env.client

    res = client.get(f"{API_BASE}/experiments")
    assert res.status_code == 200, res.text
    assert res.json()["total"] == 0

    created = client.post(
        f"{API_BASE}/experiments", json={"display_name": "F1", "params": {"epochs": 4}}
    )
    assert created.status_code == 201, created.text
    config_id = created.json()["experiment"]["id"]

    full_api_env.write(
        "frozen_dataset",
        [{"frozen_dataset_id": "fd-1", "frozen_dataset__created_at": "2026-01-01"}],
    )
    req = client.post(
        f"{API_BASE}/requests",
        json={
            "frozen_dataset_id": "fd-1",
            "train_config_id": config_id,
            "client_request_id": "cli-full-1",
            "launch": False,
        },
    )
    assert req.status_code == 201, req.text
    request_id = req.json()["request"]["id"]

    # No pipeline step carries the run_labels, so the real run_steps callable is
    # a no-op, but the route must still be wired and return 200.
    launched = client.post(f"{API_BASE}/requests/{request_id}/launch")
    assert launched.status_code == 200, launched.text
    assert launched.json()["training_request_id"] == request_id
    assert launched.json()["started"] is False

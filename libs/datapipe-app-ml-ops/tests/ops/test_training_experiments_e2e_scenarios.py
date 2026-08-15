"""API-level e2e scenarios for Custom Training Experiments (spec §38)."""

from __future__ import annotations

import uuid

import pytest

from datapipe_app_ml_ops.ops.training_experiments_models import (
    CreateTrainingExperimentRequest,
    CreateTrainingRequestRequest,
    DuplicateTrainingExperimentRequest,
    TrainingExperimentError,
    UpdateTrainingExperimentRequest,
)
from datapipe_app_ml_ops.ops.training_experiments_service import TrainingExperimentsService


def _svc(env) -> TrainingExperimentsService:
    return TrainingExperimentsService(
        env.registry, ds=env.ds, catalog=env.catalog, run_steps=env.run_steps
    )


def test_scenario_crud_before_launch(env):
    """§38 scenario 1: create → edit → delete unused custom experiment."""
    svc = _svc(env)
    exp = svc.create_experiment(
        env.spec_id,
        CreateTrainingExperimentRequest(
            display_name="Large image test",
            description="Detection on high-resolution images",
            params={"model": "yolov8m.pt", "imgsz": 1280, "batch": 4, "epochs": 50},
        ),
    ).experiment
    assert exp.capabilities.can_edit is True
    assert exp.source == "custom"

    updated = svc.update_experiment(
        env.spec_id,
        exp.id,
        UpdateTrainingExperimentRequest(
            display_name="Large image test",
            description="Detection on high-resolution images",
            params={"model": "yolov8m.pt", "imgsz": 1280, "batch": 4, "epochs": 60},
            expected_revision=exp.revision,
        ),
    ).experiment
    assert updated.revision == exp.revision + 1
    assert updated.params["epochs"] == 60

    svc.delete_experiment(env.spec_id, updated.id)
    with pytest.raises(TrainingExperimentError) as excinfo:
        svc.get_experiment(env.spec_id, updated.id)
    assert excinfo.value.code == "experiment_not_found"
    assert excinfo.value.status_code == 404


def test_scenario_lock_after_request(env):
    """§38 scenario 2: request locks custom experiment."""
    svc = _svc(env)
    dataset_id = env.write_frozen_dataset("fd-lock")
    exp = svc.create_experiment(
        env.spec_id,
        CreateTrainingExperimentRequest(
            display_name="Lock me",
            params={"model": "yolov8s.pt", "imgsz": 640, "batch": 8, "epochs": 10},
        ),
    ).experiment

    svc.create_training_request(
        env.spec_id,
        CreateTrainingRequestRequest(
            frozen_dataset_id=dataset_id,
            train_config_id=exp.id,
            client_request_id=str(uuid.uuid4()),
            launch=False,
        ),
    )

    locked = svc.get_experiment(env.spec_id, exp.id).experiment
    assert locked.capabilities.can_edit is False
    assert locked.capabilities.can_delete is False
    assert locked.capabilities.lock_reason is not None

    with pytest.raises(TrainingExperimentError) as excinfo:
        svc.update_experiment(
            env.spec_id,
            exp.id,
            UpdateTrainingExperimentRequest(
                display_name="Nope",
                params={"model": "yolov8s.pt", "imgsz": 640, "batch": 8, "epochs": 99},
                expected_revision=locked.revision,
            ),
        )
    assert excinfo.value.code == "experiment_locked"
    assert excinfo.value.status_code == 409


def test_scenario_duplicate_builtin(env):
    """§38 scenario 3: duplicate built-in → editable custom."""
    svc = _svc(env)
    builtin_id = env.write_builtin_config("builtin-std", display_name="Standard YOLOv8s 640")
    dup = svc.duplicate_experiment(
        env.spec_id,
        builtin_id,
        DuplicateTrainingExperimentRequest(),
    ).experiment
    assert dup.source == "custom"
    assert dup.id.startswith("custom_")
    assert dup.capabilities.can_edit is True
    assert dup.display_name.startswith("Standard") or "copy" in dup.display_name.lower()


def test_scenario_snapshot_immutable(env):
    """§38 scenario 4: request snapshot stays fixed after lock."""
    svc = _svc(env)
    dataset_id = env.write_frozen_dataset("fd-snap")
    exp = svc.create_experiment(
        env.spec_id,
        CreateTrainingExperimentRequest(
            display_name="Snap",
            params={"model": "yolov8s.pt", "imgsz": 640, "batch": 4, "epochs": 10},
        ),
    ).experiment

    req_resp = svc.create_training_request(
        env.spec_id,
        CreateTrainingRequestRequest(
            frozen_dataset_id=dataset_id,
            train_config_id=exp.id,
            client_request_id=str(uuid.uuid4()),
            launch=False,
        ),
    )
    request = req_resp.request
    snapshot = getattr(request, "config_params_snapshot", None)
    if snapshot is None:
        # Normative response may omit full snapshot; read from table.
        df = env.read("training_request")
        row = df[df["training_request_id"] == request.id].iloc[0]
        snapshot = row["training_request__config_params_snapshot"]
    assert snapshot["epochs"] == 10

    with pytest.raises(TrainingExperimentError) as excinfo:
        svc.update_experiment(
            env.spec_id,
            exp.id,
            UpdateTrainingExperimentRequest(
                display_name="Snap",
                params={"model": "yolov8s.pt", "imgsz": 640, "batch": 4, "epochs": 99},
                expected_revision=exp.revision,
            ),
        )
    assert excinfo.value.code == "experiment_locked"

    df = env.read("training_request")
    row = df[df["training_request_id"] == request.id].iloc[0]
    assert row["training_request__config_params_snapshot"]["epochs"] == 10

from __future__ import annotations

import pytest

from datapipe_app_ml_ops.ops.training_experiments_models import (
    CreateTrainingExperimentRequest,
    DuplicateTrainingExperimentRequest,
    TrainingExperimentError,
    UpdateTrainingExperimentRequest,
)
from datapipe_app_ml_ops.ops.training_experiments_service import (
    TrainingExperimentsService,
    build_capabilities,
)


def _svc(env) -> TrainingExperimentsService:
    return TrainingExperimentsService(
        env.registry, ds=env.ds, catalog=env.catalog, run_steps=env.run_steps
    )


def test_build_capabilities_builtin_read_only():
    caps = build_capabilities(source="builtin", active=True, requests_count=0)
    assert caps.can_edit is False
    assert caps.can_delete is False
    assert caps.can_duplicate is True
    assert caps.can_archive is False
    assert caps.can_launch is True
    assert caps.lock_reason == "Built-in experiments are managed by pipeline code"


def test_build_capabilities_custom_unlocked_when_no_requests():
    caps_empty = build_capabilities(source="custom", active=True, requests_count=0)
    assert caps_empty.can_edit is True
    assert caps_empty.can_delete is True
    assert caps_empty.can_archive is True
    assert caps_empty.lock_reason is None


def test_build_capabilities_custom_locked_with_requests():
    # A custom experiment that has already been used by a training request
    # MUST become read-only (can_edit=False), even though it can still be
    # duplicated and archived.
    caps = build_capabilities(source="custom", active=True, requests_count=2)
    assert caps.can_edit is False
    assert caps.can_delete is False
    assert caps.can_duplicate is True
    assert caps.can_archive is True
    assert caps.can_launch is True
    assert caps.lock_reason == (
        "Experiment parameters are locked because "
        "the experiment has already been used by a training request"
    )


def test_build_capabilities_can_launch_follows_active():
    assert build_capabilities(source="custom", active=False, requests_count=0).can_launch is False
    assert build_capabilities(source="builtin", active=False, requests_count=0).can_launch is False


def test_list_experiments_empty(env):
    resp = _svc(env).list_experiments(env.spec_id)
    assert resp.total == 0
    assert resp.rows == []
    assert resp.summary.total == 0


def test_create_experiment_and_get(env):
    svc = _svc(env)
    resp = svc.create_experiment(
        env.spec_id,
        CreateTrainingExperimentRequest(
            params={"model": "yolov8m.pt", "epochs": 10, "imgsz": 640},
            display_name="My experiment",
        ),
    )
    exp = resp.experiment
    assert exp.source == "custom"
    assert exp.id.startswith("custom_")
    assert exp.display_name == "My experiment"
    assert exp.params["epochs"] == 10
    assert exp.revision == 1
    assert exp.active is True
    assert exp.archived is False
    assert exp.capabilities.can_edit is True
    assert exp.capabilities.can_delete is True

    fetched = svc.get_experiment(env.spec_id, exp.id).experiment
    assert fetched.id == exp.id
    assert fetched.config_hash is not None


def test_create_experiment_invalid_params(env):
    with pytest.raises(TrainingExperimentError) as excinfo:
        _svc(env).create_experiment(
            env.spec_id,
            CreateTrainingExperimentRequest(display_name="Bad", params={"not_a_field": 1}),
        )
    assert excinfo.value.code == "train_config_validation_failed"
    assert excinfo.value.status_code == 422


def test_get_experiment_not_found(env):
    with pytest.raises(TrainingExperimentError) as excinfo:
        _svc(env).get_experiment(env.spec_id, "nope")
    assert excinfo.value.code == "experiment_not_found"
    assert excinfo.value.status_code == 404


def test_list_experiment_models(env):
    svc = _svc(env)
    exp = svc.create_experiment(
        env.spec_id,
        CreateTrainingExperimentRequest(display_name="With models", params={"epochs": 3}),
    ).experiment
    env.write_frozen_dataset("fd-a")
    env.write_frozen_dataset("fd-b")
    env.write(
        "model_is_trained_on_frozen_dataset",
        [
            {
                "model_id": "m-1",
                "frozen_dataset_id": "fd-a",
                "train_config_id": exp.id,
                "created_at": "2026-03-01T10:00:00+00:00",
            },
            {
                "model_id": "m-2",
                "frozen_dataset_id": "fd-b",
                "train_config_id": exp.id,
                "created_at": "2026-03-02T10:00:00+00:00",
            },
            {
                "model_id": "m-other",
                "frozen_dataset_id": "fd-a",
                "train_config_id": "other-config",
                "created_at": "2026-03-03T10:00:00+00:00",
            },
        ],
    )

    detail = svc.get_experiment(env.spec_id, exp.id)
    assert len(detail.models) == 2
    assert {row.model_id for row in detail.models} == {"m-1", "m-2"}
    assert detail.models[0].model_id == "m-2"  # newest first
    assert detail.models[0].frozen_dataset_id == "fd-b"

    listed = svc.list_experiment_models(env.spec_id, exp.id)
    assert listed.total == 2
    assert listed.experiment.id == exp.id


def test_update_experiment_bumps_revision(env):
    svc = _svc(env)
    exp = svc.create_experiment(
        env.spec_id,
        CreateTrainingExperimentRequest(display_name="Base", params={"epochs": 5}),
    ).experiment
    updated = svc.update_experiment(
        env.spec_id,
        exp.id,
        UpdateTrainingExperimentRequest(
            display_name="Renamed", params={"epochs": 5}, expected_revision=1
        ),
    ).experiment
    assert updated.display_name == "Renamed"
    assert updated.revision == 2


def test_update_experiment_revision_mismatch(env):
    svc = _svc(env)
    exp = svc.create_experiment(
        env.spec_id,
        CreateTrainingExperimentRequest(display_name="Base", params={"epochs": 5}),
    ).experiment
    with pytest.raises(TrainingExperimentError) as excinfo:
        svc.update_experiment(
            env.spec_id,
            exp.id,
            UpdateTrainingExperimentRequest(display_name="X", params={"epochs": 5}, expected_revision=99),
        )
    assert excinfo.value.code == "experiment_revision_conflict"
    assert excinfo.value.status_code == 409


def test_update_builtin_is_read_only(env):
    config_id = env.write_builtin_config()
    with pytest.raises(TrainingExperimentError) as excinfo:
        _svc(env).update_experiment(
            env.spec_id,
            config_id,
            UpdateTrainingExperimentRequest(display_name="x", params={}, expected_revision=1),
        )
    assert excinfo.value.code == "builtin_experiment_read_only"
    assert excinfo.value.status_code == 403


def test_update_locked_experiment_is_blocked(env):
    """A custom experiment that already has a training request MUST become
    read-only: can_edit is False and updates are rejected with
    ``experiment_locked``."""
    svc = _svc(env)
    env.write_frozen_dataset()
    exp = svc.create_experiment(
        env.spec_id,
        CreateTrainingExperimentRequest(display_name="Base", params={"epochs": 5}),
    ).experiment
    from datapipe_app_ml_ops.ops.training_experiments_models import CreateTrainingRequestRequest

    svc.create_training_request(
        env.spec_id,
        CreateTrainingRequestRequest(
            frozen_dataset_id="fd-1", train_config_id=exp.id, client_request_id="cli-lock"
        ),
    )
    locked = svc.get_experiment(env.spec_id, exp.id).experiment
    assert locked.requests_count == 1
    assert locked.capabilities.can_edit is False

    with pytest.raises(TrainingExperimentError) as excinfo:
        svc.update_experiment(
            env.spec_id,
            exp.id,
            UpdateTrainingExperimentRequest(display_name="New", params={"epochs": 5}, expected_revision=1),
        )
    assert excinfo.value.code == "experiment_locked"
    assert excinfo.value.status_code == 409


def test_delete_experiment_success(env):
    svc = _svc(env)
    exp = svc.create_experiment(
        env.spec_id,
        CreateTrainingExperimentRequest(display_name="Base", params={"epochs": 5}),
    ).experiment
    svc.delete_experiment(env.spec_id, exp.id)
    assert svc.list_experiments(env.spec_id).total == 0


def test_delete_experiment_blocked_with_requests(env):
    svc = _svc(env)
    env.write_frozen_dataset()
    exp = svc.create_experiment(
        env.spec_id,
        CreateTrainingExperimentRequest(display_name="Base", params={"epochs": 5}),
    ).experiment
    from datapipe_app_ml_ops.ops.training_experiments_models import CreateTrainingRequestRequest

    svc.create_training_request(
        env.spec_id,
        CreateTrainingRequestRequest(
            frozen_dataset_id="fd-1", train_config_id=exp.id, client_request_id="cli-1"
        ),
    )
    with pytest.raises(TrainingExperimentError) as excinfo:
        svc.delete_experiment(env.spec_id, exp.id)
    assert excinfo.value.code == "experiment_locked"


def test_delete_builtin_blocked(env):
    config_id = env.write_builtin_config()
    with pytest.raises(TrainingExperimentError) as excinfo:
        _svc(env).delete_experiment(env.spec_id, config_id)
    assert excinfo.value.code == "builtin_experiment_read_only"
    assert excinfo.value.status_code == 403


def test_duplicate_experiment(env):
    svc = _svc(env)
    src = svc.create_experiment(
        env.spec_id,
        CreateTrainingExperimentRequest(params={"epochs": 7}, display_name="Base"),
    ).experiment
    dup = svc.duplicate_experiment(
        env.spec_id, src.id, DuplicateTrainingExperimentRequest()
    ).experiment
    assert dup.id != src.id
    assert dup.source == "custom"
    assert dup.params["epochs"] == 7
    assert "copy" in (dup.display_name or "").lower()


def test_duplicate_builtin_creates_custom(env):
    config_id = env.write_builtin_config(display_name="Std")
    dup = _svc(env).duplicate_experiment(
        env.spec_id, config_id, DuplicateTrainingExperimentRequest(display_name="Mine")
    ).experiment
    assert dup.source == "custom"
    assert dup.display_name == "Mine"


def test_archive_unarchive(env):
    svc = _svc(env)
    exp = svc.create_experiment(
        env.spec_id,
        CreateTrainingExperimentRequest(display_name="Base", params={"epochs": 5}),
    ).experiment
    archived = svc.archive_experiment(env.spec_id, exp.id).experiment
    assert archived.active is False
    assert archived.archived is True
    assert archived.capabilities.can_launch is False

    # Archived experiments are hidden unless include_archived.
    assert svc.list_experiments(env.spec_id).total == 0
    assert svc.list_experiments(env.spec_id, include_archived=True).total == 1

    unarchived = svc.unarchive_experiment(env.spec_id, exp.id).experiment
    assert unarchived.active is True
    assert unarchived.archived is False


def test_archive_builtin_blocked(env):
    config_id = env.write_builtin_config()
    with pytest.raises(TrainingExperimentError) as excinfo:
        _svc(env).archive_experiment(env.spec_id, config_id)
    assert excinfo.value.code == "builtin_experiment_read_only"
    assert excinfo.value.status_code == 403


def test_get_config_schema(env):
    resp = _svc(env).get_config_schema(env.spec_id)
    assert resp.config_type == "yolov8_detection"
    assert "properties" in resp.json_schema


def test_list_experiments_filters(env):
    svc = _svc(env)
    env.write_builtin_config(config_id="builtin-1", display_name="Builtin one")
    svc.create_experiment(
        env.spec_id,
        CreateTrainingExperimentRequest(display_name="Alpha", params={"epochs": 1}),
    )
    svc.create_experiment(
        env.spec_id,
        CreateTrainingExperimentRequest(display_name="Beta", params={"epochs": 2}),
    )

    all_resp = svc.list_experiments(env.spec_id)
    assert all_resp.total == 3
    assert all_resp.summary.builtin == 1
    assert all_resp.summary.custom == 2
    assert all_resp.summary.editable == 2
    assert all_resp.summary.locked == 0
    assert all_resp.summary.archived == 0

    only_custom = svc.list_experiments(env.spec_id, source="custom")
    assert only_custom.total == 2

    searched = svc.list_experiments(env.spec_id, search="alpha")
    assert searched.total == 1
    assert searched.rows[0].display_name == "Alpha"


def test_summary_reflects_locked_and_archived(env):
    svc = _svc(env)
    env.write_frozen_dataset()
    exp = svc.create_experiment(
        env.spec_id,
        CreateTrainingExperimentRequest(display_name="Base", params={"epochs": 5}),
    ).experiment
    from datapipe_app_ml_ops.ops.training_experiments_models import CreateTrainingRequestRequest

    svc.create_training_request(
        env.spec_id,
        CreateTrainingRequestRequest(
            frozen_dataset_id="fd-1", train_config_id=exp.id, client_request_id="cli-1"
        ),
    )
    resp = svc.list_experiments(env.spec_id, include_archived=True)
    assert resp.summary.locked == 1
    assert resp.summary.editable == 0

    svc.archive_experiment(env.spec_id, exp.id)
    resp2 = svc.list_experiments(env.spec_id, include_archived=True)
    assert resp2.summary.archived == 1
    # Once archived, the experiment is no longer counted as "locked".
    assert resp2.summary.locked == 0

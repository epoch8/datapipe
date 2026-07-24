from __future__ import annotations

import pytest

from datapipe_app_ml_ops.ops.training_experiments_models import (
    CreateTrainingExperimentRequest,
    CreateTrainingRequestRequest,
    TrainingExperimentError,
)
from datapipe_app_ml_ops.ops.training_experiments_service import (
    TrainingExperimentsService,
)


def _svc(env, *, run_steps=...) -> TrainingExperimentsService:
    return TrainingExperimentsService(
        env.registry,
        ds=env.ds,
        catalog=env.catalog,
        run_steps=env.run_steps if run_steps is ... else run_steps,
    )


def _make_experiment(env) -> str:
    exp = _svc(env).create_experiment(
        env.spec_id,
        CreateTrainingExperimentRequest(display_name="Exp", params={"model": "yolov8n.pt", "epochs": 5}),
    ).experiment
    return exp.id


def test_create_training_request_success(env):
    svc = _svc(env)
    env.write_frozen_dataset("fd-1")
    config_id = _make_experiment(env)

    resp = svc.create_training_request(
        env.spec_id,
        CreateTrainingRequestRequest(
            frozen_dataset_id="fd-1",
            train_config_id=config_id,
            client_request_id="cli-1",
            launch=True,
        ),
    )
    req = resp.request
    assert req.id.startswith("request_")
    assert req.train_config_id == config_id
    assert req.frozen_dataset_id == "fd-1"
    assert req.kind == "manual"
    assert req.state == "queued"
    # Config is snapshotted onto the request.
    assert req.config_params_snapshot["epochs"] == 5
    assert req.config_hash is not None
    assert req.config_name == "Exp"

    assert resp.launch is not None
    assert resp.launch.started is True
    assert resp.launch.run_id == "run-123"

    # Row is persisted.
    df = env.read("training_request")
    assert len(df) == 1


def test_create_training_request_defaults_to_no_launch(env):
    svc = _svc(env)
    env.write_frozen_dataset("fd-1")
    config_id = _make_experiment(env)

    resp = svc.create_training_request(
        env.spec_id,
        CreateTrainingRequestRequest(
            frozen_dataset_id="fd-1", train_config_id=config_id, client_request_id="cli-default"
        ),
    )
    assert resp.launch is None
    assert len(env.run_steps.calls) == 0


def test_create_training_request_no_launch(env):
    svc = _svc(env)
    env.write_frozen_dataset("fd-1")
    config_id = _make_experiment(env)

    resp = svc.create_training_request(
        env.spec_id,
        CreateTrainingRequestRequest(
            frozen_dataset_id="fd-1",
            train_config_id=config_id,
            client_request_id="cli-no-launch",
            launch=False,
        ),
    )
    assert resp.launch is None
    assert len(env.run_steps.calls) == 0


def test_create_training_request_idempotent(env):
    svc = _svc(env)
    env.write_frozen_dataset("fd-1")
    config_id = _make_experiment(env)

    first = svc.create_training_request(
        env.spec_id,
        CreateTrainingRequestRequest(
            frozen_dataset_id="fd-1", train_config_id=config_id, client_request_id="cli-1"
        ),
    )
    second = svc.create_training_request(
        env.spec_id,
        CreateTrainingRequestRequest(
            frozen_dataset_id="fd-1", train_config_id=config_id, client_request_id="cli-1"
        ),
    )
    assert first.request.id == second.request.id
    # The idempotent replay does not re-launch.
    assert second.launch is None
    assert len(env.read("training_request")) == 1


def test_create_training_request_experiment_not_found(env):
    env.write_frozen_dataset("fd-1")
    with pytest.raises(TrainingExperimentError) as excinfo:
        _svc(env).create_training_request(
            env.spec_id,
            CreateTrainingRequestRequest(
                frozen_dataset_id="fd-1", train_config_id="nope", client_request_id="cli-1"
            ),
        )
    assert excinfo.value.code == "experiment_not_found"
    assert excinfo.value.status_code == 404


def test_create_training_request_frozen_not_found(env):
    config_id = _make_experiment(env)
    with pytest.raises(TrainingExperimentError) as excinfo:
        _svc(env).create_training_request(
            env.spec_id,
            CreateTrainingRequestRequest(
                frozen_dataset_id="missing", train_config_id=config_id, client_request_id="cli-1"
            ),
        )
    assert excinfo.value.code == "frozen_dataset_not_found"
    assert excinfo.value.status_code == 404


def test_create_training_request_inactive_experiment(env):
    svc = _svc(env)
    env.write_frozen_dataset("fd-1")
    config_id = _make_experiment(env)
    svc.archive_experiment(env.spec_id, config_id)
    with pytest.raises(TrainingExperimentError) as excinfo:
        svc.create_training_request(
            env.spec_id,
            CreateTrainingRequestRequest(
                frozen_dataset_id="fd-1", train_config_id=config_id, client_request_id="cli-1"
            ),
        )
    assert excinfo.value.code == "experiment_inactive"
    assert excinfo.value.status_code == 409


def test_launch_training_request_uses_pk_filter(env):
    svc = _svc(env)
    env.write_frozen_dataset("fd-1")
    config_id = _make_experiment(env)
    req = svc.create_training_request(
        env.spec_id,
        CreateTrainingRequestRequest(
            frozen_dataset_id="fd-1",
            train_config_id=config_id,
            client_request_id="cli-1",
            launch=False,
        ),
    ).request

    resp = svc.launch_training_request(env.spec_id, req.id)
    assert resp.started is True
    assert resp.run_id == "run-123"
    assert resp.training_request_id == req.id

    # run_steps must be called with the training_request_id primary-key filter.
    assert len(env.run_steps.calls) == 1
    call_request_id, run_labels, filters = env.run_steps.calls[0]
    assert call_request_id == req.id
    assert run_labels == [("stage", "train")]
    assert "training_request_id" in filters
    assert filters["training_request_id"] == [req.id]


def test_launch_training_request_not_found(env):
    with pytest.raises(TrainingExperimentError) as excinfo:
        _svc(env).launch_training_request(env.spec_id, "request_missing")
    assert excinfo.value.status_code == 404


def test_launch_without_run_steps_configured(env):
    svc = _svc(env, run_steps=None)
    env.write_frozen_dataset("fd-1")
    config_id = _make_experiment(env)
    req = svc.create_training_request(
        env.spec_id,
        CreateTrainingRequestRequest(
            frozen_dataset_id="fd-1",
            train_config_id=config_id,
            client_request_id="cli-1",
            launch=False,
        ),
    ).request
    with pytest.raises(TrainingExperimentError) as excinfo:
        svc.launch_training_request(env.spec_id, req.id)
    assert excinfo.value.code == "training_launch_failed"
    assert excinfo.value.status_code == 503


def test_create_training_request_launch_failed_when_no_run_steps(env):
    svc = _svc(env, run_steps=None)
    env.write_frozen_dataset("fd-1")
    config_id = _make_experiment(env)
    with pytest.raises(TrainingExperimentError) as excinfo:
        svc.create_training_request(
            env.spec_id,
            CreateTrainingRequestRequest(
                frozen_dataset_id="fd-1",
                train_config_id=config_id,
                client_request_id="cli-1",
                launch=True,
            ),
        )
    assert excinfo.value.code == "training_launch_failed"
    assert excinfo.value.status_code == 503


def test_launch_rejected_when_run_labels_empty(env):
    from dataclasses import replace

    from datapipe_app_ml_ops.ops.spec_registry import OpsSpecRegistry

    spec = env.registry.get(env.spec_id)
    assert spec.training is not None and spec.training.requests is not None
    empty_requests = replace(spec.training.requests, run_labels=[])
    empty_training = replace(spec.training, requests=empty_requests)
    empty_spec = replace(spec, training=empty_training)
    registry = OpsSpecRegistry()
    registry.add_many([empty_spec])

    svc = TrainingExperimentsService(
        registry,
        ds=env.ds,
        catalog=env.catalog,
        run_steps=env.run_steps,
    )
    env.write_frozen_dataset("fd-1")
    # Seed an experiment via the original env registry tables.
    config_id = _make_experiment(env)

    with pytest.raises(TrainingExperimentError) as excinfo:
        svc.create_training_request(
            env.spec_id,
            CreateTrainingRequestRequest(
                frozen_dataset_id="fd-1",
                train_config_id=config_id,
                client_request_id="cli-no-labels",
                launch=True,
            ),
        )
    assert excinfo.value.code == "training_launch_not_configured"
    assert excinfo.value.status_code == 400

    # Creating without launch still works.
    resp = svc.create_training_request(
        env.spec_id,
        CreateTrainingRequestRequest(
            frozen_dataset_id="fd-1",
            train_config_id=config_id,
            client_request_id="cli-no-labels-ok",
            launch=False,
        ),
    )
    assert resp.launch is None
    with pytest.raises(TrainingExperimentError) as excinfo:
        svc.launch_training_request(env.spec_id, resp.request.id)
    assert excinfo.value.code == "training_launch_not_configured"

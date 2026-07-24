from __future__ import annotations

from dataclasses import replace

import pytest

from datapipe_app_ml_ops.ops.frozen_datasets_launch_service import FrozenDatasetsLaunchService
from datapipe_app_ml_ops.ops.spec_registry import OpsSpecRegistry
from datapipe_app_ml_ops.ops.training_experiments_models import TrainingExperimentError


def test_launch_freeze_uses_run_labels(env):
    svc = FrozenDatasetsLaunchService(env.registry, run_steps=env.run_steps)
    # Default conftest frozen_dataset has no run_labels → configure them.
    spec = env.registry.get(env.spec_id)
    assert spec.frozen_dataset is not None
    configured = replace(spec.frozen_dataset, run_labels=[("stage", "train-prepare")])
    registry = OpsSpecRegistry()
    registry.add_many([replace(spec, frozen_dataset=configured)])
    svc = FrozenDatasetsLaunchService(registry, run_steps=env.run_steps)

    resp = svc.launch_freeze(env.spec_id)
    assert resp.started is True
    assert resp.run_id == "run-123"
    assert len(env.run_steps.calls) == 1
    request_id, run_labels, filters = env.run_steps.calls[0]
    assert request_id == "freeze"
    assert run_labels == [("stage", "train-prepare")]
    assert filters == {}


def test_launch_freeze_rejected_when_run_labels_empty(env):
    svc = FrozenDatasetsLaunchService(env.registry, run_steps=env.run_steps)
    with pytest.raises(TrainingExperimentError) as excinfo:
        svc.launch_freeze(env.spec_id)
    assert excinfo.value.code == "freeze_launch_not_configured"
    assert excinfo.value.status_code == 400
    assert len(env.run_steps.calls) == 0


def test_launch_freeze_requires_run_steps(env):
    spec = env.registry.get(env.spec_id)
    assert spec.frozen_dataset is not None
    configured = replace(spec.frozen_dataset, run_labels=[("stage", "train-prepare")])
    registry = OpsSpecRegistry()
    registry.add_many([replace(spec, frozen_dataset=configured)])
    svc = FrozenDatasetsLaunchService(registry, run_steps=None)
    with pytest.raises(TrainingExperimentError) as excinfo:
        svc.launch_freeze(env.spec_id)
    assert excinfo.value.code == "freeze_launch_failed"
    assert excinfo.value.status_code == 503

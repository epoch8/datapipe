from __future__ import annotations

import threading
from typing import Any, List
from unittest.mock import MagicMock

from datapipe.compute import ComputeOutput, ComputeStep
from datapipe.step.batch_transform import BaseBatchTransformStep

from datapipe_app.api.v1alpha3 import make_run_steps_callable
from datapipe_app.observability.runs.recorder import RecordingRunCallback
from datapipe_app.observability.runs.run_scope import labels_from_json


class _FakeStep(ComputeStep):
    def __init__(self, name: str, labels=None) -> None:
        super().__init__(name=name, input_dts=[], output_dts=[], labels=labels or [])

    def run_full(self, ds, run_config=None, executor=None):
        return None


class _FakeBatchStep(BaseBatchTransformStep):
    """Batch-transform test double without meta/DB wiring."""

    def __init__(
        self,
        name: str,
        labels=None,
        *,
        transform_keys: list[str],
        output_dts: list[ComputeOutput] | None = None,
    ) -> None:
        ComputeStep.__init__(
            self,
            name=name,
            input_dts=[],
            output_dts=output_dts or [],
            labels=labels or [],
        )
        self.transform_keys = transform_keys
        self.filters = None

    def run_full(self, ds, run_config=None, executor=None):
        return None


def test_make_run_steps_callable_records_stage_labels_and_returns_immediately(monkeypatch):
    selected_holder: List[Any] = []
    entered = threading.Event()
    release = threading.Event()
    finished = threading.Event()

    def _fake_run_steps(*, ds, steps, run_config=None, executor=None):
        entered.set()
        # Hold the background thread until the caller has already returned.
        assert release.wait(timeout=5)
        selected_holder.extend(steps)
        callback = run_config.callback if run_config is not None else None
        if callback is not None:
            callback.on_run_success()
        finished.set()

    monkeypatch.setattr("datapipe_app.api.v1alpha3.run_steps", _fake_run_steps)

    real_recorder = MagicMock()
    real_recorder.start_run.side_effect = lambda **kwargs: "run-abc"
    real_recorder.finish_run = MagicMock()

    recorder = MagicMock()

    def _create_callback(*, trigger=None, labels_json=None, cli_announce=False):
        run_id = real_recorder.start_run(trigger=trigger, labels_json=labels_json)
        return RecordingRunCallback(recorder=real_recorder, run_id=run_id)

    recorder.create_callback.side_effect = _create_callback

    steps = [
        _FakeStep("prep", [("stage", "train-prepare")]),
        _FakeStep("train", [("stage", "train-without-freeze")]),
        _FakeStep("metrics", [("stage", "train-without-freeze")]),
    ]
    run = make_run_steps_callable(
        ds=MagicMock(),
        steps=steps,
        recorder=recorder,
        resolve_executor=lambda: None,
    )

    result = run(
        "req-1",
        [("stage", "train-without-freeze")],
        {"training_request_id": ["req-1"]},
    )

    # Returned while background work is still blocked on ``release``.
    assert result == {"started": True, "run_id": "run-abc"}
    assert selected_holder == []
    assert entered.wait(timeout=2)

    assert recorder.create_callback.call_count == 1
    call_kwargs = recorder.create_callback.call_args.kwargs
    assert call_kwargs["trigger"] == "api:stage:train-without-freeze"
    assert labels_from_json(call_kwargs["labels_json"]) == [("stage", "train-without-freeze")]

    release.set()
    assert finished.wait(timeout=2)
    assert [s.name for s in selected_holder] == ["train", "metrics"]


def test_filtered_launch_skips_steps_that_stamp_filter_onto_output_pk(monkeypatch):
    """Auto-request (keys=dataset×config, out PK=request id) must not run under
    a launch filter from OpsTrainingRequestSpec.id_column."""
    selected_holder: List[Any] = []
    done = threading.Event()

    def _fake_run_steps(*, ds, steps, run_config=None, executor=None):
        selected_holder.extend(steps)
        callback = run_config.callback if run_config is not None else None
        if callback is not None:
            callback.on_run_success()
        done.set()

    monkeypatch.setattr("datapipe_app.api.v1alpha3.run_steps", _fake_run_steps)

    request_dt = MagicMock()
    request_dt.primary_keys = ["training_request_id"]
    size_dt = MagicMock()
    size_dt.primary_keys = ["training_request_id", "width", "height"]
    model_dt = MagicMock()
    model_dt.primary_keys = ["model_id"]

    labels = [("stage", "train-without-freeze")]
    auto_req = _FakeBatchStep(
        "auto_request",
        labels,
        transform_keys=["frozen_dataset_id", "train_config_id"],
        output_dts=[ComputeOutput(dt=request_dt)],
    )
    size_step = _FakeBatchStep(
        "size_from_request",
        labels,
        transform_keys=["training_request_id"],
        output_dts=[ComputeOutput(dt=size_dt)],
    )
    train = _FakeBatchStep(
        "train",
        labels,
        transform_keys=["training_request_id"],
        output_dts=[ComputeOutput(dt=model_dt)],
    )

    recorder = MagicMock()
    recorder.create_callback.side_effect = lambda **kwargs: RecordingRunCallback(
        recorder=MagicMock(start_run=MagicMock(return_value="run-x"), finish_run=MagicMock()),
        run_id="run-x",
    )

    run = make_run_steps_callable(
        ds=MagicMock(),
        steps=[auto_req, size_step, train],
        recorder=recorder,
        resolve_executor=lambda: None,
    )
    # Filter key comes from OpsTrainingRequestSpec.id_column at launch time.
    run("req-1", labels, {"training_request_id": ["req-1"]})

    assert done.wait(timeout=2)
    assert [s.name for s in selected_holder] == ["size_from_request", "train"]

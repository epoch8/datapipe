from __future__ import annotations

import time
from typing import Any, List
from unittest.mock import MagicMock

from datapipe.compute import ComputeStep

from datapipe_app.api.v1alpha3 import make_run_steps_callable
from datapipe_app.observability.runs.recorder import RecordingRunCallback
from datapipe_app.observability.runs.run_scope import labels_from_json


class _FakeStep(ComputeStep):
    def __init__(self, name: str, labels=None) -> None:
        super().__init__(name=name, input_dts=[], output_dts=[], labels=labels or [])

    def run_full(self, ds, run_config=None, executor=None, progress=None):
        return None


def test_make_run_steps_callable_records_stage_labels_and_returns_immediately(monkeypatch):
    selected_holder: List[Any] = []

    def _fake_run_steps(*, ds, steps, run_config=None, executor=None, callbacks=None):
        time.sleep(0.15)
        selected_holder.extend(steps)
        if callbacks:
            for cb in callbacks:
                cb.on_run_success()

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

    t0 = time.monotonic()
    result = run(
        "req-1",
        [("stage", "train-without-freeze")],
        {"training_request_id": ["req-1"]},
    )
    elapsed = time.monotonic() - t0

    assert result == {"started": True, "run_id": "run-abc"}
    # Must return before the background work finishes.
    assert elapsed < 0.1
    assert recorder.create_callback.call_count == 1
    call_kwargs = recorder.create_callback.call_args.kwargs
    assert call_kwargs["trigger"] == "api:stage:train-without-freeze"
    assert labels_from_json(call_kwargs["labels_json"]) == [("stage", "train-without-freeze")]

    deadline = time.monotonic() + 2.0
    while not selected_holder and time.monotonic() < deadline:
        time.sleep(0.02)
    assert [s.name for s in selected_holder] == ["train", "metrics"]

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock

import pytest

from datapipe.compute import ComputeStep, run_steps
from datapipe_app.observability.cli.cli_runner import create_run_callback
from datapipe_app.observability.logging.log_buffer import RunLogBuffer
from datapipe_app.observability.runs.recorder import RunRecorder
from datapipe_app.observability.store.db import ObservabilityStore


class FakeStep(ComputeStep):
    def __init__(self, name: str, *, fail: bool = False, unknown_total: bool = False):
        super().__init__(name=name, input_dts=[], output_dts=[])
        self.fail = fail
        self.unknown_total = unknown_total

    def run_full(self, ds, run_config=None, executor=None):
        if self.fail:
            raise RuntimeError(f"{self.name} failed")

    def run_full_observed(self, ds, run_config=None, executor=None, progress=None):
        if progress is not None:
            if self.unknown_total:
                progress(3, None)
            else:
                progress(0, 1)
                progress(1, 1)
        if self.fail:
            raise RuntimeError(f"{self.name} failed")


def test_recorder_callback_completes_run(tmp_path):
    store = ObservabilityStore.from_url(f"sqlite:///{tmp_path / 'obs.db'}")
    store.register_pipeline("pipe")
    buffer = RunLogBuffer(store)
    recorder = RunRecorder(store, pipeline_id="pipe", log_buffer=buffer)

    cb = recorder.create_callback(trigger="test")
    run_steps(MagicMock(), [FakeStep("a"), FakeStep("b")], callbacks=[cb])

    run = store.get_run(cb.run_id)
    assert run is not None
    assert run.status == "completed"
    steps = {s.step_name: s for s in store.get_run_steps(cb.run_id)}
    assert steps["a"].status == "completed"
    assert steps["b"].status == "completed"
    assert steps["a"].processed == 1
    assert steps["a"].total == 1


def test_recorder_callback_fails_run(tmp_path):
    store = ObservabilityStore.from_url(f"sqlite:///{tmp_path / 'obs.db'}")
    store.register_pipeline("pipe")
    recorder = RunRecorder(store, pipeline_id="pipe")

    cb = recorder.create_callback(trigger="test")
    with pytest.raises(RuntimeError, match="bad failed"):
        run_steps(
            MagicMock(),
            [FakeStep("ok"), FakeStep("bad", fail=True)],
            callbacks=[cb],
        )

    run = store.get_run(cb.run_id)
    assert run is not None
    assert run.status == "failed"
    steps = {s.step_name: s for s in store.get_run_steps(cb.run_id)}
    assert steps["ok"].status == "completed"
    assert steps["bad"].status == "failed"


def test_two_concurrent_runs_do_not_share_run_id(tmp_path):
    store = ObservabilityStore.from_url(f"sqlite:///{tmp_path / 'obs.db'}")
    store.register_pipeline("pipe")
    recorder = RunRecorder(store, pipeline_id="pipe")

    def run_one(name: str) -> str:
        cb = recorder.create_callback(trigger=name)
        run_steps(MagicMock(), [FakeStep(name)], callbacks=[cb])
        return cb.run_id

    with ThreadPoolExecutor(max_workers=2) as pool:
        ids = list(pool.map(run_one, ["a", "b"]))

    assert ids[0] != ids[1]
    for run_id, step_name in zip(ids, ["a", "b"]):
        run = store.get_run(run_id)
        assert run is not None
        assert run.status == "completed"
        steps = store.get_run_steps(run_id)
        assert len(steps) == 1
        assert steps[0].step_name == step_name
        assert steps[0].status == "completed"


def test_record_false_disables_recording_callback():
    assert create_run_callback(MagicMock(), [FakeStep("x")], record=False) is None


def test_run_scope_completes_on_success(tmp_path):
    store = ObservabilityStore.from_url(f"sqlite:///{tmp_path / 'obs.db'}")
    store.register_pipeline("pipe")
    recorder = RunRecorder(store, pipeline_id="pipe")

    with recorder.run_scope(trigger="scope") as cb:
        pass

    run = store.get_run(cb.run_id)
    assert run is not None
    assert run.status == "completed"


def test_run_scope_fails_on_exception(tmp_path):
    store = ObservabilityStore.from_url(f"sqlite:///{tmp_path / 'obs.db'}")
    store.register_pipeline("pipe")
    recorder = RunRecorder(store, pipeline_id="pipe")

    with pytest.raises(RuntimeError, match="boom"):
        with recorder.run_scope(trigger="scope") as cb:
            raise RuntimeError("boom")

    run = store.get_run(cb.run_id)
    assert run is not None
    assert run.status == "failed"


def test_unknown_total_is_not_coerced_to_zero(tmp_path):
    store = ObservabilityStore.from_url(f"sqlite:///{tmp_path / 'obs.db'}")
    store.register_pipeline("pipe")
    recorder = RunRecorder(store, pipeline_id="pipe")

    cb = recorder.create_callback(trigger="test")
    run_steps(MagicMock(), [FakeStep("u", unknown_total=True)], callbacks=[cb])

    step = store.get_run_steps(cb.run_id)[0]
    assert step.processed == 3
    assert step.total is None

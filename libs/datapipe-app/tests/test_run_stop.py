from __future__ import annotations

import threading
import time
from unittest.mock import MagicMock

from datapipe.cancel import (
    CancelToken,
    RunCancelledError,
    cancel_token_scope,
    get_cancel_token,
)
from datapipe.compute import ComputeStep

from datapipe_app.api.v1alpha3 import make_run_steps_callable
from datapipe_app.observability.runs.active_runs import get_active_run_registry
from datapipe_app.observability.runs.recorder import RecordingRunCallback
from datapipe_app.observability.store.db import ObservabilityStore


class _FakeStep(ComputeStep):
    def __init__(self, name: str, labels=None, *, hold: threading.Event | None = None) -> None:
        super().__init__(name=name, input_dts=[], output_dts=[], labels=labels or [])
        self.hold = hold

    def run_full(self, ds, run_config=None, executor=None):
        if self.hold is not None:
            # Simulate long training that polls cancel.
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline:
                token = get_cancel_token()
                if token is not None and token.is_cancelled():
                    raise RunCancelledError("stopped")
                if self.hold.wait(0.05):
                    return
                time.sleep(0.05)
        return None


def test_cancel_token_terminates_registered_process():
    token = CancelToken()
    process = MagicMock()
    process.is_alive.return_value = True
    token.register_process(process)
    token.request_cancel()
    assert token.is_cancelled()
    process.terminate.assert_called_once()


def test_make_run_steps_callable_registers_and_unregisters_cancel_token(monkeypatch):
    done = threading.Event()
    seen_registered = threading.Event()

    def _fake_run_steps(*, ds, steps, run_config=None, executor=None):
        token = get_cancel_token()
        assert token is not None
        assert not token.is_cancelled()
        assert get_active_run_registry().get("run-cancel-1") is token
        seen_registered.set()
        callback = run_config.callback if run_config is not None else None
        if callback is not None:
            callback.on_run_success()
        done.set()

    monkeypatch.setattr("datapipe_app.api.v1alpha3.run_steps", _fake_run_steps)

    real_recorder = MagicMock()
    real_recorder.start_run.side_effect = lambda **kwargs: "run-cancel-1"
    real_recorder.finish_run = MagicMock()
    recorder = MagicMock()
    recorder.create_callback.side_effect = lambda **kwargs: RecordingRunCallback(
        recorder=real_recorder,
        run_id=real_recorder.start_run(**kwargs),
    )

    run = make_run_steps_callable(
        ds=MagicMock(),
        steps=[_FakeStep("train", [("stage", "train")])],
        recorder=recorder,
        resolve_executor=lambda: None,
    )
    result = run("req-1", [("stage", "train")], {"training_request_id": ["req-1"]})
    assert result["run_id"] == "run-cancel-1"
    assert seen_registered.wait(2.0)
    assert done.wait(2.0)
    deadline = time.monotonic() + 2.0
    while get_active_run_registry().get("run-cancel-1") is not None and time.monotonic() < deadline:
        time.sleep(0.02)
    assert get_active_run_registry().get("run-cancel-1") is None


def test_request_stop_cancels_active_run(monkeypatch):
    hold = threading.Event()
    saw_cancel = threading.Event()

    def _fake_run_steps(*, ds, steps, run_config=None, executor=None):
        token = get_cancel_token()
        assert token is not None
        while not token.is_cancelled():
            time.sleep(0.02)
        saw_cancel.set()
        callback = run_config.callback if run_config is not None else None
        if callback is not None:
            callback.on_run_error(RunCancelledError("stopped"))

    monkeypatch.setattr("datapipe_app.api.v1alpha3.run_steps", _fake_run_steps)

    real_recorder = MagicMock()
    real_recorder.start_run.side_effect = lambda **kwargs: "run-stop-1"
    real_recorder.finish_run = MagicMock()
    recorder = MagicMock()
    recorder.create_callback.side_effect = lambda **kwargs: RecordingRunCallback(
        recorder=real_recorder,
        run_id=real_recorder.start_run(**kwargs),
    )

    run = make_run_steps_callable(
        ds=MagicMock(),
        steps=[_FakeStep("train", [("stage", "train")], hold=hold)],
        recorder=recorder,
        resolve_executor=lambda: None,
    )
    result = run("req-1", [("stage", "train")], {"training_request_id": ["req-1"]})
    assert result["run_id"] == "run-stop-1"

    assert get_active_run_registry().request_stop("run-stop-1") is True
    assert saw_cancel.wait(2.0)
    hold.set()


def test_finish_run_does_not_overwrite_interrupted(tmp_path):
    store = ObservabilityStore.from_url(f"sqlite:///{tmp_path / 'obs.db'}", create_tables=True)
    run_id = store.create_run("demo", trigger="test")
    store.finish_run(run_id, status="interrupted", error="Stopped by user")
    store.finish_run(run_id, status="failed", error="later")
    row = store.get_run(run_id)
    assert row is not None
    assert row.status == "interrupted"
    assert row.error == "Stopped by user"


def test_run_steps_stops_between_steps_when_cancelled(monkeypatch):
    from datapipe.cancel import CancelToken, cancel_token_scope
    from datapipe.compute import run_steps
    from datapipe.cancel import RunCancelledError

    ran: list[str] = []
    token = CancelToken()

    class _Step(ComputeStep):
        def __init__(self, name: str) -> None:
            super().__init__(name=name, input_dts=[], output_dts=[])

        def run_full(self, ds, run_config=None, executor=None):
            ran.append(self.name)
            if self.name == "a":
                token.request_cancel()

    with cancel_token_scope(token):
        try:
            run_steps(MagicMock(), [_Step("a"), _Step("b")])
            assert False, "expected RunCancelledError"
        except RunCancelledError:
            pass

    assert ran == ["a"]


def test_single_thread_executor_stops_between_batches():
    from datapipe.cancel import CancelToken, RunCancelledError, cancel_token_scope
    from datapipe.compute import ComputeStep
    from datapipe.executor import SingleThreadExecutor
    from datapipe.types import ChangeList, IndexDF
    import pandas as pd

    token = CancelToken()
    processed: list[int] = []

    class _Step(ComputeStep):
        def __init__(self) -> None:
            super().__init__(name="t", input_dts=[], output_dts=[])

    def _gen():
        for i in range(5):
            yield IndexDF(pd.DataFrame({"id": [i]}))

    def _process(ds, idx, run_config=None):
        processed.append(int(idx.iloc[0]["id"]))
        if len(processed) == 2:
            token.request_cancel()
        return ChangeList()

    with cancel_token_scope(token):
        try:
            SingleThreadExecutor().run_process_batch(
                step=_Step(),
                ds=MagicMock(),
                idx_count=5,
                idx_gen=_gen(),
                process_fn=_process,
            )
            assert False, "expected RunCancelledError"
        except RunCancelledError:
            pass

    assert processed == [0, 1]

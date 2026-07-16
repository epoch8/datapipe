from __future__ import annotations

from dataclasses import dataclass, field
from typing import Sequence
from unittest.mock import MagicMock

import pytest

from datapipe.compute import ComputeStep, run_steps
from datapipe.run_callback import RunCallback
from datapipe.run_config import RunConfig


@dataclass
class RecordingCallback:
    events: list[tuple] = field(default_factory=list)

    def on_run_start(self, steps: Sequence[ComputeStep]) -> None:
        self.events.append(("run_start", [s.name for s in steps]))

    def on_step_start(self, step: ComputeStep) -> None:
        self.events.append(("step_start", step.name))

    def on_step_progress(
        self,
        step: ComputeStep,
        completed: int,
        total: int | None,
    ) -> None:
        self.events.append(("step_progress", step.name, completed, total))

    def on_step_success(self, step: ComputeStep) -> None:
        self.events.append(("step_success", step.name))

    def on_step_error(self, step: ComputeStep, error: BaseException) -> None:
        self.events.append(("step_error", step.name, type(error).__name__, str(error)))

    def on_run_success(self) -> None:
        self.events.append(("run_success",))

    def on_run_error(self, error: BaseException) -> None:
        self.events.append(("run_error", type(error).__name__, str(error)))


@dataclass
class ExplodingCallback:
    explode_on: str
    events: list[str] = field(default_factory=list)

    def on_run_start(self, steps: Sequence[ComputeStep]) -> None:
        self.events.append("on_run_start")
        if self.explode_on == "on_run_start":
            raise RuntimeError("callback boom on_run_start")

    def on_step_start(self, step: ComputeStep) -> None:
        self.events.append("on_step_start")
        if self.explode_on == "on_step_start":
            raise RuntimeError("callback boom on_step_start")

    def on_step_progress(self, step, completed, total) -> None:
        self.events.append("on_step_progress")

    def on_step_success(self, step: ComputeStep) -> None:
        self.events.append("on_step_success")
        if self.explode_on == "on_step_success":
            raise RuntimeError("callback boom on_step_success")

    def on_step_error(self, step: ComputeStep, error: BaseException) -> None:
        self.events.append("on_step_error")
        if self.explode_on == "on_step_error":
            raise RuntimeError("callback boom on_step_error")

    def on_run_success(self) -> None:
        self.events.append("on_run_success")
        if self.explode_on == "on_run_success":
            raise RuntimeError("callback boom on_run_success")

    def on_run_error(self, error: BaseException) -> None:
        self.events.append("on_run_error")
        if self.explode_on == "on_run_error":
            raise RuntimeError("callback boom on_run_error")


class FakeStep(ComputeStep):
    def __init__(self, name: str, *, fail: bool = False, report_progress: bool = False):
        super().__init__(name=name, input_dts=[], output_dts=[])
        self.fail = fail
        self.report_progress = report_progress
        self.run_config_seen = None

    def run_full(self, ds, run_config=None, executor=None):
        self.run_config_seen = run_config
        if self.fail:
            raise RuntimeError(f"{self.name} failed")

    def run_full_observed(self, ds, run_config=None, executor=None, progress=None):
        self.run_config_seen = run_config
        if self.report_progress and progress is not None:
            progress(0, 2)
            progress(1, 2)
            progress(2, 2)
        if self.fail:
            raise RuntimeError(f"{self.name} failed")


class LegacyCustomStep(ComputeStep):
    """Old-style subclass: only overrides run_full without progress."""

    def __init__(self, name: str = "legacy"):
        super().__init__(name=name, input_dts=[], output_dts=[])
        self.ran = False

    def run_full(self, ds, run_config=None, executor=None):
        self.ran = True


def test_run_steps_notifies_callbacks_in_order():
    ds = MagicMock()
    steps = [FakeStep("a"), FakeStep("b")]
    cb1 = RecordingCallback()
    cb2 = RecordingCallback()

    run_steps(ds, steps, callbacks=[cb1, cb2])

    expected = [
        ("run_start", ["a", "b"]),
        ("step_start", "a"),
        ("step_success", "a"),
        ("step_start", "b"),
        ("step_success", "b"),
        ("run_success",),
    ]
    assert cb1.events == expected
    assert cb2.events == expected


def test_run_steps_supports_multiple_callbacks():
    ds = MagicMock()
    cbs = [RecordingCallback(), RecordingCallback(), RecordingCallback()]
    run_steps(ds, [FakeStep("only")], callbacks=cbs)
    assert all(cb.events[-1] == ("run_success",) for cb in cbs)


def test_run_steps_records_step_failure():
    ds = MagicMock()
    steps = [FakeStep("ok"), FakeStep("bad", fail=True), FakeStep("skipped")]
    cb = RecordingCallback()

    with pytest.raises(RuntimeError, match="bad failed"):
        run_steps(ds, steps, callbacks=[cb])

    assert ("step_error", "bad", "RuntimeError", "bad failed") in cb.events
    assert ("run_error", "RuntimeError", "bad failed") in cb.events
    assert ("step_start", "skipped") not in cb.events


def test_callback_error_does_not_mask_step_error():
    ds = MagicMock()
    exploding = ExplodingCallback(explode_on="on_step_error")
    good = RecordingCallback()

    with pytest.raises(RuntimeError, match="bad failed"):
        run_steps(
            ds,
            [FakeStep("bad", fail=True)],
            callbacks=[exploding, good],
        )

    assert ("step_error", "bad", "RuntimeError", "bad failed") in good.events
    assert ("run_error", "RuntimeError", "bad failed") in good.events


def test_callback_error_on_success_is_swallowed():
    ds = MagicMock()
    exploding = ExplodingCallback(explode_on="on_run_success")
    # Pipeline itself must still complete without raising.
    run_steps(ds, [FakeStep("ok")], callbacks=[exploding])
    assert "on_run_success" in exploding.events


def test_run_config_is_passed_with_callback():
    ds = MagicMock()
    step = FakeStep("p")
    cfg = RunConfig(labels={"k": "v"})
    run_steps(ds, [step], run_config=cfg, callbacks=[RecordingCallback()])
    assert step.run_config_seen is cfg


def test_legacy_custom_step_without_progress_still_runs():
    ds = MagicMock()
    step = LegacyCustomStep()
    run_steps(ds, [step], callbacks=[RecordingCallback()])
    assert step.ran is True


def test_progress_is_forwarded():
    ds = MagicMock()
    step = FakeStep("p", report_progress=True)
    cb = RecordingCallback()
    run_steps(ds, [step], callbacks=[cb])
    assert ("step_progress", "p", 0, 2) in cb.events
    assert ("step_progress", "p", 2, 2) in cb.events


def test_empty_steps_complete_run():
    ds = MagicMock()
    cb = RecordingCallback()
    run_steps(ds, [], callbacks=[cb])
    assert cb.events == [
        ("run_start", []),
        ("run_success",),
    ]


def test_recording_callback_satisfies_protocol():
    cb: RunCallback = RecordingCallback()
    assert cb is not None

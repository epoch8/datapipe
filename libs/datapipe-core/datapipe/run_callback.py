"""Lifecycle callbacks for pipeline step execution.

Core owns the execution loop; optional callbacks receive events for
persistence, metrics, UI progress, etc. Callbacks must not replace the runner.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Protocol, Sequence

if TYPE_CHECKING:
    from datapipe.compute import ComputeStep

ProgressCallback = Callable[[int, int | None], None]


class RunCallback(Protocol):
    def on_run_start(self, steps: Sequence[ComputeStep]) -> None: ...

    def on_step_start(self, step: ComputeStep) -> None: ...

    def on_step_progress(
        self,
        step: ComputeStep,
        completed: int,
        total: int | None,
    ) -> None: ...

    def on_step_success(self, step: ComputeStep) -> None: ...

    def on_step_error(self, step: ComputeStep, error: BaseException) -> None: ...

    def on_run_success(self) -> None: ...

    def on_run_error(self, error: BaseException) -> None: ...

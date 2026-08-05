"""Lifecycle callbacks for pipeline step execution.

Core owns the execution loop; optional callbacks receive events for
persistence, metrics, UI progress, etc. Callbacks must not replace the runner.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, Sequence

if TYPE_CHECKING:
    from datapipe.compute import ComputeStep

logger = logging.getLogger("datapipe.run_callback")


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


@dataclass
class CompositeRunCallback:
    """Fans out to multiple RunCallback instances, e.g. for attaching to RunConfig.callback.

    Fail-open: a broken callback is logged and must not mask pipeline errors
    or block the other callbacks.
    """

    callbacks: Sequence[RunCallback]

    def on_run_start(self, steps: Sequence[ComputeStep]) -> None:
        for callback in self.callbacks:
            try:
                callback.on_run_start(steps)
            except Exception:
                logger.exception("Run callback %r failed during on_run_start", callback)

    def on_step_start(self, step: ComputeStep) -> None:
        for callback in self.callbacks:
            try:
                callback.on_step_start(step)
            except Exception:
                logger.exception("Run callback %r failed during on_step_start", callback)

    def on_step_progress(
        self,
        step: ComputeStep,
        completed: int,
        total: int | None,
    ) -> None:
        for callback in self.callbacks:
            try:
                callback.on_step_progress(step, completed, total)
            except Exception:
                logger.exception("Run callback %r failed during on_step_progress", callback)

    def on_step_success(self, step: ComputeStep) -> None:
        for callback in self.callbacks:
            try:
                callback.on_step_success(step)
            except Exception:
                logger.exception("Run callback %r failed during on_step_success", callback)

    def on_step_error(self, step: ComputeStep, error: BaseException) -> None:
        for callback in self.callbacks:
            try:
                callback.on_step_error(step, error)
            except Exception:
                logger.exception("Run callback %r failed during on_step_error", callback)

    def on_run_success(self) -> None:
        for callback in self.callbacks:
            try:
                callback.on_run_success()
            except Exception:
                logger.exception("Run callback %r failed during on_run_success", callback)

    def on_run_error(self, error: BaseException) -> None:
        for callback in self.callbacks:
            try:
                callback.on_run_error(error)
            except Exception:
                logger.exception("Run callback %r failed during on_run_error", callback)

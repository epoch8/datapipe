from __future__ import annotations

import logging
import time
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass, field
from typing import Generator, Optional, Sequence

from datapipe.compute import Catalog, ComputeStep, DataStore

from datapipe_app.observability.store.db import ObservabilityStore
from datapipe_app.observability.logging.log_buffer import RunLogBuffer
from datapipe_app.observability.plugins.registry import ObservabilityRegistry
from datapipe_app.ops.spec_registry import OpsSpecRegistry
from datapipe_app.observability.runs.run_output_capture import capture_run_output

logger = logging.getLogger(__name__)


@dataclass
class RecordingRunCallback:
    """Per-run ``RunCallback`` with a stable ``run_id``.

    Safe for concurrent API runs: each callback binds writes to its own
    ``run_id`` and does not rely on ``RunRecorder.current_run_id``.
    """

    recorder: RunRecorder
    run_id: str
    cli_announce: bool = False
    _finished: bool = field(default=False, init=False, repr=False)

    def on_run_start(self, steps: Sequence[ComputeStep]) -> None:
        if self.cli_announce:
            print(
                f"Recording run {self.run_id} for pipeline "
                f"'{self.recorder.pipeline_id}' (view in Ops UI → Runs)"
            )

    def on_step_start(self, step: ComputeStep) -> None:
        self.recorder.start_step(step.name, run_id=self.run_id)

    def on_step_progress(
        self,
        step: ComputeStep,
        completed: int,
        total: int | None,
    ) -> None:
        self.recorder.update_step_progress(
            step.name,
            processed=completed,
            total=total,
            run_id=self.run_id,
        )

    def on_step_success(self, step: ComputeStep) -> None:
        self.recorder.finish_step(step.name, status="completed", run_id=self.run_id)

    def on_step_error(self, step: ComputeStep, error: BaseException) -> None:
        self.recorder.finish_step(
            step.name,
            status="failed",
            error=str(error),
            run_id=self.run_id,
        )

    def on_run_success(self) -> None:
        self.recorder.finish_run(status="completed", run_id=self.run_id)
        self._finished = True
        if self.cli_announce:
            print(f"Run {self.run_id} completed")

    def on_run_error(self, error: BaseException) -> None:
        self.recorder.finish_run(status="failed", error=str(error), run_id=self.run_id)
        self._finished = True
        if self.cli_announce:
            print(f"Run {self.run_id} failed (details in Ops UI → Runs → {self.run_id})")


class RunRecorder:
    """Persistence service for Run / RunStep rows.

    Does not execute pipeline steps. Prefer a per-run callback for concurrent
    or background runs::

        cb = recorder.create_callback(trigger="api")
        run_steps(ds, steps, executor=executor, callbacks=[cb])

    ``current_run_id`` and methods called without an explicit ``run_id`` are
    single-run convenience only — not safe if multiple runs share one recorder.
    """

    def __init__(
        self,
        store: ObservabilityStore,
        pipeline_id: str,
        *,
        registry: Optional[ObservabilityRegistry] = None,
        ds: Optional[DataStore] = None,
        catalog: Optional[Catalog] = None,
        ops_specs: Optional[OpsSpecRegistry] = None,
        log_buffer: Optional[RunLogBuffer] = None,
    ):
        self.store = store
        self.pipeline_id = pipeline_id
        self.registry = registry
        self.ds = ds
        self.catalog = catalog
        self.ops_specs = ops_specs
        self.log_buffer = log_buffer
        # Last started run; single-run convenience, not concurrency-safe.
        self._current_run_id: Optional[str] = None
        self._output_captures: dict[str, AbstractContextManager[None]] = {}
        # Throttle progress log lines per run/step (monotonic seconds).
        self._last_progress_log_at: dict[str, float] = {}
        self._last_progress_logged: dict[str, tuple[int, int | None]] = {}

    @property
    def current_run_id(self) -> Optional[str]:
        """Most recently started run id (single-run convenience; not concurrency-safe)."""
        return self._current_run_id

    def create_callback(
        self,
        *,
        trigger: Optional[str] = None,
        labels_json: Optional[str] = None,
        cli_announce: bool = False,
    ) -> RecordingRunCallback:
        run_id = self.start_run(trigger=trigger, labels_json=labels_json)
        return RecordingRunCallback(
            recorder=self,
            run_id=run_id,
            cli_announce=cli_announce,
        )

    def _attach_log_handler(self, run_id: str) -> None:
        if not self.log_buffer:
            return
        self.log_buffer.start_run(run_id)
        capture = capture_run_output(self.log_buffer, run_id)
        capture.__enter__()
        self._output_captures[run_id] = capture
        self.log_buffer.append(run_id, "INFO", f"Run {run_id} started")

    def _detach_log_handler(self, run_id: str) -> None:
        capture = self._output_captures.pop(run_id, None)
        if capture is not None:
            capture.__exit__(None, None, None)
        if self.log_buffer:
            self.log_buffer.append(run_id, "INFO", f"Run {run_id} finished")
            self.log_buffer.finish_run(run_id)

    def start_run(self, *, trigger: Optional[str] = None, labels_json: Optional[str] = None) -> str:
        run_id = self.store.create_run(
            self.pipeline_id,
            trigger=trigger,
            labels_json=labels_json,
        )
        self._attach_log_handler(run_id)
        self._current_run_id = run_id
        if trigger and self.log_buffer:
            self.log_buffer.append(run_id, "INFO", f"Trigger: {trigger}")
        return run_id

    def _run_id(self, run_id: Optional[str] = None) -> Optional[str]:
        return run_id or self._current_run_id

    def start_step(self, step_name: str, *, run_id: Optional[str] = None) -> None:
        rid = self._run_id(run_id)
        if not rid:
            return
        self.store.upsert_run_step(
            rid,
            step_name,
            status="running",
        )
        if self.log_buffer:
            self.log_buffer.append(rid, "INFO", f"Step started: {step_name}")

    def update_step_progress(
        self,
        step_name: str,
        *,
        processed: int,
        total: int | None = None,
        run_id: Optional[str] = None,
    ) -> None:
        rid = self._run_id(run_id)
        if not rid:
            return
        self.store.upsert_run_step(
            rid,
            step_name,
            status="running",
            processed=processed,
            total=total,
        )
        self._maybe_log_step_progress(rid, step_name, processed=processed, total=total)

    def _maybe_log_step_progress(
        self,
        run_id: str,
        step_name: str,
        *,
        processed: int,
        total: int | None,
    ) -> None:
        """Emit tqdm-like progress lines into the run log (throttled).

        ``tqdm_loggable`` is also captured, but only every ~10s by default and
        only if that logger is attached. This path always mirrors batch
        progress into the Ops UI logs with a 2s / milestone throttle.
        """
        if not self.log_buffer:
            return
        key = f"{run_id}:{step_name}"
        now = time.monotonic()
        last_at = self._last_progress_log_at.get(key, 0.0)
        last_vals = self._last_progress_logged.get(key)
        is_start = processed == 0
        is_done = total is not None and total > 0 and processed >= total
        changed = last_vals != (processed, total)
        due = (now - last_at) >= 2.0
        if not changed:
            return
        if not (is_start or is_done or due or last_vals is None):
            return

        self._last_progress_log_at[key] = now
        self._last_progress_logged[key] = (processed, total)
        if total is not None and total > 0:
            pct = int(round(100.0 * processed / total))
            msg = f"Progress: {step_name} {processed}/{total} ({pct}%)"
        else:
            msg = f"Progress: {step_name} {processed}/?"
        self.log_buffer.append(run_id, "INFO", msg)

    def finish_step(
        self,
        step_name: str,
        *,
        status: str = "completed",
        error: Optional[str] = None,
        run_id: Optional[str] = None,
    ) -> None:
        rid = self._run_id(run_id)
        if not rid:
            return
        from datapipe_app.observability.store.db import utc_now

        self.store.upsert_run_step(
            rid,
            step_name,
            status=status,
            finished_at=utc_now(),
            error=error,
        )
        key = f"{rid}:{step_name}"
        self._last_progress_log_at.pop(key, None)
        self._last_progress_logged.pop(key, None)
        if self.log_buffer:
            level = "ERROR" if error else "INFO"
            msg = f"Step {status}: {step_name}" + (f" — {error}" if error else "")
            self.log_buffer.append(rid, level, msg)

    def finish_run(
        self,
        *,
        status: str = "completed",
        error: Optional[str] = None,
        run_id: Optional[str] = None,
    ) -> None:
        """Finish a run. Pass ``run_id`` explicitly under concurrent use."""
        rid = self._run_id(run_id)
        if not rid:
            return
        try:
            self.store.finish_run(rid, status=status, error=error)
            if error and self.log_buffer:
                self.log_buffer.append(rid, "ERROR", error)
        finally:
            self._detach_log_handler(rid)
            if self._current_run_id == rid:
                self._current_run_id = None

    @contextmanager
    def run_scope(
        self,
        *,
        trigger: Optional[str] = None,
        labels_json: Optional[str] = None,
        cli_announce: bool = False,
    ) -> Generator[RecordingRunCallback, None, None]:
        """Create a per-run callback and ensure the run is finished on exit.

        Typical use::

            with recorder.run_scope(trigger="api") as cb:
                run_steps(ds, steps, callbacks=[cb])
        """
        cb = self.create_callback(
            trigger=trigger,
            labels_json=labels_json,
            cli_announce=cli_announce,
        )
        try:
            yield cb
        except BaseException as exc:
            if not cb._finished:
                self.finish_run(status="failed", error=str(exc), run_id=cb.run_id)
                cb._finished = True
            raise
        else:
            if not cb._finished:
                self.finish_run(status="completed", run_id=cb.run_id)
                cb._finished = True

from __future__ import annotations

import logging
import time
from contextlib import AbstractContextManager, contextmanager
from typing import Generator, Literal, Optional

from datapipe.compute import Catalog, ComputeStep, DataStore
from datapipe.executor import Executor
from datapipe.step.batch_transform import BaseBatchTransformStep

from datapipe_app.observability.store.db import ObservabilityStore
from datapipe_app.observability.logging.log_buffer import RunLogBuffer
from datapipe_app.observability.plugins.registry import ObservabilityRegistry
from datapipe_app.ops.spec_registry import OpsSpecRegistry
from datapipe_app.observability.runs.run_output_capture import capture_run_output

logger = logging.getLogger(__name__)


class RunRecorder:
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
        self._current_run_id: Optional[str] = None
        self._output_capture: Optional[AbstractContextManager[None]] = None
        # Throttle progress log lines per run/step (monotonic seconds).
        self._last_progress_log_at: dict[str, float] = {}
        self._last_progress_logged: dict[str, tuple[int, int]] = {}

    @property
    def current_run_id(self) -> Optional[str]:
        return self._current_run_id

    def _attach_log_handler(self, run_id: str) -> None:
        if not self.log_buffer:
            return
        self.log_buffer.start_run(run_id)
        self._output_capture = capture_run_output(self.log_buffer, run_id)
        self._output_capture.__enter__()
        self.log_buffer.append(run_id, "INFO", f"Run {run_id} started")

    def _detach_log_handler(self, run_id: str) -> None:
        if self._output_capture is not None:
            self._output_capture.__exit__(None, None, None)
            self._output_capture = None
        if self.log_buffer:
            self.log_buffer.append(run_id, "INFO", f"Run {run_id} finished")
            self.log_buffer.finish_run(run_id)

    def start_run(self, *, trigger: Optional[str] = None, labels_json: Optional[str] = None) -> str:
        self._current_run_id = self.store.create_run(
            self.pipeline_id,
            trigger=trigger,
            labels_json=labels_json,
        )
        self._attach_log_handler(self._current_run_id)
        if trigger:
            self.log_buffer and self.log_buffer.append(
                self._current_run_id, "INFO", f"Trigger: {trigger}"
            )
        return self._current_run_id

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
        total: int,
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
        total: int,
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
        is_done = total > 0 and processed >= total
        changed = last_vals != (processed, total)
        due = (now - last_at) >= 2.0
        if not changed:
            return
        if not (is_start or is_done or due or last_vals is None):
            return

        self._last_progress_log_at[key] = now
        self._last_progress_logged[key] = (processed, total)
        if total > 0:
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
        rid = self._run_id(run_id)
        if not rid:
            return
        try:
            self.store.finish_run(rid, status=status, error=error)
            if error and self.log_buffer:
                self.log_buffer.append(rid, "ERROR", error)
        finally:
            if self._current_run_id == rid:
                self._detach_log_handler(rid)
                self._current_run_id = None

    def execute_steps(
        self,
        steps: list[ComputeStep],
        *,
        ds: DataStore,
        run_id: Optional[str] = None,
        executor: Executor | None = None,
        on_step_failure: Literal["raise", "return"] = "raise",
    ) -> None:
        rid = self._run_id(run_id)
        if not rid:
            raise ValueError("run_id is required to execute steps")
        for step in steps:
            self.start_step(step.name, run_id=rid)
            try:
                run_kwargs: dict = {"ds": ds, "run_config": None, "executor": executor}
                if isinstance(step, BaseBatchTransformStep):

                    def on_batch_progress(processed: int, total: int) -> None:
                        self.update_step_progress(
                            step.name,
                            processed=processed,
                            total=total,
                            run_id=rid,
                        )

                    run_kwargs["on_batch_progress"] = on_batch_progress

                step.run_full(**run_kwargs)  # type: ignore[arg-type]
                self.finish_step(step.name, status="completed", run_id=rid)
            except Exception as exc:
                error = str(exc)
                self.finish_step(step.name, status="failed", error=error, run_id=rid)
                self.finish_run(status="failed", error=error, run_id=rid)
                if on_step_failure == "raise":
                    raise
                return
        self.finish_run(status="completed", run_id=rid)

    @contextmanager
    def record_steps(
        self,
        steps: list[ComputeStep],
        *,
        trigger: Optional[str] = None,
    ) -> Generator[str, None, None]:
        if self.ds is None:
            raise ValueError("DataStore is required to record steps")
        run_id = self.start_run(trigger=trigger)
        try:
            self.execute_steps(steps, ds=self.ds, run_id=run_id)
            yield run_id
        except Exception:
            raise

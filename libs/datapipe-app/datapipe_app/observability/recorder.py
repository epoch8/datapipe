from __future__ import annotations

import logging
from contextlib import AbstractContextManager, contextmanager
from typing import Generator, Literal, Optional

from datapipe.compute import Catalog, ComputeStep, DataStore
from datapipe.executor import Executor
from datapipe.step.batch_transform import BaseBatchTransformStep

from datapipe_app.observability.db import ObservabilityStore
from datapipe_app.observability.log_buffer import RunLogBuffer
from datapipe_app.observability.registry import ObservabilityRegistry
from datapipe_ml.spec_registry import OpsSpecRegistry
from datapipe_app.observability.run_output_capture import capture_run_output

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
        from datapipe_app.observability.db import utc_now

        self.store.upsert_run_step(
            rid,
            step_name,
            status=status,
            finished_at=utc_now(),
            error=error,
        )
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
            if status == "completed" and self.registry and self.ds and self.catalog:
                self._publish_metrics()
            if error and self.log_buffer:
                self.log_buffer.append(rid, "ERROR", error)
        finally:
            if self._current_run_id == rid:
                self._detach_log_handler(rid)
                self._current_run_id = None

    def _publish_metrics(self) -> None:
        if not self.registry or not self.ds or not self.catalog:
            return
        try:
            from datapipe_ml.observability.analytics_views import refresh_analytics_views
            from datapipe_ml.observability.training_service import TrainingService

            training_rows = []
            try:
                svc = TrainingService(
                    store=self.store,
                    ds=self.ds,
                    catalog=self.catalog,
                    ops_specs=self.ops_specs,
                )
                training_rows = [r.model_dump() for r in svc.list_runs(self.pipeline_id, limit=500).rows]
            except Exception:
                training_rows = []
            refresh_analytics_views(
                self.store.engine,
                pipeline_id=self.pipeline_id,
                store=self.store,
                ds=self.ds,
                catalog=self.catalog,
                ops_specs=self.ops_specs,
                training_runs=training_rows,
            )
        except Exception:
            logger.exception("Analytics view refresh failed for pipeline %s", self.pipeline_id)

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

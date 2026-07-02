from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Generator, Optional

from datapipe.compute import Catalog, ComputeStep, DataStore

from datapipe_app.observability.db import ObservabilityStore
from datapipe_app.observability.log_buffer import RunLogBuffer, RunLogHandler
from datapipe_app.observability.registry import ObservabilityRegistry

logger = logging.getLogger(__name__)
_DATAPIPE_LOGGER = logging.getLogger("datapipe")


class RunRecorder:
    def __init__(
        self,
        store: ObservabilityStore,
        pipeline_id: str,
        *,
        registry: Optional[ObservabilityRegistry] = None,
        ds: Optional[DataStore] = None,
        catalog: Optional[Catalog] = None,
        log_buffer: Optional[RunLogBuffer] = None,
    ):
        self.store = store
        self.pipeline_id = pipeline_id
        self.registry = registry
        self.ds = ds
        self.catalog = catalog
        self.log_buffer = log_buffer
        self._current_run_id: Optional[str] = None
        self._log_handler: Optional[RunLogHandler] = None

    @property
    def current_run_id(self) -> Optional[str]:
        return self._current_run_id

    def _attach_log_handler(self, run_id: str) -> None:
        if not self.log_buffer:
            return
        self.log_buffer.start_run(run_id)
        self._log_handler = RunLogHandler(self.log_buffer, run_id)
        _DATAPIPE_LOGGER.addHandler(self._log_handler)
        self.log_buffer.append(run_id, "INFO", f"Run {run_id} started")

    def _detach_log_handler(self, run_id: str) -> None:
        if self._log_handler:
            _DATAPIPE_LOGGER.removeHandler(self._log_handler)
            self._log_handler = None
        if self.log_buffer:
            self.log_buffer.append(run_id, "INFO", f"Run {run_id} finished")
            self.log_buffer.finish_run(run_id)

    def start_run(self, *, trigger: Optional[str] = None) -> str:
        self._current_run_id = self.store.create_run(self.pipeline_id, trigger=trigger)
        self._attach_log_handler(self._current_run_id)
        if trigger:
            self.log_buffer and self.log_buffer.append(
                self._current_run_id, "INFO", f"Trigger: {trigger}"
            )
        return self._current_run_id

    def start_step(self, step_name: str) -> None:
        if not self._current_run_id:
            return
        self.store.upsert_run_step(
            self._current_run_id,
            step_name,
            status="running",
        )
        if self.log_buffer:
            self.log_buffer.append(self._current_run_id, "INFO", f"Step started: {step_name}")

    def update_step_progress(
        self,
        step_name: str,
        *,
        processed: int,
        total: int,
    ) -> None:
        if not self._current_run_id:
            return
        self.store.upsert_run_step(
            self._current_run_id,
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
    ) -> None:
        if not self._current_run_id:
            return
        from datapipe_app.observability.db import utc_now

        self.store.upsert_run_step(
            self._current_run_id,
            step_name,
            status=status,
            finished_at=utc_now(),
            error=error,
        )
        if self.log_buffer:
            level = "ERROR" if error else "INFO"
            msg = f"Step {status}: {step_name}" + (f" — {error}" if error else "")
            self.log_buffer.append(self._current_run_id, level, msg)

    def finish_run(self, *, status: str = "completed", error: Optional[str] = None) -> None:
        if not self._current_run_id:
            return
        run_id = self._current_run_id
        self.store.finish_run(run_id, status=status, error=error)
        if status == "completed" and self.registry and self.ds and self.catalog:
            self._publish_metrics()
        if error and self.log_buffer:
            self.log_buffer.append(run_id, "ERROR", error)
        self._detach_log_handler(run_id)
        self._current_run_id = None

    def _publish_metrics(self) -> None:
        if not self.registry or not self.ds or not self.catalog:
            return
        for publisher in self.registry.publishers:
            try:
                publisher.publish_metrics(
                    self.store,
                    pipeline_id=self.pipeline_id,
                    ds=self.ds,
                    catalog=self.catalog,
                )
            except Exception:
                logger.exception("Metrics publisher failed for pipeline %s", self.pipeline_id)
        try:
            from datapipe_app.observability.analytics_views import refresh_analytics_views
            from datapipe_app.observability.training_service import TrainingService

            training_rows = []
            try:
                svc = TrainingService(store=self.store, ds=self.ds, catalog=self.catalog)
                training_rows = [r.model_dump() for r in svc.list_runs(self.pipeline_id, limit=500).rows]
            except Exception:
                training_rows = []
            refresh_analytics_views(
                self.store.engine,
                pipeline_id=self.pipeline_id,
                store=self.store,
                ds=self.ds,
                catalog=self.catalog,
                training_runs=training_rows,
            )
        except Exception:
            logger.exception("Analytics view refresh failed for pipeline %s", self.pipeline_id)

    @contextmanager
    def record_steps(
        self,
        steps: list[ComputeStep],
        *,
        trigger: Optional[str] = None,
    ) -> Generator[str, None, None]:
        run_id = self.start_run(trigger=trigger)
        run_error: Optional[str] = None
        run_status = "completed"
        try:
            for step in steps:
                self.start_step(step.name)
                try:
                    step.run_full(ds=self.ds, run_config=None, executor=None)  # type: ignore[arg-type]
                    self.finish_step(step.name, status="completed")
                except Exception as exc:
                    run_status = "failed"
                    run_error = str(exc)
                    self.finish_step(step.name, status="failed", error=run_error)
                    raise
            yield run_id
        except Exception:
            if run_status != "failed":
                run_status = "failed"
            raise
        finally:
            self.finish_run(status=run_status, error=run_error)

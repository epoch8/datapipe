from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Optional

from datapipe.compute import ComputeStep
from datapipe.executor import Executor
from datapipe.types import Labels

from datapipe_app.app.datapipe_api import DatapipeAPI
from datapipe_app.observability.store.db import ObservabilityStore
from datapipe_app.observability.connections.dbconn import dbconn_same_target
from datapipe_app.observability.logging.log_buffer import RunLogBuffer, get_log_buffer
from datapipe_app.observability.runs.recorder import RunRecorder
from datapipe_app.observability.runs.run_scope import labels_to_json
from datapipe_app.observability.runs.run_triggers import cli_trigger_from_labels
from datapipe_app.observability.run_logs import warn_if_run_logs_share_pipeline_db
from datapipe_app.observability.config.settings import resolve_ops_settings
from datapipe_app.observability.config.tables import (
    ObservabilityTableConfig,
    ensure_observability_tables_compatible_with_pipeline,
)
from datapipe_app.pipeline.pipeline_binding import pipeline_module_for

if TYPE_CHECKING:
    from datapipe.compute import Catalog, DatapipeApp

logger = logging.getLogger(__name__)


def _labels_from_steps(steps: list[ComputeStep]) -> Labels:
    if not steps:
        return []
    return list(steps[0].labels)


def _build_recorder(app: DatapipeApp, *, pipeline_spec: Optional[str] = None) -> tuple[RunRecorder, str]:
    settings = resolve_ops_settings(
        pipeline_spec=pipeline_spec,
        pipeline_module=pipeline_module_for(app),
    )
    if not settings.pipeline_id:
        raise ValueError("pipeline_id is required to record CLI runs (pass pipeline_id= to DatapipeAPI)")

    tables = getattr(app, "observability_table_config", None) or ObservabilityTableConfig()

    if isinstance(app, DatapipeAPI):
        observability_dbconn = app.observability_dbconn
        run_logs_backend = app.run_logs_backend
        store = app.observability_store
    else:
        observability_dbconn = app.ds.meta_dbconn
        run_logs_backend = None
        ensure_observability_tables_compatible_with_pipeline(
            observability_dbconn=observability_dbconn,
            pipeline_dbconn=app.ds.meta_dbconn,
            config=tables,
            catalog=app.catalog,
        )
        warn_if_run_logs_share_pipeline_db(
            pipeline_dbconn=app.ds.meta_dbconn,
            observability_dbconn=observability_dbconn,
            run_logs_backend=run_logs_backend,
        )
        from datapipe_app.app.db_schema import register_observability_tables_in_metadata

        register_observability_tables_in_metadata(
            app.ds.meta_dbconn,
            tables=tables,
            include_run_logs=run_logs_backend is None,
        )
        store = ObservabilityStore.from_dbconn(
            observability_dbconn,
            tables=tables,
            run_logs_backend=run_logs_backend,
        )
        store.register_pipeline(
            settings.pipeline_id,
            display_name=settings.pipeline_id.replace("_", " ").title(),
        )

    if dbconn_same_target(observability_dbconn, app.ds.meta_dbconn):
        ensure_observability_tables_compatible_with_pipeline(
            observability_dbconn=observability_dbconn,
            pipeline_dbconn=app.ds.meta_dbconn,
            config=tables,
            catalog=app.catalog,
        )

    log_buffer = get_log_buffer(store)
    if not isinstance(log_buffer, RunLogBuffer):
        log_buffer = RunLogBuffer(store)

    catalog: Optional[Catalog] = getattr(app, "catalog", None)
    recorder = RunRecorder(
        store,
        settings.pipeline_id,
        ds=app.ds,
        catalog=catalog,
        log_buffer=log_buffer,
    )
    return recorder, settings.pipeline_id


def try_run_steps_observed(
    app: DatapipeApp,
    steps: list[ComputeStep],
    *,
    executor: Executor,
    labels: Optional[Labels] = None,
    pipeline_spec: Optional[str] = None,
    record: bool = True,
) -> bool:
    """Execute steps through RunRecorder so the Ops UI can list logs.

    Returns True when the observed path was used, False to fall back to bare ``run_steps``.
    """
    if not record or not steps:
        return False

    settings = resolve_ops_settings(
        pipeline_spec=pipeline_spec,
        pipeline_module=pipeline_module_for(app),
    )
    if not settings.record_cli_runs:
        return False

    try:
        recorder, pipeline_id = _build_recorder(app, pipeline_spec=pipeline_spec)
    except Exception as exc:
        logger.warning("CLI run recording disabled: %s", exc)
        return False

    effective_labels = labels if labels is not None else _labels_from_steps(steps)
    trigger = cli_trigger_from_labels(effective_labels)
    labels_json = labels_to_json(effective_labels)

    run_id = recorder.start_run(trigger=trigger, labels_json=labels_json)
    print(f"Recording run {run_id} for pipeline '{pipeline_id}' (view in Ops UI → Runs)")
    try:
        recorder.execute_steps(steps, ds=app.ds, run_id=run_id, executor=executor)
    except Exception:
        print(f"Run {run_id} failed (details in Ops UI → Runs → {run_id})")
        raise
    print(f"Run {run_id} completed")
    return True

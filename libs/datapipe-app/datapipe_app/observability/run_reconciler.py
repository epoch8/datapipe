from __future__ import annotations

import logging

from datapipe_app.observability.db import ObservabilityStore

logger = logging.getLogger(__name__)

_DEFAULT_REASON = "Agent process stopped while the run was still in progress"


def reconcile_orphaned_runs(
    store: ObservabilityStore,
    pipeline_id: str,
    *,
    reason: str = _DEFAULT_REASON,
) -> list[str]:
    """Mark in-flight runs for this pipeline as interrupted (e.g. after API restart)."""
    running = store.list_running_runs(pipeline_id)
    if not running:
        return []

    reconciled: list[str] = []
    for row in running:
        run_id = row.run_id
        store.finish_running_steps(run_id, status="interrupted", error=reason)
        store.finish_run(run_id, status="interrupted", error=reason)
        last_logs = store.get_run_logs(run_id, after=0, limit=20)
        if not any(reason in log.message for log in last_logs):
            store.append_run_log_line(run_id, "ERROR", reason)
        reconciled.append(run_id)
        logger.warning(
            "Reconciled orphaned run %s for pipeline %s as interrupted",
            run_id,
            pipeline_id,
        )
    return reconciled


def reconcile_orphaned_runs_on_startup(store: ObservabilityStore, pipeline_id: str) -> list[str]:
    return reconcile_orphaned_runs(store, pipeline_id)

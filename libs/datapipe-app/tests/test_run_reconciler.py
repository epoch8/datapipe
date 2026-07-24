from __future__ import annotations

from datapipe_app.observability.store.db import ObservabilityStore
from datapipe_app.observability.runs.run_reconciler import reconcile_orphaned_runs
from datapipe_app.observability.run_logs import RunLogsBackend


def _store(tmp_path) -> ObservabilityStore:
    return ObservabilityStore.from_url(
        f"sqlite:///{tmp_path / 'obs.db'}",
        run_logs_backend=RunLogsBackend.memory(),
    )


def test_reconcile_orphaned_runs_marks_running_run_and_steps(tmp_path):
    store = _store(tmp_path)
    store.register_pipeline("demo")
    run_id = store.create_run("demo", trigger="api:stage:train")
    store.upsert_run_step(run_id, "step_a", status="completed")
    store.upsert_run_step(run_id, "step_b", status="running")

    reconciled = reconcile_orphaned_runs(store, "demo", reason="test restart")

    assert reconciled == [run_id]
    run = store.get_run(run_id)
    assert run is not None
    assert run.status == "interrupted"
    assert run.error == "test restart"
    assert run.finished_at is not None

    steps = {step.step_name: step for step in store.get_run_steps(run_id)}
    assert steps["step_a"].status == "completed"
    assert steps["step_b"].status == "interrupted"
    assert steps["step_b"].error == "test restart"

    logs = store.get_run_logs(run_id)
    assert any("test restart" in log.message for log in logs)


def test_reconcile_orphaned_runs_ignores_other_pipelines(tmp_path):
    store = _store(tmp_path)
    store.register_pipeline("demo")
    store.register_pipeline("other")
    run_id = store.create_run("other", trigger="api:pipeline")

    reconciled = reconcile_orphaned_runs(store, "demo")

    assert reconciled == []
    assert store.get_run(run_id).status == "running"


def test_reconcile_orphaned_runs_is_idempotent(tmp_path):
    store = _store(tmp_path)
    store.register_pipeline("demo")
    store.create_run("demo")

    assert reconcile_orphaned_runs(store, "demo") != []
    assert reconcile_orphaned_runs(store, "demo") == []

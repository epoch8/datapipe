from __future__ import annotations

from datapipe_app.observability.store.db import ObservabilityStore


def test_finish_run_updates_created_run(tmp_path):
    store = ObservabilityStore.from_url(f"sqlite:///{tmp_path / 'obs.db'}")
    store.register_pipeline("demo")
    run_id = store.create_run("demo", trigger="test")

    store.finish_run(run_id, status="completed")

    row = store.get_run(run_id)
    assert row is not None
    assert row.status == "completed"
    assert row.finished_at is not None


def test_finish_run_missing_row_does_not_raise(tmp_path):
    store = ObservabilityStore.from_url(f"sqlite:///{tmp_path / 'obs.db'}")
    store.finish_run("missing-run-id", status="failed", error="gone")

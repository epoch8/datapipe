from __future__ import annotations

from datapipe_app.observability.db import ObservabilityStore


def test_list_runs_filters_and_paginates(tmp_path) -> None:
    store = ObservabilityStore.from_url(f"sqlite:///{tmp_path / 'obs.db'}")
    store.register_pipeline("demo")

    store.create_run("demo", trigger="api:pipeline")
    store.create_run("demo", trigger="api:stage:train")
    store.create_run("demo", trigger="api:stage:inference")
    store.create_run("other", trigger="api:pipeline")

    rows, total, filters, counts = store.list_runs(pipeline_id="demo", limit=2, offset=0)
    assert total == 3
    assert len(rows) == 2
    assert "train" in filters["stages"]
    assert "all labels" in filters["stages"]
    assert counts["running"] == 3

    stage_rows, stage_total, _, _ = store.list_runs(pipeline_id="demo", stage="train")
    assert stage_total == 1
    assert stage_rows[0].trigger == "api:stage:train"

    full_rows, full_total, _, _ = store.list_runs(pipeline_id="demo", stage="all labels")
    assert full_total == 1
    assert full_rows[0].trigger == "api:pipeline"

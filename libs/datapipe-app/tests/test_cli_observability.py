from datapipe_app.observability.store.db import ObservabilityStore
from datapipe_app.observability.runs.run_triggers import cli_trigger_from_labels


def test_cli_trigger_from_labels() -> None:
    assert cli_trigger_from_labels([]) == "cli:pipeline"
    assert cli_trigger_from_labels([("stage", "train")]) == "cli:stage:train"
    assert cli_trigger_from_labels([("tag", "night")]) == "cli"


def test_list_runs_includes_cli_stage(tmp_path) -> None:
    store = ObservabilityStore.from_url(f"sqlite:///{tmp_path / 'obs.db'}")
    store.register_pipeline("demo")

    store.create_run("demo", trigger="cli:stage:train")
    store.create_run("demo", trigger="api:stage:train")

    rows, total, filters, _ = store.list_runs(pipeline_id="demo", stage="train")
    assert total == 2
    assert {row.trigger for row in rows} == {"cli:stage:train", "api:stage:train"}
    assert "train" in filters["stages"]

    pipeline_rows, pipeline_total, _, _ = store.list_runs(pipeline_id="demo", stage="all labels")
    store.create_run("demo", trigger="cli:pipeline")
    pipeline_rows, pipeline_total, _, _ = store.list_runs(pipeline_id="demo", stage="all labels")
    assert pipeline_total >= 1

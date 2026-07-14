from datapipe_app.observability.runs.run_scope import (
    derive_run_scope,
    labels_from_json,
    labels_to_json,
    trigger_from_labels,
)


def test_trigger_from_labels() -> None:
    assert trigger_from_labels([]) == "api:pipeline"
    assert trigger_from_labels([("stage", "train")]) == "api:stage:train"


def test_derive_run_scope_full_pipeline() -> None:
    scope = derive_run_scope(labels=[], trigger="api:pipeline")
    assert scope["run_scope"] == "full_pipeline"
    assert scope["target_label_display"] == "all labels"


def test_derive_run_scope_stage() -> None:
    scope = derive_run_scope(labels=[("stage", "train")], trigger="api:stage:train")
    assert scope["run_scope"] == "stage_run"
    assert scope["target_label_display"] == "train"
    assert scope["target_labels"] == [["stage", "train"]]


def test_labels_json_roundtrip() -> None:
    labels = [("stage", "inference")]
    raw = labels_to_json(labels)
    assert raw is not None
    assert labels_from_json(raw) == labels


def test_derive_from_trigger_only() -> None:
    scope = derive_run_scope(labels=[], trigger="api:stage:fiftyone")
    assert scope["run_scope"] == "stage_run"
    assert scope["target_label_display"] == "fiftyone"

    cli_scope = derive_run_scope(labels=[], trigger="cli:stage:train")
    assert cli_scope["run_scope"] == "stage_run"
    assert cli_scope["target_label_display"] == "train"

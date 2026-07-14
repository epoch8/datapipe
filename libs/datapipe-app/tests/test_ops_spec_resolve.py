import pytest

from datapipe_app.ops.ops_spec_resolve import (
    column_source_by_link,
    entity_links_from_columns,
    is_db_column,
    query_columns,
)
from datapipe_app.ops.specs import OpsColumn


def test_column_source_by_link():
    columns = [
        OpsColumn("run_id", "Run ID", "training_status__run_key", link_to="training_run"),
        OpsColumn("detection_frozen_dataset_id", "Dataset", "detection_frozen_dataset_id", link_to="frozen_dataset"),
    ]
    assert column_source_by_link(columns, "training_run") == "training_status__run_key"
    assert column_source_by_link(columns, "missing") is None


def test_entity_links_from_columns():
    columns = [
        OpsColumn("detection_model_id", "Model", "detection_model_id", link_to="model"),
        OpsColumn("run_id", "Run ID", "training_status__run_key", link_to="training_run"),
    ]
    assert entity_links_from_columns(columns) == {
        "model": "detection_model_id",
        "training_run": "training_status__run_key",
    }


def test_query_columns_skips_computed():
    columns = [
        OpsColumn("detection_frozen_dataset_id", "Dataset", "detection_frozen_dataset_id"),
        OpsColumn("split", "Split", "split", kind="split"),
        OpsColumn("models", "Models", "models_count", kind="models_count"),
        OpsColumn("duration", "Duration", "duration_seconds", kind="duration"),
    ]
    assert [column.source for column in query_columns(columns)] == ["detection_frozen_dataset_id"]
    assert is_db_column(columns[1]) is False

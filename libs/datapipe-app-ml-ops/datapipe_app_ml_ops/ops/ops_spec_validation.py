from __future__ import annotations

from datapipe.compute import Catalog
from datapipe.datatable import DataStore

from datapipe_app_ml_ops.ops.spec_registry import OpsSpecRegistry
from datapipe_app_ml_ops.ops.ops_specs import DatapipeOpsSpec


def validate_ml_spec_extensions(
    spec: DatapipeOpsSpec,
    catalog: Catalog | None,
    db: DataStore | None,
    *,
    registry: OpsSpecRegistry,
) -> None:
    frozen_dataset = spec.frozen_dataset
    if frozen_dataset is not None:
        schema = registry._schema_for(spec, frozen_dataset.table, catalog, db)
        for column in [
            frozen_dataset.id_column,
            frozen_dataset.created_at_column,
            frozen_dataset.display_name_column,
            *frozen_dataset.split_columns.values(),
        ]:
            if column:
                registry._require_column(spec, frozen_dataset.table, column, schema)
        registry._validate_page_columns(spec, frozen_dataset.table, "frozen_dataset", frozen_dataset.columns, schema)

    model = spec.model
    if model is not None:
        schema = registry._schema_for(spec, model.table, catalog, db)
        for column in [
            model.id_column,
            model.display_name_column,
            model.created_at_column,
            model.artifact_uri_column,
        ]:
            if column:
                registry._require_column(spec, model.table, column, schema)
        if model.is_best_table:
            best_schema = registry._schema_for(spec, model.is_best_table, catalog, db)
            if model.is_best_column:
                registry._require_column(
                    spec,
                    model.is_best_table,
                    model.is_best_column,
                    best_schema,
                )

    training = spec.training
    if training is not None:
        schema = registry._schema_for(spec, training.status_table, catalog, db)
        for column in [*training.artifact_columns.values()]:
            if column:
                registry._require_column(spec, training.status_table, column, schema)
        registry._validate_page_columns(spec, training.status_table, "training", training.columns, schema)
        if any(column.source == "duration_seconds" for column in training.columns):
            started_col = next(
                (column.source for column in training.columns if column.source.endswith("started_at")),
                None,
            )
            if started_col:
                finished_col = started_col.replace("started_at", "finished_at")
                registry._require_column(spec, training.status_table, finished_col, schema)


def collect_ml_table_ids(spec: DatapipeOpsSpec) -> set[str]:
    tables: set[str] = set()
    data = spec.data
    image_view = data.image_view if data is not None else None
    if image_view is not None:
        tables.add(image_view.image_table)
        if image_view.subset_table:
            tables.add(image_view.subset_table)
        if image_view.ground_truth is not None:
            tables.add(image_view.ground_truth.table)
    frozen_dataset = spec.frozen_dataset
    if frozen_dataset:
        tables.add(frozen_dataset.table)
        if frozen_dataset.record_view is not None and frozen_dataset.record_view.kind == "image":
            record_view = frozen_dataset.record_view
            tables.add(record_view.table)
            if record_view.image_url_table:
                tables.add(record_view.image_url_table)
    model = spec.model
    if model:
        tables.add(model.table)
        if model.is_best_table:
            tables.add(model.is_best_table)
        if model.prediction_view is not None:
            pred_view = model.prediction_view
            tables.add(pred_view.table)
            tables.add(pred_view.image_url_table)
            if pred_view.subset_table:
                tables.add(pred_view.subset_table)
            if pred_view.prediction is not None:
                tables.add(pred_view.prediction.table)
            if pred_view.ground_truth is not None:
                tables.add(pred_view.ground_truth.table)
            if pred_view.metrics_on_image is not None:
                tables.add(pred_view.metrics_on_image.table)
    training = spec.training
    if training:
        tables.add(training.status_table)
    tables.update(metric.table for metric in spec.class_metrics)
    return tables

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence

from datapipe.compute import Catalog
from datapipe.datatable import DataStore

from datapipe_app.ops_spec_resolve import is_db_column
from datapipe_app.specs import (
    DatapipeOpsSpec,
    OpsClassMetricTableSpec,
    OpsColumn,
    OpsColumnGroup,
    OpsFilterRule,
    OpsMetricTableSpec,
)


class OpsSpecValidationError(ValueError):
    pass


@dataclass(frozen=True)
class TableSchema:
    columns: set[str]


class OpsSpecRegistry:
    def __init__(self) -> None:
        self._specs: dict[str, DatapipeOpsSpec] = {}

    def add_many(self, specs: Sequence[DatapipeOpsSpec]) -> None:
        for spec in specs:
            if spec.id in self._specs:
                raise OpsSpecValidationError(f'Spec "{spec.id}" is already registered.')
            self._specs[spec.id] = spec

    def get(self, spec_id: str) -> DatapipeOpsSpec:
        try:
            return self._specs[spec_id]
        except KeyError as exc:
            raise KeyError(f'Spec "{spec_id}" is not registered.') from exc

    def list(self) -> list[DatapipeOpsSpec]:
        return list(self._specs.values())

    def validate(self, catalog: Catalog | None, db: DataStore | None = None, *, strict: bool = True) -> None:
        if not strict or not self._specs:
            return

        seen: set[str] = set()
        for spec in self.list():
            if spec.id in seen:
                raise OpsSpecValidationError(f'Duplicate spec id "{spec.id}".')
            seen.add(spec.id)
            self._validate_spec(spec, catalog, db)

    def _validate_spec(self, spec: DatapipeOpsSpec, catalog: Catalog | None, db: DataStore | None) -> None:
        table_ids = self._table_ids(spec)
        for table in table_ids:
            self._schema_for(spec, table, catalog, db)

        metric_ids: set[str] = set()
        for table_spec in [*spec.metrics, *spec.class_metrics]:
            if table_spec.id in metric_ids:
                raise OpsSpecValidationError(f'Spec "{spec.id}" has duplicate metric table id "{table_spec.id}".')
            metric_ids.add(table_spec.id)
            self._validate_metric_table(spec, table_spec, catalog, db)

        if spec.frozen_dataset is not None:
            schema = self._schema_for(spec, spec.frozen_dataset.table, catalog, db)
            frozen = spec.frozen_dataset
            for column in [
                frozen.id_column,
                frozen.created_at_column,
                frozen.display_name_column,
                *frozen.split_columns.values(),
            ]:
                if column:
                    self._require_column(spec, frozen.table, column, schema)
            self._validate_page_columns(spec, frozen.table, "frozen_dataset", frozen.columns, schema)

        if spec.model is not None:
            schema = self._schema_for(spec, spec.model.table, catalog, db)
            for column in [
                spec.model.id_column,
                spec.model.display_name_column,
                spec.model.created_at_column,
                spec.model.artifact_uri_column,
            ]:
                if column:
                    self._require_column(spec, spec.model.table, column, schema)
            if spec.model.is_best_table:
                best_schema = self._schema_for(spec, spec.model.is_best_table, catalog, db)
                if spec.model.is_best_column:
                    self._require_column(spec, spec.model.is_best_table, spec.model.is_best_column, best_schema)

        if spec.training is not None:
            schema = self._schema_for(spec, spec.training.status_table, catalog, db)
            training = spec.training
            for column in [*training.artifact_columns.values()]:
                if column:
                    self._require_column(spec, training.status_table, column, schema)
            self._validate_page_columns(spec, training.status_table, "training", training.columns, schema)
            if any(column.source == "duration_seconds" for column in training.columns):
                started_col = next((column.source for column in training.columns if column.source.endswith("started_at")), None)
                if started_col:
                    finished_col = started_col.replace("started_at", "finished_at")
                    self._require_column(spec, training.status_table, finished_col, schema)

        for relation in spec.relations:
            schema = self._schema_for(spec, relation.table, catalog, db)
            self._require_column(spec, relation.table, relation.from_column, schema)
            self._require_column(spec, relation.table, relation.to_column, schema)

    def _validate_page_columns(
        self,
        spec: DatapipeOpsSpec,
        table: str,
        section: str,
        columns: Sequence[OpsColumn],
        schema: TableSchema,
    ) -> None:
        if not columns:
            raise OpsSpecValidationError(f'Spec "{spec.id}": {section} must define at least one column.')
        seen: set[str] = set()
        for column in columns:
            if column.id in seen:
                raise OpsSpecValidationError(f'Spec "{spec.id}": {section} has duplicate column id "{column.id}".')
            seen.add(column.id)
            if is_db_column(column):
                self._require_column(spec, table, column.source, schema)

    def _validate_metric_table(
        self,
        spec: DatapipeOpsSpec,
        table_spec: OpsMetricTableSpec | OpsClassMetricTableSpec,
        catalog: Catalog | None,
        db: DataStore | None,
    ) -> None:
        schema = self._schema_for(spec, table_spec.table, catalog, db)
        for column in table_spec.primary_key_columns:
            self._require_column(spec, table_spec.table, column, schema)
        for column in table_spec.entity_links.values():
            self._require_column(spec, table_spec.table, column, schema)

        self._validate_columns_unique(spec, table_spec, "primary_columns", table_spec.primary_columns)
        self._validate_columns_unique(spec, table_spec, "filters", table_spec.filters)
        metric_columns = list(_flatten_columns(table_spec.metric_columns))
        self._validate_columns_unique(spec, table_spec, "metric_columns", metric_columns)
        for column in [*table_spec.primary_columns, *metric_columns, *table_spec.filters]:
            self._require_column(spec, table_spec.table, column.source, schema)
        if table_spec.best_metric_column:
            self._require_column(spec, table_spec.table, table_spec.best_metric_column, schema)
        allowed_sort_ids = {c.id for c in [*table_spec.primary_columns, *metric_columns, *table_spec.filters]}
        allowed_source_ids = {c.source for c in [*table_spec.primary_columns, *metric_columns, *table_spec.filters]}
        for sort_column, _direction in table_spec.default_sort:
            if sort_column not in allowed_sort_ids and sort_column not in allowed_source_ids:
                raise OpsSpecValidationError(
                    f'Spec "{spec.id}": metric table "{table_spec.id}" default sort references unknown column "{sort_column}".'
                )
        self._validate_default_filters(spec, table_spec, metric_columns)

    def _validate_default_filters(
        self,
        spec: DatapipeOpsSpec,
        table_spec: OpsMetricTableSpec | OpsClassMetricTableSpec,
        metric_columns: Sequence[OpsColumn],
    ) -> None:
        if not table_spec.default_filters:
            return
        allowed_ids: set[str] = set()
        allowed_sources: set[str] = set()
        for column in [*table_spec.primary_columns, *metric_columns, *table_spec.filters]:
            if column.filterable or column in table_spec.filters:
                allowed_ids.add(column.id)
                allowed_sources.add(column.source)
        valid_operators = {"contains", "not_contains", "regex", "equal", "not_equal", "is_empty"}
        for rule in table_spec.default_filters:
            if not isinstance(rule, OpsFilterRule):
                raise OpsSpecValidationError(
                    f'Spec "{spec.id}": metric table "{table_spec.id}" default_filters must contain OpsFilterRule values.'
                )
            if rule.operator not in valid_operators:
                raise OpsSpecValidationError(
                    f'Spec "{spec.id}": metric table "{table_spec.id}" default filter uses unknown operator "{rule.operator}".'
                )
            if rule.column_id not in allowed_ids and rule.column_id not in allowed_sources:
                raise OpsSpecValidationError(
                    f'Spec "{spec.id}": metric table "{table_spec.id}" default filter references unknown column "{rule.column_id}".'
                )
            if rule.operator != "is_empty" and (rule.value is None or not str(rule.value).strip()):
                raise OpsSpecValidationError(
                    f'Spec "{spec.id}": metric table "{table_spec.id}" default filter operator "{rule.operator}" requires a value.'
                )

    def _validate_columns_unique(
        self,
        spec: DatapipeOpsSpec,
        table_spec: OpsMetricTableSpec | OpsClassMetricTableSpec,
        section: str,
        columns: Sequence[OpsColumn],
    ) -> None:
        seen: set[str] = set()
        for column in columns:
            if column.id in seen:
                raise OpsSpecValidationError(
                    f'Spec "{spec.id}": metric table "{table_spec.id}" has duplicate {section} id "{column.id}".'
                )
            seen.add(column.id)

    def _table_ids(self, spec: DatapipeOpsSpec) -> set[str]:
        tables = set(spec.data.tables if spec.data else [])
        if spec.data and spec.data.image_view is not None:
            view = spec.data.image_view
            tables.add(view.image_table)
            if view.subset_table:
                tables.add(view.subset_table)
            if view.ground_truth is not None:
                tables.add(view.ground_truth.table)
        if spec.frozen_dataset:
            tables.add(spec.frozen_dataset.table)
            if spec.frozen_dataset.record_view is not None and spec.frozen_dataset.record_view.kind == "image":
                record_view = spec.frozen_dataset.record_view
                tables.add(record_view.table)
                if record_view.image_url_table:
                    tables.add(record_view.image_url_table)
        if spec.model:
            tables.add(spec.model.table)
            if spec.model.is_best_table:
                tables.add(spec.model.is_best_table)
            if spec.model.prediction_view is not None:
                pred_view = spec.model.prediction_view
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
        if spec.training:
            tables.add(spec.training.status_table)
        tables.update(relation.table for relation in spec.relations)
        tables.update(metric.table for metric in spec.metrics)
        tables.update(metric.table for metric in spec.class_metrics)
        return tables

    def _schema_for(
        self,
        spec: DatapipeOpsSpec,
        table: str,
        catalog: Catalog | None,
        db: DataStore | None,
    ) -> TableSchema:
        if catalog is not None and table in catalog.catalog:
            store = catalog.catalog[table].store
            if getattr(store.caps, "supports_get_schema", False):
                return TableSchema(columns={col.name for col in store.get_schema()})
            if db is not None and table in db.tables:
                return TableSchema(columns={col.name for col in db.get_table(table).primary_schema})
            raise OpsSpecValidationError(f'Spec "{spec.id}": table "{table}" does not expose a schema.')
        if db is not None and table in db.tables:
            dt = db.get_table(table)
            columns = {col.name for col in dt.primary_schema}
            if getattr(dt.table_store.caps, "supports_get_schema", False):
                columns.update(col.name for col in dt.table_store.get_schema())
            return TableSchema(columns=columns)
        raise OpsSpecValidationError(f'Spec "{spec.id}" references missing table "{table}".')

    def _require_column(self, spec: DatapipeOpsSpec, table: str, column: str, schema: TableSchema) -> None:
        if column not in schema.columns:
            raise OpsSpecValidationError(
                f'Spec "{spec.id}": table "{table}" references missing column "{column}".'
            )


def _flatten_columns(columns: Iterable[OpsColumn | OpsColumnGroup]) -> Iterable[OpsColumn]:
    for column in columns:
        if isinstance(column, OpsColumnGroup):
            yield from column.columns
        else:
            yield column

from __future__ import annotations

from typing import Any, Literal, Sequence

from datapipe.compute import Catalog
from datapipe.datatable import DataStore

from datapipe_app_ml_ops.observability.schemas.models import OpsImageRecordsCountResponse
from datapipe_app.ops.ops_filters import OpsFilterMode, OpsFilterRule
from datapipe_app_ml_ops.ops.ops_image_records import OpsImageRecordsSupport
from datapipe_app.ops.ops_query import OpsQuery, format_snapshot_label, metric_table_columns
from datapipe_app.ops.ops_spec_resolve import (
    column_by_kind,
    column_source_by_link,
    entity_links_from_columns,
    query_columns,
)
from datapipe_app_ml_ops.ops.spec_registry import OpsSpecRegistry
from datapipe_app_ml_ops.ops.ops_specs import DatapipeOpsSpec
from datapipe_app.ops.specs import OpsColumn, OpsRelationSpec, OpsMetricTableSpec, ops_spec_to_dict


class OpsSpecsService:
    def __init__(self, registry: OpsSpecRegistry, *, ds: DataStore | None, catalog: Catalog | None):
        self.registry = registry
        self.query = OpsQuery(ds, catalog)
        self._image_records = OpsImageRecordsSupport(registry, self.query)

    def list_specs(self, pipeline_id: str) -> dict[str, Any]:
        return {"pipeline_id": pipeline_id, "specs": [self._spec_summary(spec) for spec in self.registry.list()]}

    def get_spec(self, spec_id: str, *, debug: bool = False) -> dict[str, Any]:
        spec = self.registry.get(spec_id)
        payload = self._spec_summary(spec)
        if debug:
            payload["debug_spec"] = ops_spec_to_dict(spec)
        else:
            payload.update(
                {
                    "metrics": [self._table_schema(t) for t in spec.metrics],
                    "class_metrics": [self._table_schema(t) for t in spec.class_metrics],
                    "data": ops_spec_to_dict(spec.data) if spec.data else None,
                    "frozen_dataset": ops_spec_to_dict(spec.frozen_dataset) if spec.frozen_dataset else None,
                    "model": ops_spec_to_dict(spec.model) if spec.model else None,
                    "training": ops_spec_to_dict(spec.training) if spec.training else None,
                    "relations": ops_spec_to_dict(spec.relations),
                }
            )
        return payload

    def frozen_overview(self) -> dict[str, Any]:
        specs = [spec for spec in self.registry.list() if spec.frozen_dataset is not None]
        cards = []
        total_snapshots = 0
        newest = None
        total_models = 0
        recent_activity = []
        for spec in specs:
            frozen = spec.frozen_dataset
            assert frozen is not None
            count = self._count_or_zero(frozen.table)
            total_snapshots += count
            latest_at = self._latest_or_none(frozen.table, frozen.created_at_column)
            newest = max([v for v in [newest, latest_at] if v is not None], default=None)
            rows_payload = self.frozen_rows(spec.id, sort_by=frozen.default_sort[0][0] if frozen.default_sort else None, limit=3)
            recent_rows = rows_payload["rows"]
            models_trained = sum(int(row.get("models_count") or 0) for row in recent_rows)
            total_models += models_trained
            id_source = frozen.id_column
            created_source = frozen.created_at_column
            cards.append(self._overview_card(spec) | {"status": "empty" if count == 0 else "healthy", "snapshots_count": count, "latest_snapshot": recent_rows[0] if recent_rows else None, "split": recent_rows[0].get("split") if recent_rows else None, "models_trained": models_trained, "recent_snapshots": recent_rows})
            for row in recent_rows[:2]:
                label = format_snapshot_label(row.get(created_source) or row.get(id_source), mode=frozen.label_mode)
                recent_activity.append({"spec_id": spec.id, "title": spec.title, "message": f"Snapshot {label or row.get(id_source)} frozen", "status": "success", "at": row.get(created_source)})
        return {"summary": {"total_snapshots": total_snapshots, "active_specs": len(specs), "newest_snapshot_at": str(newest) if newest is not None else None, "models_trained_from_snapshots": total_models}, "filters": {}, "specs": cards, "recent_activity": recent_activity[:8], "lifecycle": ["Data", "Frozen Dataset", "Training", "Metrics"]}

    def training_overview(self) -> dict[str, Any]:
        specs = [spec for spec in self.registry.list() if spec.training is not None]
        cards = []
        running_now = queued = completed = failed = 0
        recent_activity = []
        for spec in specs:
            training = spec.training
            assert training is not None
            rows_payload = self.training_rows(spec.id, limit=200)
            rows = rows_payload["rows"]
            status_column = column_by_kind(training.columns, "status")
            status_source = status_column.source if status_column else None
            statuses = [str(row.get(status_source) or "").lower() for row in rows] if status_source else []
            spec_running = sum(1 for status in statuses if "run" in status)
            spec_queued = sum(1 for status in statuses if "queue" in status or "pending" in status)
            spec_completed = sum(1 for status in statuses if "complete" in status or "success" in status or "done" in status)
            spec_failed = sum(1 for status in statuses if "fail" in status or "error" in status)
            running_now += spec_running; queued += spec_queued; completed += spec_completed; failed += spec_failed
            model_source = column_source_by_link(training.columns, "model")
            frozen_source = column_source_by_link(training.columns, "frozen_dataset")
            cards.append(self._overview_card(spec) | {"status": "attention" if spec_failed else ("running" if spec_running else "healthy"), "running_count": spec_running, "completed_count": spec_completed, "failed_count": spec_failed, "latest_model": rows[0].get(model_source) if rows and model_source else None, "last_frozen_dataset": rows[0].get(frozen_source) if rows and frozen_source else None, "recent_runs": rows[:3]})
            run_source = column_source_by_link(training.columns, "training_run")
            started_column = next((column for column in training.columns if column.kind == "datetime"), None)
            for row in rows[:2]:
                recent_activity.append({
                    "spec_id": spec.id,
                    "title": spec.title,
                    "message": f"Training run {row.get(run_source) if run_source else row.get(model_source or '')}",
                    "status": row.get(status_source) if status_source else None,
                    "at": row.get(started_column.source) if started_column else None,
                })
        return {"summary": {"running_now": running_now, "queued": queued, "completed_today": completed, "needs_attention": failed}, "filters": {}, "specs": cards, "run_board": {"running": [], "recently_completed": [], "needs_attention": []}, "recent_activity": recent_activity[:8]}

    def metrics_overview(self) -> dict[str, Any]:
        specs = [spec for spec in self.registry.list() if spec.metrics]
        cards = []
        for spec in specs:
            metric_sources = [table.metric_source for table in spec.metrics]
            cards.append(self._overview_card(spec) | {"metric_tables_count": len(spec.metrics), "metric_sources": metric_sources, "dimensions_supported": sorted({key for table in spec.metrics for key in table.entity_links})})
        return {"summary": {"metric_tables": sum(len(spec.metrics) for spec in specs), "metric_sources": len({table.metric_source for spec in specs for table in spec.metrics}), "models_covered": self._entity_count("model"), "frozen_datasets_linked": self._entity_count("frozen_dataset")}, "filters": {}, "specs": cards, "architecture": ["Frozen Dataset", "Model", "Metric Table", "Dashboard Views"], "available_views": ["Model Metrics", "Tag Metrics", "Class Metrics", "Comparison Views", "Snapshot Views"]}

    def class_metrics_overview(self) -> dict[str, Any]:
        specs = [spec for spec in self.registry.list() if spec.class_metrics]
        cards = []
        total_classes = 0
        for spec in specs:
            class_keys: set[Any] = set()
            for table in spec.class_metrics:
                class_column = table.entity_links.get("class") or table.entity_links.get("tag")
                if class_column:
                    class_keys.update(self.query.value_counts(table.table, class_column))
            total_classes += len(class_keys)
            cards.append(self._overview_card(spec) | {"class_metric_tables_count": len(spec.class_metrics), "classes_tracked": len(class_keys), "coverage": None, "supported_filters": sorted({key for table in spec.class_metrics for key in table.entity_links})})
        return {"summary": {"class_metric_tables": sum(len(spec.class_metrics) for spec in specs), "total_tracked_classes": total_classes, "specifications_covered": len(specs), "filters_available": len({key for spec in specs for table in spec.class_metrics for key in table.entity_links})}, "filters": {}, "specs": cards, "coverage_map": [], "explore_tiles": ["Browse by class", "Browse by tag", "Compare subsets", "Compare models"]}

    def frozen_rows(self, spec_id: str, **params: Any) -> dict[str, Any]:
        spec = self.registry.get(spec_id)
        frozen = spec.frozen_dataset
        if frozen is None:
            return {"rows": [], "total": 0}
        columns = list(frozen.columns)
        if params.get("sort_by") is None and frozen.default_sort:
            params["sort_by"], params["sort_dir"] = frozen.default_sort[0]
        rows, total = self.query.rows(frozen.table, allowed_columns=query_columns(columns), **params)
        relation_counts = self._frozen_model_counts(spec)
        needs_split = any(column.source == "split" for column in columns)
        needs_models = any(column.source == "models_count" for column in columns)
        for row in rows:
            dataset_id = row.get(frozen.id_column)
            if needs_split:
                split_counts = {split: row.get(column) for split, column in frozen.split_columns.items()}
                row["split"] = self._split_label(split_counts)
            if needs_models:
                row["models_count"] = relation_counts.get(dataset_id, 0)
        entity_links = entity_links_from_columns(columns)
        return {
            "rows": rows,
            "total": total,
            "columns": ops_spec_to_dict(columns),
            "filter_columns": self._filter_columns_payload(columns),
            "entity_links": entity_links,
        }

    def frozen_filter_columns(self, spec_id: str) -> list[OpsColumn]:
        spec = self.registry.get(spec_id)
        frozen = spec.frozen_dataset
        if frozen is None:
            return []
        return list(frozen.columns)

    def training_rows(self, spec_id: str, **params: Any) -> dict[str, Any]:
        spec = self.registry.get(spec_id)
        training = spec.training
        if training is None:
            return {"rows": [], "total": 0}
        columns = list(training.columns)
        if params.get("sort_by") is None and training.default_sort:
            params["sort_by"], params["sort_dir"] = training.default_sort[0]
        needs_duration = any(column.source == "duration_seconds" for column in columns)
        started_col = next((column.source for column in columns if column.source.endswith("started_at")), None)
        finished_col = started_col.replace("started_at", "finished_at") if started_col else None
        allowed_columns = query_columns(columns)
        if needs_duration and finished_col and finished_col not in {column.source for column in allowed_columns}:
            allowed_columns = [*allowed_columns, OpsColumn(finished_col, finished_col, finished_col)]
        rows, total = self.query.rows(training.status_table, allowed_columns=allowed_columns, **params)
        for row in rows:
            if needs_duration and started_col and finished_col:
                row["duration_seconds"] = self._duration_seconds(row.get(started_col), row.get(finished_col))
            row["artifact_uris"] = {key: row.get(column) for key, column in training.artifact_columns.items()}
        entity_links = entity_links_from_columns(columns)
        return {
            "rows": rows,
            "total": total,
            "columns": ops_spec_to_dict(columns),
            "filter_columns": self._filter_columns_payload(columns),
            "entity_links": entity_links,
        }

    def metric_table_rows(
        self,
        spec_id: str,
        table_id: str,
        *,
        class_metrics: bool = False,
        filters: dict[str, str | Sequence[str]] | None = None,
        filter_rules: Sequence[OpsFilterRule] | None = None,
        filter_mode: OpsFilterMode = "or",
        **params: Any,
    ) -> dict[str, Any]:
        spec = self.registry.get(spec_id)
        table_specs = spec.class_metrics if class_metrics else spec.metrics
        table_spec = next((table for table in table_specs if table.id == table_id), None)
        if table_spec is None:
            raise KeyError(f'Metric table "{table_id}" is not configured for spec "{spec_id}".')
        if params.get("sort_by") is None and table_spec.default_sort:
            params["sort_by"], params["sort_dir"] = table_spec.default_sort[0]
        rows, total = self.query.rows(
            table_spec.table,
            allowed_columns=metric_table_columns(table_spec),
            filters=filters,
            filter_rules=filter_rules,
            filter_mode=filter_mode,
            **params,
        )
        for row in rows:
            for entity, column in table_spec.entity_links.items():
                row[f"{entity}_id"] = row.get(column)
        return {"rows": rows, "total": total, "table": self._table_schema(table_spec)}

    def image_records(self, *, pipeline_id: str, spec_id: str, limit: int = 10, offset: int = 0, include_total: bool = False, **params: Any):
        return self._image_records.image_records(
            pipeline_id=pipeline_id, spec_id=spec_id, limit=limit, offset=offset, include_total=include_total, **params
        )

    def image_records_count(self, *, pipeline_id: str, spec_id: str, **params: Any) -> OpsImageRecordsCountResponse:
        total = self._image_records.image_records_count(pipeline_id=pipeline_id, spec_id=spec_id, **params)
        return OpsImageRecordsCountResponse(
            pipeline_id=pipeline_id, spec_id=spec_id, scope="data", parent_id=None, total=total
        )

    def image_record_detail(self, *, pipeline_id: str, spec_id: str, record_key: str):
        return self._image_records.image_record_detail(pipeline_id=pipeline_id, spec_id=spec_id, record_key=record_key)

    def frozen_dataset_records(
        self, *, pipeline_id: str, spec_id: str, dataset_id: str, limit: int = 10, offset: int = 0, include_total: bool = False, **params: Any
    ):
        return self._image_records.frozen_dataset_records(
            pipeline_id=pipeline_id,
            spec_id=spec_id,
            dataset_id=dataset_id,
            limit=limit,
            offset=offset,
            include_total=include_total,
            **params,
        )

    def frozen_dataset_records_count(self, *, pipeline_id: str, spec_id: str, dataset_id: str, **params: Any) -> OpsImageRecordsCountResponse:
        total = self._image_records.frozen_dataset_records_count(spec_id=spec_id, dataset_id=dataset_id, **params)
        return OpsImageRecordsCountResponse(
            pipeline_id=pipeline_id,
            spec_id=spec_id,
            scope="frozen_dataset",
            parent_id=dataset_id,
            total=total,
        )

    def frozen_dataset_record_detail(self, *, pipeline_id: str, spec_id: str, dataset_id: str, record_key: str):
        return self._image_records.frozen_dataset_record_detail(
            pipeline_id=pipeline_id, spec_id=spec_id, dataset_id=dataset_id, record_key=record_key
        )

    def model_prediction_records(
        self, *, pipeline_id: str, spec_id: str, model_id: str, limit: int = 10, offset: int = 0, include_total: bool = False, **params: Any
    ):
        return self._image_records.model_prediction_records(
            pipeline_id=pipeline_id,
            spec_id=spec_id,
            model_id=model_id,
            limit=limit,
            offset=offset,
            include_total=include_total,
            **params,
        )

    def model_prediction_records_count(self, *, pipeline_id: str, spec_id: str, model_id: str, **params: Any) -> OpsImageRecordsCountResponse:
        total = self._image_records.model_prediction_records_count(spec_id=spec_id, model_id=model_id, **params)
        return OpsImageRecordsCountResponse(
            pipeline_id=pipeline_id,
            spec_id=spec_id,
            scope="model_prediction",
            parent_id=model_id,
            total=total,
        )

    def model_prediction_record_detail(self, *, pipeline_id: str, spec_id: str, model_id: str, record_key: str):
        return self._image_records.model_prediction_record_detail(
            pipeline_id=pipeline_id, spec_id=spec_id, model_id=model_id, record_key=record_key
        )

    def image_record_bytes(self, **kwargs):
        return self._image_records.image_record_bytes(**kwargs)

    def _spec_summary(self, spec: DatapipeOpsSpec) -> dict[str, Any]:
        image_view = spec.data.image_view if spec.data is not None else None
        model = spec.model
        prediction_view = model.prediction_view if model is not None else None
        return {
            "id": spec.id,
            "title": spec.title,
            "description": spec.description,
            "icon": spec.icon,
            "color": spec.color,
            "has_frozen_datasets": spec.frozen_dataset is not None,
            "has_training": spec.training is not None,
            "has_image": image_view is not None,
            "has_model_predictions": prediction_view is not None,
            "metric_tables_count": len(spec.metrics),
            "class_metric_tables_count": len(spec.class_metrics),
            "tags": list(spec.tags),
        }

    def _split_label(self, split_counts: dict[str, Any]) -> str:
        if not split_counts:
            return "-"
        preferred = ["train", "val", "test"]
        keys = [key for key in preferred if key in split_counts] + [key for key in split_counts if key not in preferred]
        return " / ".join(str(split_counts.get(key) or 0) for key in keys)

    def _duration_seconds(self, started: Any, finished: Any) -> int | None:
        if started is None or finished is None:
            return None
        try:
            from datetime import datetime

            if isinstance(started, str):
                started = datetime.fromisoformat(started.replace("Z", "+00:00"))
            if isinstance(finished, str):
                finished = datetime.fromisoformat(finished.replace("Z", "+00:00"))
            return max(int((finished - started).total_seconds()), 0)
        except (TypeError, ValueError):
            return None

    def _filter_columns_payload(self, columns: Sequence[OpsColumn]) -> list[Any]:
        by_source: dict[str, OpsColumn] = {}
        for col in columns:
            if not col.filterable or not col.source:
                continue
            if col.source not in by_source:
                by_source[col.source] = col
        return ops_spec_to_dict(list(by_source.values()))

    def _table_schema(self, table: OpsMetricTableSpec) -> dict[str, Any]:
        return {
            "id": table.id,
            "title": table.title,
            "table": table.table,
            "metric_source": table.metric_source,
            "primary_columns": ops_spec_to_dict(table.primary_columns),
            "metric_columns": ops_spec_to_dict(table.metric_columns),
            "filters": ops_spec_to_dict(table.filters),
            "default_filters": ops_spec_to_dict(table.default_filters),
            "default_sort": list(table.default_sort),
            "entity_links": dict(table.entity_links),
        }

    def _overview_card(self, spec: DatapipeOpsSpec) -> dict[str, Any]:
        return {"spec_id": spec.id, "id": spec.id, "title": spec.title, "description": spec.description, "icon": spec.icon, "color": spec.color, "status": "healthy"}

    def _count_or_zero(self, table: str) -> int:
        try:
            return self.query.count_rows(table)
        except Exception:
            return 0

    def _latest_or_none(self, table: str, column: str) -> Any:
        try:
            return self.query.latest_value(table, column)
        except Exception:
            return None

    def _entity_count(self, entity: str) -> int:
        values: set[Any] = set()
        for spec in self.registry.list():
            model = spec.model
            if entity == "model" and model is not None:
                values.update(self.query.value_counts(model.table, model.id_column))
            frozen_dataset = spec.frozen_dataset
            if entity == "frozen_dataset" and frozen_dataset is not None:
                values.update(self.query.value_counts(frozen_dataset.table, frozen_dataset.id_column))
        return len(values)

    def _frozen_model_counts(self, spec: DatapipeOpsSpec) -> dict[Any, int]:
        frozen = spec.frozen_dataset
        if frozen is None or not frozen.models_count_relation_id:
            return {}
        relation = self._relation_by_id(spec, frozen.models_count_relation_id)
        if relation is None:
            return {}
        key_column = relation.to_column if relation.to_entity == "frozen_dataset" else relation.from_column
        return self.query.value_counts(relation.table, key_column)

    def _relation_by_id(self, spec: DatapipeOpsSpec, relation_id: str) -> OpsRelationSpec | None:
        return next((relation for relation in spec.relations if relation.id == relation_id), None)

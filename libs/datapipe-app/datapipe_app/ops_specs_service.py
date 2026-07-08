from __future__ import annotations

from typing import Any, Literal, Sequence

from datapipe.compute import Catalog
from datapipe.datatable import DataStore

from datapipe_app.ops_filters import OpsFilterMode, OpsFilterRule
from datapipe_app.ops_query import OpsQuery, format_snapshot_label, metric_table_columns
from datapipe_app.spec_registry import OpsSpecRegistry
from datapipe_app.specs import DatapipeOpsSpec, OpsColumn, OpsRelationSpec, ops_spec_to_dict


class OpsSpecsService:
    def __init__(self, registry: OpsSpecRegistry, *, ds: DataStore | None, catalog: Catalog | None):
        self.registry = registry
        self.query = OpsQuery(ds, catalog)

    def list_specs(self, pipeline_id: str) -> dict[str, Any]:
        return {"pipeline_id": pipeline_id, "specs": [self._spec_summary(spec) for spec in self.registry.list()]}

    def get_spec(self, spec_id: str, *, debug: bool = False) -> dict[str, Any]:
        spec = self.registry.get(spec_id)
        payload = self._spec_summary(spec)
        if debug:
            payload["debug_spec"] = ops_spec_to_dict(spec)
        else:
            payload.update({"metrics": [self._table_schema(t) for t in spec.metrics], "class_metrics": [self._table_schema(t) for t in spec.class_metrics]})
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
            rows_payload = self.frozen_rows(spec.id, sort_by="frozen_at", limit=3)
            recent_rows = rows_payload["rows"]
            models_trained = sum(int(row.get("models_count") or 0) for row in recent_rows)
            total_models += models_trained
            cards.append(self._overview_card(spec) | {"status": "empty" if count == 0 else "healthy", "snapshots_count": count, "latest_snapshot": recent_rows[0] if recent_rows else None, "split": recent_rows[0].get("split") if recent_rows else None, "models_trained": models_trained, "recent_snapshots": recent_rows})
            for row in recent_rows[:2]:
                recent_activity.append({"spec_id": spec.id, "title": spec.title, "message": f"Snapshot {row.get('label') or row.get('dataset_id')} frozen", "status": "success", "at": row.get("frozen_at")})
        return {"summary": {"total_snapshots": total_snapshots, "active_specs": len(specs), "newest_snapshot_at": str(newest) if newest is not None else None, "models_trained_from_snapshots": total_models}, "filters": {}, "specs": cards, "recent_activity": recent_activity[:8], "lifecycle": ["Data", "Frozen Dataset", "Training", "Metrics"]}

    def training_overview(self) -> dict[str, Any]:
        specs = [spec for spec in self.registry.list() if spec.training is not None]
        cards = []
        running_now = queued = completed = failed = 0
        recent_activity = []
        for spec in specs:
            rows_payload = self.training_rows(spec.id, limit=200)
            rows = rows_payload["rows"]
            statuses = [str(row.get("status") or "").lower() for row in rows]
            spec_running = sum(1 for status in statuses if "run" in status)
            spec_queued = sum(1 for status in statuses if "queue" in status or "pending" in status)
            spec_completed = sum(1 for status in statuses if "complete" in status or "success" in status or "done" in status)
            spec_failed = sum(1 for status in statuses if "fail" in status or "error" in status)
            running_now += spec_running; queued += spec_queued; completed += spec_completed; failed += spec_failed
            cards.append(self._overview_card(spec) | {"status": "attention" if spec_failed else ("running" if spec_running else "healthy"), "running_count": spec_running, "completed_count": spec_completed, "failed_count": spec_failed, "latest_model": rows[0].get("model_id") if rows else None, "last_frozen_dataset": rows[0].get("frozen_dataset_id") if rows else None, "recent_runs": rows[:3]})
            for row in rows[:2]:
                recent_activity.append({"spec_id": spec.id, "title": spec.title, "message": f"Training run {row.get('run_id') or row.get('model_id')}", "status": row.get("status"), "at": row.get("started_at")})
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
            class_keys = set()
            for table in spec.class_metrics:
                class_column = table.entity_links.get("class") or table.entity_links.get("tag")
                if class_column:
                    class_keys.update(self.query.value_counts(table.table, class_column))
            total_classes += len(class_keys)
            cards.append(self._overview_card(spec) | {"class_metric_tables_count": len(spec.class_metrics), "classes_tracked": len(class_keys), "coverage": None, "supported_filters": sorted({key for table in spec.class_metrics for key in table.entity_links})})
        return {"summary": {"class_metric_tables": sum(len(spec.class_metrics) for spec in specs), "total_tracked_classes": total_classes, "specifications_covered": len(specs), "filters_available": len({key for spec in specs for table in spec.class_metrics for key in table.entity_links})}, "filters": {}, "specs": cards, "coverage_map": [], "explore_tiles": ["Browse by class", "Browse by tag", "Compare subsets", "Compare models"]}

    def frozen_rows(self, spec_id: str, *, search: str | None = None, sort_by: str | None = None, sort_dir: Literal["asc", "desc"] = "desc", limit: int = 50, offset: int = 0) -> dict[str, Any]:
        spec = self.registry.get(spec_id)
        if spec.frozen_dataset is None:
            return {"rows": [], "total": 0}
        frozen = spec.frozen_dataset
        columns = [OpsColumn("dataset_id", "Dataset", frozen.id_column, "link", filterable=True, link_to="frozen_dataset"), OpsColumn("dataset", "Dataset", frozen.display_name_column or frozen.id_column, "text", filterable=True), OpsColumn("frozen_at", "Frozen at", frozen.created_at_column, "datetime"), *[OpsColumn(f"split_{split}", split.title(), column, "number", filterable=True) for split, column in frozen.split_columns.items()]]
        rows, total = self.query.rows(frozen.table, allowed_columns=columns, search=search, sort_by=sort_by or "frozen_at", sort_dir=sort_dir, limit=limit, offset=offset)
        relation_counts = self._frozen_model_counts(spec)
        for row in rows:
            dataset_id = row.get(frozen.id_column)
            display = row.get(frozen.display_name_column or frozen.id_column)
            raw_label = display or row.get(frozen.created_at_column) or dataset_id
            split_counts = {split: row.get(column) for split, column in frozen.split_columns.items()}
            row["dataset_id"] = dataset_id
            row["dataset_label"] = display or dataset_id
            row["label"] = format_snapshot_label(raw_label, mode=frozen.label_mode)
            row["frozen_at"] = row.get(frozen.created_at_column)
            row["split_counts"] = split_counts
            row["split"] = self._split_label(split_counts)
            row["models_count"] = relation_counts.get(dataset_id, 0)
        return {"rows": rows, "total": total}

    def training_rows(self, spec_id: str, **params: Any) -> dict[str, Any]:
        spec = self.registry.get(spec_id)
        if spec.training is None:
            return {"rows": [], "total": 0}
        training = spec.training
        columns = [OpsColumn("model", "Model", training.model_id_column, "link", filterable=True, link_to="model"), OpsColumn("status", "Status", training.status_column, "status", filterable=True), *([OpsColumn("started", "Started", training.started_at_column, "datetime")] if training.started_at_column else []), *([OpsColumn("finished", "Finished", training.finished_at_column, "datetime")] if training.finished_at_column else []), *([OpsColumn("duration", "Duration", training.duration_seconds_column, "duration")] if training.duration_seconds_column else []), *training.extra_columns]
        if params.get("sort_by") is None and training.started_at_column:
            params["sort_by"] = "started"
        rows, total = self.query.rows(training.status_table, allowed_columns=columns, **params)
        for row in rows:
            run_id = self._first_present(row, ["training_status__run_key", "run_key", training.model_id_column])
            frozen_dataset_id = self._first_present(row, ["detection_frozen_dataset_id", "frozen_dataset_id"])
            row["run_id"] = run_id; row["model_id"] = row.get(training.model_id_column); row["status"] = row.get(training.status_column)
            if training.started_at_column: row["started_at"] = row.get(training.started_at_column)
            if training.finished_at_column: row["finished_at"] = row.get(training.finished_at_column)
            if training.duration_seconds_column: row["duration_seconds"] = row.get(training.duration_seconds_column)
            row["frozen_dataset_id"] = frozen_dataset_id
            row["artifact_uris"] = {key: row.get(column) for key, column in training.artifact_columns.items()}
        return {"rows": rows, "total": total}

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

    def _spec_summary(self, spec: DatapipeOpsSpec) -> dict[str, Any]:
        return {"id": spec.id, "title": spec.title, "description": spec.description, "icon": spec.icon, "color": spec.color, "has_frozen_datasets": spec.frozen_dataset is not None, "has_training": spec.training is not None, "metric_tables_count": len(spec.metrics), "class_metric_tables_count": len(spec.class_metrics), "tags": list(spec.tags)}

    def _table_schema(self, table: Any) -> dict[str, Any]:
        return {"id": table.id, "title": table.title, "table": table.table, "metric_source": table.metric_source, "primary_columns": ops_spec_to_dict(table.primary_columns), "metric_columns": ops_spec_to_dict(table.metric_columns), "filters": ops_spec_to_dict(table.filters), "default_sort": list(table.default_sort), "entity_links": dict(getattr(table, "entity_links", {}) or {})}

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
        values = set()
        for spec in self.registry.list():
            if entity == "model" and spec.model is not None:
                values.update(self.query.value_counts(spec.model.table, spec.model.id_column))
            if entity == "frozen_dataset" and spec.frozen_dataset is not None:
                values.update(self.query.value_counts(spec.frozen_dataset.table, spec.frozen_dataset.id_column))
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

    def _split_label(self, split_counts: dict[str, Any]) -> str:
        if not split_counts:
            return "-"
        preferred = ["train", "val", "test"]
        keys = [key for key in preferred if key in split_counts] + [key for key in split_counts if key not in preferred]
        return " / ".join(str(split_counts.get(key) or 0) for key in keys)

    def _first_present(self, row: dict[str, Any], keys: list[str]) -> Any:
        for key in keys:
            value = row.get(key)
            if value is not None and value != "":
                return value
        return None
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import Column, DateTime, Float, Integer, MetaData, String, Table, Text, delete, func, insert, select

from datapipe_app.observability.metrics_service import MetricsService
from datapipe_app.observability.tables import ObservabilityTableConfig

_analytics_metadata = MetaData()

analytics_metrics_on_subset = Table(
    "analytics_metrics_on_subset",
    _analytics_metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("pipeline_id", String(255), index=True),
    Column("run_id", String(64)),
    Column("model_id", String(512)),
    Column("subset", String(255)),
    Column("metric_timestamp", DateTime(timezone=True)),
    Column("mAP50", Float),
    Column("mAP50_95", Float),
    Column("precision", Float),
    Column("recall", Float),
    Column("f1_score", Float),
    Column("iou_mean", Float),
    Column("images_support", Float),
    Column("support", Float),
)

analytics_metrics_by_class = Table(
    "analytics_metrics_by_class",
    _analytics_metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("pipeline_id", String(255), index=True),
    Column("label", String(512)),
    Column("subset", String(255)),
    Column("support", Float),
    Column("precision", Float),
    Column("recall", Float),
    Column("f1_score", Float),
    Column("TP", Float),
    Column("FP", Float),
    Column("FN", Float),
)

analytics_training_runs = Table(
    "analytics_training_runs",
    _analytics_metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("pipeline_id", String(255), index=True),
    Column("run_key", String(512)),
    Column("model_id", String(512)),
    Column("status", String(64)),
    Column("started_at", DateTime(timezone=True)),
    Column("duration_s", Integer),
    Column("best_metric_name", String(128)),
    Column("best_metric_value", Float),
    Column("metadata_json", Text),
)


def apply_analytics_table_config(
    *,
    tables: ObservabilityTableConfig,
    schema: str | None,
) -> None:
    analytics_metrics_on_subset.name = tables.analytics_metrics_on_subset
    analytics_metrics_by_class.name = tables.analytics_metrics_by_class
    analytics_training_runs.name = tables.analytics_training_runs
    analytics_metrics_on_subset.schema = schema
    analytics_metrics_by_class.schema = schema
    analytics_training_runs.schema = schema


def analytics_metadata() -> MetaData:
    return _analytics_metadata


def ensure_analytics_tables(
    engine: Any,
    *,
    tables: Optional["ObservabilityTableConfig"] = None,
    schema: Optional[str] = None,
) -> None:
    from datapipe_app.observability.tables import ObservabilityTableConfig

    tables = tables or ObservabilityTableConfig()
    apply_analytics_table_config(tables=tables, schema=schema)
    _analytics_metadata.create_all(engine)


def refresh_analytics_views(
    engine: Any,
    *,
    pipeline_id: str,
    store: Any = None,
    ds: Any = None,
    catalog: Any = None,
    training_runs: Optional[list[dict[str, Any]]] = None,
) -> None:
    ensure_analytics_tables(engine)
    svc = MetricsService(store=store, ds=ds, catalog=catalog)
    runs_resp = svc.list_runs(pipeline_id, limit=500, offset=0)
    classes_resp = svc.list_classes(pipeline_id, limit=5000, offset=0)

    with engine.begin() as conn:
        conn.execute(delete(analytics_metrics_on_subset).where(analytics_metrics_on_subset.c.pipeline_id == pipeline_id))
        conn.execute(delete(analytics_metrics_by_class).where(analytics_metrics_by_class.c.pipeline_id == pipeline_id))
        conn.execute(delete(analytics_training_runs).where(analytics_training_runs.c.pipeline_id == pipeline_id))

        for row in runs_resp.rows:
            ts = None
            if row.started_at:
                try:
                    ts = datetime.fromisoformat(row.started_at.replace("Z", "+00:00"))
                except ValueError:
                    ts = datetime.now(timezone.utc)
            conn.execute(
                insert(analytics_metrics_on_subset).values(
                    pipeline_id=pipeline_id,
                    run_id=row.run_id,
                    model_id=row.model_id,
                    subset=row.subset,
                    metric_timestamp=ts,
                    mAP50=row.metrics.get("mAP50"),
                    mAP50_95=row.metrics.get("mAP50_95"),
                    precision=row.metrics.get("precision"),
                    recall=row.metrics.get("recall"),
                    f1_score=row.metrics.get("f1_score"),
                    iou_mean=row.metrics.get("iou_mean"),
                    images_support=row.metrics.get("images_support"),
                    support=row.metrics.get("support"),
                )
            )

        for cls in classes_resp.rows:
            conn.execute(
                insert(analytics_metrics_by_class).values(
                    pipeline_id=pipeline_id,
                    label=cls.label,
                    subset="",
                    support=cls.support,
                    precision=cls.precision,
                    recall=cls.recall,
                    f1_score=cls.f1_score,
                    TP=cls.TP,
                    FP=cls.FP,
                    FN=cls.FN,
                )
            )

        for tr in training_runs or []:
            started = tr.get("started_at")
            ts = None
            if started:
                try:
                    ts = datetime.fromisoformat(str(started).replace("Z", "+00:00"))
                except ValueError:
                    pass
            eval_m = tr.get("eval_metric") or {}
            conn.execute(
                insert(analytics_training_runs).values(
                    pipeline_id=pipeline_id,
                    run_key=tr.get("run_key"),
                    model_id=tr.get("model_id"),
                    status=tr.get("status"),
                    started_at=ts,
                    duration_s=tr.get("duration_s"),
                    best_metric_name=eval_m.get("metric_name") or tr.get("best_metric_name"),
                    best_metric_value=eval_m.get("metric_value") or tr.get("best_metric_value"),
                    metadata_json=json.dumps({k: tr[k] for k in ("task_type", "framework", "tags") if k in tr}, default=str),
                )
            )


def get_schema(engine: Any) -> dict[str, Any]:
    ensure_analytics_tables(engine)
    tables = []
    mapping = {
        "metrics_on_subset": analytics_metrics_on_subset,
        "metrics_by_class": analytics_metrics_by_class,
        "training_runs": analytics_training_runs,
    }
    with engine.connect() as conn:
        for name, tbl in mapping.items():
            count = conn.execute(select(func.count()).select_from(tbl)).scalar()
            columns = [{"name": c.name, "type": str(c.type)} for c in tbl.columns if c.name != "id"]
            tables.append({"name": name, "columns": columns, "row_count": count})
    return {"datasource": "datapipe_analytics", "tables": tables}

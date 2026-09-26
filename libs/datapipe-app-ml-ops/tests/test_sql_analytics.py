from datapipe_app.ops.specs import OpsColumn, OpsMetricTableSpec
from datapipe_app_ml_ops.observability.analytics.sql_analytics import build_sql_analytics_context
from datapipe_app_ml_ops.ops.ops_specs import DatapipeOpsSpec, OpsDataSpec
from datapipe_app_ml_ops.ops.spec_registry import OpsSpecRegistry


def test_build_sql_analytics_context_maps_spec_tables() -> None:
    registry = OpsSpecRegistry()
    registry.add_many(
        [
            DatapipeOpsSpec(
                id="demo",
                title="Demo",
                data=OpsDataSpec(tables=["pipeline_model__metrics_on_subset"]),
                metrics=[
                    OpsMetricTableSpec(
                        id="model_metrics",
                        title="Model metrics",
                        table="pipeline_model__metrics_on_subset",
                        metric_source="pipeline_model__metrics_on_subset",
                        primary_key_columns=["detection_model_id", "subset_id"],
                        entity_links={"model": "detection_model_id", "subset": "subset_id"},
                        primary_columns=[
                            OpsColumn("detection_model_id", "model", "detection_model_id"),
                            OpsColumn("subset_id", "subset", "subset_id"),
                        ],
                        metric_columns=[OpsColumn("f1", "F1", "calc__weighted_f1_score")],
                    )
                ],
            )
        ]
    )

    ctx = build_sql_analytics_context(registry)

    assert ctx.schema_tables[0]["name"] == "metrics_on_subset"
    assert ctx.schema_tables[0]["physical_table"] == "pipeline_model__metrics_on_subset"

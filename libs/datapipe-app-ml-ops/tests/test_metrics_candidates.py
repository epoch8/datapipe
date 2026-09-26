from __future__ import annotations

from datapipe_app.observability.store.db import ObservabilityStore
from datapipe_app_ml_ops.observability.metrics.metrics_service import MetricsService, _normalize_metric_nulls
from datapipe_app_ml_ops.observability.schemas.models import MetricsCandidateCreate


def test_metrics_candidates_persist_in_store(tmp_path) -> None:
    store = ObservabilityStore.from_url(f"sqlite:///{tmp_path / 'obs.db'}")
    svc = MetricsService(store=store)

    created = svc.add_candidate(
        "pipeline_a",
        MetricsCandidateCreate(
            model_id="yolov8n",
            dataset_id="ds-1",
            subset="val",
        ),
    )
    assert created.id
    assert created.metrics_state == "not_computed"

    listed = svc.list_candidates("pipeline_a")
    assert listed.total == 1
    assert listed.rows[0].model_id == "yolov8n"

    store2 = ObservabilityStore.from_url(f"sqlite:///{tmp_path / 'obs.db'}")
    svc2 = MetricsService(store=store2)
    reloaded = svc2.list_candidates("pipeline_a")
    assert reloaded.total == 1
    assert reloaded.rows[0].id == created.id

    assert svc.delete_candidate("pipeline_a", created.id) is True
    assert svc.list_candidates("pipeline_a").total == 0


def test_normalize_metric_nulls_fills_zero_recall_gaps() -> None:
    metrics = _normalize_metric_nulls(
        {
            "weighted_recall": 0.0,
            "weighted_precision": None,
            "weighted_f1_score": None,
            "macro_recall": 0.0,
            "macro_precision": None,
            "macro_f1_score": None,
            "accuracy": 0.0,
        }
    )
    assert metrics["weighted_precision"] == 0.0
    assert metrics["weighted_f1_score"] == 0.0
    assert metrics["macro_precision"] == 0.0
    assert metrics["macro_f1_score"] == 0.0


def test_parse_model_ids_or_filter() -> None:
    from datapipe_app_ml_ops.observability.metrics.metrics_service import _parse_model_ids

    assert _parse_model_ids("a,b, c") == ["a", "b", "c"]
    assert _parse_model_ids(None) is None
    assert _parse_model_ids("") is None

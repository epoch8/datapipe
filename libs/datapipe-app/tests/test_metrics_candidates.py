from __future__ import annotations

from datapipe_app.observability.db import ObservabilityStore
from datapipe_app.observability.metrics_service import MetricsService
from datapipe_app.observability.schemas import MetricsCandidateCreate


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

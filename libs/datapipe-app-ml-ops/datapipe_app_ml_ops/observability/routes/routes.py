from __future__ import annotations

from typing import Any, Callable, Literal, Optional

from datapipe.compute import Catalog
from datapipe.datatable import DataStore
from datapipe.types import Labels
from fastapi import BackgroundTasks, FastAPI, HTTPException
from pydantic import BaseModel

from datapipe_app.observability.store.db import ObservabilityStore
from datapipe_app.observability.plugins.registry import ObservabilityRegistry
from datapipe_app.observability.plugins.schemas import (
    SqlSchemaResponse,
    StartRunRequest,
    StartRunResponse,
)
from datapipe_app_ml_ops.observability.analytics.sql_analytics import (
    build_sql_analytics_context,
    get_sql_schema_response,
)
from datapipe_app.observability.config.settings import get_ops_settings
from datapipe_app_ml_ops.observability.metrics.metrics_service import MetricsService
from datapipe_app_ml_ops.observability.routes.ops_spec_routes import register_ops_spec_routes
from datapipe_app_ml_ops.observability.schemas.models import (
    ClassMetricDetailResponse,
    ClassMetricsResponse,
    FrozenDatasetDetailResponse,
    FrozenDatasetsResponse,
    MetricsCandidateCreate,
    MetricsCandidateRow,
    MetricsEvaluateRequest,
    MetricsEvaluateResponse,
    MetricsModelDetailResponse,
    MetricsRunsResponse,
    MetricsSummaryResponse,
    MetricsTableSchema,
    MetricsTimeseriesResponse,
    TrainingCompareResponse,
    TrainingRunsResponse,
)
from datapipe_app_ml_ops.observability.training.training_service import TrainingService
from datapipe_app_ml_ops.ops.ops_specs_service import OpsSpecsService
from datapipe_app_ml_ops.ops.spec_registry import OpsSpecRegistry


def register_ml_observability_routes(
    app: FastAPI,
    *,
    store: ObservabilityStore,
    registry: ObservabilityRegistry,
    ds: DataStore,
    catalog: Catalog,
    ops_spec_registry: OpsSpecRegistry,
    start_run_handler: Callable[[StartRunRequest, BackgroundTasks], StartRunResponse],
    require_pipeline: Callable[[str], None],
) -> None:
    register_ops_spec_routes(
        app,
        OpsSpecsService(ops_spec_registry, ds=ds, catalog=catalog),
    )

    def _metrics_svc() -> MetricsService:
        return MetricsService(store=store, ds=ds, catalog=catalog, ops_specs=ops_spec_registry)

    @app.get("/pipelines/{pipeline_id}/training/runs", response_model=TrainingRunsResponse)
    def list_training_runs_extended(
        pipeline_id: str,
        task_type: Optional[str] = None,
        framework: Optional[str] = None,
        status: Optional[str] = None,
        tags: Optional[str] = None,
        search: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_dir: str = "desc",
        limit: int = 25,
        offset: int = 0,
    ) -> TrainingRunsResponse:
        svc = TrainingService(store=store, ds=ds, catalog=catalog, ops_specs=ops_spec_registry)
        return svc.list_runs(
            pipeline_id,
            task_type=task_type.split(",") if task_type else None,
            framework=framework.split(",") if framework else None,
            status=status.split(",") if status else None,
            tags=tags.split(",") if tags else None,
            search=search,
            sort_by=sort_by,
            sort_dir=sort_dir,
            limit=min(limit, 200),
            offset=offset,
        )

    @app.get("/pipelines/{pipeline_id}/metrics/frozen-datasets", response_model=FrozenDatasetsResponse)
    def get_frozen_datasets(pipeline_id: str) -> FrozenDatasetsResponse:
        return _metrics_svc().list_frozen_datasets(pipeline_id)

    @app.get("/pipelines/{pipeline_id}/metrics/frozen-datasets/{dataset_id}", response_model=FrozenDatasetDetailResponse)
    def get_frozen_dataset_detail(
        pipeline_id: str,
        dataset_id: str,
        subset: Optional[str] = None,
    ) -> FrozenDatasetDetailResponse:
        return _metrics_svc().get_frozen_dataset_detail(pipeline_id, dataset_id, subset=subset)

    @app.get("/pipelines/{pipeline_id}/metrics/models/{model_id}", response_model=MetricsModelDetailResponse)
    def get_model_detail(
        pipeline_id: str,
        model_id: str,
        dataset_id: Optional[str] = None,
        subset: Optional[str] = None,
    ) -> MetricsModelDetailResponse:
        return _metrics_svc().get_model_detail(pipeline_id, model_id, dataset_id=dataset_id, subset=subset)

    @app.get("/pipelines/{pipeline_id}/metrics/runs", response_model=MetricsRunsResponse)
    def get_metrics_runs(
        pipeline_id: str,
        subset: Optional[str] = None,
        model_id: Optional[str] = None,
        search: Optional[str] = None,
        task_type: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_dir: str = "desc",
        limit: int = 25,
        offset: int = 0,
    ) -> MetricsRunsResponse:
        return _metrics_svc().list_runs(
            pipeline_id,
            subset=subset,
            model_id=model_id,
            search=search,
            task_type=task_type,
            sort_by=sort_by,
            sort_dir=sort_dir,
            limit=min(limit, 200),
            offset=offset,
        )

    @app.get("/pipelines/{pipeline_id}/metrics/schema", response_model=MetricsTableSchema)
    def get_metrics_schema(pipeline_id: str, task_type: Optional[str] = None) -> MetricsTableSchema:
        return _metrics_svc().get_schema(pipeline_id, task_type=task_type)

    @app.post("/pipelines/{pipeline_id}/metrics/candidates", response_model=MetricsCandidateRow)
    def create_metrics_candidate(pipeline_id: str, body: MetricsCandidateCreate) -> MetricsCandidateRow:
        return _metrics_svc().add_candidate(pipeline_id, body)

    @app.delete("/pipelines/{pipeline_id}/metrics/candidates/{candidate_id}")
    def delete_metrics_candidate(pipeline_id: str, candidate_id: str) -> dict[str, str]:
        if not _metrics_svc().delete_candidate(pipeline_id, candidate_id):
            raise HTTPException(404, f"Candidate {candidate_id} not found")
        return {"status": "ok"}

    @app.post("/pipelines/{pipeline_id}/metrics/evaluate", response_model=MetricsEvaluateResponse)
    def evaluate_metrics(
        pipeline_id: str,
        req: MetricsEvaluateRequest,
        background_tasks: BackgroundTasks,
    ) -> MetricsEvaluateResponse:
        require_pipeline(pipeline_id)
        labels = Labels([(pair[0], pair[1]) for pair in req.labels if len(pair) >= 2]) if req.labels else Labels([("stage", "count-metrics")])
        run_resp = start_run_handler(StartRunRequest(labels=labels, background=req.background), background_tasks)
        return MetricsEvaluateResponse(run_id=run_resp.run_id, status=run_resp.status)

    @app.get("/pipelines/{pipeline_id}/metrics/summary", response_model=MetricsSummaryResponse)
    def get_pipeline_metrics_summary(
        pipeline_id: str,
        subset: Optional[str] = None,
        model_id: Optional[str] = None,
        primary_metric: Optional[str] = None,
    ) -> MetricsSummaryResponse:
        return _metrics_svc().summary(
            pipeline_id,
            subset=subset,
            model_id=model_id,
            primary_metric=primary_metric,
        )

    @app.get("/pipelines/{pipeline_id}/metrics/timeseries", response_model=MetricsTimeseriesResponse)
    def get_metrics_timeseries(
        pipeline_id: str,
        metrics: str,
        subset: Optional[str] = None,
        group_by: str = "run",
    ) -> MetricsTimeseriesResponse:
        metric_list = [m.strip() for m in metrics.split(",") if m.strip()]
        subset_list = [s.strip() for s in subset.split(",")] if subset else None
        return _metrics_svc().timeseries(
            pipeline_id,
            metrics=metric_list,
            subset=subset_list,
            group_by=group_by,
        )

    @app.get("/pipelines/{pipeline_id}/metrics/classes", response_model=ClassMetricsResponse)
    def get_class_metrics(
        pipeline_id: str,
        subset: Optional[str] = None,
        model_id: Optional[str] = None,
        label_search: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_dir: str = "desc",
        limit: int = 50,
        offset: int = 0,
    ) -> ClassMetricsResponse:
        return _metrics_svc().list_classes(
            pipeline_id,
            subset=subset,
            model_id=model_id,
            label_search=label_search,
            sort_by=sort_by,
            sort_dir=sort_dir,
            limit=min(limit, 500),
            offset=offset,
        )

    @app.get("/pipelines/{pipeline_id}/metrics/classes/{label}", response_model=ClassMetricDetailResponse)
    def get_class_metric_detail(
        pipeline_id: str,
        label: str,
        subset: Optional[str] = None,
    ) -> ClassMetricDetailResponse:
        return _metrics_svc().class_detail(pipeline_id, label, subset=subset)

    @app.get("/sql/schema", response_model=SqlSchemaResponse)
    def get_sql_schema() -> SqlSchemaResponse:
        ctx = build_sql_analytics_context(
            ops_spec_registry,
            ds=ds,
            catalog=catalog,
            engine=store.engine,
        )
        schema = get_sql_schema_response(ctx)
        return SqlSchemaResponse(**schema)

    @app.get("/pipelines/{pipeline_id}/training/runs/legacy")
    def list_training_runs_legacy(pipeline_id: str) -> dict[str, Any]:
        enrichments: list[dict[str, Any]] = []
        for enricher in registry.enrichers:
            try:
                enrichments.extend(
                    enricher.enrich_pipeline_detail(
                        pipeline_id=pipeline_id,
                        ds=ds,
                        catalog=catalog,
                        store=store,
                    )
                )
            except Exception:
                pass
        runs: list[dict[str, Any]] = []
        for item in enrichments:
            if item.get("type") == "ml_training_runs":
                runs = item.get("payload", {}).get("rows", [])
        return {"pipeline_id": pipeline_id, "runs": runs}

    @app.get("/training/{run_key}")
    def get_training_run(run_key: str, pipeline_id: Optional[str] = None) -> dict[str, Any]:
        pid = pipeline_id or get_ops_settings().pipeline_id or ""
        detail: dict[str, Any] = {"run_key": run_key, "pipeline_id": pid}
        for enricher in registry.enrichers:
            try:
                items = enricher.enrich_pipeline_detail(
                    pipeline_id=pid,
                    ds=ds,
                    catalog=catalog,
                    store=store,
                )
                for item in items:
                    if item.get("type") == "ml_training_detail":
                        payload = item.get("payload", {})
                        if payload.get("run_key") == run_key:
                            detail.update(payload)
            except Exception:
                pass
        return detail

    @app.get("/training/compare", response_model=TrainingCompareResponse)
    def compare_training_runs(
        run_keys: str,
        pipeline_id: Optional[str] = None,
        metrics: Optional[str] = None,
    ) -> TrainingCompareResponse:
        keys = [k.strip() for k in run_keys.split(",") if k.strip()]
        if len(keys) < 1 or len(keys) > 4:
            raise HTTPException(400, "Provide 1-4 run_keys comma-separated")
        metric_list = [m.strip() for m in metrics.split(",") if m.strip()] if metrics else None
        svc = TrainingService(store=store, ds=ds, catalog=catalog, ops_specs=ops_spec_registry)
        try:
            return svc.compare(keys, metrics=metric_list, pipeline_id=pipeline_id)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc

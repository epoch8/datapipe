from __future__ import annotations

from typing import Any, Callable, Optional

from datapipe.compute import Catalog
from datapipe.datatable import DataStore
from fastapi import FastAPI, HTTPException

from datapipe_app.observability.store.db import ObservabilityStore
from datapipe_app.observability.plugins.registry import ObservabilityRegistry
from datapipe_app.observability.config.settings import get_ops_settings
from datapipe_app_ml_ops.observability.metrics.metrics_service import MetricsService
from datapipe_app_ml_ops.observability.routes.ops_spec_routes import register_ops_spec_routes
from datapipe_app_ml_ops.observability.schemas.models import (
    ClassMetricDetailResponse,
    ClassMetricsResponse,
    FrozenDatasetDetailResponse,
    FrozenDatasetsResponse,
    MetricsEntityResolveResponse,
    MetricsModelDetailResponse,
    MetricsRunsResponse,
    MetricsSummaryResponse,
    TrainingRunsResponse,
)
from datapipe_app_ml_ops.observability.training.training_service import TrainingService
from datapipe_app_ml_ops.ops.ops_specs_service import OpsSpecsService
from datapipe_app_ml_ops.ops.spec_registry import OpsSpecRegistry
from datapipe_app_ml_ops.ops.frozen_datasets_launch_routes import (
    register_frozen_datasets_launch_routes,
)
from datapipe_app_ml_ops.ops.training_experiments_routes import (
    register_training_experiments_routes,
)


def register_ml_observability_routes(
    app: FastAPI,
    *,
    store: ObservabilityStore,
    registry: ObservabilityRegistry,
    ds: DataStore,
    catalog: Catalog,
    ops_spec_registry: OpsSpecRegistry,
    run_steps: Optional[Callable[..., Any]] = None,
) -> None:
    register_ops_spec_routes(
        app,
        OpsSpecsService(ops_spec_registry, ds=ds, catalog=catalog),
    )

    register_training_experiments_routes(
        app,
        registry=ops_spec_registry,
        ds=ds,
        catalog=catalog,
        run_steps=run_steps,
    )
    register_frozen_datasets_launch_routes(
        app,
        registry=ops_spec_registry,
        run_steps=run_steps,
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

    @app.get("/pipelines/{pipeline_id}/metrics/resolve-entity", response_model=MetricsEntityResolveResponse)
    def resolve_metrics_entity(
        pipeline_id: str,
        model_id: Optional[str] = None,
        dataset_id: Optional[str] = None,
    ) -> MetricsEntityResolveResponse:
        try:
            return _metrics_svc().resolve_entity(pipeline_id, model_id=model_id, dataset_id=dataset_id)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

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

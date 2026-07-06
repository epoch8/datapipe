from __future__ import annotations

import importlib.metadata
from datetime import datetime
from typing import Any, List, Literal, Optional

from datapipe.compute import Catalog, ComputeStep, DataStore, Pipeline, run_steps
from datapipe.step.batch_transform import BaseBatchTransformStep
from datapipe.types import Labels
from fastapi import BackgroundTasks, FastAPI, HTTPException, Query
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from pydantic import BaseModel, Field

from datapipe_app.api_v1alpha1 import filter_steps_by_labels
from datapipe_app.observability.run_scope import (
    derive_run_scope,
    labels_from_json,
    labels_to_json,
    trigger_from_labels,
)
from datapipe_app.observability.db import ObservabilityStore, PipelineRunRow, utc_now
from datapipe_app.observability.discovery import build_stage_summary, build_stage_edges, discover_pipeline_stages
from datapipe_app.observability.label_graph import build_label_graph
from datapipe_app.observability.analytics_views import ensure_analytics_tables, get_schema, refresh_analytics_views
from datapipe_app.observability.metrics_service import MetricsService
from datapipe_app.observability.queries import build_chart_specs, build_overview, build_training_curves
from datapipe_app.observability.schemas import (
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
    SqlQueryRequest,
    SqlQueryResponse,
    SqlSchemaResponse,
    TrainingCompareResponse,
    TrainingRunsResponse,
)
from datapipe_app.observability.sql_executor import execute_readonly_query
from datapipe_app.observability.training_service import TrainingService
from datapipe_app.observability.recorder import RunRecorder
from datapipe_app.observability.registry import ObservabilityRegistry
from datapipe_app.observability.settings import get_ops_settings


class StartRunRequest(BaseModel):
    labels: Labels = []
    background: bool = True


class StartRunResponse(BaseModel):
    run_id: str
    status: str = "running"


class ResetTransformMetadataResponse(BaseModel):
    transform_name: str
    status: str = "ok"


class CapabilitiesResponse(BaseModel):
    ml_metrics: bool
    ml_training: bool
    mode: str
    pipeline_id: Optional[str] = None


class SettingsResponse(BaseModel):
    pipeline_id: Optional[str]
    mode: str
    observability_db_connected: bool
    version: str


def make_app(
    store: ObservabilityStore,
    registry: ObservabilityRegistry,
    *,
    ds: Optional[DataStore] = None,
    catalog: Optional[Catalog] = None,
    pipeline: Optional[Pipeline] = None,
    steps: Optional[List[ComputeStep]] = None,
    recorder: Optional[RunRecorder] = None,
) -> FastAPI:
    app = FastAPI(title="Datapipe Ops API v1alpha3")

    def _pipeline_id() -> Optional[str]:
        return get_ops_settings().pipeline_id

    def _require_agent_pipeline(pipeline_id: str) -> None:
        if get_ops_settings().mode == "central":
            raise HTTPException(400, "Run triggers are not available in central mode")
        if ds is None or steps is None:
            raise HTTPException(503, "Local pipeline not available")

    def _has_catalog_metrics() -> bool:
        if ds is None or catalog is None:
            return False
        try:
            from datapipe_ml.observability.discovery import discover_metrics_tables

            return any(True for _table_name, _dt in discover_metrics_tables(catalog, ds))
        except Exception:
            return False

    @app.get("/overview")
    def get_overview() -> dict[str, Any]:
        return build_overview(store, registry, ds=ds, catalog=catalog)

    @app.get("/capabilities", response_model=CapabilitiesResponse)
    def get_capabilities() -> CapabilitiesResponse:
        has_ml_plugin = len(registry.enrichers) > 0 or len(registry.publishers) > 0
        return CapabilitiesResponse(
            ml_metrics=has_ml_plugin and _has_catalog_metrics(),
            ml_training=has_ml_plugin,
            mode=get_ops_settings().mode,
            pipeline_id=get_ops_settings().pipeline_id,
        )

    @app.get("/settings", response_model=SettingsResponse)
    def get_settings() -> SettingsResponse:
        try:
            importlib.metadata.version("datapipe-app")
            version = importlib.metadata.version("datapipe-app")
        except Exception:
            version = "unknown"
        connected = False
        try:
            with store.session() as session:
                session.connection()
            connected = True
        except Exception:
            pass
        return SettingsResponse(
            pipeline_id=get_ops_settings().pipeline_id,
            mode=get_ops_settings().mode,
            observability_db_connected=connected,
            version=version,
        )

    @app.get("/version")
    def get_version() -> dict[str, str]:
        try:
            return {"version": importlib.metadata.version("datapipe-app")}
        except Exception:
            return {"version": "unknown"}

    def _serialize_recent_runs(runs: list[Any]) -> list[dict[str, Any]]:
        return [
            {
                "run_id": r.run_id,
                "status": r.status,
                "started_at": r.started_at.isoformat() if r.started_at else None,
                "finished_at": r.finished_at.isoformat() if r.finished_at else None,
                "trigger": r.trigger,
            }
            for r in runs
        ]


    @app.get("/pipelines/{pipeline_id}")
    def get_pipeline_detail(pipeline_id: str) -> dict[str, Any]:
        reg = store.get_pipeline(pipeline_id)
        if reg is None:
            raise HTTPException(404, f"Pipeline {pipeline_id} not found")

        last_run = store.get_last_run(pipeline_id)
        recent_runs = store.list_recent_runs(pipeline_id)
        schedule = store.get_schedule(pipeline_id)

        stages: list[dict[str, Any]] = []
        stage_edges: list[dict[str, Any]] = []
        label_graph: dict[str, Any] | None = None
        if ds is not None and steps is not None and get_ops_settings().pipeline_id == pipeline_id:
            status_cache: dict[str, dict[str, Any]] = {}
            stages = build_stage_summary(steps, ds, status_cache)
            stage_edges = build_stage_edges(steps)
            label_graph = build_label_graph(steps, ds, status_cache=status_cache)
        elif pipeline is not None:
            stages = [{"stage": s, "status": "unknown", "steps": []} for s in discover_pipeline_stages(pipeline)]

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

        for collector in registry.collectors:
            try:
                rows = collector.collect_pipeline_status(
                    pipeline_id=pipeline_id,
                    ds=ds,
                    catalog=catalog,
                )
                if rows:
                    enrichments.append({"type": "ml_training", "payload": {"rows": rows}})
            except Exception:
                pass

        return {
            "pipeline_id": pipeline_id,
            "display_name": reg.display_name,
            "task_type": reg.task_type,
            "health": "failed" if last_run and last_run.status == "failed" else "healthy",
            "stages": stages,
            "stage_edges": stage_edges,
            "label_graph": label_graph,
            "recent_runs": _serialize_recent_runs(recent_runs),
            "next_run_at": schedule.next_run_at.isoformat() if schedule and schedule.next_run_at else None,
            "last_error": last_run.error if last_run else None,
            "enrichments": enrichments,
            "agent_mode": get_ops_settings().mode == "agent",
        }

    @app.get("/pipelines/{pipeline_id}/stages/{stage_name}/recent-runs")
    def get_stage_recent_runs(pipeline_id: str, stage_name: str, limit: int = 10) -> dict[str, Any]:
        reg = store.get_pipeline(pipeline_id)
        if reg is None:
            raise HTTPException(404, f"Pipeline {pipeline_id} not found")
        if ds is None or steps is None or get_ops_settings().pipeline_id != pipeline_id:
            return {"pipeline_id": pipeline_id, "stage": stage_name, "recent_runs": []}

        stage_steps = filter_steps_by_labels(steps, labels=[("stage", stage_name)])
        if not stage_steps:
            return {"pipeline_id": pipeline_id, "stage": stage_name, "recent_runs": []}

        stage_step_names = [s.name for s in stage_steps]
        runs = store.list_recent_runs_for_stage(
            pipeline_id,
            stage_step_names,
            stage_name=stage_name,
            limit=min(limit, 50),
        )
        return {
            "pipeline_id": pipeline_id,
            "stage": stage_name,
            "recent_runs": _serialize_recent_runs(runs),
        }

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
        svc = TrainingService(store=store, ds=ds, catalog=catalog)
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

    def _metrics_svc() -> MetricsService:
        return MetricsService(store=store, ds=ds, catalog=catalog)

    @app.get("/pipelines/{pipeline_id}/metrics/frozen-datasets", response_model=FrozenDatasetsResponse)
    def get_frozen_datasets(pipeline_id: str) -> FrozenDatasetsResponse:
        return _metrics_svc().list_frozen_datasets(pipeline_id)

    @app.get("/pipelines/{pipeline_id}/metrics/frozen-datasets/{dataset_id}", response_model=FrozenDatasetDetailResponse)
    def get_frozen_dataset_detail(
        pipeline_id: str,
        dataset_id: str,
        subset: Optional[str] = None,
    ) -> FrozenDatasetDetailResponse:
        return _metrics_svc().get_frozen_dataset_detail(
            pipeline_id,
            dataset_id,
            subset=subset,
        )

    @app.get("/pipelines/{pipeline_id}/metrics/models/{model_id}", response_model=MetricsModelDetailResponse)
    def get_model_detail(
        pipeline_id: str,
        model_id: str,
        dataset_id: Optional[str] = None,
        subset: Optional[str] = None,
    ) -> MetricsModelDetailResponse:
        return _metrics_svc().get_model_detail(
            pipeline_id,
            model_id,
            dataset_id=dataset_id,
            subset=subset,
        )

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
        _require_agent_pipeline(pipeline_id)
        labels = Labels([(pair[0], pair[1]) for pair in req.labels if len(pair) >= 2]) if req.labels else Labels([("stage", "count-metrics")])
        run_resp = start_run(StartRunRequest(labels=labels, background=req.background), background_tasks)
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

    @app.post("/sql/query", response_model=SqlQueryResponse)
    def run_sql_query(req: SqlQueryRequest) -> SqlQueryResponse:
        ensure_analytics_tables(store.engine)
        if get_ops_settings().pipeline_id:
            try:
                refresh_analytics_views(
                    store.engine,
                    pipeline_id=get_ops_settings().pipeline_id or "",
                    store=store,
                    ds=ds,
                    catalog=catalog,
                )
            except Exception:
                pass
        try:
            result = execute_readonly_query(
                store.engine,
                req.sql,
                limit=req.limit or 1000,
                offset=req.offset or 0,
            )
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        except Exception as exc:
            raise HTTPException(400, f"Query failed: {exc}") from exc
        return SqlQueryResponse(**result)

    @app.get("/sql/schema", response_model=SqlSchemaResponse)
    def get_sql_schema() -> SqlSchemaResponse:
        ensure_analytics_tables(store.engine)
        schema = get_schema(store.engine)
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

    @app.get("/metrics/charts")
    def get_metrics_charts(
        pipeline_id: str,
        model_id: Optional[str] = None,
    ) -> dict[str, Any]:
        if not pipeline_id:
            raise HTTPException(400, "pipeline_id is required")
        charts = build_chart_specs(store, pipeline_id, model_id=model_id)
        return {"pipeline_id": pipeline_id, "charts": charts}

    @app.get("/metrics/summary")
    def get_metrics_summary(pipeline_id: str) -> dict[str, Any]:
        rows = store.list_metrics(pipeline_id)
        if not rows:
            return {"pipeline_id": pipeline_id, "has_metrics": False}
        latest = rows[-1]
        return {
            "pipeline_id": pipeline_id,
            "has_metrics": True,
            "model_id": latest.model_id,
            "subset_id": latest.subset_id,
            "computed_at": latest.computed_at.isoformat() if latest.computed_at else None,
        }

    def _serialize_run_list_row(run: PipelineRunRow) -> dict[str, Any]:
        labels = labels_from_json(run.labels_json)
        scope = derive_run_scope(labels=labels, trigger=run.trigger)
        duration_s: int | None = None
        if run.started_at and run.finished_at:
            duration_s = int((run.finished_at - run.started_at).total_seconds())
        return {
            "run_id": run.run_id,
            "pipeline_id": run.pipeline_id,
            "status": run.status,
            "scope": scope["run_scope"],
            "target_label": scope.get("target_label_display"),
            "started_at": run.started_at.isoformat() if run.started_at else None,
            "finished_at": run.finished_at.isoformat() if run.finished_at else None,
            "duration_s": duration_s,
            "trigger": run.trigger,
        }

    @app.get("/runs")
    def list_runs(
        pipeline_id: Optional[str] = None,
        status: Optional[str] = None,
        stage: Optional[str] = None,
        trigger: Optional[str] = None,
        search: Optional[str] = None,
        from_: Optional[str] = Query(None, alias="from"),
        to: Optional[str] = None,
        limit: int = 25,
        offset: int = 0,
        sort_by: str = "started_at",
        sort_dir: Literal["asc", "desc"] = "desc",
    ) -> dict[str, Any]:
        pid = pipeline_id or _pipeline_id()
        from_parsed = datetime.fromisoformat(from_) if from_ else None
        to_parsed = datetime.fromisoformat(to) if to else None
        rows, total, filters, counts_by_status = store.list_runs(
            pipeline_id=pid,
            status=status,
            stage=stage,
            trigger=trigger,
            search=search,
            from_dt=from_parsed,
            to_dt=to_parsed,
            sort_by=sort_by,
            sort_dir=sort_dir,
            limit=min(limit, 200),
            offset=offset,
        )
        return {
            "rows": [_serialize_run_list_row(r) for r in rows],
            "total": total,
            "filters": filters,
            "counts_by_status": counts_by_status,
        }

    @app.get("/runs/{run_id}")
    def get_run(run_id: str) -> dict[str, Any]:
        run = store.get_run(run_id)
        if run is None:
            raise HTTPException(404, f"Run {run_id} not found")
        steps_rows = store.get_run_steps(run_id)
        labels = labels_from_json(run.labels_json)
        scope = derive_run_scope(labels=labels, trigger=run.trigger)
        return {
            "run_id": run.run_id,
            "pipeline_id": run.pipeline_id,
            "status": run.status,
            "started_at": run.started_at.isoformat() if run.started_at else None,
            "finished_at": run.finished_at.isoformat() if run.finished_at else None,
            "error": run.error,
            "trigger": run.trigger,
            **scope,
            "steps": [
                {
                    "step_name": s.step_name,
                    "status": s.status,
                    "started_at": s.started_at.isoformat() if s.started_at else None,
                    "finished_at": s.finished_at.isoformat() if s.finished_at else None,
                    "processed": s.processed,
                    "total": s.total,
                    "error": s.error,
                }
                for s in steps_rows
            ],
        }

    @app.get("/runs/{run_id}/logs")
    def get_run_logs(
        run_id: str,
        after: int = 0,
        limit: int = 500,
    ) -> dict[str, Any]:
        run = store.get_run(run_id)
        if run is None:
            raise HTTPException(404, f"Run {run_id} not found")
        from datapipe_app.observability.log_buffer import get_log_buffer

        buf = get_log_buffer(store)
        lines = buf.get_lines(run_id, after=after, limit=min(limit, 1000))
        return {
            "run_id": run_id,
            "lines": [
                {
                    "seq": ln.seq,
                    "logged_at": ln.logged_at,
                    "level": ln.level,
                    "message": ln.message,
                }
                for ln in lines
            ],
            "last_seq": lines[-1].seq if lines else after,
        }

    @app.post("/runs", response_model=StartRunResponse)
    def start_run(req: StartRunRequest, background_tasks: BackgroundTasks) -> StartRunResponse:
        pid = _pipeline_id()
        if not pid:
            raise HTTPException(400, "PIPELINE_ID not configured")
        _require_agent_pipeline(pid)
        assert ds is not None and steps is not None
        assert recorder is not None

        selected = filter_steps_by_labels(steps, labels=req.labels) if req.labels else steps
        trigger = trigger_from_labels(req.labels)
        labels_json = labels_to_json(req.labels)

        if req.background:
            run_id = recorder.start_run(trigger=trigger, labels_json=labels_json)

            def _execute() -> None:
                recorder.execute_steps(
                    selected,
                    ds=ds,
                    run_id=run_id,
                    on_step_failure="return",
                )

            background_tasks.add_task(_execute)
            return StartRunResponse(run_id=run_id, status="running")

        run_id = recorder.start_run(trigger=trigger, labels_json=labels_json)
        try:
            recorder.execute_steps(selected, ds=ds, run_id=run_id)
        except Exception as exc:
            raise HTTPException(500, str(exc)) from exc
        return StartRunResponse(run_id=run_id, status="completed")

    @app.post(
        "/pipelines/{pipeline_id}/transforms/{transform_name}/reset-metadata",
        response_model=ResetTransformMetadataResponse,
    )
    def reset_transform_metadata(pipeline_id: str, transform_name: str) -> ResetTransformMetadataResponse:
        _require_agent_pipeline(pipeline_id)
        assert ds is not None and steps is not None
        if get_ops_settings().pipeline_id != pipeline_id:
            raise HTTPException(404, f"Pipeline {pipeline_id} not available on this agent")

        filtered_steps = filter_steps_by_labels(steps, name_prefix=transform_name)
        if len(filtered_steps) != 1:
            raise HTTPException(404, f"Transform {transform_name} not found")
        step = filtered_steps[0]
        if not isinstance(step, BaseBatchTransformStep):
            raise HTTPException(400, f"Transform {transform_name} does not have SQL metadata")
        step.reset_metadata(ds)
        return ResetTransformMetadataResponse(transform_name=transform_name)

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
        detail["curves"] = build_training_curves(store, run_key)
        return detail

    @app.get("/training/{run_key}/curves")
    def get_training_curves(
        run_key: str,
        limit_epochs: Optional[int] = None,
    ) -> dict[str, Any]:
        charts = build_training_curves(store, run_key, limit_epochs=limit_epochs)
        return {"run_key": run_key, "charts": charts}

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
        svc = TrainingService(store=store, ds=ds, catalog=catalog)
        try:
            return svc.compare(keys, metrics=metric_list, pipeline_id=pipeline_id)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc

    FastAPIInstrumentor.instrument_app(app, excluded_urls="docs")
    return app

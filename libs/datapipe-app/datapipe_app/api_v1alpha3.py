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
from datapipe_app.observability.queries import build_overview
from datapipe_app.observability.recorder import RunRecorder
from datapipe_app.observability.registry import ObservabilityRegistry
from datapipe_app.observability.settings import get_ops_settings
from datapipe_app.pipeline_ui import register_pipeline_ui_routes
from datapipe_ml.observability.routes import register_ml_observability_routes
from datapipe_ml.spec_registry import OpsSpecRegistry


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
    ops_specs: Optional[OpsSpecRegistry] = None,
) -> FastAPI:
    app = FastAPI(title="Datapipe Ops API v1alpha3")
    ops_spec_registry = ops_specs or OpsSpecRegistry()

    def _pipeline_id() -> Optional[str]:
        return get_ops_settings().pipeline_id

    def _require_agent_pipeline(pipeline_id: str) -> None:
        if get_ops_settings().mode == "central":
            raise HTTPException(400, "Run triggers are not available in central mode")
        if ds is None or steps is None:
            raise HTTPException(503, "Local pipeline not available")

    def _has_catalog_metrics() -> bool:
        return any(
            table_spec.best_metric_column
            for spec in ops_spec_registry.list()
            for table_spec in spec.metrics
        )

    @app.get("/overview")
    def get_overview() -> dict[str, Any]:
        return build_overview(store, registry, ds=ds, catalog=catalog)

    @app.get("/capabilities", response_model=CapabilitiesResponse)
    def get_capabilities() -> CapabilitiesResponse:
        has_ml_plugin = len(registry.enrichers) > 0 or len(registry.collectors) > 0
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

    @app.post("/run")
    def run_pipeline_legacy() -> dict[str, Any]:
        """Legacy full-pipeline run endpoint (formerly v1alpha1 /run with recorder)."""
        assert ds is not None and steps is not None
        if recorder is None:
            run_steps(ds=ds, steps=steps)
            return {"result": "ok"}
        run_id = recorder.start_run(trigger="v1alpha1")
        try:
            recorder.execute_steps(steps, ds=ds, run_id=run_id, on_step_failure="raise")
        except Exception:
            return {"run_id": run_id, "status": "failed"}
        return {"run_id": run_id, "status": "completed"}

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

    register_ml_observability_routes(
        app,
        store=store,
        registry=registry,
        ds=ds,
        catalog=catalog,
        ops_spec_registry=ops_spec_registry,
        start_run_handler=start_run,
        require_agent_pipeline=_require_agent_pipeline,
    )

    if ds is not None and catalog is not None and pipeline is not None and steps is not None:
        register_pipeline_ui_routes(
            app,
            ds=ds,
            catalog=catalog,
            pipeline=pipeline,
            steps=steps,
            recorder=recorder,
        )

    FastAPIInstrumentor.instrument_app(app, excluded_urls="docs")
    return app

from __future__ import annotations

import importlib.metadata
import threading
from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Literal, Optional, Sequence, Tuple

from datapipe.cancel import (
    CancelToken,
    cancel_token_scope,
)
from datapipe.compute import Catalog, ComputeStep, DataStore, Pipeline, run_steps
from datapipe.executor import Executor
from datapipe.run_config import RunConfig
from datapipe.step.batch_transform import BaseBatchTransformStep
from fastapi import BackgroundTasks, FastAPI, HTTPException, Query
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from pydantic import BaseModel

from datapipe_app.api.v1alpha1 import filter_steps_by_labels
from datapipe_app.observability.runs.active_runs import get_active_run_registry
from datapipe_app.observability.runs.run_scope import (
    derive_run_scope,
    labels_from_json,
    labels_to_json,
    trigger_from_labels,
)
from datapipe_app.observability.store.db import ObservabilityStore, PipelineRunRow
from datapipe_app.observability.graph.discovery import build_stage_summary, build_stage_edges
from datapipe_app.observability.graph.label_graph import build_label_graph
from datapipe_app.observability.runs.recorder import RunRecorder
from datapipe_app.observability.plugins.registry import ObservabilityRegistry
from datapipe_app.observability.plugins.schemas import StartRunRequest, StartRunResponse
from datapipe_app.observability.config.settings import get_ops_settings
from datapipe_app.pipeline.pipeline_ui import register_pipeline_ui_routes
from datapipe_app.ops.spec_registry import OpsSpecRegistry
from datapipe_app.observability.extensions import register_v1alpha3_extensions

if TYPE_CHECKING:
    from datapipe_app.app.datapipe_api import DatapipeAPI


_STOP_REASON = "Stopped by user"


class ResetTransformMetadataResponse(BaseModel):
    transform_name: str
    status: str = "ok"


class StopRunResponse(BaseModel):
    run_id: str
    status: str
    stopped: bool


class CapabilitiesResponse(BaseModel):
    ml_metrics: bool
    ml_training: bool
    pipeline_id: Optional[str] = None
    run_logs_configured: bool = False


class SettingsResponse(BaseModel):
    pipeline_id: Optional[str]
    observability_db_connected: bool
    version: str
    run_logs_configured: bool = False


def _step_compatible_with_pk_filters(step: ComputeStep, filter_keys: set[str]) -> bool:
    """Skip steps where a PK RunConfig filter would delete scoped rows.

    ``OpsTrainingRequestSpec`` launches with ``filters={id_column: [request_id]}``.
    Datapipe stamps filters that are not transform keys onto the batch idx;
    ``store_chunk`` then treats matching output primary keys as
    ``processed_idx`` and deletes missing rows. Auto-request materialization
    (transform keys = dataset × config, output PK = request id) would wipe the
    manual request being launched. Skip those steps for filtered runs; they
    still run on unfiltered ``labels=…`` CLI trains.
    """
    if not filter_keys:
        return True
    if not isinstance(step, BaseBatchTransformStep):
        return True
    stamped_extras = filter_keys - set(step.transform_keys)
    if not stamped_extras:
        return True
    for out in step.output_dts:
        if stamped_extras & set(out.dt.primary_keys):
            return False
    return True


def make_run_steps_callable(
    *,
    ds: DataStore,
    steps: List[ComputeStep],
    recorder: Optional[RunRecorder],
    resolve_executor: Callable[[], Executor | None],
) -> Callable[[str, Sequence[Tuple[str, str]], Dict[str, List[Any]]], Dict[str, Any]]:
    """Build the ``run_steps`` callback passed to v1alpha3 extensions (spec §18).

    The returned callable selects steps by ``run_labels`` and runs them with a
    primary-key ``RunConfig`` filter so only the requested row is materialized.
    Single-element list filter values are unwrapped to scalars because
    ``RunConfig.filters`` applies equality on primary keys.

    Steps incompatible with those PK filters (see
    ``_step_compatible_with_pk_filters``) are omitted from the filtered run.

    Execution is always started in a background thread so the HTTP request can
    return the ``run_id`` immediately (UI navigates to the run page).
    """
    registry = get_active_run_registry()

    def _run_steps_for_request(
        request_id: str,
        run_labels: Sequence[Tuple[str, str]],
        filters: Dict[str, List[Any]],
    ) -> Dict[str, Any]:
        labels = list(run_labels)
        selected = filter_steps_by_labels(steps, labels=labels) if labels else list(steps)
        filter_keys = set(filters.keys())
        if filter_keys:
            selected = [s for s in selected if _step_compatible_with_pk_filters(s, filter_keys)]
        if not selected:
            return {"started": False, "run_id": None}

        run_config_filters = {
            key: (value[0] if isinstance(value, (list, tuple)) and len(value) == 1 else value)
            for key, value in filters.items()
        }
        run_config = RunConfig(filters=run_config_filters)

        if recorder is not None:
            trigger = trigger_from_labels(labels)
            labels_json = labels_to_json(labels)
            cb = recorder.create_callback(trigger=trigger, labels_json=labels_json)
            token = CancelToken()
            registry.register(cb.run_id, token)

            def _execute() -> None:
                try:
                    with cancel_token_scope(token):
                        run_steps(
                            ds=ds,
                            steps=selected,
                            run_config=run_config,
                            executor=resolve_executor(),
                            callbacks=[cb],
                        )
                except BaseException:
                    # Failure / interrupt already recorded via the RunCallback hooks.
                    return
                finally:
                    registry.unregister(cb.run_id)

            thread = threading.Thread(
                target=_execute,
                name=f"datapipe-run-{cb.run_id}",
                daemon=True,
            )
            thread.start()
            return {"started": True, "run_id": cb.run_id}

        def _execute_unrecorded() -> None:
            try:
                run_steps(
                    ds=ds,
                    steps=selected,
                    run_config=run_config,
                    executor=resolve_executor(),
                )
            except Exception:
                return

        threading.Thread(
            target=_execute_unrecorded,
            name=f"datapipe-run-{request_id}",
            daemon=True,
        ).start()
        return {"started": True, "run_id": None}

    return _run_steps_for_request


def make_app(
    store: ObservabilityStore,
    registry: ObservabilityRegistry,
    *,
    ds: DataStore,
    catalog: Catalog,
    pipeline: Pipeline,
    steps: List[ComputeStep],
    recorder: Optional[RunRecorder] = None,
    ops_specs: Optional[OpsSpecRegistry] = None,
    executor: Executor | None = None,
    executor_host: Optional["DatapipeAPI"] = None,
) -> FastAPI:
    app = FastAPI(title="Datapipe Ops API v1alpha3")
    ops_spec_registry = ops_specs or OpsSpecRegistry()

    def _resolve_executor() -> Executor | None:
        if executor_host is not None:
            return executor_host.executor
        return executor

    def _pipeline_id() -> Optional[str]:
        return get_ops_settings().pipeline_id

    def _require_pipeline(requested_pipeline_id: str) -> None:
        pid = _pipeline_id()
        if not pid or pid != requested_pipeline_id:
            raise HTTPException(404, f"Pipeline {requested_pipeline_id} not available on this instance")

    def _has_catalog_metrics() -> bool:
        return any(
            table_spec.best_metric_column
            for spec in ops_spec_registry.list()
            for table_spec in spec.metrics
        )

    @app.get("/capabilities", response_model=CapabilitiesResponse)
    def get_capabilities() -> CapabilitiesResponse:
        has_ml_plugin = len(registry.enrichers) > 0 or len(registry.collectors) > 0
        return CapabilitiesResponse(
            ml_metrics=has_ml_plugin and _has_catalog_metrics(),
            ml_training=has_ml_plugin,
            pipeline_id=get_ops_settings().pipeline_id,
            run_logs_configured=store.run_logs_configured,
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
            observability_db_connected=connected,
            version=version,
            run_logs_configured=store.run_logs_configured,
        )

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

        stages: list[dict[str, Any]] = []
        stage_edges: list[dict[str, Any]] = []
        label_graph: dict[str, Any] | None = None
        if get_ops_settings().pipeline_id == pipeline_id:
            status_cache: dict[str, dict[str, Any]] = {}
            stages = build_stage_summary(steps, ds, status_cache)
            stage_edges = build_stage_edges(steps)
            label_graph = build_label_graph(steps, ds, status_cache=status_cache)

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
            "last_error": last_run.error if last_run else None,
            "enrichments": enrichments,
        }

    @app.get("/pipelines/{pipeline_id}/stages/{stage_name}/recent-runs")
    def get_stage_recent_runs(pipeline_id: str, stage_name: str, limit: int = 10) -> dict[str, Any]:
        reg = store.get_pipeline(pipeline_id)
        if reg is None:
            raise HTTPException(404, f"Pipeline {pipeline_id} not found")
        if get_ops_settings().pipeline_id != pipeline_id:
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
        from datapipe_app.observability.logging.log_buffer import get_log_buffer

        buf = get_log_buffer(store)
        lines = buf.get_lines(run_id, after=after, limit=min(limit, 1000))
        max_seq = buf.get_max_seq(run_id)
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
            "max_seq": max_seq,
        }

    @app.post("/runs", response_model=StartRunResponse)
    def start_run(req: StartRunRequest, background_tasks: BackgroundTasks) -> StartRunResponse:
        pid = _pipeline_id()
        if not pid:
            raise HTTPException(400, "PIPELINE_ID not configured")
        _require_pipeline(pid)
        assert recorder is not None

        selected = filter_steps_by_labels(steps, labels=req.labels) if req.labels else steps
        trigger = trigger_from_labels(req.labels)
        labels_json = labels_to_json(req.labels)
        registry = get_active_run_registry()

        if req.background:
            cb = recorder.create_callback(trigger=trigger, labels_json=labels_json)
            token = CancelToken()
            registry.register(cb.run_id, token)

            def _execute() -> None:
                try:
                    with cancel_token_scope(token):
                        run_steps(
                            ds=ds,
                            steps=selected,
                            executor=_resolve_executor(),
                            callbacks=[cb],
                        )
                except BaseException:
                    # Failure / interrupt already recorded via RunCallback hooks.
                    return
                finally:
                    registry.unregister(cb.run_id)

            background_tasks.add_task(_execute)
            return StartRunResponse(run_id=cb.run_id, status="running")

        cb = recorder.create_callback(trigger=trigger, labels_json=labels_json)
        token = CancelToken()
        registry.register(cb.run_id, token)
        try:
            with cancel_token_scope(token):
                run_steps(
                    ds=ds,
                    steps=selected,
                    executor=_resolve_executor(),
                    callbacks=[cb],
                )
        except Exception as exc:
            raise HTTPException(500, str(exc)) from exc
        finally:
            registry.unregister(cb.run_id)
        return StartRunResponse(run_id=cb.run_id, status="completed")

    @app.post("/runs/{run_id}/stop", response_model=StopRunResponse)
    def stop_run(run_id: str) -> StopRunResponse:
        """Urgent stop: kill training subprocesses and mark the run interrupted."""
        row = store.get_run(run_id)
        if row is None:
            raise HTTPException(404, f"Run {run_id} not found")
        if row.status != "running":
            raise HTTPException(
                409,
                f"Run {run_id} is not running (status={row.status})",
            )

        registry = get_active_run_registry()
        stopped = registry.request_stop(run_id)
        store.finish_running_steps(run_id, status="interrupted", error=_STOP_REASON)
        store.finish_run(run_id, status="interrupted", error=_STOP_REASON)
        if not stopped:
            # Run is recorded as running but this process has no live token
            # (e.g. after API restart). Status is still flipped to interrupted.
            return StopRunResponse(run_id=run_id, status="interrupted", stopped=False)
        return StopRunResponse(run_id=run_id, status="interrupted", stopped=True)

    @app.post(
        "/pipelines/{pipeline_id}/transforms/{transform_name}/reset-metadata",
        response_model=ResetTransformMetadataResponse,
    )
    def reset_transform_metadata(pipeline_id: str, transform_name: str) -> ResetTransformMetadataResponse:
        _require_pipeline(pipeline_id)

        filtered_steps = filter_steps_by_labels(steps, name_prefix=transform_name)
        if len(filtered_steps) != 1:
            raise HTTPException(404, f"Transform {transform_name} not found")
        step = filtered_steps[0]
        if not isinstance(step, BaseBatchTransformStep):
            raise HTTPException(400, f"Transform {transform_name} does not have SQL metadata")
        step.reset_metadata(ds)
        return ResetTransformMetadataResponse(transform_name=transform_name)

    register_v1alpha3_extensions(
        app=app,
        store=store,
        registry=registry,
        ds=ds,
        catalog=catalog,
        ops_spec_registry=ops_spec_registry,
        recorder=recorder,
        steps=steps,
        pipeline=pipeline,
        run_steps=make_run_steps_callable(
            ds=ds,
            steps=steps,
            recorder=recorder,
            resolve_executor=_resolve_executor,
        ),
    )

    register_pipeline_ui_routes(
        app,
        ds=ds,
        catalog=catalog,
        pipeline=pipeline,
        steps=steps,
        recorder=recorder,
        executor=_resolve_executor,
    )

    FastAPIInstrumentor.instrument_app(app, excluded_urls="docs")
    return app

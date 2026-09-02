"""Slim Datapipe Ops API v1alpha3 (cloud): pipeline UI + meta, no runs/logs."""

from __future__ import annotations

import asyncio
import importlib.metadata
from typing import List, Optional, Sequence, Set

from datapipe.compute import Catalog, ComputeStep, DataStore, Pipeline, PipelineStep
from datapipe.step.batch_generate import BatchGenerate
from datapipe.step.batch_transform import (
    BaseBatchTransformStep,
    BatchTransform,
    DatatableBatchTransform,
)
from datapipe.step.datatable_transform import DatatableTransform
from datapipe.step.update_external_table import UpdateExternalTable
from datapipe.store.database import TableStoreDB
from datapipe.store.table_store import TableStore
from datapipe.types import Labels
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from sqlalchemy.sql.expression import select
from sqlalchemy.sql.functions import count

from datapipe_app import models
from datapipe_app import settings as app_settings
from datapipe_app.api_v1alpha2 import (
    RunningStepsHelper,
    get_table_data,
    get_transform_data,
    run_step,
)
from datapipe_app.capabilities import collect_addon_capabilities
from datapipe_app.graph.discovery import build_stage_edges, build_stage_summary, extract_stages
from datapipe_app.graph.label_graph import (
    available_label_keys,
    build_label_graph,
    default_label_key,
)
from datapipe_app.meta_sql import require_sql_transform_meta
from datapipe_app.pipeline_steps import pipeline_step_labels

_PRIMITIVE_STEP_TYPES = (
    BatchGenerate,
    BatchTransform,
    DatatableBatchTransform,
    DatatableTransform,
    UpdateExternalTable,
)


def _package_version() -> str:
    try:
        return importlib.metadata.version("datapipe-app")
    except Exception:
        return "unknown"


def filter_steps_by_labels(
    steps: List[ComputeStep],
    labels: Labels = [],
    name_prefix: str = "",
) -> List[ComputeStep]:
    """Filter steps by label pairs (v1alpha3 semantics).

    Values for the *same* key are OR'd (match any); different keys are AND'd.
    """
    by_key: dict[str, set[str]] = {}
    for key, value in labels:
        by_key.setdefault(key, set()).add(value)

    res: List[ComputeStep] = []
    for step in steps:
        step_pairs = set(step.labels or [])
        matched = True
        for key, values in by_key.items():
            step_values = {v for k, v in step_pairs if k == key}
            if step_values.isdisjoint(values):
                matched = False
                break
        if matched and step.name.startswith(name_prefix):
            res.append(step)

    return res


def _is_primitive_step(pipeline_step: PipelineStep) -> bool:
    return isinstance(pipeline_step, _PRIMITIVE_STEP_TYPES)


def _pipeline_step_group_name(pipeline_step: PipelineStep) -> str:
    return pipeline_step.__class__.__name__


def _tables_for_steps(step_list: List[ComputeStep]) -> Set[str]:
    used: Set[str] = set()
    for step in step_list:
        used.update(i.dt.name for i in step.input_dts)
        used.update(o.dt.name for o in step.output_dts)
    return used


def _group_boundaries(group_steps: List[ComputeStep]) -> tuple[Set[str], Set[str]]:
    produced = {o.dt.name for step in group_steps for o in step.output_dts}
    consumed = {i.dt.name for step in group_steps for i in step.input_dts}
    return consumed - produced, produced - consumed


def get_table_store_schema(table_store: TableStore) -> List[models.TableColumnResponse]:
    if isinstance(table_store, TableStoreDB):
        return [
            models.TableColumnResponse(name=column.name, type=str(column.type))
            for column in table_store.data_sql_schema
        ]

    return [
        models.TableColumnResponse(name=column.name, type=str(column.type))
        for column in table_store.get_primary_schema()
    ]


def make_app(
    ds: DataStore,
    catalog: Catalog,
    pipeline: Pipeline,
    steps: List[ComputeStep],
    *,
    addons: Optional[Sequence[models.AddonCapability]] = None,
) -> FastAPI:
    app = FastAPI(title="Datapipe Ops API v1alpha3")

    @app.get("/capabilities", response_model=models.CapabilitiesResponse)
    def get_capabilities() -> models.CapabilitiesResponse:
        return models.CapabilitiesResponse(
            addons=collect_addon_capabilities(extra=addons),
        )

    @app.get("/settings", response_model=models.SettingsResponse)
    def get_settings() -> models.SettingsResponse:
        return models.SettingsResponse(version=_package_version())

    @app.get("/pipeline", response_model=models.PipelineDetailResponse)
    def get_pipeline_detail(
        label_key: Optional[str] = Query(None),
    ) -> models.PipelineDetailResponse:
        status_cache: dict[str, dict] = {}
        active_label_key = default_label_key(steps, label_key)
        return models.PipelineDetailResponse(
            stages=build_stage_summary(steps, ds, status_cache),
            stage_edges=build_stage_edges(steps),
            label_graph=build_label_graph(
                steps,
                ds,
                label_key=active_label_key,
                status_cache=status_cache,
            ),
            available_label_keys=available_label_keys(steps),
        )

    @app.get("/graph", response_model=models.GraphResponse)
    def get_graph(
        stage: Optional[str] = Query(None),
        label_key: str = Query("stage"),
    ) -> models.GraphResponse:
        selected_steps = (
            filter_steps_by_labels(steps, labels=[(label_key, stage)]) if stage else steps
        )

        def table_response(table_name: str) -> models.TableResponse:
            tbl = catalog.get_datatable(ds, table_name)
            return models.TableResponse(
                name=tbl.name,
                indexes=tbl.primary_keys,
                size=None,
                store_class=tbl.table_store.__class__.__name__,
                schema=get_table_store_schema(tbl.table_store),
            )

        def pipeline_step_response(step: ComputeStep) -> models.PipelineStepResponse:
            inputs = [i.dt.name for i in step.input_dts]
            outputs = [o.dt.name for o in step.output_dts]
            step_labels = [[k, v] for k, v in (step.labels or [])]

            if isinstance(step, BaseBatchTransformStep):
                step_status = None
                if app_settings.API_SETTINGS.show_step_status:
                    try:
                        step_status = step.get_status(ds=ds)
                    except Exception:
                        step_status = None

                return models.PipelineStepResponse(
                    type="transform",
                    transform_type=step.__class__.__name__,
                    name=step.get_name(),
                    indexes=step.transform_keys,
                    inputs=inputs,
                    outputs=outputs,
                    labels=step_labels,
                    has_transform_meta=True,
                    total_idx_count=(step_status.total_idx_count if step_status else None),
                    changed_idx_count=(step_status.changed_idx_count if step_status else None),
                )

            return models.PipelineStepResponse(
                type="transform",
                transform_type=step.__class__.__name__,
                name=step.get_name(),
                inputs=inputs,
                outputs=outputs,
                labels=step_labels,
            )

        selected_names = {step.get_name() for step in selected_steps}
        pipeline_nodes: List[models.PipelineNodeResponse] = []
        top_level_tables: Set[str] = set()

        for pipeline_step in pipeline.steps:
            group_steps = pipeline_step.build_compute(ds, catalog)
            visible = [step for step in group_steps if step.get_name() in selected_names]
            if not visible:
                continue

            if len(group_steps) <= 1 and _is_primitive_step(pipeline_step):
                pipeline_nodes.append(pipeline_step_response(visible[0]))
                top_level_tables.update(_tables_for_steps(visible))
                continue

            group_name = _pipeline_step_group_name(pipeline_step)
            external_inputs, external_outputs = _group_boundaries(visible)
            internal_tables = _tables_for_steps(visible)
            subgroup = models.GraphResponse(
                catalog={table_name: table_response(table_name) for table_name in sorted(internal_tables)},
                pipeline=[pipeline_step_response(step) for step in visible],
                stages=extract_stages(visible),
            )
            pipeline_nodes.append(
                models.MetaPipelineStepResponse(
                    type="meta",
                    name=group_name,
                    transform_type=group_name,
                    inputs=sorted(external_inputs),
                    outputs=sorted(external_outputs),
                    labels=[[k, v] for k, v in pipeline_step_labels(pipeline_step)],
                    graph=subgroup,
                )
            )
            top_level_tables.update(external_inputs)
            top_level_tables.update(external_outputs)

        catalog_names = (
            [name for name in catalog.catalog.keys() if name in top_level_tables]
            if stage
            else list(catalog.catalog.keys())
        )

        return models.GraphResponse(
            catalog={table_name: table_response(table_name) for table_name in catalog_names},
            pipeline=pipeline_nodes,
            stages=extract_stages(selected_steps),
        )

    @app.post("/get-table-data", response_model=models.GetDataResponse)
    def get_data_post_api(req: models.GetDataRequest) -> models.GetDataResponse:
        return get_table_data(ds, catalog, req)

    @app.post("/get-transform-data", response_model=models.GetDataResponse)
    def get_meta_data_api(req: models.GetDataRequest) -> models.GetDataResponse:
        filtered_steps = filter_steps_by_labels(steps, name_prefix=req.table)
        if len(filtered_steps) != 1:
            raise HTTPException(status_code=404, detail="Step not found")
        step = filtered_steps[0]

        if not isinstance(step, BaseBatchTransformStep):
            return models.GetDataResponse(
                page=req.page,
                page_size=req.page_size,
                total=0,
                data=[],
            )

        return get_transform_data(step, req)

    @app.get("/tables/{table_name}/size", response_model=models.TableSizeResponse)
    def get_table_size(table_name: str) -> models.TableSizeResponse:
        if table_name not in catalog.catalog:
            raise HTTPException(status_code=404, detail=f"Table {table_name} not found")
        tbl = catalog.get_datatable(ds, table_name)
        return models.TableSizeResponse(table=table_name, size=tbl.get_size())

    @app.get("/transforms/{transform_name}/meta-size", response_model=models.TableSizeResponse)
    def get_transform_meta_size(transform_name: str) -> models.TableSizeResponse:
        filtered_steps = filter_steps_by_labels(steps, name_prefix=transform_name)
        if len(filtered_steps) != 1:
            raise HTTPException(status_code=404, detail="Step not found")
        step = filtered_steps[0]
        if not isinstance(step, BaseBatchTransformStep):
            raise HTTPException(status_code=400, detail="Transform does not have SQL metadata")
        transform_meta = require_sql_transform_meta(step.meta)
        sql_count = select(count()).select_from(transform_meta.sql_table)
        with transform_meta.dbconn.con.begin() as conn:
            total = conn.execute(sql_count).scalar_one_or_none()
            assert total is not None
        return models.TableSizeResponse(table=transform_name, size=total)

    @app.post(
        "/transforms/{transform_name}/reset-metadata",
        response_model=models.ResetTransformMetadataResponse,
    )
    def reset_transform_metadata(transform_name: str) -> models.ResetTransformMetadataResponse:
        filtered_steps = filter_steps_by_labels(steps, name_prefix=transform_name)
        if len(filtered_steps) != 1:
            raise HTTPException(status_code=404, detail=f"Transform {transform_name} not found")
        step = filtered_steps[0]
        if not isinstance(step, BaseBatchTransformStep):
            raise HTTPException(
                status_code=400,
                detail=f"Transform {transform_name} does not have SQL metadata",
            )
        step.reset_metadata(ds)
        return models.ResetTransformMetadataResponse(transform_name=transform_name)

    running_steps_helper = RunningStepsHelper()

    @app.websocket("/ws/transform/{transform}/run-status")
    async def ws_transform_run_status(websocket: WebSocket, transform: str) -> None:
        await running_steps_helper.add_ws(websocket, transform)
        try:
            while True:
                payload = await websocket.receive_json()
                json_data = models.RunStepRequest.model_validate(payload)
                if json_data.operation != "run-step":
                    continue
                if state := running_steps_helper.get(transform):
                    await websocket.send_json(state.model_dump(mode="json"))
                    continue
                filtered_steps = filter_steps_by_labels(steps, name_prefix=transform)
                if len(filtered_steps) != 1:
                    await websocket.send_json({"status": "not found"})
                    continue

                step = filtered_steps[0]
                if not isinstance(step, BaseBatchTransformStep):
                    await websocket.send_json({"status": "not allowed"})
                    continue

                running_steps_helper[transform] = models.RunStepResponse(
                    status="starting",
                    processed=0,
                    total=0,
                )
                _ = asyncio.create_task(running_steps_helper.update_transform_status(transform=transform))
                run_step_thread = asyncio.to_thread(
                    run_step,
                    ds,
                    step,
                    running_steps_helper[transform],
                    json_data.filters,
                )
                run_steps_task = asyncio.create_task(run_step_thread)
                run_steps_task.add_done_callback(
                    lambda _: running_steps_helper.set_job_as_finished(transform)
                )
        except WebSocketDisconnect:
            running_steps_helper.remove_ws(websocket, transform)

    FastAPIInstrumentor.instrument_app(app, excluded_urls="docs")
    return app

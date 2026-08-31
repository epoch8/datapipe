"""Slim Datapipe Ops API v1alpha3 (cloud): pipeline UI + meta, no runs/logs."""

from __future__ import annotations

import asyncio
import importlib.metadata
from typing import List, Optional, Sequence

from datapipe.compute import Catalog, ComputeStep, DataStore, Pipeline
from datapipe.step.batch_transform import BaseBatchTransformStep
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from sqlalchemy.sql.expression import select
from sqlalchemy.sql.functions import count

from datapipe_app import models
from datapipe_app import settings as app_settings
from datapipe_app.api_v1alpha2 import (
    RunningStepsHelper,
    filter_steps_by_labels,
    get_table_data,
    get_transform_data,
    run_step,
)
from datapipe_app.capabilities import collect_addon_capabilities
from datapipe_app.meta_sql import require_sql_transform_meta

def _package_version() -> str:
    try:
        return importlib.metadata.version("datapipe-app")
    except Exception:
        return "unknown"


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

    @app.get("/graph", response_model=models.GraphResponse)
    def get_graph() -> models.GraphResponse:
        def table_response(table_name: str) -> models.TableResponse:
            tbl = catalog.get_datatable(ds, table_name)
            return models.TableResponse(
                name=tbl.name,
                indexes=tbl.primary_keys,
                size=tbl.get_size(),
                store_class=tbl.table_store.__class__.__name__,
            )

        def pipeline_step_response(step: ComputeStep) -> models.PipelineStepResponse:
            inputs = [i.dt.name for i in step.input_dts]
            outputs = [i.dt.name for i in step.output_dts]

            if isinstance(step, BaseBatchTransformStep):
                step_status = (
                    step.get_status(ds=ds) if app_settings.API_SETTINGS.show_step_status else None
                )
                return models.PipelineStepResponse(
                    type="transform",
                    transform_type=step.__class__.__name__,
                    name=step.get_name(),
                    indexes=step.transform_keys,
                    inputs=inputs,
                    outputs=outputs,
                    total_idx_count=(step_status.total_idx_count if step_status else None),
                    changed_idx_count=(step_status.changed_idx_count if step_status else None),
                )

            return models.PipelineStepResponse(
                type="transform",
                transform_type=step.__class__.__name__,
                name=step.get_name(),
                inputs=inputs,
                outputs=outputs,
            )

        return models.GraphResponse(
            catalog={table_name: table_response(table_name) for table_name in catalog.catalog.keys()},
            pipeline=[pipeline_step_response(step) for step in steps],
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

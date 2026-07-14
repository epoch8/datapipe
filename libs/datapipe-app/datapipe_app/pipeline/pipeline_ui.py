"""Pipeline graph, table browsing, and transform run WebSocket routes for api v1alpha3."""

from __future__ import annotations

import asyncio
import math
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

import pandas as pd
from datapipe.compute import Catalog, ComputeStep, DataStore, Pipeline, PipelineStep
from datapipe.step.batch_generate import BatchGenerate
from datapipe.step.batch_transform import BaseBatchTransformStep, BatchTransform, DatatableBatchTransform
from datapipe.step.datatable_transform import DatatableTransform
from datapipe.step.update_external_table import UpdateExternalTable
from datapipe.store.database import TableStoreDB
from datapipe.types import IndexDF
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from sqlalchemy.sql.expression import and_, or_, select
from sqlalchemy.sql.functions import count, func

from datapipe_app.app import models
from datapipe_app.sql.query import apply_table_order_by
from datapipe_app.api.v1alpha1 import filter_steps_by_labels
from datapipe_app.app.meta_sql import require_sql_transform_meta
from datapipe_app.observability.graph.discovery import extract_stages
from datapipe_app.observability.runs.recorder import RunRecorder
from datapipe_app.pipeline.pipeline_steps import pipeline_step_labels
from datapipe_app.app.settings import API_SETTINGS

_PRIMITIVE_STEP_TYPES = (
    BatchGenerate,
    BatchTransform,
    DatatableBatchTransform,
    DatatableTransform,
    UpdateExternalTable,
)


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


def get_table_store_db_data(table_store: TableStoreDB, req: models.GetDataRequest) -> models.GetDataResponse:
    sql_schema = table_store.data_sql_schema
    sql_table = table_store.data_table

    sql = select(*sql_schema).select_from(sql_table)
    if req.focus is not None:
        filtered_focus_idx = [
            {k: v for k, v in row.items() if k in table_store.primary_keys} for row in req.focus.items_idx
        ]
        primary_key_selectors = [and_(*[sql_table.c[k] == v for k, v in row.items()]) for row in filtered_focus_idx]
        sql = sql.where(or_(*primary_key_selectors))

    for col, val in req.filters.items():
        sql = sql.where(sql_table.c[col] == val)

    sql_count = select(count()).select_from(sql.subquery())

    if req.order_by:
        sql = apply_table_order_by(sql, sql_table, req.order_by, req.order)

    sql = sql.offset(req.page * req.page_size).limit(req.page_size)

    data_df = pd.read_sql_query(sql, con=table_store.dbconn.con)

    total: Optional[int] = None
    if req.include_total:
        with table_store.dbconn.con.begin() as conn:
            total = conn.execute(sql_count).scalar_one_or_none()
            assert total is not None

    return models.GetDataResponse(
        page=req.page,
        page_size=req.page_size,
        total=total,
        data=data_df.fillna("-").to_dict(orient="records"),
    )


def get_table_data(ds: DataStore, catalog: Catalog, req: models.GetDataRequest) -> models.GetDataResponse:
    dt = catalog.get_datatable(ds, req.table)
    table_store = dt.table_store

    if isinstance(table_store, TableStoreDB):
        return get_table_store_db_data(table_store, req)

    raise HTTPException(status_code=500, detail="Not implemented")


def get_table_store_schema(table_store: Any) -> List[models.TableColumnResponse]:
    if not isinstance(table_store, TableStoreDB):
        return []

    return [
        models.TableColumnResponse(name=column.name, type=str(column.type))
        for column in table_store.data_sql_schema
    ]


def get_transform_data(step: BaseBatchTransformStep, req: models.GetDataRequest) -> models.GetDataResponse:
    transform_meta = require_sql_transform_meta(step.meta)
    sql_table = transform_meta.sql_table
    sql_schema = transform_meta.sql_schema

    sql = select(*sql_schema).select_from(sql_table)

    if req.focus is not None:
        filtered_focus_idx = [
            {k: v for k, v in row.items() if k in transform_meta.transform_keys} for row in req.focus.items_idx
        ]
        primary_key_selectors = [and_(*[sql_table.c[k] == v for k, v in row.items()]) for row in filtered_focus_idx]
        sql = sql.where(or_(*primary_key_selectors))

    for col, val in req.filters.items():
        if col == "process_ts":
            val = datetime.fromisoformat(val).timestamp()
            sql = sql.where(func.abs(sql_table.c[col] - val) < 0.000001)
        else:
            sql = sql.where(sql_table.c[col] == val)

    sql_count = select(count()).select_from(sql.subquery())

    if req.order_by:
        sql = apply_table_order_by(sql, sql_table, req.order_by, req.order)

    sql = sql.offset(req.page * req.page_size).limit(req.page_size)

    transform_data = pd.read_sql_query(sql, con=transform_meta.dbconn.con)

    transform_data = transform_data.drop("priority", axis=1)
    transform_data["process_ts"] = pd.to_datetime(transform_data["process_ts"], unit="s", utc=True)

    total: Optional[int] = None
    if req.include_total:
        with transform_meta.dbconn.con.begin() as conn:
            total = conn.execute(sql_count).scalar_one_or_none()
            assert total is not None

    return models.GetDataResponse(
        page=req.page,
        page_size=req.page_size,
        total=total,
        data=transform_data.fillna("-").to_dict(orient="records"),
    )


class WebSocketManager:
    def __init__(self) -> None:
        self._connections: Set[WebSocket] = set()

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self._connections.add(websocket)

    def disconnect(self, websocket: WebSocket) -> None:
        self._connections.remove(websocket)

    async def broadcast_json(self, data: Dict[str, Any]) -> None:
        for ws in self._connections:
            try:
                await ws.send_json(data)
            except RuntimeError:
                pass

    def __len__(self) -> int:
        return len(self._connections)


class RunningStepsHelper(Dict[str, models.RunStepResponse]):
    def __init__(self) -> None:
        self._transform_web_sockets: Dict[str, WebSocketManager] = {}

    async def add_ws(self, websocket: WebSocket, transform: str) -> None:
        if transform not in self._transform_web_sockets:
            self._transform_web_sockets[transform] = WebSocketManager()
        await self._transform_web_sockets[transform].connect(websocket)

    def remove_ws(self, websocket: WebSocket, transform: str) -> None:
        if transform not in self._transform_web_sockets:
            return
        self._transform_web_sockets[transform].disconnect(websocket)
        if len(self._transform_web_sockets[transform]) == 0:
            del self._transform_web_sockets[transform]

    async def update_transform_status(self, transform: str) -> None:
        while self[transform].status != "finished":
            await self._update_transform_status(transform)
            await asyncio.sleep(0.5)
        await self._update_transform_status(transform=transform)
        del self[transform]

    async def _update_transform_status(self, transform: str) -> None:
        if transform not in self._transform_web_sockets:
            return
        state = self.get(transform)
        if state is None:
            return
        await self._transform_web_sockets[transform].broadcast_json(
            state.model_dump(mode="json"),
        )

    def set_job_as_finished(self, transform: str) -> None:
        self[transform].status = "finished"


def run_step(
    ds: DataStore,
    step: BaseBatchTransformStep,
    transform_state: models.RunStepResponse,
    filters: Optional[List[Dict]],
    recorder: Optional[RunRecorder] = None,
) -> None:
    def on_batch_progress(batches_done: int, total_batches: int) -> None:
        transform_state.total = total_batches
        transform_state.status = "running"
        transform_state.processed = batches_done * step.chunk_size

    def _run() -> None:
        if filters is not None:
            transform_meta = require_sql_transform_meta(step.meta)
            selected_data = [
                {k: v for k, v in row.items() if k in transform_meta.transform_keys} for row in filters
            ]
            idx = pd.DataFrame.from_records(selected_data)
            transform_state.total = len(selected_data)
            transform_state.status = "running"
            chunk_count = math.ceil(len(idx) / step.chunk_size) if len(idx) else 0
            for i in range(chunk_count):
                start = i * step.chunk_size
                end = (i + 1) * step.chunk_size
                step.run_idx(ds, IndexDF(idx.iloc[start:end]))
                transform_state.processed += step.chunk_size
            return

        step.run_full(ds=ds, on_batch_progress=on_batch_progress)

    if recorder is not None:
        run_id = recorder.start_run(trigger="websocket")
        transform_state.run_id = run_id
        recorder.start_step(step.name, run_id=run_id)
        try:
            _run()
            recorder.finish_step(step.name, status="completed", run_id=run_id)
            recorder.finish_run(status="completed", run_id=run_id)
        except Exception as exc:
            recorder.finish_step(step.name, status="failed", error=str(exc), run_id=run_id)
            recorder.finish_run(status="failed", error=str(exc), run_id=run_id)
            raise
    else:
        _run()


def register_pipeline_ui_routes(
    app: FastAPI,
    ds: DataStore,
    catalog: Catalog,
    pipeline: Pipeline,
    steps: List[ComputeStep],
    recorder: Optional[RunRecorder] = None,
) -> None:
    @app.get("/graph", response_model=models.GraphResponse)
    def get_graph(stage: Optional[str] = Query(None)) -> models.GraphResponse:
        selected_steps = (
            filter_steps_by_labels(steps, labels=[("stage", stage)]) if stage else steps
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
                if API_SETTINGS.show_step_status:
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

    @app.get("/tables/{table_name}/size", response_model=models.TableSizeResponse)
    def get_table_size(table_name: str) -> models.TableSizeResponse:
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

    @app.post("/get-transform-data")
    def get_meta_data_api(req: models.GetDataRequest) -> models.GetDataResponse:
        filtered_steps = filter_steps_by_labels(steps, name_prefix=req.table)
        if len(filtered_steps) != 1:
            raise HTTPException(status_code=404, detail="Step not found")
        step = filtered_steps[0]

        if not isinstance(step, BaseBatchTransformStep):
            return models.GetDataResponse(
                page=req.page,
                page_size=req.page_size,
                total=None,
                data=[],
            )

        return get_transform_data(step, req)

    running_steps_helper = RunningStepsHelper()

    @app.websocket("/ws/transform/{transform}/run-status")
    async def ws_transform_run_status(websocket: WebSocket, transform: str) -> None:
        await running_steps_helper.add_ws(websocket, transform)
        try:
            while True:
                json = await websocket.receive_json()
                json_data = models.RunStepRequest.model_validate(json)
                if json_data.operation != "run-step":
                    continue
                if state := running_steps_helper.get(transform):
                    await websocket.send_json(
                        state.model_dump(mode="json"),
                    )
                    continue
                filtered_steps = filter_steps_by_labels(steps, name_prefix=transform)
                if len(filtered_steps) != 1:
                    await websocket.send_json({"status": "not found"})

                step = filtered_steps[0]
                if not isinstance(step, BaseBatchTransformStep):
                    await websocket.send_json({"status": "not allowed"})

                else:
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
                        recorder,
                    )
                    run_steps_task = asyncio.create_task(run_step_thread)
                    run_steps_task.add_done_callback(
                        lambda _: running_steps_helper.set_job_as_finished(transform)
                    )
        except WebSocketDisconnect:
            running_steps_helper.remove_ws(websocket, transform)

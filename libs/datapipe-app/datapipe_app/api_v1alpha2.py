import asyncio
import copy
import math
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

import pandas as pd
from datapipe.compute import Catalog, ComputeStep, DataStore, Pipeline, run_steps
from datapipe.run_config import RunConfig
from datapipe.step.batch_transform import BaseBatchTransformStep
from datapipe.store.database import TableStoreDB
from datapipe.types import IndexDF, Labels
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from sqlalchemy.sql.expression import and_, or_, select, text
from sqlalchemy.sql.functions import count, func

from datapipe_app import models
from datapipe_app.meta_sql import require_sql_transform_meta
from datapipe_app.settings import API_SETTINGS


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
        sql = sql.where(text(f"{req.order_by} is not null"))
        sql = sql.order_by(text(f"{req.order_by} {req.order}"))

    sql.offset(req.page * req.page_size).limit(req.page_size)

    data_df = pd.read_sql_query(sql, con=table_store.dbconn.con)

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
            # Postgres 7 digits precision, datetime.timestamp() has 6 digits so we need to add some tolerance
            sql = sql.where(func.abs(sql_table.c[col] - val) < 0.000001)
        else:
            sql = sql.where(sql_table.c[col] == val)

    sql_count = select(count()).select_from(sql.subquery())

    if req.order_by:
        sql = sql.where(text(f"{req.order_by} is not null"))
        sql = sql.order_by(text(f"{req.order_by} {req.order}"))

    sql = sql.offset(req.page * req.page_size).limit(req.page_size)

    transform_data = pd.read_sql_query(sql, con=transform_meta.dbconn.con)

    transform_data = transform_data.drop("priority", axis=1)
    transform_data["process_ts"] = pd.to_datetime(transform_data["process_ts"], unit="s", utc=True)

    with transform_meta.dbconn.con.begin() as conn:
        total = conn.execute(sql_count).scalar_one_or_none()
        assert total is not None

    return models.GetDataResponse(
        page=req.page,
        page_size=req.page_size,
        total=total,
        data=transform_data.fillna("-").to_dict(orient="records"),
    )


def filter_steps_by_labels(steps: List[ComputeStep], labels: Labels = [], name_prefix: str = "") -> List[ComputeStep]:
    res = []
    for step in steps:
        for k, v in labels:
            if (k, v) not in step.labels:
                break
        else:
            if step.name.startswith(name_prefix):
                res.append(step)

    return res


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
) -> None:
    # Before we progress callback to datapipe-core we need to do this shenanigans 💀
    _step = copy.copy(step)
    transform_meta = require_sql_transform_meta(_step.meta)
    if filters is not None:
        selected_data = [{k: v for k, v in row.items() if k in transform_meta.transform_keys} for row in filters]
        idx = pd.DataFrame.from_records(selected_data)
        transform_state.total = len(selected_data)
        transform_state.status = "running"

        def get_full_process_ids(
            ds: DataStore,
            chunk_size: Optional[int] = None,
            run_config: Optional[RunConfig] = None,
        ):
            idx_total = len(selected_data)

            def updating_idx_gen():
                chunk_count = math.ceil(idx_total / _step.chunk_size)
                for i in range(chunk_count):
                    start = i * _step.chunk_size
                    end = (i + 1) * _step.chunk_size
                    yield IndexDF(idx.iloc[start:end])
                    transform_state.processed += _step.chunk_size

            return idx_total, updating_idx_gen()

    else:
        _get_full_process_ids = _step.get_full_process_ids

        def get_full_process_ids(
            ds: DataStore,
            chunk_size: Optional[int] = None,
            run_config: Optional[RunConfig] = None,
        ):
            idx_total, idx_gen = _get_full_process_ids(ds, chunk_size, run_config)
            transform_state.total = idx_total
            transform_state.status = "running"

            def updating_idx_gen():
                for idx in idx_gen:
                    yield idx
                    transform_state.processed += _step.chunk_size

            return idx_total, updating_idx_gen()

    # FIXME this should not be necessary
    _step.get_full_process_ids = get_full_process_ids  # type: ignore
    run_steps(ds=ds, steps=[_step])


def make_app(
    ds: DataStore,
    catalog: Catalog,
    pipeline: Pipeline,
    steps: List[ComputeStep],
) -> FastAPI:
    app = FastAPI()

    @app.get("/graph", response_model=models.GraphResponse)
    def get_graph() -> models.GraphResponse:
        def table_response(table_name) -> models.TableResponse:
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
                step_status = step.get_status(ds=ds) if API_SETTINGS.show_step_status else None

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

            else:
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

    @app.post("/get-transform-data")
    def get_meta_data_api(req: models.GetDataRequest) -> models.GetDataResponse:
        filtered_steps = filter_steps_by_labels(steps, name_prefix=req.table)
        if len(filtered_steps) != 1:
            raise HTTPException(status_code=404, detail="Step not found")
        step = filtered_steps[0]

        # maybe infer on type or smth?
        if not isinstance(step, BaseBatchTransformStep):
            return models.GetDataResponse(
                page=req.page,
                page_size=req.page_size,
                total=0,
                data=[],
            )

        return get_transform_data(step, req)

    _running_steps_helper = RunningStepsHelper()

    @app.websocket("/ws/transform/{transform}/run-status")
    async def ws_transform_run_status(websocket: WebSocket, transform: str):
        await _running_steps_helper.add_ws(websocket, transform)
        try:
            while True:
                json = await websocket.receive_json()
                json_data = models.RunStepRequest.model_validate(json)
                if json_data.operation != "run-step":
                    continue
                if state := _running_steps_helper.get(transform):
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
                    _running_steps_helper[transform] = models.RunStepResponse(
                        status="starting",
                        processed=0,
                        total=0,
                    )
                    _ = asyncio.create_task(_running_steps_helper.update_transform_status(transform=transform))
                    run_step_thread = asyncio.to_thread(
                        run_step, ds, step, _running_steps_helper[transform], json_data.filters
                    )
                    run_steps_task = asyncio.create_task(run_step_thread)
                    run_steps_task.add_done_callback(lambda _: _running_steps_helper.set_job_as_finished(transform))
        except WebSocketDisconnect:
            _running_steps_helper.remove_ws(websocket, transform)

    FastAPIInstrumentor.instrument_app(app, excluded_urls="docs")

    return app

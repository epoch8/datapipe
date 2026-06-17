from typing import Any, Dict, List, Literal, Optional, Tuple, cast

import pandas as pd
from datapipe.compute import (
    Catalog,
    ComputeStep,
    DataStore,
    Pipeline,
    run_steps,
    run_steps_changelist,
)
from datapipe.step.batch_transform import BaseBatchTransformStep
from datapipe.store.database import TableStoreDB
from datapipe.types import ChangeList, IndexDF, Labels
from fastapi import BackgroundTasks, FastAPI, Query, Response
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from pydantic import BaseModel, Field
from sqlalchemy.sql.expression import and_, asc, desc, select, text
from sqlalchemy.sql.functions import count

from datapipe_app.meta_sql import require_sql_table_meta
from datapipe_app.observability.recorder import RunRecorder
from datapipe_app.settings import API_SETTINGS


class PipelineStepResponse(BaseModel):
    name: str

    type_: str = Field(alias="type")
    transform_type: str

    indexes: Optional[List[str]] = None

    inputs: List[str]
    outputs: List[str]

    total_idx_count: Optional[int] = None
    changed_idx_count: Optional[int] = None


class TableResponse(BaseModel):
    name: str

    indexes: List[str]

    size: int
    store_class: str


class GraphResponse(BaseModel):
    catalog: Dict[str, TableResponse]
    pipeline: List[PipelineStepResponse]


class UpdateDataRequest(BaseModel):
    table_name: str
    upsert: Optional[List[Dict]] = None
    enable_changelist: bool = True
    background: bool = False
    labels: Labels = []
    # delete: List[Dict] = None


class UpdateDataResponse(BaseModel):
    result: str


class GetDataRequest(BaseModel):
    table: str
    filters: Dict[str, Any] = {}
    page: int = 0
    page_size: int = 20
    order_by: Optional[str] = None
    order: Literal["asc", "desc"] = "asc"


class GetDataResponse(BaseModel):
    page: int
    page_size: int
    total: int
    data: List[Dict]


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


def update_data(
    ds: DataStore,
    catalog: Catalog,
    steps: List[ComputeStep],
    background_tasks: BackgroundTasks,
    table_name: str,
    upsert: Optional[List[Dict]],
    background: bool,
    enable_changelist: bool = True,
) -> UpdateDataResponse:
    dt = catalog.get_datatable(ds, table_name)

    cl = ChangeList()

    if upsert is not None and len(upsert) > 0:
        idx = dt.store_chunk(pd.DataFrame.from_records(upsert))

        cl.append(dt.name, idx)

    # if req.delete is not None and len(req.delete) > 0:
    #     idx = dt.delete_by_idx(
    #         pd.DataFrame.from_records(req.delete)
    #     )

    #     cl.append(dt.name, idx)
    if enable_changelist:
        if background:
            background_tasks.add_task(run_steps_changelist, ds=ds, steps=steps, changelist=cl)
        else:
            run_steps_changelist(ds=ds, steps=steps, changelist=cl)

    return UpdateDataResponse(result="ok")


def get_data_get_pd(
    ds: DataStore,
    catalog: Catalog,
    table: str,
    page: int,
    page_size: int,
    filters: Optional[IndexDF],
    order_by: Optional[List[str]],
    order: Literal["asc", "desc"],
) -> Tuple[int, pd.DataFrame, pd.DataFrame]:
    dt = catalog.get_datatable(ds, table)
    table_meta = require_sql_table_meta(dt.meta)

    meta_schema = table_meta.sql_schema
    meta_tbl = table_meta.sql_table

    sql: Any = select(*meta_schema)  # type: ignore
    sql = sql.where(meta_tbl.c.delete_ts.is_(None))
    if filters is not None:
        sql = sql.where(
            and_(
                *[meta_tbl.c[column].in_(filters[column]) for column in filters.columns],
                meta_tbl.c["delete_ts"].is_(None),
            )
        )
    else:
        sql = sql.where(meta_tbl.c["delete_ts"].is_(None))
    sql_count = select(count()).select_from(sql)
    with ds.meta_dbconn.con.begin() as conn:
        total_count = conn.execute(sql_count).scalar()

    assert total_count is not None

    data_df: pd.DataFrame
    if page * page_size > total_count:
        meta_df = pd.DataFrame(columns=[x.name for x in meta_schema])  # type: ignore
        data_df = dt.get_data(cast(IndexDF, meta_df))
    else:
        if order_by is not None:
            if order == "asc":
                sql = sql.order_by(
                    asc(*[meta_tbl.c[column] for column in order_by]),
                )
            elif order == "desc":
                sql = sql.order_by(
                    desc(*[meta_tbl.c[column] for column in order_by]),
                )
        sql = sql.offset(page * page_size).limit(page_size)
        meta_df = pd.read_sql_query(
            sql,
            con=ds.meta_dbconn.con,
        )

        if not meta_df.empty:
            data_df = dt.get_data(cast(IndexDF, meta_df))
            data_df = meta_df.merge(data_df)[data_df.columns]  # save order
        else:
            data_df = pd.DataFrame(columns=[x.name for x in meta_schema])  # type: ignore

    return total_count, meta_df, data_df


def get_data_get(
    ds: DataStore,
    catalog: Catalog,
    table: str,
    page: int = 0,
    page_size: int = 20,
    filters: Optional[IndexDF] = None,
    order_by: Optional[List[str]] = None,
    order: Optional[Literal["asc", "desc"]] = None,
) -> GetDataResponse:
    if order is None:
        order = "asc"

    total_count, meta_df, data_df = get_data_get_pd(
        ds=ds,
        catalog=catalog,
        table=table,
        page=page,
        page_size=page_size,
        filters=filters,
        order_by=order_by,
        order=order,
    )
    return GetDataResponse(
        page=page,
        page_size=page_size,
        total=total_count,
        data=data_df.fillna("").to_dict(orient="records"),
    )


def get_data_post(ds: DataStore, catalog: Catalog, req: GetDataRequest) -> GetDataResponse:
    dt = catalog.get_datatable(ds, req.table)

    assert isinstance(dt.table_store, TableStoreDB)

    sql_schema = dt.table_store.data_sql_schema
    sql_table = dt.table_store.data_table

    sql = select(*sql_schema).select_from(sql_table)
    # Data table has no delete_ts
    # sql = sql.where(sql_table.c.delete_ts.is_(None))
    if req.order_by:
        sql = sql.where(text(f"{req.order_by} is not null"))
        sql = sql.order_by(text(f"{req.order_by} {req.order}"))
    sql = sql.offset(req.page * req.page_size).limit(req.page_size)

    for col, val in req.filters.items():
        sql = sql.where(sql_table.c[col] == val)

    sql_count = select(count()).select_from(sql_table)
    for col, val in req.filters.items():
        sql_count = sql_count.where(sql_table.c[col] == val)

    meta_df = pd.read_sql_query(
        sql,
        con=ds.meta_dbconn.con,
    )

    if not meta_df.empty:
        data_df: pd.DataFrame = dt.get_data(cast(IndexDF, meta_df))
        if req.order_by is not None:
            ascending = req.order == "asc"
            data_df.sort_values(by=req.order_by, ascending=ascending, inplace=True)
    else:
        data_df = pd.DataFrame()

    with dt.table_store.dbconn.con.begin() as conn:
        total_res = conn.execute(sql_count).fetchone()
        assert total_res is not None
        return GetDataResponse(
            page=req.page,
            page_size=req.page_size,
            total=total_res[0],
            data=data_df.fillna("").to_dict(orient="records"),
        )


def make_app(
    ds: DataStore,
    catalog: Catalog,
    pipeline: Pipeline,
    steps: List[ComputeStep],
    recorder: Optional[RunRecorder] = None,
) -> FastAPI:
    app = FastAPI()

    @app.get("/graph", response_model=GraphResponse)
    def get_graph() -> GraphResponse:
        def table_response(table_name) -> TableResponse:
            tbl = catalog.get_datatable(ds, table_name)

            return TableResponse(
                name=tbl.name,
                indexes=tbl.primary_keys,
                size=tbl.get_size(),
                store_class=tbl.table_store.__class__.__name__,
            )

        def pipeline_step_response(step: ComputeStep) -> PipelineStepResponse:
            inputs = [i.dt.name for i in step.input_dts]
            outputs = [i.dt.name for i in step.output_dts]

            if isinstance(step, BaseBatchTransformStep):

                step_status = step.get_status(ds=ds) if API_SETTINGS.show_step_status else None

                return PipelineStepResponse(
                    type="transform",
                    transform_type=step.__class__.__name__,
                    name=step.get_name(),
                    indexes=step.transform_keys,
                    inputs=inputs,
                    outputs=outputs,
                    total_idx_count=step_status.total_idx_count if step_status else None,
                    changed_idx_count=step_status.changed_idx_count if step_status else None,
                )

            else:
                return PipelineStepResponse(
                    type="transform",
                    transform_type=step.__class__.__name__,
                    name=step.get_name(),
                    inputs=inputs,
                    outputs=outputs,
                )

        return GraphResponse(
            catalog={table_name: table_response(table_name) for table_name in catalog.catalog.keys()},
            pipeline=[pipeline_step_response(step) for step in steps],
        )

    @app.post("/update-data", response_model=UpdateDataResponse)
    def update_data_api(
        req: UpdateDataRequest,
        background_tasks: BackgroundTasks,
    ) -> UpdateDataResponse:
        return update_data(
            ds=ds,
            catalog=catalog,
            steps=filter_steps_by_labels(steps, labels=req.labels),
            background_tasks=background_tasks,
            table_name=req.table_name,
            upsert=req.upsert,
            background=req.background,
            enable_changelist=req.enable_changelist,
        )

    # /table/<table_name>?page=1&id=111&another_filter=value&sort=<+|->column_name
    @app.get("/get-data", response_model=GetDataResponse)
    def get_data_get_api(
        table: str,
        page: int = 0,
        page_size: int = 20,
    ) -> GetDataResponse:
        return get_data_get(ds, catalog, table, page, page_size)

    @app.post("/get-data", response_model=GetDataResponse)
    def get_data_post_api(req: GetDataRequest) -> GetDataResponse:
        return get_data_post(ds, catalog, req)

    class FocusFilter(BaseModel):
        table_name: str
        items_idx: List[Dict]

    class GetDataWithFocusRequest(BaseModel):
        table_name: str

        page: int = 0
        page_size: int = 20

        focus: Optional[FocusFilter] = None

    @app.post("/get-data-with-focus", response_model=GetDataResponse)
    def get_data_with_focus(req: GetDataWithFocusRequest) -> GetDataResponse:
        dt = catalog.get_datatable(ds, req.table_name)

        if req.focus is not None:
            idx = cast(
                IndexDF,
                pd.DataFrame.from_records(
                    [{k: v for item in req.focus.items_idx for k, v in item.items() if k in dt.primary_keys}]
                ).dropna(),
            )
        else:
            idx = None

        existing_idx = dt.meta.get_existing_idx(idx=idx)

        start_index = req.page * req.page_size
        end_index = (req.page + 1) * req.page_size
        data_df: pd.DataFrame = dt.get_data(cast(IndexDF, existing_idx.iloc[start_index:end_index]))
        return GetDataResponse(
            page=req.page,
            page_size=req.page_size,
            total=len(existing_idx),
            data=data_df.to_dict(orient="records"),
        )

    class GetDataByIdxRequest(BaseModel):
        table_name: str
        idx: List[Dict]

    @app.post("/get-data-by-idx")
    def get_data_by_idx(req: GetDataByIdxRequest):
        dt = catalog.get_datatable(ds, req.table_name)

        res: pd.DataFrame = dt.get_data(idx=cast(IndexDF, pd.DataFrame.from_records(req.idx)))

        return res.to_dict(orient="records")

    @app.post("/run")
    def run():
        if recorder is not None:
            run_id = recorder.start_run(trigger="v1alpha1")
            run_error: Optional[str] = None
            run_status = "completed"
            try:
                for step in steps:
                    recorder.start_step(step.name)
                    try:
                        step.run_full(ds=ds, run_config=None, executor=None)
                        recorder.finish_step(step.name, status="completed")
                    except Exception as exc:
                        run_status = "failed"
                        run_error = str(exc)
                        recorder.finish_step(step.name, status="failed", error=run_error)
                        raise
            except Exception:
                if run_status != "failed":
                    run_status = "failed"
                    run_error = run_error or "run failed"
            finally:
                recorder.finish_run(status=run_status, error=run_error)
            return {"run_id": run_id, "status": run_status}
        run_steps(ds=ds, steps=steps)
        return {"result": "ok"}

    # TODO refactor out to component based extension system
    # TODO automatic setup of webhook on project creation
    @app.post("/labelstudio-webhook")
    def labelstudio_webhook(
        request: Dict,
        background_tasks: BackgroundTasks,
        table_name: str = Query(..., title="Input table name"),
        data_field: List = Query(..., title="Fields to get from data"),
        background: bool = Query(False, title="Run as Background Task (default = False)"),
    ) -> UpdateDataResponse:
        upsert = [
            {
                **{k: v for k, v in request["task"]["data"].items() if k in data_field},
                "annotations": [request["annotation"]],
            }
        ]

        return update_data(
            ds=ds,
            catalog=catalog,
            steps=steps,
            background_tasks=background_tasks,
            table_name=table_name,
            upsert=upsert,
            background=background,
        )

    @app.get("/get-file")
    def get_file(filepath: str):
        import mimetypes

        import fsspec

        with fsspec.open(filepath) as f:
            mime = mimetypes.guess_type(filepath)
            assert mime[0] is not None
            return Response(content=f.read(), media_type=mime[0])

    FastAPIInstrumentor.instrument_app(app, excluded_urls="docs")

    return app

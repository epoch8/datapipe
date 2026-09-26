"""Browse helpers for ``/get-table-data`` across TableStore backends."""

from __future__ import annotations

from typing import Any, List, Optional, Sequence

import pandas as pd
from datapipe.compute import Catalog, DataStore
from datapipe.datatable import DataTable
from datapipe.store.database import TableStoreDB
from datapipe.store.filedir import JSONFile, PandasParquetFile, TableStoreFiledir
from datapipe.store.table_store import TableDataSingleFileStore, TableStore
from datapipe.types import IndexDF
from sqlalchemy.sql.expression import and_, or_, select
from sqlalchemy.sql.functions import count

from datapipe_app.app import models
from datapipe_app.app.meta_sql import require_sql_table_meta
from datapipe_app.sql.query import apply_table_order_by

# Adapters whose file payload is reasonably JSON-serializable for the UI.
_FILEDIR_READ_DATA_ADAPTERS = (JSONFile, PandasParquetFile)


def _json_safe_cell(value: Any) -> Any:
    if value is None or isinstance(value, (bool, str)):
        return value
    if isinstance(value, bytes):
        return f"<bytes len={len(value)}>"
    if isinstance(value, (list, dict)):
        return value
    # numpy / pandas scalars → plain Python
    item = getattr(value, "item", None)
    if callable(item):
        try:
            return item()
        except (ValueError, TypeError):
            pass
    if isinstance(value, (int, float)):
        return value
    # PIL.Image, ndarray, Path, etc.
    type_name = type(value).__name__
    shape = getattr(value, "shape", None)
    if shape is not None:
        return f"<{type_name} shape={tuple(shape)}>"
    return f"<{type_name}>"


def _df_to_records(df: pd.DataFrame) -> List[dict]:
    if df.empty:
        return []
    records = df.where(pd.notnull(df), None).to_dict(orient="records")
    return [{k: _json_safe_cell(v) for k, v in row.items()} for row in records]


def _response(req: models.GetDataRequest, data_df: pd.DataFrame, total: Optional[int]) -> models.GetDataResponse:
    return models.GetDataResponse(
        page=req.page,
        page_size=req.page_size,
        total=total,
        data=_df_to_records(data_df),
    )


def _apply_focus_and_filters(
    sql: Any,
    sql_table: Any,
    allowed_cols: Sequence[str],
    req: models.GetDataRequest,
) -> Any:
    allowed = set(allowed_cols)
    if req.focus is not None:
        filtered_focus_idx = [{k: v for k, v in row.items() if k in allowed} for row in req.focus.items_idx]
        filtered_focus_idx = [row for row in filtered_focus_idx if row]
        if filtered_focus_idx:
            primary_key_selectors = [and_(*[sql_table.c[k] == v for k, v in row.items()]) for row in filtered_focus_idx]
            sql = sql.where(or_(*primary_key_selectors))

    for col, val in req.filters.items():
        if col in allowed:
            sql = sql.where(sql_table.c[col] == val)

    return sql


def page_meta_pk(
    dt: DataTable,
    req: models.GetDataRequest,
) -> tuple[pd.DataFrame, Optional[int]]:
    """Paginate the table's SQL meta rows, returning primary-key columns only."""
    table_meta = require_sql_table_meta(dt.meta)
    sql_table = table_meta.sql_table
    pk_cols = list(table_meta.primary_keys)
    if not pk_cols:
        return pd.DataFrame(), 0 if req.include_total else None

    sql = select(*[sql_table.c[c] for c in pk_cols]).select_from(sql_table)
    sql = sql.where(sql_table.c.delete_ts.is_(None))
    sql = _apply_focus_and_filters(sql, sql_table, pk_cols, req)

    total: Optional[int] = None
    if req.include_total:
        sql_count = select(count()).select_from(sql.subquery())
        with table_meta.dbconn.con.begin() as conn:
            total = conn.execute(sql_count).scalar_one_or_none()
            assert total is not None

    order_col = req.order_by if req.order_by in pk_cols else (pk_cols[0] if pk_cols else None)
    if order_col:
        sql = apply_table_order_by(sql, sql_table, order_col, req.order)

    sql = sql.offset(req.page * req.page_size).limit(req.page_size)
    pk_df = pd.read_sql_query(sql, con=table_meta.dbconn.con)
    return pk_df, total


def get_table_store_db_data(table_store: TableStoreDB, req: models.GetDataRequest) -> models.GetDataResponse:
    sql_schema = table_store.data_sql_schema
    sql_table = table_store.data_table

    sql = select(*sql_schema).select_from(sql_table)
    if req.focus is not None:
        filtered_focus_idx = [
            {k: v for k, v in row.items() if k in table_store.primary_keys} for row in req.focus.items_idx
        ]
        primary_key_selectors = [and_(*[sql_table.c[k] == v for k, v in row.items()]) for row in filtered_focus_idx]
        if primary_key_selectors:
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

    # Keep v1alpha3 DB preview identical to the historical TableStoreDB path.
    return models.GetDataResponse(
        page=req.page,
        page_size=req.page_size,
        total=total,
        data=data_df.fillna("-").to_dict(orient="records"),
    )


def get_table_meta_pk_data(dt: DataTable, req: models.GetDataRequest) -> models.GetDataResponse:
    pk_df, total = page_meta_pk(dt, req)
    return _response(req, pk_df, total)


def _read_rows_for_pk_page(dt: DataTable, pk_df: pd.DataFrame, *, read_data: bool = True) -> pd.DataFrame:
    if pk_df.empty:
        return pd.DataFrame(columns=list(dt.primary_keys))

    idx = IndexDF(pk_df[list(dt.primary_keys)].copy())
    store = dt.table_store
    try:
        if isinstance(store, TableStoreFiledir):
            data_df = store.read_rows(idx, read_data=read_data)
        else:
            data_df = store.read_rows(idx)
    except FileNotFoundError:
        # Meta can lag deleted files; still show the page of primary keys.
        return pk_df.copy()

    if data_df.empty:
        return pk_df.copy()

    # Preserve meta page order.
    return pk_df.merge(data_df, on=list(dt.primary_keys), how="left")


def get_table_store_filedir_data(dt: DataTable, req: models.GetDataRequest) -> models.GetDataResponse:
    store = dt.table_store
    assert isinstance(store, TableStoreFiledir)

    pk_df, total = page_meta_pk(dt, req)
    read_data = isinstance(store.adapter, _FILEDIR_READ_DATA_ADAPTERS)
    data_df = _read_rows_for_pk_page(dt, pk_df, read_data=read_data)
    return _response(req, data_df, total)


def get_table_store_single_file_data(dt: DataTable, req: models.GetDataRequest) -> models.GetDataResponse:
    pk_df, total = page_meta_pk(dt, req)
    data_df = _read_rows_for_pk_page(dt, pk_df, read_data=True)
    return _response(req, data_df, total)


def get_table_data(ds: DataStore, catalog: Catalog, req: models.GetDataRequest) -> models.GetDataResponse:
    dt = catalog.get_datatable(ds, req.table)
    table_store = dt.table_store

    if isinstance(table_store, TableStoreDB):
        return get_table_store_db_data(table_store, req)
    if isinstance(table_store, TableStoreFiledir):
        return get_table_store_filedir_data(dt, req)
    if isinstance(table_store, TableDataSingleFileStore):
        return get_table_store_single_file_data(dt, req)

    # Redis / Elastic / Milvus / Neo4j / unknown: show meta primary keys only.
    return get_table_meta_pk_data(dt, req)


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

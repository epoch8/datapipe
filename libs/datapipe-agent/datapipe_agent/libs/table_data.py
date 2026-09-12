from __future__ import annotations

from typing import Any, List, Dict, Optional, Sequence, Literal
from dataclasses import dataclass

import pandas as pd
import pyarrow as pa
from datapipe.compute import Catalog, DataStore
from datapipe.datatable import DataTable
from datapipe.store.database import TableStoreDB
from datapipe.store.filedir import JSONFile, PandasParquetFile, TableStoreFiledir
from datapipe.store.table_store import TableDataSingleFileStore
from datapipe.types import IndexDF
from sqlalchemy.sql.expression import and_, or_, select
from sqlalchemy.sql.functions import count

from sqlalchemy import Table
from sqlalchemy.sql import Select
from sqlalchemy.sql.expression import asc, desc

from datapipe.meta.base import TableMeta
from datapipe.meta.sql_meta import SQLTableMeta

from datapipe_router.types import TableData


# Adapters whose file payload is reasonably JSON-serializable for the UI.
_FILEDIR_READ_DATA_ADAPTERS = (JSONFile, PandasParquetFile)


@dataclass
class TableDataSettings:
    page: int 
    page_size: int
    include_total: int
    filters: Dict[str, Any] 
    focus: List[Dict] 
    order: Literal["asc", "desc"] = "asc"
    order_by: str = None
    

def require_sql_table_meta(meta: TableMeta) -> SQLTableMeta:
    if not isinstance(meta, SQLTableMeta):
        raise RuntimeError("SQL-backed table metadata is required")
    return meta


def _apply_focus_and_filters(
    sql: Any,
    sql_table: Any,
    allowed_cols: Sequence[str],
    settings: TableDataSettings,
) -> Any:
    allowed = set(allowed_cols)
    if settings.focus is not None:
        filtered_focus_idx = [{k: v for k, v in row.items() if k in allowed} for row in settings.focus]
        filtered_focus_idx = [row for row in filtered_focus_idx if row]
        if filtered_focus_idx:
            primary_key_selectors = [and_(*[sql_table.c[k] == v for k, v in row.items()]) for row in filtered_focus_idx]
            sql = sql.where(or_(*primary_key_selectors))

    for col, val in settings.filters.items():
        if col in allowed:
            sql = sql.where(sql_table.c[col] == val)

    return sql


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


def apply_table_order_by(
    sql: Select,
    table: Table,
    order_by: Optional[str],
    order: Optional[str] = "asc",
) -> Select:
    if not order_by:
        return sql
    if order_by not in table.c:
        raise ValueError(f"Unknown order_by column: {order_by}")

    direction: Literal["asc", "desc"] = "asc" if (order or "asc").lower() != "desc" else "desc"
    column = table.c[order_by]
    sql = sql.where(column.is_not(None))
    return sql.order_by(asc(column) if direction == "asc" else desc(column))


def page_meta_pk(dt: DataTable, settings: TableDataSettings) -> tuple[pd.DataFrame, int]:
    """Paginate the table's SQL meta rows, returning primary-key columns only."""
    table_meta = require_sql_table_meta(dt.meta)
    sql_table = table_meta.sql_table
    pk_cols = list(table_meta.primary_keys)
    if not pk_cols:
        return pd.DataFrame(), 0 if settings.include_total else None

    sql = select(*[sql_table.c[c] for c in pk_cols]).select_from(sql_table)
    sql = sql.where(sql_table.c.delete_ts.is_(None))
    sql = _apply_focus_and_filters(sql, sql_table, pk_cols, settings)

    total: Optional[int] = None
    if settings.include_total:
        sql_count = select(count()).select_from(sql.subquery())
        with table_meta.dbconn.con.begin() as conn:
            total = conn.execute(sql_count).scalar_one_or_none()
            assert total is not None

    order_col = settings.order_by if settings.order_by in pk_cols else (pk_cols[0] if pk_cols else None)
    if order_col:
        sql = apply_table_order_by(sql, sql_table, order_col, settings.order)

    sql = sql.offset(settings.page * settings.page_size).limit(settings.page_size)
    pk_df = pd.read_sql_query(sql, con=table_meta.dbconn.con)
    return pk_df, total


def _make_table_data(df: pd.DataFrame, total: int, settings:TableDataSettings) -> TableData:
    return TableData(
        page=settings.page,
        page_size=settings.page_size,
        total=total,
        data=pa.Table.from_pandas(df)
    )


def get_table_store_db_data(table_store: TableStoreDB, settings: TableDataSettings) -> TableData:
    sql_schema = table_store.data_sql_schema
    sql_table = table_store.data_table

    sql = select(*sql_schema).select_from(sql_table)
    if settings.focus is not None:
        filtered_focus_idx = [
            {k: v for k, v in row.items() if k in table_store.primary_keys} for row in settings.focus
        ]
        primary_key_selectors = [and_(*[sql_table.c[k] == v for k, v in row.items()]) for row in filtered_focus_idx]
        if primary_key_selectors:
            sql = sql.where(or_(*primary_key_selectors))

    for col, val in settings.filters.items():
        sql = sql.where(sql_table.c[col] == val)

    sql_count = select(count()).select_from(sql.subquery())

    if settings.order_by:
        sql = apply_table_order_by(sql, sql_table, settings.order_by, settings.order)

    sql = sql.offset(settings.page * settings.page_size).limit(settings.page_size)

    data_df = pd.read_sql_query(sql, con=table_store.dbconn.con)

    total: Optional[int] = None
    if settings.include_total:
        with table_store.dbconn.con.begin() as conn:
            total = conn.execute(sql_count).scalar_one_or_none()
            assert total is not None


    return _make_table_data(data_df.fillna("-"), total, settings)


def get_table_store_filedir_data(dt: DataTable, settings: TableDataSettings) -> TableData:
    store = dt.table_store
    assert isinstance(store, TableStoreFiledir)

    pk_df, total = page_meta_pk(dt, settings)
    read_data = isinstance(store.adapter, _FILEDIR_READ_DATA_ADAPTERS)
    data_df = _read_rows_for_pk_page(dt, pk_df, read_data=read_data)
    return _make_table_data(data_df, total, settings)


def get_table_store_single_file_data(dt: DataTable, settings: TableDataSettings) -> TableData:
    pk_df, total = page_meta_pk(dt, settings)
    data_df = _read_rows_for_pk_page(dt, pk_df, read_data=True)
    return _make_table_data(data_df, total, settings)


def get_table_meta_pk_data(dt: DataTable, settings: TableDataSettings) -> TableData:
    pk_df, total = page_meta_pk(dt, settings)
    return _make_table_data(pk_df, total, settings)


def get_table_data(ds: DataStore, catalog: Catalog, table: str, settings: TableDataSettings) -> TableData:
    dt = catalog.get_datatable(ds, table)
    table_store = dt.table_store

    if isinstance(table_store, TableStoreDB):
        return get_table_store_db_data(table_store, settings)
    if isinstance(table_store, TableStoreFiledir):
        return get_table_store_filedir_data(dt, settings)
    if isinstance(table_store, TableDataSingleFileStore):
        return get_table_store_single_file_data(dt, settings)

    return get_table_meta_pk_data(dt, settings)
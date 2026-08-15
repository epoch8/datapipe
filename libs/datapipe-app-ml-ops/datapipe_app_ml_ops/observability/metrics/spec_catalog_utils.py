from __future__ import annotations

from typing import Any

import pandas as pd
from datapipe.store.database import TableStoreDB


def table_columns(dt) -> list[str]:
    store = dt.table_store
    if isinstance(store, TableStoreDB):
        return [column.name for column in store.data_sql_schema]
    return []


def jsonable(val: Any) -> Any:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if hasattr(val, "isoformat"):
        return val.isoformat()
    if isinstance(val, (str, int, float, bool)):
        return val
    return str(val)


def row_to_record(row: pd.Series, columns: list[str]) -> dict[str, Any]:
    return {col: jsonable(row.get(col)) for col in columns}


def row_pk(row: pd.Series, pk_cols: list[str]) -> dict[str, Any] | None:
    if not pk_cols:
        return None
    pk = {col: jsonable(row.get(col)) for col in pk_cols}
    if all(v is None for v in pk.values()):
        return None
    return pk


def safe_float(val: Any) -> float | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def safe_int(val: Any) -> int | None:
    value = safe_float(val)
    return int(value) if value is not None else None


def format_timestamp(value: Any) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)

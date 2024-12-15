from typing import Any, Dict, List, Optional

from sqlalchemy import Column, Integer, String, Table, tuple_

from datapipe.run_config import RunConfig
from datapipe.types import IndexDF


def sql_apply_idx_filter_to_table(
    sql: Any,
    table: Table,
    primary_keys: List[str],
    idx: IndexDF,
) -> Any:
    if len(primary_keys) == 1:
        # Когда ключ один - сравниваем напрямую
        key = primary_keys[0]
        sql = sql.where(table.c[key].in_(idx[key].to_list()))

    else:
        # Когда ключей много - сравниваем по кортежу
        keys = tuple_(*[table.c[key] for key in primary_keys])  # type: ignore

        sql = sql.where(
            keys.in_([tuple([r[key] for key in primary_keys]) for r in idx.to_dict(orient="records")])  # type: ignore
        )

    return sql


def sql_apply_runconfig_filter(
    sql: Any,
    table: Table,
    primary_keys: List[str],
    run_config: Optional[RunConfig] = None,
) -> Any:
    if run_config is not None:
        for k, v in run_config.filters.items():
            if k in primary_keys:
                sql = sql.where(table.c[k] == v)

    return sql


SCHEMA_TO_DTYPE_LOOKUP = {
    String: str,
    Integer: int,
}


def sql_schema_to_dtype(schema: List[Column]) -> Dict[str, Any]:
    return {i.name: SCHEMA_TO_DTYPE_LOOKUP[i.type.__class__] for i in schema}  # type: ignore

from collections import defaultdict
from typing import Any, Dict, List, Optional

import pandas as pd
from sqlalchemy import Column, Integer, String, Table, column, tuple_
from sqlalchemy.sql.expression import and_, or_

from datapipe.run_config import RunConfig
from datapipe.types import IndexDF, LabelDict

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
            keys.in_(
                [
                    tuple([r[key] for key in primary_keys])  # type: ignore
                    for r in idx.to_dict(orient="records")
                ]
            )
        )

    return sql


def sql_apply_runconfig_filters(
    sql: Any,
    table: Table,
    keys: List[str],
    run_config: Optional[RunConfig] = None,
) -> Any:
    if run_config is not None:
        filters_idx = pd.DataFrame(run_config.filters)
        keys = [key for key in table.c if key in keys]
        sql = sql_apply_idx_filter_to_table(sql, table, keys, filters_idx)

    return sql


SCHEMA_TO_DTYPE_LOOKUP = {
    String: str,
    Integer: int,
}


def sql_schema_to_dtype(schema: List[Column]) -> Dict[str, Any]:
    return {i.name: SCHEMA_TO_DTYPE_LOOKUP[i.type.__class__] for i in schema}
from typing import TYPE_CHECKING, Any, List, Optional, Tuple

import pandas as pd
import sqlalchemy as sa

if TYPE_CHECKING:
    from datapipe.datatable import DataStore


def sql_apply_filters_idx_to_subquery(
    sql: Any,
    keys: List[str],
    filters_idx: Optional[pd.DataFrame],
) -> Any:
    if filters_idx is None:
        return sql

    applicable_filter_keys = [i for i in filters_idx.columns if i in keys]
    if len(applicable_filter_keys) > 0:
        sql = sql.where(
            sa.tuple_(*[sa.column(i) for i in applicable_filter_keys]).in_(
                [
                    sa.tuple_(*[r[k] for k in applicable_filter_keys])
                    for r in filters_idx.to_dict(orient="records")
                ]
            )
        )

    return sql


def make_agg_of_agg(
    ds: "DataStore",
    transform_keys: List[str],
    common_transform_keys: List[str],
    agg_col: str,
    ctes: List[Tuple[List[str], Any]],
) -> Any:
    assert len(ctes) > 0

    if len(ctes) == 1:
        return ctes[0][1]

    coalesce_keys = []

    for key in transform_keys:
        ctes_with_key = [subq for (subq_keys, subq) in ctes if key in subq_keys]

        if len(ctes_with_key) == 0:
            raise ValueError(f"Key {key} not found in any of the input tables")

        if len(ctes_with_key) == 1:
            coalesce_keys.append(ctes_with_key[0].c[key])
        else:
            coalesce_keys.append(
                sa.func.coalesce(*[cte.c[key] for cte in ctes_with_key]).label(key)
            )

    agg = sa.func.max(
        ds.meta_dbconn.func_greatest(*[subq.c[agg_col] for (_, subq) in ctes])
    ).label(agg_col)

    _, first_cte = ctes[0]

    sql = sa.select(*coalesce_keys + [agg]).select_from(first_cte)

    for _, cte in ctes[1:]:
        if len(common_transform_keys) > 0:
            sql = sql.outerjoin(
                cte,
                onclause=sa.and_(
                    *[first_cte.c[key] == cte.c[key] for key in common_transform_keys]
                ),
                full=True,
            )
        else:
            sql = sql.outerjoin(
                cte,
                onclause=sa.literal(True),
                full=True,
            )

    sql = sql.group_by(*coalesce_keys)

    return sql.cte(name=f"all__{agg_col}")

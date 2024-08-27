import itertools
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    List,
    Literal,
    Optional,
    Tuple,
    cast,
)

import pandas as pd
import sqlalchemy as sa

from datapipe.compute import ComputeInput
from datapipe.run_config import RunConfig
from datapipe.sql_util import sql_apply_runconfig_filter
from datapipe.store.database import DBConn
from datapipe.types import DataSchema, IndexDF

if TYPE_CHECKING:
    from datapipe.datatable import DataStore


class TransformMetaTable:
    def __init__(
        self,
        dbconn: DBConn,
        name: str,
        primary_schema: DataSchema,
        create_table: bool = False,
    ) -> None:
        self.dbconn = dbconn
        self.name = name
        self.primary_schema = primary_schema
        self.primary_keys = [i.name for i in primary_schema]

        self.sql_schema = [i._copy() for i in primary_schema + TRANSFORM_META_SCHEMA]  # type: ignore

        self.sql_table = sa.Table(
            name,
            dbconn.sqla_metadata,
            *self.sql_schema,
        )

        if create_table:
            self.sql_table.create(self.dbconn.con, checkfirst=True)

    def __reduce__(self) -> Tuple[Any, ...]:
        return self.__class__, (
            self.dbconn,
            self.name,
            self.primary_schema,
        )

    def insert_rows(
        self,
        idx: IndexDF,
    ) -> None:
        """
        Создает строки в таблице метаданных для указанных индексов. Если строки
        уже существуют - не делает ничего.
        """

        idx = cast(IndexDF, idx[self.primary_keys])

        insert_sql = self.dbconn.insert(self.sql_table).values(
            [
                {
                    "process_ts": 0,
                    "is_success": False,
                    "priority": 0,
                    "error": None,
                    **idx_dict,  # type: ignore
                }
                for idx_dict in idx.to_dict(orient="records")
            ]
        )

        sql = insert_sql.on_conflict_do_nothing(index_elements=self.primary_keys)

        with self.dbconn.con.begin() as con:
            con.execute(sql)

    def mark_rows_processed_success(
        self,
        idx: IndexDF,
        process_ts: float,
        run_config: Optional[RunConfig] = None,
    ) -> None:
        idx = cast(
            IndexDF, idx[self.primary_keys].drop_duplicates().dropna()
        )  # FIXME: сделать в основном запросе distinct
        if len(idx) == 0:
            return

        if idx.empty:
            # DataFrame считает, что он пустой, если в нем нет колонок
            # При этом мы хотим создать строки в БД

            # Мы можем обработать только случай с одной строкой
            assert len(idx) == 1

            with self.dbconn.con.begin() as con:
                insert_sql = self.dbconn.insert(self.sql_table).values(
                    [
                        {
                            "process_ts": process_ts,
                            "is_success": True,
                            "priority": 0,
                            "error": None,
                        }
                    ]
                )

                # удалить все из таблицы
                con.execute(self.sql_table.delete())
                con.execute(insert_sql)

        else:
            insert_sql = self.dbconn.insert(self.sql_table).values(
                [
                    {
                        "process_ts": process_ts,
                        "is_success": True,
                        "priority": 0,
                        "error": None,
                        **idx_dict,  # type: ignore
                    }
                    for idx_dict in idx.to_dict(orient="records")
                ]
            )

            sql = insert_sql.on_conflict_do_update(
                index_elements=self.primary_keys,
                set_={
                    "process_ts": process_ts,
                    "is_success": True,
                    "error": None,
                },
            )

            # execute
            with self.dbconn.con.begin() as con:
                con.execute(sql)

    def mark_rows_processed_error(
        self,
        idx: IndexDF,
        process_ts: float,
        error: str,
        run_config: Optional[RunConfig] = None,
    ) -> None:
        idx = cast(
            IndexDF, idx[self.primary_keys].drop_duplicates().dropna()
        )  # FIXME: сделать в основном запросе distinct
        if len(idx) == 0:
            return

        insert_sql = self.dbconn.insert(self.sql_table).values(
            [
                {
                    "process_ts": process_ts,
                    "is_success": False,
                    "priority": 0,
                    "error": error,
                    **idx_dict,  # type: ignore
                }
                for idx_dict in idx.to_dict(orient="records")
            ]
        )

        sql = insert_sql.on_conflict_do_update(
            index_elements=self.primary_keys,
            set_={
                "process_ts": process_ts,
                "is_success": False,
                "error": error,
            },
        )

        # execute
        with self.dbconn.con.begin() as con:
            con.execute(sql)

    def get_metadata_size(self) -> int:
        """
        Получить количество строк метаданных трансформации.
        """

        sql = sa.select(sa.func.count()).select_from(self.sql_table)
        with self.dbconn.con.begin() as con:
            res = con.execute(sql).fetchone()

        assert res is not None and len(res) == 1
        return res[0]

    def mark_all_rows_unprocessed(
        self,
        run_config: Optional[RunConfig] = None,
    ) -> None:
        update_sql = (
            sa.update(self.sql_table)
            .values(
                {
                    "process_ts": 0,
                    "is_success": False,
                    "error": None,
                }
            )
            .where(self.sql_table.c.is_success == True)
        )

        sql = sql_apply_runconfig_filter(
            update_sql, self.sql_table, self.primary_keys, run_config
        )

        # execute
        with self.dbconn.con.begin() as con:
            con.execute(sql)


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


def _make_agg_of_agg(
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


def build_changed_idx_sql(
    ds: DataStore,
    meta_table: "TransformMetaTable",
    input_dts: List[ComputeInput],
    transform_keys: List[str],
    filters_idx: Optional[IndexDF] = None,
    order_by: Optional[List[str]] = None,
    order: Literal["asc", "desc"] = "asc",
    run_config: Optional[RunConfig] = None,  # TODO remove
) -> Tuple[Iterable[str], Any]:
    all_input_keys_counts: Dict[str, int] = {}
    for col in itertools.chain(*[inp.dt.primary_schema for inp in input_dts]):
        all_input_keys_counts[col.name] = all_input_keys_counts.get(col.name, 0) + 1

    inp_ctes = [
        inp.dt.get_agg_cte(
            transform_keys=transform_keys,
            filters_idx=filters_idx,
            run_config=run_config,
        )
        for inp in input_dts
    ]

    common_keys = [k for k, v in all_input_keys_counts.items() if v == len(input_dts)]

    common_transform_keys = [k for k in transform_keys if k in common_keys]

    inp = _make_agg_of_agg(
        ds=ds,
        transform_keys=transform_keys,
        common_transform_keys=common_transform_keys,
        ctes=inp_ctes,
        agg_col="update_ts",
    )

    tr_tbl = meta_table.sql_table
    out: Any = (
        sa.select(
            *[sa.column(k) for k in transform_keys]
            + [tr_tbl.c.process_ts, tr_tbl.c.priority, tr_tbl.c.is_success]
        )
        .select_from(tr_tbl)
        .group_by(*[sa.column(k) for k in transform_keys])
    )

    out = sql_apply_filters_idx_to_subquery(out, transform_keys, filters_idx)

    out = out.cte(name="transform")

    if len(transform_keys) == 0:
        join_onclause_sql: Any = sa.literal(True)
    elif len(transform_keys) == 1:
        join_onclause_sql = inp.c[transform_keys[0]] == out.c[transform_keys[0]]
    else:  # len(transform_keys) > 1:
        join_onclause_sql = sa.and_(
            *[inp.c[key] == out.c[key] for key in transform_keys]
        )

    sql = (
        sa.select(
            # Нам нужно выбирать хотя бы что-то, чтобы не было ошибки при
            # пустом transform_keys
            sa.literal(1).label("_datapipe_dummy"),
            *[
                sa.func.coalesce(inp.c[key], out.c[key]).label(key)
                for key in transform_keys
            ],
        )
        .select_from(inp)
        .outerjoin(
            out,
            onclause=join_onclause_sql,
            full=True,
        )
        .where(
            sa.or_(
                sa.and_(
                    out.c.is_success == True,  # noqa
                    inp.c.update_ts > out.c.process_ts,
                ),
                out.c.is_success != True,  # noqa
                out.c.process_ts == None,  # noqa
            )
        )
    )
    if order_by is None:
        sql = sql.order_by(
            out.c.priority.desc().nullslast(),
            *[sa.column(k) for k in transform_keys],
        )
    else:
        if order == "desc":
            sql = sql.order_by(
                *[sa.desc(sa.column(k)) for k in order_by],
                out.c.priority.desc().nullslast(),
            )
        elif order == "asc":
            sql = sql.order_by(
                *[sa.asc(sa.column(k)) for k in order_by],
                out.c.priority.desc().nullslast(),
            )
    return (transform_keys, sql)

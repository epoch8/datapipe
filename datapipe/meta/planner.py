import itertools
import logging
import math
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Literal, Optional, Tuple, cast

import pandas as pd
import sqlalchemy as sa
from opentelemetry import trace
from sqlalchemy import alias, func, select
from tqdm_loggable.auto import tqdm

from datapipe.compute import ComputeInput, StepStatus
from datapipe.datatable import DataStore, DataTable
from datapipe.meta.sql_meta import (
    MetaTable,
    TransformMetaTable,
    sql_apply_filters_idx_to_subquery,
)
from datapipe.run_config import LabelDict, RunConfig
from datapipe.types import ChangeList, IndexDF, MetaSchema, data_to_index

logger = logging.getLogger("datapipe.step.batch_transform")
tracer = trace.get_tracer("datapipe.step.batch_transform")


class TransformPlanner:
    def __init__(
        self,
        ds: DataStore,
        step_name: str,
        transform_keys: List[str] | None,
        inputs: List[ComputeInput],
        output_dts: List[DataTable],
    ) -> None:
        self.step_name = step_name
        self.inputs = inputs

        self.transform_keys, self.transform_schema = self.compute_transform_schema(
            [inp.dt.meta_table for inp in inputs],
            [out.meta_table for out in output_dts],
            transform_keys,
        )

        self.meta_table = TransformMetaTable(
            dbconn=ds.meta_dbconn,
            name=f"{self.step_name}_meta",
            primary_schema=self.transform_schema,
            create_table=ds.create_meta_table,
        )

    @classmethod
    def compute_transform_schema(
        cls,
        input_mts: List[MetaTable],
        output_mts: List[MetaTable],
        transform_keys: Optional[List[str]],
    ) -> Tuple[List[str], MetaSchema]:
        # Hacky way to collect all the primary keys into a single set. Possible
        # problem that is not handled here is that theres a possibility that the
        # same key is defined differently in different input tables.
        all_keys = {
            col.name: col
            for col in itertools.chain(
                *(
                    [dt.primary_schema for dt in input_mts]
                    + [dt.primary_schema for dt in output_mts]
                )
            )
        }

        if transform_keys is not None:
            return (transform_keys, [all_keys[k] for k in transform_keys])

        assert len(input_mts) > 0

        inp_p_keys = set.intersection(*[set(inp.primary_keys) for inp in input_mts])
        assert len(inp_p_keys) > 0

        if len(output_mts) == 0:
            return (list(inp_p_keys), [all_keys[k] for k in inp_p_keys])

        out_p_keys = set.intersection(*[set(out.primary_keys) for out in output_mts])
        assert len(out_p_keys) > 0

        inp_out_p_keys = set.intersection(inp_p_keys, out_p_keys)
        assert len(inp_out_p_keys) > 0

        return (list(inp_out_p_keys), [all_keys[k] for k in inp_out_p_keys])

    def get_status(self, ds: DataStore) -> StepStatus:
        return StepStatus(
            name=self.step_name,
            total_idx_count=self.meta_table.get_metadata_size(),
            changed_idx_count=self.get_changed_idx_count(ds),
        )

    def get_changed_idx_count(
        self,
        ds: DataStore,
        run_config: Optional[RunConfig] = None,
    ) -> int:
        _, sql = build_changed_idx_sql(
            ds=ds,
            meta_table=self.meta_table,
            input_dts=self.inputs,
            transform_keys=self.transform_keys,
            run_config=run_config,
        )

        with ds.meta_dbconn.con.begin() as con:
            idx_count = con.execute(
                select(*[func.count()]).select_from(
                    alias(sql.subquery(), name="union_select")
                )
            ).scalar()

        return cast(int, idx_count)

    def get_full_process_ids(
        self,
        ds: DataStore,
        chunk_size: int,
        order_by: Optional[List[str]] = None,
        order: Literal["asc", "desc"] = "asc",
        run_config: Optional[RunConfig] = None,
    ) -> Tuple[int, Iterable[IndexDF]]:
        """
        Метод для получения перечня индексов для обработки.

        Returns: (idx_size, iterator<idx_df>)

        - idx_size - количество индексов требующих обработки
        - idx_df - датафрейм без колонок с данными, только индексная колонка
        """
        with tracer.start_as_current_span("compute ids to process"):
            if len(self.inputs) == 0:
                return (0, iter([]))

            idx_count = self.get_changed_idx_count(
                ds=ds,
                run_config=run_config,
            )

            join_keys, u1 = build_changed_idx_sql(
                ds=ds,
                meta_table=self.meta_table,
                input_dts=self.inputs,
                transform_keys=self.transform_keys,
                run_config=run_config,
                order_by=order_by,
                order=order,
            )

            # Список ключей из фильтров, которые нужно добавить в результат
            extra_filters: LabelDict
            if run_config is not None:
                extra_filters = {
                    k: v for k, v in run_config.filters.items() if k not in join_keys
                }
            else:
                extra_filters = {}

            def alter_res_df():
                with ds.meta_dbconn.con.begin() as con:
                    for df in pd.read_sql_query(u1, con=con, chunksize=chunk_size):
                        df = df[self.transform_keys]

                        for k, v in extra_filters.items():
                            df[k] = v

                        yield cast(IndexDF, df)

            return math.ceil(idx_count / chunk_size), alter_res_df()

    def get_change_list_process_ids(
        self,
        ds: DataStore,
        change_list: ChangeList,
        chunk_size: int,
        run_config: Optional[RunConfig] = None,
    ) -> Tuple[int, Iterable[IndexDF]]:
        with tracer.start_as_current_span("compute ids to process"):
            changes = [pd.DataFrame(columns=self.transform_keys)]

            for inp in self.inputs:
                if inp.dt.name in change_list.changes:
                    idx = change_list.changes[inp.dt.name]
                    if any([key not in idx.columns for key in self.transform_keys]):
                        # TODO пересмотреть эту логику, выглядит избыточной
                        # (возможно, достаточно посчитать один раз для всех
                        # input таблиц)
                        _, sql = build_changed_idx_sql(
                            ds=ds,
                            meta_table=self.meta_table,
                            input_dts=self.inputs,
                            transform_keys=self.transform_keys,
                            filters_idx=idx,
                            run_config=run_config,
                        )
                        with ds.meta_dbconn.con.begin() as con:
                            table_changes_df = pd.read_sql_query(
                                sql,
                                con=con,
                            )
                            table_changes_df = table_changes_df[self.transform_keys]

                        changes.append(table_changes_df)
                    else:
                        changes.append(data_to_index(idx, self.transform_keys))

            idx_df = pd.concat(changes).drop_duplicates(subset=self.transform_keys)
            idx = IndexDF(idx_df[self.transform_keys])

            chunk_count = math.ceil(len(idx) / chunk_size)

            def gen():
                for i in range(chunk_count):
                    yield cast(IndexDF, idx[i * chunk_size : (i + 1) * chunk_size])

            return chunk_count, gen()

    def fill_metadata(self, ds: DataStore) -> None:
        idx_len, idx_gen = self.get_full_process_ids(ds=ds, chunk_size=1000)

        for idx in tqdm(idx_gen, total=idx_len):
            self.meta_table.insert_rows(idx)

    def reset_metadata(self, ds: DataStore) -> None:
        self.meta_table.mark_all_rows_unprocessed()

    def mark_processed_success(
        self,
        idx: IndexDF,
        process_ts: float,
        run_config: Optional[RunConfig] = None,
    ) -> None:
        return self.meta_table.mark_rows_processed_success(
            idx,
            process_ts=process_ts,
            run_config=run_config,
        )

    def mark_processed_error(
        self,
        idx: IndexDF,
        process_ts: float,
        error: str,
        run_config: Optional[RunConfig] = None,
    ) -> None:
        return self.meta_table.mark_rows_processed_error(
            idx,
            process_ts=process_ts,
            error=error,
            run_config=run_config,
        )


@dataclass
class ComputeInputCTE:
    cte: Any
    keys: List[str]
    join_type: Literal["inner", "full"]


def _make_agg_of_agg(
    ds: "DataStore",
    transform_keys: List[str],
    agg_col: str,
    ctes: List[ComputeInputCTE],
) -> Any:
    assert len(ctes) > 0

    if len(ctes) == 1:
        return ctes[0].cte

    coalesce_keys = []

    for key in transform_keys:
        ctes_with_key = [cte.cte for cte in ctes if key in cte.keys]

        if len(ctes_with_key) == 0:
            raise ValueError(f"Key {key} not found in any of the input tables")

        if len(ctes_with_key) == 1:
            coalesce_keys.append(ctes_with_key[0].c[key])
        else:
            coalesce_keys.append(
                sa.func.coalesce(*[cte.c[key] for cte in ctes_with_key]).label(key)
            )

    agg = sa.func.max(
        ds.meta_dbconn.func_greatest(*[cte.cte.c[agg_col] for cte in ctes])
    ).label(agg_col)

    first_cte = ctes[0].cte

    sql = sa.select(*coalesce_keys + [agg]).select_from(first_cte)

    prev_ctes = [ctes[0]]

    for cte in ctes[1:]:
        onclause = []

        for prev_cte in prev_ctes:
            for key in cte.keys:
                if key in prev_cte.keys:
                    onclause.append(prev_cte.cte.c[key] == cte.cte.c[key])

        if len(onclause) > 0:
            sql = sql.outerjoin(
                cte.cte,
                onclause=sa.and_(*onclause),
                full=True,
            )
        else:
            sql = sql.outerjoin(
                cte.cte,
                onclause=sa.literal(True),
                full=True,
            )

        if cte.join_type == "inner":
            sql = sql.where(sa.and_(*[cte.cte.c[key].isnot(None) for key in cte.keys]))

        prev_ctes.append(cte)

    sql = sql.group_by(*coalesce_keys)

    return sql.cte(name=f"all__{agg_col}")


def build_changed_idx_sql(
    ds: "DataStore",
    meta_table: "TransformMetaTable",
    input_dts: List["ComputeInput"],
    transform_keys: List[str],
    filters_idx: Optional[IndexDF] = None,
    order_by: Optional[List[str]] = None,
    order: Literal["asc", "desc"] = "asc",
    run_config: Optional[RunConfig] = None,  # TODO remove
) -> Tuple[Iterable[str], Any]:
    all_input_keys_counts: Dict[str, int] = {}
    for col in itertools.chain(*[inp.dt.primary_schema for inp in input_dts]):
        all_input_keys_counts[col.name] = all_input_keys_counts.get(col.name, 0) + 1

    inp_ctes = []
    for inp in input_dts:
        keys, cte = inp.dt.meta_table.get_agg_cte(
            transform_keys=transform_keys,
            filters_idx=filters_idx,
            run_config=run_config,
        )
        inp_ctes.append(ComputeInputCTE(cte=cte, keys=keys, join_type=inp.join_type))

    agg_of_aggs = _make_agg_of_agg(
        ds=ds,
        transform_keys=transform_keys,
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
        join_onclause_sql = agg_of_aggs.c[transform_keys[0]] == out.c[transform_keys[0]]
    else:  # len(transform_keys) > 1:
        join_onclause_sql = sa.and_(
            *[agg_of_aggs.c[key] == out.c[key] for key in transform_keys]
        )

    sql = (
        sa.select(
            # Нам нужно выбирать хотя бы что-то, чтобы не было ошибки при
            # пустом transform_keys
            sa.literal(1).label("_datapipe_dummy"),
            *[
                sa.func.coalesce(agg_of_aggs.c[key], out.c[key]).label(key)
                for key in transform_keys
            ],
        )
        .select_from(agg_of_aggs)
        .outerjoin(
            out,
            onclause=join_onclause_sql,
            full=True,
        )
        .where(
            sa.or_(
                sa.and_(
                    out.c.is_success == True,  # noqa
                    agg_of_aggs.c.update_ts > out.c.process_ts,
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

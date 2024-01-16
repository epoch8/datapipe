import copy
import inspect
import itertools
import logging
import math
import time
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Literal,
    Optional,
    Protocol,
    Tuple,
    Union,
    cast,
)

import pandas as pd
from opentelemetry import trace
from sqlalchemy import (
    Boolean,
    Column,
    Float,
    Integer,
    String,
    Table,
    alias,
    and_,
    asc,
    column,
    desc,
    func,
    literal,
    or_,
    select,
    update,
)
from sqlalchemy.sql.expression import select
from tqdm_loggable.auto import tqdm

from datapipe.compute import Catalog, ComputeStep, PipelineStep
from datapipe.datatable import DataStore, DataTable, MetaTable
from datapipe.executor import Executor, ExecutorConfig, SingleThreadExecutor
from datapipe.run_config import LabelDict, RunConfig
from datapipe.sql_util import (
    sql_apply_filters_idx_to_subquery,
    sql_apply_runconfig_filter,
)
from datapipe.store.database import DBConn
from datapipe.types import (
    ChangeList,
    DataDF,
    DataSchema,
    IndexDF,
    Labels,
    MetaSchema,
    TransformResult,
    data_to_index,
)

logger = logging.getLogger("datapipe.step.batch_transform")
tracer = trace.get_tracer("datapipe.step.batch_transform")


# TODO подумать, может быть мы хотим дать возможность возвращать итератор TransformResult
class DatatableBatchTransformFunc(Protocol):
    __name__: str

    def __call__(
        self,
        ds: DataStore,
        idx: IndexDF,
        input_dts: List[DataTable],
        run_config: Optional[RunConfig] = None,
        kwargs: Optional[Dict[str, Any]] = None,
    ) -> TransformResult:
        ...


BatchTransformFunc = Callable[..., TransformResult]


TRANSFORM_META_SCHEMA: DataSchema = [
    Column("process_ts", Float),  # Время последней успешной обработки
    Column("is_success", Boolean),  # Успешно ли обработана строка
    Column("priority", Integer),  # Приоритет обработки (чем больше, тем выше)
    Column("error", String),  # Текст ошибки
]


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

        self.sql_table = Table(
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

        sql = select(func.count()).select_from(self.sql_table)
        with self.dbconn.con.begin() as con:
            res = con.execute(sql).fetchone()

        assert res is not None and len(res) == 1
        return res[0]

    def mark_all_rows_unprocessed(
        self,
        run_config: Optional[RunConfig] = None,
    ) -> None:
        update_sql = (
            update(self.sql_table)
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


class BaseBatchTransformStep(ComputeStep):
    """
    Abstract class for batch transform steps
    """

    def __init__(
        self,
        ds: DataStore,
        name: str,
        input_dts: List[DataTable],
        output_dts: List[DataTable],
        transform_keys: Optional[List[str]] = None,
        chunk_size: int = 1000,
        labels: Optional[Labels] = None,
        executor_config: Optional[ExecutorConfig] = None,
        filters: Optional[Union[LabelDict, Callable[[], LabelDict]]] = None,
        order_by: Optional[List[str]] = None,
        order: Literal["asc", "desc"] = "asc",
    ) -> None:
        ComputeStep.__init__(
            self,
            name=name,
            input_dts=input_dts,
            output_dts=output_dts,
            labels=labels,
            executor_config=executor_config,
        )

        self.chunk_size = chunk_size
        self.transform_keys, self.transform_schema = self.compute_transform_schema(
            [i.meta_table for i in input_dts],
            [i.meta_table for i in output_dts],
            transform_keys,
        )

        self.meta_table = TransformMetaTable(
            dbconn=ds.meta_dbconn,
            name=f"{self.get_name()}_meta",
            primary_schema=self.transform_schema,
            create_table=ds.create_meta_table,
        )
        self.filters = filters
        self.order_by = order_by
        self.order = order

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

    def _build_changed_idx_sql(
        self,
        ds: DataStore,
        filters_idx: Optional[IndexDF] = None,
        order_by: Optional[List[str]] = None,
        order: Literal["asc", "desc"] = "asc",
        run_config: Optional[RunConfig] = None,  # TODO remove
    ) -> Tuple[Iterable[str], Any]:
        if len(self.transform_keys) == 0:
            raise NotImplementedError()

        all_input_keys_counts: Dict[str, int] = {}
        for col in itertools.chain(*[dt.primary_schema for dt in self.input_dts]):
            all_input_keys_counts[col.name] = all_input_keys_counts.get(col.name, 0) + 1

        common_keys = [
            k for k, v in all_input_keys_counts.items() if v == len(self.input_dts)
        ]

        common_transform_keys = [k for k in self.transform_keys if k in common_keys]

        def _make_agg_of_agg(ctes, agg_col):
            assert len(ctes) > 0

            if len(ctes) == 1:
                return ctes[0][1]

            coalesce_keys = []

            for key in self.transform_keys:
                ctes_with_key = [subq for (subq_keys, subq) in ctes if key in subq_keys]

                if len(ctes_with_key) == 0:
                    raise ValueError(f"Key {key} not found in any of the input tables")

                if len(ctes_with_key) == 1:
                    coalesce_keys.append(ctes_with_key[0].c[key])
                else:
                    coalesce_keys.append(
                        func.coalesce(*[cte.c[key] for cte in ctes_with_key]).label(key)
                    )

            agg = func.max(
                ds.meta_dbconn.func_greatest(*[subq.c[agg_col] for (_, subq) in ctes])
            ).label(agg_col)

            _, first_cte = ctes[0]

            sql = select(*coalesce_keys + [agg]).select_from(first_cte)

            for _, cte in ctes[1:]:
                if len(common_transform_keys) > 0:
                    sql = sql.outerjoin(
                        cte,
                        onclause=and_(
                            *[
                                first_cte.c[key] == cte.c[key]
                                for key in common_transform_keys
                            ]
                        ),
                        full=True,
                    )
                else:
                    sql = sql.outerjoin(
                        cte,
                        onclause=literal(True),
                        full=True,
                    )

            sql = sql.group_by(*coalesce_keys)

            return sql.cte(name=f"all__{agg_col}")

        inp_ctes = [
            tbl.get_agg_cte(
                transform_keys=self.transform_keys,
                filters_idx=filters_idx,
                run_config=run_config,
            )
            for tbl in self.input_dts
        ]

        # Filter out ctes that do not have intersection with transform keys.
        # These ctes do not affect the result, but may dramatically increase
        # size of the temporary table during query execution.
        inp_ctes = [(keys, cte) for (keys, cte) in inp_ctes if len(keys) > 0]

        inp = _make_agg_of_agg(inp_ctes, "update_ts")

        tr_tbl = self.meta_table.sql_table
        out: Any = (
            select(
                *[column(k) for k in self.transform_keys]
                + [tr_tbl.c.process_ts, tr_tbl.c.priority, tr_tbl.c.is_success]
            )
            .select_from(tr_tbl)
            .group_by(*[column(k) for k in self.transform_keys])
        )

        out = sql_apply_filters_idx_to_subquery(out, self.transform_keys, filters_idx)

        out = out.cte(name="transform")

        sql = (
            select(
                *[
                    func.coalesce(inp.c[key], out.c[key]).label(key)
                    for key in self.transform_keys
                ],
            )
            .select_from(inp)
            .outerjoin(
                out,
                onclause=and_(
                    *[inp.c[key] == out.c[key] for key in self.transform_keys]
                ),
                full=True,
            )
            .where(
                or_(
                    and_(
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
                *[column(k) for k in self.transform_keys],
            )
        else:
            if order == "desc":
                sql = sql.order_by(
                    *[desc(column(k)) for k in order_by],
                    out.c.priority.desc().nullslast(),
                )
            elif order == "asc":
                sql = sql.order_by(
                    *[asc(column(k)) for k in order_by],
                    out.c.priority.desc().nullslast(),
                )
        return (self.transform_keys, sql)

    def _apply_filters_to_run_config(
        self, run_config: Optional[RunConfig] = None
    ) -> Optional[RunConfig]:
        if self.filters is None:
            return run_config
        else:
            if isinstance(self.filters, dict):
                filters = self.filters
            elif isinstance(self.filters, Callable):  # type: ignore
                filters = self.filters()

            if run_config is None:
                return RunConfig(filters=filters)
            else:
                run_config = copy.deepcopy(run_config)
                filters = copy.deepcopy(filters)
                filters.update(run_config.filters)
                run_config.filters = filters
                return run_config

    def get_changed_idx_count(
        self,
        ds: DataStore,
        run_config: Optional[RunConfig] = None,
    ) -> int:
        run_config = self._apply_filters_to_run_config(run_config)
        _, sql = self._build_changed_idx_sql(ds, run_config=run_config)

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
        chunk_size: Optional[int] = None,
        run_config: Optional[RunConfig] = None,
    ) -> Tuple[int, Iterable[IndexDF]]:
        """
        Метод для получения перечня индексов для обработки.

        Returns: (idx_size, iterator<idx_df>)

        - idx_size - количество индексов требующих обработки
        - idx_df - датафрейм без колонок с данными, только индексная колонка
        """
        run_config = self._apply_filters_to_run_config(run_config)
        chunk_size = chunk_size or self.chunk_size

        with tracer.start_as_current_span("compute ids to process"):
            if len(self.input_dts) == 0:
                return (0, iter([]))

            idx_count = self.get_changed_idx_count(
                ds=ds,
                run_config=run_config,
            )

            join_keys, u1 = self._build_changed_idx_sql(
                ds=ds,
                run_config=run_config,
                order_by=self.order_by,
                order=self.order,
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
                        for k, v in extra_filters.items():
                            df[k] = v

                        yield df

            return math.ceil(idx_count / chunk_size), alter_res_df()

    def get_change_list_process_ids(
        self,
        ds: DataStore,
        change_list: ChangeList,
        run_config: Optional[RunConfig] = None,
    ) -> Tuple[int, Iterable[IndexDF]]:
        run_config = self._apply_filters_to_run_config(run_config)
        with tracer.start_as_current_span("compute ids to process"):
            changes = [pd.DataFrame(columns=self.transform_keys)]

            for inp in self.input_dts:
                if inp.name in change_list.changes:
                    idx = change_list.changes[inp.name]
                    if any([key not in idx.columns for key in self.transform_keys]):
                        _, sql = self._build_changed_idx_sql(
                            ds=ds,
                            filters_idx=idx,
                            run_config=run_config,
                        )
                        with ds.meta_dbconn.con.begin() as con:
                            table_changes_df = pd.read_sql_query(
                                sql,
                                con=con,
                            )

                        changes.append(table_changes_df)
                    else:
                        changes.append(data_to_index(idx, self.transform_keys))

            idx_df = pd.concat(changes).drop_duplicates(subset=self.transform_keys)
            idx = IndexDF(idx_df[self.transform_keys])

            chunk_count = math.ceil(len(idx) / self.chunk_size)

            def gen():
                for i in range(chunk_count):
                    yield idx[i * self.chunk_size : (i + 1) * self.chunk_size]

            return chunk_count, gen()

    def store_batch_result(
        self,
        ds: DataStore,
        idx: IndexDF,
        output_dfs: Optional[TransformResult],
        process_ts: float,
        run_config: Optional[RunConfig] = None,
    ) -> ChangeList:
        run_config = self._apply_filters_to_run_config(run_config)

        changes = ChangeList()

        if output_dfs is not None:
            with tracer.start_as_current_span("store output batch"):
                if isinstance(output_dfs, (list, tuple)):
                    assert len(output_dfs) == len(self.output_dts)
                else:
                    assert len(self.output_dts) == 1
                    output_dfs = [output_dfs]

                for k, res_dt in enumerate(self.output_dts):
                    # Берем k-ое значение функции для k-ой таблички
                    # Добавляем результат в результирующие чанки
                    change_idx = res_dt.store_chunk(
                        data_df=output_dfs[k],
                        processed_idx=idx,
                        now=process_ts,
                        run_config=run_config,
                    )

                    changes.append(res_dt.name, change_idx)

        else:
            with tracer.start_as_current_span("delete missing data from output"):
                for k, res_dt in enumerate(self.output_dts):
                    del_idx = res_dt.meta_table.get_existing_idx(idx)

                    res_dt.delete_by_idx(del_idx, run_config=run_config)

                    changes.append(res_dt.name, del_idx)

        self.meta_table.mark_rows_processed_success(
            idx, process_ts=process_ts, run_config=run_config
        )

        return changes

    def store_batch_err(
        self,
        ds: DataStore,
        idx: IndexDF,
        e: Exception,
        process_ts: float,
        run_config: Optional[RunConfig] = None,
    ) -> None:
        run_config = self._apply_filters_to_run_config(run_config)

        logger.error(f"Process batch failed: {str(e)}")
        ds.event_logger.log_exception(
            e,
            run_config=RunConfig.add_labels(
                run_config,
                {"idx": idx.to_dict(orient="records"), "process_ts": process_ts},
            ),
        )

        self.meta_table.mark_rows_processed_error(
            idx,
            process_ts=process_ts,
            error=str(e),
            run_config=run_config,
        )

    def fill_metadata(self, ds: DataStore) -> None:
        idx_len, idx_gen = self.get_full_process_ids(ds=ds, chunk_size=1000)

        for idx in tqdm(idx_gen, total=idx_len):
            self.meta_table.insert_rows(idx)

    def reset_metadata(self, ds: DataStore) -> None:
        self.meta_table.mark_all_rows_unprocessed()

    def get_batch_input_dfs(
        self,
        ds: DataStore,
        idx: IndexDF,
        run_config: Optional[RunConfig] = None,
    ) -> List[DataDF]:
        return [inp.get_data(idx) for inp in self.input_dts]

    def process_batch_dfs(
        self,
        ds: DataStore,
        idx: IndexDF,
        input_dfs: List[DataDF],
        run_config: Optional[RunConfig] = None,
    ) -> TransformResult:
        raise NotImplementedError()

    def process_batch_dts(
        self,
        ds: DataStore,
        idx: IndexDF,
        run_config: Optional[RunConfig] = None,
    ) -> Optional[TransformResult]:
        with tracer.start_as_current_span("get input data"):
            input_dfs = self.get_batch_input_dfs(ds, idx, run_config)

        if sum(len(j) for j in input_dfs) == 0:
            return None

        with tracer.start_as_current_span("run transform"):
            output_dfs = self.process_batch_dfs(
                ds=ds,
                idx=idx,
                input_dfs=input_dfs,
                run_config=run_config,
            )

        return output_dfs

    def process_batch(
        self,
        ds: DataStore,
        idx: IndexDF,
        run_config: Optional[RunConfig] = None,
    ) -> ChangeList:
        with tracer.start_as_current_span("process batch"):
            logger.debug(f"Idx to process: {idx.to_records()}")

            process_ts = time.time()

            try:
                output_dfs = self.process_batch_dts(ds, idx, run_config)

                return self.store_batch_result(
                    ds, idx, output_dfs, process_ts, run_config
                )

            except Exception as e:
                self.store_batch_err(ds, idx, e, process_ts, run_config)

                return ChangeList()

    def run_full(
        self,
        ds: DataStore,
        run_config: Optional[RunConfig] = None,
        executor: Optional[Executor] = None,
    ) -> None:
        if executor is None:
            executor = SingleThreadExecutor()

        logger.info(f"Running: {self.name}")
        run_config = RunConfig.add_labels(run_config, {"step_name": self.name})

        (idx_count, idx_gen) = self.get_full_process_ids(ds=ds, run_config=run_config)

        logger.info(f"Batches to process {idx_count}")

        if idx_count is not None and idx_count == 0:
            return

        executor.run_process_batch(
            name=self.name,
            ds=ds,
            idx_count=idx_count,
            idx_gen=idx_gen,
            process_fn=self.process_batch,
            run_config=run_config,
            executor_config=self.executor_config,
        )

        ds.event_logger.log_step_full_complete(self.name)

    def run_changelist(
        self,
        ds: DataStore,
        change_list: ChangeList,
        run_config: Optional[RunConfig] = None,
        executor: Optional[Executor] = None,
    ) -> ChangeList:
        if executor is None:
            executor = SingleThreadExecutor()

        run_config = RunConfig.add_labels(run_config, {"step_name": self.name})

        (idx_count, idx_gen) = self.get_change_list_process_ids(
            ds, change_list, run_config
        )

        logger.info(f"Batches to process {idx_count}")

        if idx_count is not None and idx_count == 0:
            return ChangeList()

        logger.info(f"Running: {self.name}")

        changes = executor.run_process_batch(
            name=self.name,
            ds=ds,
            idx_count=idx_count,
            idx_gen=idx_gen,
            process_fn=self.process_batch,
            run_config=run_config,
            executor_config=self.executor_config,
        )

        return changes

    def run_idx(
        self,
        ds: DataStore,
        idx: IndexDF,
        run_config: Optional[RunConfig] = None,
        executor: Optional[Executor] = None,
    ) -> ChangeList:
        if executor is None:
            executor = SingleThreadExecutor()

        logger.info(f"Running: {self.name}")
        run_config = RunConfig.add_labels(run_config, {"step_name": self.name})

        return self.process_batch(
            ds=ds,
            idx=idx,
            run_config=run_config,
        )


@dataclass
class DatatableBatchTransform(PipelineStep):
    func: DatatableBatchTransformFunc
    inputs: List[str]
    outputs: List[str]
    chunk_size: int = 1000
    transform_keys: Optional[List[str]] = None
    kwargs: Optional[Dict] = None
    labels: Optional[Labels] = None

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        input_dts = [catalog.get_datatable(ds, name) for name in self.inputs]
        output_dts = [catalog.get_datatable(ds, name) for name in self.outputs]

        return [
            DatatableBatchTransformStep(
                ds=ds,
                name=f"{self.func.__name__}",
                func=self.func,
                input_dts=input_dts,
                output_dts=output_dts,
                kwargs=self.kwargs,
                transform_keys=self.transform_keys,
                chunk_size=self.chunk_size,
                labels=self.labels,
            )
        ]


class DatatableBatchTransformStep(BaseBatchTransformStep):
    def __init__(
        self,
        ds: DataStore,
        name: str,
        func: DatatableBatchTransformFunc,
        input_dts: List[DataTable],
        output_dts: List[DataTable],
        kwargs: Optional[Dict] = None,
        transform_keys: Optional[List[str]] = None,
        chunk_size: int = 1000,
        labels: Optional[Labels] = None,
    ) -> None:
        super().__init__(
            ds=ds,
            name=name,
            input_dts=input_dts,
            output_dts=output_dts,
            transform_keys=transform_keys,
            chunk_size=chunk_size,
            labels=labels,
        )

        self.func = func
        self.kwargs = kwargs

    def process_batch_dts(
        self,
        ds: DataStore,
        idx: IndexDF,
        run_config: Optional[RunConfig] = None,
    ) -> Optional[TransformResult]:
        return self.func(
            ds=ds,
            idx=idx,
            input_dts=self.input_dts,
            run_config=run_config,
            kwargs=self.kwargs,
        )


@dataclass
class BatchTransform(PipelineStep):
    func: BatchTransformFunc
    inputs: List[str]
    outputs: List[str]
    chunk_size: int = 1000
    kwargs: Optional[Dict[str, Any]] = None
    transform_keys: Optional[List[str]] = None
    labels: Optional[Labels] = None
    executor_config: Optional[ExecutorConfig] = None
    filters: Optional[Union[LabelDict, Callable[[], LabelDict]]] = None
    order_by: Optional[List[str]] = None
    order: Literal["asc", "desc"] = "asc"

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        input_dts = [catalog.get_datatable(ds, name) for name in self.inputs]
        output_dts = [catalog.get_datatable(ds, name) for name in self.outputs]

        return [
            BatchTransformStep(
                ds=ds,
                name=f"{self.func.__name__}",  # type: ignore # mypy bug: https://github.com/python/mypy/issues/10976
                input_dts=input_dts,
                output_dts=output_dts,
                func=self.func,
                kwargs=self.kwargs,
                transform_keys=self.transform_keys,
                chunk_size=self.chunk_size,
                labels=self.labels,
                executor_config=self.executor_config,
                filters=self.filters,
                order_by=self.order_by,
                order=self.order,
            )
        ]


class BatchTransformStep(BaseBatchTransformStep):
    def __init__(
        self,
        ds: DataStore,
        name: str,
        func: BatchTransformFunc,
        input_dts: List[DataTable],
        output_dts: List[DataTable],
        kwargs: Optional[Dict[str, Any]] = None,
        transform_keys: Optional[List[str]] = None,
        chunk_size: int = 1000,
        labels: Optional[Labels] = None,
        executor_config: Optional[ExecutorConfig] = None,
        filters: Optional[Union[LabelDict, Callable[[], LabelDict]]] = None,
        order_by: Optional[List[str]] = None,
        order: Literal["asc", "desc"] = "asc",
    ) -> None:
        super().__init__(
            ds=ds,
            name=name,
            input_dts=input_dts,
            output_dts=output_dts,
            transform_keys=transform_keys,
            chunk_size=chunk_size,
            labels=labels,
            executor_config=executor_config,
            filters=filters,
            order_by=order_by,
            order=order,
        )

        self.func = func
        self.kwargs = kwargs
        self.parameters = inspect.signature(self.func).parameters

    def process_batch_dts(
        self,
        ds: DataStore,
        idx: IndexDF,
        run_config: Optional[RunConfig] = None,
    ) -> Optional[TransformResult]:
        with tracer.start_as_current_span("get input data"):
            input_dfs = self.get_batch_input_dfs(ds, idx, run_config)

        if "idx" not in self.parameters and sum(len(j) for j in input_dfs) == 0:
            return None

        with tracer.start_as_current_span("run transform"):
            output_dfs = self.process_batch_dfs(
                ds=ds,
                idx=idx,
                input_dfs=input_dfs,
                run_config=run_config,
            )

        return output_dfs

    def process_batch_dfs(
        self,
        ds: DataStore,
        idx: IndexDF,
        input_dfs: List[DataDF],
        run_config: Optional[RunConfig] = None,
    ) -> TransformResult:
        kwargs = {
            **({"ds": ds} if "ds" in self.parameters else {}),
            **({"idx": idx} if "idx" in self.parameters else {}),
            **({"run_config": run_config} if "run_config" in self.parameters else {}),
            **(self.kwargs or {}),
        }
        return self.func(*input_dfs, **kwargs)

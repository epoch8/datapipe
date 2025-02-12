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
from sqlalchemy import alias, func, select
from sqlalchemy.sql.expression import select
from tqdm_loggable.auto import tqdm

from datapipe.compute import (
    Catalog,
    ComputeInput,
    ComputeStep,
    PipelineStep,
    StepStatus,
)
from datapipe.datatable import DataStore, DataTable, MetaTable
from datapipe.executor import Executor, ExecutorConfig, SingleThreadExecutor
from datapipe.meta.sql_meta import TransformMetaTable, build_changed_idx_sql
from datapipe.run_config import LabelDict, RunConfig
from datapipe.types import (
    ChangeList,
    DataDF,
    IndexDF,
    JoinSpec,
    Labels,
    MetaSchema,
    PipelineInput,
    Required,
    TableOrName,
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
    ) -> TransformResult: ...


BatchTransformFunc = Callable[..., TransformResult]


class BaseBatchTransformStep(ComputeStep):
    """
    Abstract class for batch transform steps
    """

    def __init__(
        self,
        ds: DataStore,
        name: str,
        input_dts: List[ComputeInput],
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

        # Force transform_keys to be a list, otherwise Pandas will not be happy
        if transform_keys is not None and not isinstance(transform_keys, list):
            transform_keys = list(transform_keys)

        self.transform_keys, self.transform_schema = self.compute_transform_schema(
            [inp.dt.meta_table for inp in input_dts],
            [out.meta_table for out in output_dts],
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

    def get_status(self, ds: DataStore) -> StepStatus:
        return StepStatus(
            name=self.name,
            total_idx_count=self.meta_table.get_metadata_size(),
            changed_idx_count=self.get_changed_idx_count(ds),
        )

    def get_changed_idx_count(
        self,
        ds: DataStore,
        run_config: Optional[RunConfig] = None,
    ) -> int:
        run_config = self._apply_filters_to_run_config(run_config)
        _, sql = build_changed_idx_sql(
            ds=ds,
            meta_table=self.meta_table,
            input_dts=self.input_dts,
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

            join_keys, u1 = build_changed_idx_sql(
                ds=ds,
                meta_table=self.meta_table,
                input_dts=self.input_dts,
                transform_keys=self.transform_keys,
                run_config=run_config,
                order_by=self.order_by,
                order=self.order,  # type: ignore  # pylance is stupid
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
        run_config: Optional[RunConfig] = None,
    ) -> Tuple[int, Iterable[IndexDF]]:
        run_config = self._apply_filters_to_run_config(run_config)
        with tracer.start_as_current_span("compute ids to process"):
            changes = [pd.DataFrame(columns=self.transform_keys)]

            for inp in self.input_dts:
                if inp.dt.name in change_list.changes:
                    idx = change_list.changes[inp.dt.name]
                    if any([key not in idx.columns for key in self.transform_keys]):
                        # TODO пересмотреть эту логику, выглядит избыточной
                        # (возможно, достаточно посчитать один раз для всех
                        # input таблиц)
                        _, sql = build_changed_idx_sql(
                            ds=ds,
                            meta_table=self.meta_table,
                            input_dts=self.input_dts,
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

            chunk_count = math.ceil(len(idx) / self.chunk_size)

            def gen():
                for i in range(chunk_count):
                    yield cast(
                        IndexDF, idx[i * self.chunk_size : (i + 1) * self.chunk_size]
                    )

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

        idx_records = idx.to_dict(orient="records")

        logger.error(
            f"Process batch in transform {self.name} on idx {idx_records} failed: {str(e)}"
        )
        ds.event_logger.log_exception(
            e,
            run_config=RunConfig.add_labels(
                run_config,
                {"idx": idx_records, "process_ts": process_ts},
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
        return [inp.dt.get_data(idx) for inp in self.input_dts]

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
    inputs: List[TableOrName]
    outputs: List[TableOrName]
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
                input_dts=[ComputeInput(dt=inp, join_type="full") for inp in input_dts],
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
        input_dts: List[ComputeInput],
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
            input_dts=[inp.dt for inp in self.input_dts],
            run_config=run_config,
            kwargs=self.kwargs,
        )


@dataclass
class BatchTransform(PipelineStep):
    func: BatchTransformFunc
    inputs: List[PipelineInput]
    outputs: List[TableOrName]
    chunk_size: int = 1000
    kwargs: Optional[Dict[str, Any]] = None
    transform_keys: Optional[List[str]] = None
    labels: Optional[Labels] = None
    executor_config: Optional[ExecutorConfig] = None
    filters: Optional[Union[LabelDict, Callable[[], LabelDict]]] = None
    order_by: Optional[List[str]] = None
    order: Literal["asc", "desc"] = "asc"

    def pipeline_input_to_compute_input(
        self, ds: DataStore, catalog: Catalog, input: PipelineInput
    ) -> ComputeInput:
        if isinstance(input, Required):
            return ComputeInput(
                dt=catalog.get_datatable(ds, input.table),
                join_type="inner",
            )
        elif isinstance(input, JoinSpec):
            # This should not happen, but just in case
            return ComputeInput(
                dt=catalog.get_datatable(ds, input.table),
                join_type="full",
            )
        else:
            return ComputeInput(dt=catalog.get_datatable(ds, input), join_type="full")

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        input_dts = [
            self.pipeline_input_to_compute_input(ds, catalog, input)
            for input in self.inputs
        ]
        output_dts = [catalog.get_datatable(ds, name) for name in self.outputs]

        return [
            BatchTransformStep(
                ds=ds,
                name=f"{self.func.__name__}",
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
        input_dts: List[ComputeInput],
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

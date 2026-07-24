import copy
import inspect
import logging
import time
from dataclasses import dataclass, replace
from typing import (
    Any,
    Callable,
    Iterable,
    Literal,
    Sequence,
    Tuple,
    cast
)

import pandas as pd
from opentelemetry import trace
from tqdm_loggable.auto import tqdm

from datapipe.compute import (
    Catalog,
    ComputeInput,
    ComputeOutput,
    ComputeStep,
    ChaimComputeStep,
    PipelineStep,
    StepStatus,
    make_mungled_step_name,
    pipeline_input_to_compute_input,
    pipeline_output_to_compute_output,
)
from datapipe.datatable import DataStore
from datapipe.executor import Executor, ExecutorConfig, SingleThreadExecutor
from datapipe.run_config import LabelDict, RunConfig
from datapipe.types import (
    ChangeList,
    DataDF,
    IndexDF,
    ChainIndexDF,
    Labels,
    PipelineInput,
    PipelineOutput,
    TransformResult,
)

logger = logging.getLogger("datapipe.step.chain_transform")
tracer = trace.get_tracer("datapipe.step.chain_transform")


# # TODO подумать, может быть мы хотим дать возможность возвращать итератор TransformResult
# class DatatableChainTransformFunc(Protocol):
#     __name__: str

#     def __call__(
#         self,
#         ds: DataStore,
#         idx: IndexDF,
#         input_dts: list[DataTable],
#         previous_dts: list[DataTable],
#         run_config: RunConfig | None = None,
#         kwargs: dict[str, Any] | None = None,
#     ) -> TransformResult: ...


ChainTransformFunc = Callable[..., TransformResult]
ChainTransformRankFunc =  Callable[..., int]


class BaseChainTransformStep(ChaimComputeStep):
    """
    Abstract class for chain transform steps
    """

    def __init__(
        self,
        ds: DataStore,
        name: str,
        input_dts: Sequence[ComputeInput],
        previous_dts: Sequence[ComputeInput],
        output_dts: Sequence[ComputeOutput],
        rank_func: ChainTransformRankFunc,
        transform_keys: list[str] | None = None,
        chunk_size: int = 1,
        window_size: int = 1,
        labels: Labels | None = None,
        executor_config: ExecutorConfig | None = None,
        filters: LabelDict | Callable[[], LabelDict] | None = None,
        order_by: list[str] | None = None,
        order: Literal["asc", "desc"] = "asc",
    ) -> None:
        if not executor_config:
            executor_config = ExecutorConfig(parallelism=1)
        elif executor_config.parallelism > 1:
            logger.warning(
                f"ChainTransform require 1 thread execution: "
                f"parallelism changed from {executor_config.parallelism} to 1 for this step"
            )

            executor_config = replace(executor_config, parallelism = 1)

        ChaimComputeStep.__init__(
            self,
            name=name,
            input_dts=input_dts,
            previous_dts=previous_dts,
            output_dts=output_dts,
            labels=labels,
            executor_config=executor_config,
        )

        self.output_specs = output_dts  # ????

        # Force transform_keys to be a list, otherwise Pandas will not be happy
        if transform_keys is not None and not isinstance(transform_keys, list):
            transform_keys = list(transform_keys)

        self.meta = ds.meta_plane.create_chain_transform_meta(
            name=f"{self.name}_meta",
            input_dts=self.input_dts,
            previous_dts=self.previous_dts,
            output_dts=self.output_dts,
            transform_keys=transform_keys,
            order=order,
        )

        self.transform_keys, self.transform_schema = self.meta.transform_keys, self.meta.transform_keys_schema

        self.filters = filters
        self.rank_func = rank_func
        self.chunk_size = chunk_size
        self.window_size = window_size
        self.order = order

    def fill_metadata(self, ds: DataStore) -> None:
        self._update_transform_ids(ds=ds)

    def get_full_process_ids(
        self,
        ds: DataStore,
        chunk_size: int | None = None,
        window_size: int | None = None,
        run_config: RunConfig | None = None,
    ) -> tuple[int, Iterable[IndexDF]]:
        """
        Метод для получения перечня индексов для обработки.

        Returns: (idx_size, iterator<idx_df>)

        - idx_size - количество индексов требующих обработки
        - idx_df - датафрейм без колонок с данными, только индексная колонка
        """
        run_config = self._apply_filters_to_run_config(run_config)
        chunk_size = chunk_size or self.chunk_size
        window_size = window_size or self.window_size

        return self.meta.get_full_process_ids(
            ds=ds, 
            chunk_size=chunk_size,
            window_size=window_size,
            run_config=run_config
        )
    
    def get_change_list_process_ids(
        self,
        ds: DataStore,
        change_list: ChangeList,
        run_config: RunConfig | None = None,
    ) -> tuple[int, Iterable[IndexDF]]:
        run_config = self._apply_filters_to_run_config(run_config)
        return self.meta.get_change_list_process_ids(
            ds=ds,
            change_list=change_list,
            chunk_size=self.chunk_size,
            window_size=self.window_size,
            run_config=run_config,
        )

    def get_idx_process_ids(
        self,
        ds: DataStore,
        idx: IndexDF,
        run_config: RunConfig | None = None,
    ) -> tuple[int, Iterable[IndexDF]]:
        run_config = self._apply_filters_to_run_config(run_config)
        return self.meta.get_idx_process_ids(
            ds=ds,
            idx=idx,
            chunk_size=self.chunk_size,
            window_size=self.window_size,
            run_config=run_config,
        )

    def get_batch_input_dfs(
        self,
        ds: DataStore,
        idx: IndexDF,
        run_config: RunConfig | None = None,
    ) -> list[DataDF]:
        # TODO consider parallel fetch through executor
        return [inp.dt.get_data(inp.dt.meta.transform_idx_to_table_idx(idx, inp.keys)) for inp in self.input_dts]
    
    def get_batch_previous_dfs(
        self,
        ds: DataStore,
        idx: IndexDF,
        run_config: RunConfig | None = None,
    ) -> list[DataDF]:
        # TODO consider parallel fetch through executor
        return [prev.dt.get_data(prev.dt.meta.transform_idx_to_table_idx(idx, prev.keys)) for prev in self.previous_dts]

    def process_batch_dts(
        self,
        ds: DataStore,
        idx: IndexDF,
        prev_idx: IndexDF,
        run_config: RunConfig | None = None,
    ) -> TransformResult | None:
        with tracer.start_as_current_span("get input data"):
            input_dfs = self.get_batch_input_dfs(ds, idx, run_config)
            previous_dfs = self.get_batch_previous_dfs(ds, prev_idx, run_config)

        if sum(len(j) for j in input_dfs) == 0 and sum(len(j) for j in previous_dfs) == 0:
            return None

        with tracer.start_as_current_span("run transform"):
            output_dfs = self.process_batch_dfs(
                ds=ds,
                idx=idx,
                input_dfs=input_dfs,
                previous_dfs=previous_dfs,
                run_config=run_config,
            )

        return output_dfs

    def process_batch_dfs(
        self,
        ds: DataStore,
        idx: IndexDF,
        input_dfs: list[DataDF],
        previous_dfs: list[DataDF],
        run_config: RunConfig | None = None,
    ) -> TransformResult:
        raise NotImplementedError()

    def process_batch(
        self,
        ds: DataStore,
        idx: Tuple[IndexDF, IndexDF],
        run_config: RunConfig | None = None,
    ) -> ChangeList:
        new_idx, prev_idx = idx

        with tracer.start_as_current_span("process batch"):
            logger.debug(f"Idx to process: {new_idx.to_records()}, previous idx: {prev_idx.to_records()}")
            
            process_ts = time.time()

            try:
                output_dfs = self.process_batch_dts(ds, new_idx, prev_idx, run_config)

                return self.store_batch_result(ds, new_idx, output_dfs, process_ts, run_config)

            except Exception as e:
                self.store_batch_err(ds, new_idx, e, process_ts, run_config)

                raise e

    def store_batch_result(
        self,
        ds: DataStore,
        idx: IndexDF,
        output_dfs: TransformResult | None,
        process_ts: float,
        run_config: RunConfig | None = None,
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

                for k, output_spec in enumerate(self.output_specs):
                    res_dt = output_spec.dt
                    # Берем k-ое значение функции для k-ой таблички
                    # Добавляем результат в результирующие чанки
                    change_idx = res_dt.store_chunk(
                        data_df=output_dfs[k],
                        processed_idx=self._transform_idx_to_output_idx(idx, output_spec),
                        now=process_ts,
                        run_config=run_config,
                    )

                    changes.append(res_dt.name, change_idx)

        else:
            with tracer.start_as_current_span("delete missing data from output"):
                for k, output_spec in enumerate(self.output_specs):
                    res_dt = output_spec.dt
                    processed_idx = self._transform_idx_to_output_idx(idx, output_spec)
                    if processed_idx is None:
                        continue

                    del_idx = res_dt.meta.get_existing_idx(processed_idx)

                    res_dt.delete_by_idx(del_idx, run_config=run_config)

                    changes.append(res_dt.name, del_idx)

        self.meta.mark_rows_processed_success(idx, process_ts=process_ts, run_config=run_config)

        return changes

    def store_batch_err(
        self,
        ds: DataStore,
        idx: IndexDF,
        e: Exception,
        process_ts: float,
        run_config: RunConfig | None = None,
    ) -> None:
        run_config = self._apply_filters_to_run_config(run_config)

        idx_records = idx.to_dict(orient="records")

        logger.error(f"Process batch in transform {self.name} on idx {idx_records} failed: {str(e)}")
        ds.event_logger.log_exception(
            e,
            run_config=RunConfig.add_labels(
                run_config,
                {"idx": idx_records, "process_ts": process_ts},
            ),
        )

        self.meta.mark_rows_processed_error(
            idx,
            process_ts=process_ts,
            error=str(e),
            run_config=run_config,
        )

    def run_full(
        self,
        ds: DataStore,
        run_config: RunConfig | None = None,
        executor: Executor | None = None,
    ) -> None:
        # TODO work with other executors 
        if executor is None:
            executor = SingleThreadExecutor()

        logger.info(f"Running: {self.name}")
        run_config = RunConfig.add_labels(run_config, {"step_name": self.name})
        
        self._update_transform_ids(ds=ds)

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
        run_config: RunConfig | None = None,
        executor: Executor | None = None,
    ) -> ChangeList:
        logger.warning(
            "ChainTransform is running in changelist mode." \
            "Use it carefully because this step update transform index and " \
            "process not only items in ChangeList"
        )

        if executor is None:
            executor = SingleThreadExecutor()

        run_config = RunConfig.add_labels(run_config, {"step_name": self.name})

        self._update_transform_ids(ds=ds)

        (idx_count, idx_gen) = self.get_change_list_process_ids(ds, change_list, run_config)

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
        run_config: RunConfig | None = None,
        executor: Executor | None = None,
    ) -> ChangeList:
        if executor is None:
            executor = SingleThreadExecutor()

        logger.warning(
            "ChainTransform is running in idx mode." \
            "Use it carefully because this step update transform index and " \
            "process not only items in idx"
        )
        
        if executor is None:
            executor = SingleThreadExecutor()

        logger.info(f"Running: {self.name}")
        run_config = RunConfig.add_labels(run_config, {"step_name": self.name})

        (idx_count, idx_gen) = self.get_idx_process_ids(ds, idx, run_config)
        
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

    def reset_metadata(self, ds: DataStore) -> None:
        self.meta.mark_all_rows_unprocessed()

    def get_status(self, ds: DataStore) -> StepStatus:
        return StepStatus(
            name=self.name,
            total_idx_count=self.meta.get_metadata_size(),
            changed_idx_count=self.meta.get_changed_idx_count(ds),
        )

    def _update_transform_ids(self, ds: DataStore) -> None:
        logger.info(f"Update chain transform index")

        idx_len, idx_gen = self.meta.get_new_process_ids(ds=ds, chunk_size=2)

        if not idx_len:
            return

        for idx in tqdm(idx_gen, total=idx_len):
            idx['rank'] = idx.apply(lambda x: self.rank_func(**x.to_dict()), axis=1)

            self.meta.insert_rows(cast(ChainIndexDF, idx))

    # TODO consider making this method a property of ComputeOutput
    # Also see TableMeta.transform_idx_to_table_idx for the same functionality
    @staticmethod
    def _transform_idx_to_output_idx(
        idx: IndexDF,
        output_spec: ComputeOutput,
    ) -> IndexDF | None:
        res_dt = output_spec.dt
        output_to_transform_keys = {
            output_key: transform_key for transform_key, output_key in (output_spec.keys or {}).items()
        }
        columns: dict[str, Any] = {}

        for pk in res_dt.primary_keys:
            if pk in idx.columns:
                columns[pk] = idx[pk]
                continue

            transform_key = output_to_transform_keys.get(pk)
            if transform_key is not None and transform_key in idx.columns:
                columns[pk] = idx[transform_key]

        if not columns:
            return None

        return IndexDF(pd.DataFrame(columns))

    def _apply_filters_to_run_config(self, run_config: RunConfig | None = None) -> RunConfig | None:
        if self.filters is None:
            return run_config
        else:
            if isinstance(self.filters, dict):
                filters = self.filters
            elif isinstance(self.filters, Callable):  # type: ignore
                filters = self.filters()
            else:
                filters = {}

            if run_config is None:
                return RunConfig(filters=filters)
            else:
                run_config = copy.deepcopy(run_config)
                filters = copy.deepcopy(filters)
                filters.update(run_config.filters)
                run_config.filters = filters
                return run_config


@dataclass
class ChainTransform(PipelineStep):
    func: ChainTransformFunc
    inputs: list[PipelineInput]
    previous: list[PipelineInput]
    outputs: list[PipelineOutput]
    rank_func: ChainTransformRankFunc
    chunk_size: int = 1
    window_size: int = 1
    name: str | None = None
    kwargs: dict[str, Any] | None = None
    transform_keys: list[str] | None = None
    labels: Labels | None = None
    executor_config: ExecutorConfig | None = None
    filters: LabelDict | Callable[[], LabelDict] | None = None
    order_by: list[str] | None = None
    order: Literal["asc", "desc"] = "asc"

    def build_compute(self, ds: DataStore, catalog: Catalog) -> list[ComputeStep]:
        input_dts = [pipeline_input_to_compute_input(ds, catalog, input) for input in self.inputs]
        previous_dts = [pipeline_input_to_compute_input(ds, catalog, prev) for prev in self.previous]
        output_dts = [pipeline_output_to_compute_output(ds, catalog, output) for output in self.outputs]

        step_name = self.name or make_mungled_step_name(ChainTransformStep, self.func.__name__, input_dts, output_dts)

        return [
            ChainTransformStep(
                ds=ds,
                name=step_name,
                input_dts=input_dts,
                previous_dts=previous_dts,
                output_dts=output_dts,
                func=self.func,
                rank_func=self.rank_func,
                kwargs=self.kwargs,
                transform_keys=self.transform_keys,
                chunk_size=self.chunk_size,
                window_size=self.window_size,
                labels=self.labels,
                executor_config=self.executor_config,
                filters=self.filters,
                order_by=self.order_by,
                order=self.order,
            )
        ]


class ChainTransformStep(BaseChainTransformStep):
    def __init__(
        self,
        ds: DataStore,
        name: str,
        func: ChainTransformFunc,
        rank_func: ChainTransformRankFunc,
        input_dts: Sequence[ComputeInput],
        previous_dts: Sequence[ComputeInput],
        output_dts: Sequence[ComputeOutput],
        kwargs: dict[str, Any] | None = None,
        transform_keys: list[str] | None = None,
        chunk_size: int = 1,
        window_size: int = 1,
        labels: Labels | None = None,
        executor_config: ExecutorConfig | None = None,
        filters: LabelDict | Callable[[], LabelDict] | None = None,
        order_by: list[str] | None = None,
        order: Literal["asc", "desc"] = "asc",
    ) -> None:
        super().__init__(
            ds=ds,
            name=name,
            input_dts=input_dts,
            previous_dts=previous_dts,
            output_dts=output_dts,
            rank_func=rank_func,
            transform_keys=transform_keys,
            chunk_size=chunk_size,
            window_size=window_size,
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
        prev_idx: IndexDF,
        run_config: RunConfig | None = None,
    ) -> TransformResult | None:
        with tracer.start_as_current_span("get input data"):
            input_dfs = self.get_batch_input_dfs(ds, idx, run_config)
            previous_dfs = self.get_batch_previous_dfs(ds, prev_idx, run_config)

        if (
            "idx" not in self.parameters and 
            sum(len(j) for j in input_dfs) == 0 and 
            sum(len(j) for j in previous_dfs) == 0
        ):
            return None

        with tracer.start_as_current_span("run transform"):
            output_dfs = self.process_batch_dfs(
                ds=ds,
                idx=idx,
                input_dfs=input_dfs,
                previous_dfs=previous_dfs,
                run_config=run_config,
            )

        return output_dfs

    def process_batch_dfs(
        self,
        ds: DataStore,
        idx: IndexDF,
        input_dfs: list[DataDF],
        previous_dfs: list[DataDF],
        run_config: RunConfig | None = None,
    ) -> TransformResult:
        kwargs = {
            **({"ds": ds} if "ds" in self.parameters else {}),
            **({"idx": idx} if "idx" in self.parameters else {}),
            **({"run_config": run_config} if "run_config" in self.parameters else {}),
            **(self.kwargs or {}),
        }
        return self.func(*input_dfs, *previous_dfs, **kwargs)

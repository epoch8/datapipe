import logging
import time
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Protocol,
    Tuple,
    cast,
)

import tqdm
from opentelemetry import trace

from datapipe.compute import Catalog, ComputeStep, PipelineStep
from datapipe.datatable import DataStore, DataTable
from datapipe.run_config import RunConfig
from datapipe.types import ChangeList, DataDF, IndexDF, Labels, TransformResult

logger = logging.getLogger("datapipe.core_steps")
tracer = trace.get_tracer("datapipe.core_steps")


class DatatableTransformFunc(Protocol):
    __name__: str

    def __call__(
        self,
        ds: DataStore,
        input_dts: List[DataTable],
        output_dts: List[DataTable],
        run_config: Optional[RunConfig],
        # Возможно, лучше передавать как переменную, а не  **
        **kwargs,
    ) -> None:
        ...


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


# TODO подумать, может быть мы хотим дать возможность возвращать итератор TransformResult
BatchTransformFunc = Callable[..., TransformResult]

BatchGenerateFunc = Callable[..., Iterator[TransformResult]]


@dataclass
class DatatableTransform(PipelineStep):
    func: DatatableTransformFunc
    inputs: List[str]
    outputs: List[str]
    check_for_changes: bool = True
    kwargs: Optional[Dict[str, Any]] = None
    labels: Optional[Labels] = None

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List["ComputeStep"]:
        return [
            DatatableTransformStep(
                name=self.func.__name__,
                input_dts=[catalog.get_datatable(ds, i) for i in self.inputs],
                output_dts=[catalog.get_datatable(ds, i) for i in self.outputs],
                func=self.func,
                kwargs=self.kwargs,
                check_for_changes=self.check_for_changes,
                labels=self.labels,
            )
        ]


class DatatableTransformStep(ComputeStep):
    def __init__(
        self,
        name: str,
        input_dts: List[DataTable],
        output_dts: List[DataTable],
        func: DatatableTransformFunc,
        kwargs: Optional[Dict] = None,
        check_for_changes: bool = True,
        labels: Optional[Labels] = None,
    ) -> None:
        ComputeStep.__init__(self, name, input_dts, output_dts, labels)

        self.func = func
        self.kwargs = kwargs or {}
        self.check_for_changes = check_for_changes

    def run_full(self, ds: DataStore, run_config: Optional[RunConfig] = None) -> None:
        logger.info(f"Running: {self.name}")

        if len(self.input_dts) > 0 and self.check_for_changes:
            with tracer.start_as_current_span("check for changes"):
                changed_idx_count = ds.get_changed_idx_count(
                    inputs=self.input_dts,
                    outputs=self.output_dts,
                    run_config=run_config,
                )

                if changed_idx_count == 0:
                    logger.debug(f"Skipping {self.get_name()} execution - nothing to compute")

                    return

        run_config = RunConfig.add_labels(run_config, {"step_name": self.get_name()})

        with tracer.start_as_current_span(f"Run {self.func}"):
            try:
                self.func(
                    ds=ds,
                    input_dts=self.input_dts,
                    output_dts=self.output_dts,
                    run_config=run_config,
                    kwargs=self.kwargs,
                )
            except Exception as e:
                logger.error(f"Datatable transform ({self.func}) run failed: {str(e)}")
                ds.event_logger.log_exception(e, run_config=run_config)


@dataclass
class BatchTransform(PipelineStep):
    func: BatchTransformFunc
    inputs: List[str]
    outputs: List[str]
    chunk_size: int = 1000
    kwargs: Optional[Dict[str, Any]] = None
    labels: Optional[Labels] = None

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        input_dts = [catalog.get_datatable(ds, name) for name in self.inputs]
        output_dts = [catalog.get_datatable(ds, name) for name in self.outputs]

        return [
            BatchTransformStep(
                f"{self.func.__name__}",  # type: ignore # mypy bug: https://github.com/python/mypy/issues/10976
                input_dts=input_dts,
                output_dts=output_dts,
                func=self.func,
                kwargs=self.kwargs,
                chunk_size=self.chunk_size,
                labels=self.labels,
            )
        ]


class BatchTransformStep(ComputeStep):
    def __init__(
        self,
        name: str,
        func: BatchTransformFunc,
        input_dts: List[DataTable],
        output_dts: List[DataTable],
        kwargs: Optional[Dict[str, Any]] = None,
        chunk_size: int = 1000,
        labels: Optional[Labels] = None,
    ) -> None:
        ComputeStep.__init__(self, name, input_dts, output_dts, labels)

        self.func = func
        self.kwargs = kwargs or {}
        self.chunk_size = chunk_size

    def get_full_process_ids(
        self,
        ds: DataStore,
        run_config: Optional[RunConfig] = None,
    ) -> Tuple[int, Iterable[IndexDF]]:
        with tracer.start_as_current_span("compute ids to process"):
            return ds.get_full_process_ids(
                inputs=self.input_dts,
                outputs=self.output_dts,
                chunk_size=self.chunk_size,
                run_config=run_config,
            )

    def get_change_list_process_ids(
        self,
        ds: DataStore,
        change_list: ChangeList,
        run_config: Optional[RunConfig] = None,
    ) -> Tuple[int, Iterable[IndexDF]]:
        with tracer.start_as_current_span("compute ids to process"):
            return ds.get_change_list_process_ids(
                inputs=self.input_dts,
                outputs=self.output_dts,
                change_list=change_list,
                chunk_size=self.chunk_size,
                run_config=run_config,
            )

    def process_batch_dfs(
        self,
        ds: DataStore,
        idx: IndexDF,
        input_dfs: List[DataDF],
        run_config: Optional[RunConfig] = None,
    ) -> TransformResult:
        return self.func(*input_dfs, **self.kwargs or {})


@dataclass
class DatatableBatchTransform(PipelineStep):
    func: DatatableBatchTransformFunc
    inputs: List[str]
    outputs: List[str]
    chunk_size: int = 1000
    kwargs: Optional[Dict] = None
    labels: Optional[Labels] = None

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        input_dts = [catalog.get_datatable(ds, name) for name in self.inputs]
        output_dts = [catalog.get_datatable(ds, name) for name in self.outputs]

        return [
            DatatableBatchTransformStep(
                f"{self.func.__name__}",
                func=self.func,
                input_dts=input_dts,
                output_dts=output_dts,
                kwargs=self.kwargs,
                chunk_size=self.chunk_size,
                labels=self.labels,
            )
        ]


class DatatableBatchTransformStep(ComputeStep):
    def __init__(
        self,
        name: str,
        func: DatatableBatchTransformFunc,
        input_dts: List[DataTable],
        output_dts: List[DataTable],
        kwargs: Optional[Dict] = None,
        chunk_size: int = 1000,
        labels: Optional[Labels] = None,
    ) -> None:
        super().__init__(name, input_dts, output_dts, labels)

        self.func = func
        self.kwargs = kwargs
        self.chunk_size = chunk_size

    def get_full_process_ids(
        self,
        ds: DataStore,
        run_config: Optional[RunConfig] = None,
    ) -> Tuple[int, Iterable[IndexDF]]:
        with tracer.start_as_current_span("compute ids to process"):
            return ds.get_full_process_ids(
                inputs=self.input_dts,
                outputs=self.output_dts,
                chunk_size=self.chunk_size,
                run_config=run_config,
            )

    def get_change_list_process_ids(
        self,
        ds: DataStore,
        change_list: ChangeList,
        run_config: Optional[RunConfig] = None,
    ) -> Tuple[int, Iterable[IndexDF]]:
        with tracer.start_as_current_span("compute ids to process"):
            return ds.get_change_list_process_ids(
                inputs=self.input_dts,
                outputs=self.output_dts,
                change_list=change_list,
                chunk_size=self.chunk_size,
                run_config=run_config,
            )

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


def do_batch_generate(
    func: BatchGenerateFunc,
    ds: DataStore,
    output_dts: List[DataTable],
    run_config: Optional[RunConfig] = None,
    kwargs: Optional[Dict] = None,
) -> None:
    import inspect

    import pandas as pd

    """
    Создание новой таблицы из результатов запуска `proc_func`.
    Функция может быть как обычной, так и генерирующейся
    """

    now = time.time()
    empty_generator = True

    assert inspect.isgeneratorfunction(func), "Starting v0.8.0 proc_func should be a generator"

    with tracer.start_as_current_span("init generator"):
        try:
            iterable = func(**kwargs or {})
        except Exception as e:
            # mypy bug: https://github.com/python/mypy/issues/10976
            logger.exception(f"Generating failed ({func.__name__}): {str(e)}")  # type: ignore
            ds.event_logger.log_exception(e, run_config=run_config)

            raise e

    while True:
        with tracer.start_as_current_span("get next batch"):
            try:
                chunk_dfs = next(iterable)

                if isinstance(chunk_dfs, pd.DataFrame):
                    chunk_dfs = (chunk_dfs,)
            except StopIteration:
                if empty_generator:
                    for k, dt_k in enumerate(output_dts):
                        dt_k.event_logger.log_state(
                            dt_k.name,
                            added_count=0,
                            updated_count=0,
                            deleted_count=0,
                            processed_count=0,
                            run_config=run_config,
                        )

                break
            except Exception as e:
                logger.exception(f"Generating failed ({func}): {str(e)}")
                ds.event_logger.log_exception(e, run_config=run_config)

                # raise e
                return

        empty_generator = False

        with tracer.start_as_current_span("store results"):
            for k, dt_k in enumerate(output_dts):
                dt_k.store_chunk(chunk_dfs[k], run_config=run_config)

    with tracer.start_as_current_span("delete stale rows"):
        for k, dt_k in enumerate(output_dts):
            dt_k.delete_stale_by_process_ts(now, run_config=run_config)


@dataclass
class BatchGenerate(PipelineStep):
    func: BatchGenerateFunc
    outputs: List[str]
    kwargs: Optional[Dict] = None
    labels: Optional[Labels] = None

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        return [
            DatatableTransformStep(
                name=self.func.__name__,
                func=cast(
                    DatatableTransformFunc,
                    lambda ds, input_dts, output_dts, run_config, kwargs: do_batch_generate(
                        func=self.func, ds=ds, output_dts=output_dts, run_config=run_config, kwargs=kwargs
                    ),
                ),
                input_dts=[],
                output_dts=[catalog.get_datatable(ds, name) for name in self.outputs],
                check_for_changes=False,
                kwargs=self.kwargs,
                labels=self.labels,
            )
        ]


def update_external_table(ds: DataStore, table: DataTable, run_config: Optional[RunConfig] = None) -> None:
    now = time.time()

    for ps_df in tqdm.tqdm(table.table_store.read_rows_meta_pseudo_df(run_config=run_config)):
        (
            new_df,
            changed_df,
            new_meta_df,
            changed_meta_df,
        ) = table.meta_table.get_changes_for_store_chunk(ps_df, now=now)

        ds.event_logger.log_state(
            table.name,
            added_count=len(new_df),
            updated_count=len(changed_df),
            deleted_count=0,
            processed_count=len(ps_df),
            run_config=run_config,
        )

        # TODO switch to iterative store_chunk and table.sync_meta_by_process_ts

        table.meta_table.insert_meta_for_store_chunk(new_meta_df)
        table.meta_table.update_meta_for_store_chunk(changed_meta_df)

    for stale_idx in table.meta_table.get_stale_idx(now, run_config=run_config):
        logger.debug(f"Deleting {len(stale_idx.index)} rows from {table.name} data")
        table.event_logger.log_state(
            table.name,
            added_count=0,
            updated_count=0,
            deleted_count=len(stale_idx),
            processed_count=len(stale_idx),
            run_config=run_config,
        )

        table.meta_table.mark_rows_deleted(stale_idx, now=now)


class UpdateExternalTable(PipelineStep):
    def __init__(
        self,
        output: str,
        labels: Optional[Labels] = None,
    ) -> None:
        self.output_table_name = output
        self.labels = labels

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        def transform_func(
            ds: DataStore,
            input_dts: List[DataTable],
            output_dts: List[DataTable],
            run_config: Optional[RunConfig],
            **kwargs,
        ):
            return update_external_table(ds, output_dts[0], run_config)

        return [
            DatatableTransformStep(
                name=f"update_{self.output_table_name}",
                func=cast(DatatableTransformFunc, transform_func),
                input_dts=[],
                output_dts=[catalog.get_datatable(ds, self.output_table_name)],
                labels=self.labels,
            )
        ]

from typing import List, Dict, Callable, Optional, Any
from dataclasses import dataclass
from abc import ABC, abstractmethod

import logging
from opentelemetry import trace

from datapipe.types import ChangeList
from datapipe.datatable import DataTable, DataStore
from datapipe.run_config import RunConfig
from datapipe.store.table_store import TableStore

logger = logging.getLogger("datapipe.compute")
tracer = trace.get_tracer("datapipe.compute")


@dataclass
class Table:
    store: TableStore


class Catalog:
    def __init__(self, catalog: Dict[str, Table]):
        self.catalog = catalog
        self.data_tables: Dict[str, DataTable] = {}

    def add_datatable(self, name: str, dt: Table):
        self.catalog[name] = dt

    def remove_datatable(self, name: str):
        del self.catalog[name]

    def get_datatable(self, ds: DataStore, name: str) -> DataTable:
        return ds.get_or_create_table(
            name=name,
            table_store=self.catalog[name].store
        )


@dataclass
class Pipeline:
    steps: List['PipelineStep']


class PipelineStep(ABC):
    """
    Пайплайн описывается через PipelineStep.

    Для выполнения у каждого pipeline_step выполняется build_compute на
    основании которых создается граф вычислений.
    """

    @abstractmethod
    def build_compute(self, ds: DataStore, catalog: Catalog) -> List['ComputeStep']:
        raise NotImplementedError


DatatableTransformFunc = Callable[[DataStore, List[DataTable], List[DataTable], Optional[RunConfig]], None]


class DatatableTransform(PipelineStep):
    def __init__(
        self,
        func: DatatableTransformFunc,
        inputs: List[str],
        outputs: List[str],
        check_for_changes: bool = True
    ) -> None:
        self.func = func
        self.inputs = inputs
        self.outputs = outputs
        self.check_for_changes = check_for_changes

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List['ComputeStep']:
        return [
            DatatableTransformStep(
                name=self.func.__name__,
                input_dts=[catalog.get_datatable(ds, i) for i in self.inputs],
                output_dts=[catalog.get_datatable(ds, i) for i in self.outputs],
                func=self.func,
                check_for_changes=self.check_for_changes
            )
        ]


class ComputeStep(ABC):
    '''
    Шаг вычислений в графе вычислений.

    Каждый шаг должен уметь отвечать на вопросы:
    - какие таблицы приходят на вход
    - какие таблицы изменяются в результате трансформации

    Шаг может запускаться в режиме полной обработки, то есть без указания какие
    объекты изменились. Или в changelist-режиме, когда на вход поступают
    измененные индексы для каждой из входных таблиц.

    В changelist-режиме шаг обрабатывает только минимально необходимое
    количество батчей, которые покрывают все измененные индексы.
    '''

    @abstractmethod
    def get_name(self) -> str:
        pass

    @abstractmethod
    def get_input_dts(self) -> List[DataTable]:
        pass

    @abstractmethod
    def get_output_dts(self) -> List[DataTable]:
        pass

    def validate(self):
        inp_p_keys_arr = [set(inp.primary_keys) for inp in self.get_input_dts() if inp]
        out_p_keys_arr = [set(out.primary_keys) for out in self.get_output_dts() if out]

        inp_p_keys = set.intersection(*inp_p_keys_arr) if len(inp_p_keys_arr) else set()
        out_p_keys = set.intersection(*out_p_keys_arr) if len(out_p_keys_arr) else set()
        join_keys = set.intersection(inp_p_keys, out_p_keys)

        key_to_column_type_inp = {
            column.name: type(column.type)
            for inp in self.get_input_dts()
            for column in inp.primary_schema if column.name in join_keys
        }
        key_to_column_type_out = {
            column.name: type(column.type)
            for inp in self.get_output_dts()
            for column in inp.primary_schema if column.name in join_keys
        }

        for key in join_keys:
            if key_to_column_type_inp[key] != key_to_column_type_out[key]:
                raise ValueError(
                    f'Primary key "{key}" in inputs and outputs must have same column\'s type: '
                    f'{key_to_column_type_inp[key]} != {key_to_column_type_out[key]}'
                )

    @abstractmethod
    def run_full(self, ds: DataStore, run_config: RunConfig = None) -> None:
        pass

    @abstractmethod
    def run_changelist(self, ds: DataStore, changelist: ChangeList, run_config: RunConfig = None) -> ChangeList:
        pass


class DatatableTransformStep(ComputeStep):
    def __init__(
        self,
        name: str,
        input_dts: List[DataTable],
        output_dts: List[DataTable],

        func: DatatableTransformFunc,
        kwargs: Dict[str, Any] = None,
        check_for_changes: bool = True,
    ) -> None:
        self.name = name
        self.input_dts = input_dts
        self.output_dts = output_dts

        self.func = func
        self.kwargs: Dict[str, Any] = kwargs or {}
        self.check_for_changes = check_for_changes

    def get_name(self) -> str:
        return self.name

    def get_input_dts(self) -> List[DataTable]:
        return self.input_dts

    def get_output_dts(self) -> List[DataTable]:
        return self.output_dts

    def run_full(self, ds: DataStore, run_config: RunConfig = None) -> None:
        if len(self.input_dts) > 0 and self.check_for_changes:
            with tracer.start_as_current_span("check for changes"):
                changed_idx_count = ds.get_changed_idx_count(
                    inputs=self.input_dts,
                    outputs=self.output_dts,
                    run_config=run_config
                )

                if changed_idx_count == 0:
                    logger.debug(f'Skipping {self.name} execution - nothing to compute')

                    return

        run_config = RunConfig.add_labels(run_config, {'step_name': self.name})

        self.func(ds, self.input_dts, self.output_dts, run_config)

    def run_changelist(self, ds: DataStore, changelist: ChangeList, run_config: RunConfig = None) -> ChangeList:
        raise NotImplementedError


def build_compute(ds: DataStore, catalog: Catalog, pipeline: Pipeline) -> List[ComputeStep]:
    with tracer.start_as_current_span("build_compute"):
        compute_pipeline: List[ComputeStep] = []

        for step in pipeline.steps:
            compute_pipeline.extend(step.build_compute(ds, catalog))

        for compute_step in compute_pipeline:
            compute_step.validate()

        return compute_pipeline


def print_compute(steps: List[ComputeStep]) -> None:
    import pprint
    pprint.pp(
        steps
    )


def run_steps(ds: DataStore, steps: List[ComputeStep], run_config: RunConfig = None) -> None:
    with tracer.start_as_current_span("run_steps"):
        for step in steps:
            with tracer.start_as_current_span(
                f'{step.get_name()} {[i.name for i in step.get_input_dts()]} -> {[i.name for i in step.get_output_dts()]}'
            ):
                logger.info(
                    f'Running {step.get_name()} '
                    f'{[i.name for i in step.get_input_dts()]} -> {[i.name for i in step.get_output_dts()]}'
                )

                step.run_full(ds, run_config)


def run_pipeline(
    ds: DataStore,
    catalog: Catalog,
    pipeline: Pipeline,
    run_config: RunConfig = None,
) -> None:
    steps = build_compute(ds, catalog, pipeline)
    run_steps(ds, steps, run_config)


def run_changelist(
    ds: DataStore,
    catalog: Catalog,
    pipeline: Pipeline,
    changelist: ChangeList,
    run_config: RunConfig = None,
) -> None:
    steps = build_compute(ds, catalog, pipeline)

    return run_steps_changelist(ds, steps, changelist, run_config)


def run_steps_changelist(
    ds: DataStore,
    steps: List[ComputeStep],
    changelist: ChangeList,
    run_config: RunConfig = None,
) -> None:
    current_changes = changelist
    next_changes = ChangeList()
    iteration = 0

    with tracer.start_as_current_span("Start pipeline for changelist"):
        while not current_changes.empty() and iteration < 100:
            with tracer.start_as_current_span("run_steps"):
                for step in steps:
                    with tracer.start_as_current_span(
                        f'{step.get_name()} '
                        f'{[i.name for i in step.get_input_dts()]} -> {[i.name for i in step.get_output_dts()]}'
                    ):
                        logger.info(
                            f'Running {step.get_name()} '
                            f'{[i.name for i in step.get_input_dts()]} -> {[i.name for i in step.get_output_dts()]}'
                        )

                        step_changes = step.run_changelist(ds, current_changes, run_config)
                        next_changes.extend(step_changes)

            current_changes = next_changes
            next_changes = ChangeList()
            iteration += 1

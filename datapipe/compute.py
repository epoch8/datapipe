import hashlib
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

import tqdm
from opentelemetry import trace

from datapipe.datatable import DataStore, DataTable
from datapipe.run_config import RunConfig
from datapipe.store.table_store import TableStore
from datapipe.types import ChangeList, DataDF, IndexDF, Labels, TransformResult

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

    def init_all_tables(self, ds: DataStore):
        for name in self.catalog.keys():
            self.get_datatable(ds, name)

    def get_datatable(self, ds: DataStore, name: str) -> DataTable:
        return ds.get_or_create_table(name=name, table_store=self.catalog[name].store)


class ComputeStep:
    """
    Шаг вычислений в графе вычислений.

    Каждый шаг должен уметь отвечать на вопросы:
    - какие таблицы приходят на вход
    - какие таблицы изменяются в результате трансформации

    Шаг может запускаться в режиме полной обработки, то есть без указания какие
    объекты изменились. Или в changelist-режиме, когда на вход поступают
    измененные индексы для каждой из входных таблиц.

    В changelist-режиме шаг обрабатывает только минимально необходимое
    количество батчей, которые покрывают все измененные индексы.
    """

    def __init__(
        self,
        name: str,
        input_dts: List[DataTable],
        output_dts: List[DataTable],
        labels: Optional[Labels] = None,
    ) -> None:
        self._name = name
        self.input_dts = input_dts
        self.output_dts = output_dts
        self._labels = labels

    def get_name(self) -> str:
        ss = [
            self.__class__.__name__,
            self._name,
            *[i.name for i in self.input_dts],
            *[o.name for o in self.output_dts],
        ]

        m = hashlib.shake_128()
        m.update("".join(ss).encode("utf-8"))

        return f"{self._name}_{m.hexdigest(5)}"

    @property
    def name(self) -> str:
        return self.get_name()

    @property
    def labels(self) -> Labels:
        return self._labels if self._labels else []

    # TODO: move to lints
    def validate(self) -> None:
        inp_p_keys_arr = [set(inp.primary_keys) for inp in self.input_dts if inp]
        out_p_keys_arr = [set(out.primary_keys) for out in self.output_dts if out]

        inp_p_keys = set.intersection(*inp_p_keys_arr) if len(inp_p_keys_arr) else set()
        out_p_keys = set.intersection(*out_p_keys_arr) if len(out_p_keys_arr) else set()
        join_keys = set.intersection(inp_p_keys, out_p_keys)

        key_to_column_type_inp = {
            column.name: type(column.type)
            for inp in self.input_dts
            for column in inp.primary_schema
            if column.name in join_keys
        }
        key_to_column_type_out = {
            column.name: type(column.type)
            for inp in self.output_dts
            for column in inp.primary_schema
            if column.name in join_keys
        }

        for key in join_keys:
            if key_to_column_type_inp[key] != key_to_column_type_out[key]:
                raise ValueError(
                    f'Primary key "{key}" in inputs and outputs must have same column\'s type: '
                    f"{key_to_column_type_inp[key]} != {key_to_column_type_out[key]}"
                )

    @abstractmethod
    def get_full_process_ids(
        self,
        ds: DataStore,
        run_config: Optional[RunConfig] = None,
    ) -> Tuple[int, Iterable[IndexDF]]:
        raise NotImplementedError()

    @abstractmethod
    def get_change_list_process_ids(
        self,
        ds: DataStore,
        change_list: ChangeList,
        run_config: Optional[RunConfig] = None,
    ) -> Tuple[int, Iterable[IndexDF]]:
        raise NotImplementedError()

    @abstractmethod
    def run_full(
        self,
        ds: DataStore,
        run_config: Optional[RunConfig] = None,
    ) -> None:
        raise NotImplementedError()

    @abstractmethod
    def run_changelist(
        self,
        ds: DataStore,
        change_list: ChangeList,
        run_config: Optional[RunConfig] = None,
    ) -> ChangeList:
        raise NotImplementedError()


class PipelineStep(ABC):
    """
    Пайплайн описывается через PipelineStep.

    Для выполнения у каждого pipeline_step выполняется build_compute на
    основании которых создается граф вычислений.
    """

    @abstractmethod
    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        raise NotImplementedError


@dataclass
class Pipeline:
    steps: List[PipelineStep]


class DatapipeApp:
    def __init__(self, ds: DataStore, catalog: Catalog, pipeline: Pipeline):
        self.ds = ds
        self.catalog = catalog
        self.pipeline = pipeline

        self.steps = build_compute(ds, catalog, pipeline)


def build_compute(
    ds: DataStore, catalog: Catalog, pipeline: Pipeline
) -> List[ComputeStep]:
    with tracer.start_as_current_span("build_compute"):
        catalog.init_all_tables(ds)

        compute_pipeline: List[ComputeStep] = []

        for step in pipeline.steps:
            compute_pipeline.extend(step.build_compute(ds, catalog))

        # TODO move to lints
        for compute_step in compute_pipeline:
            compute_step.validate()

        return compute_pipeline


def print_compute(steps: List[ComputeStep]) -> None:
    import pprint

    pprint.pp(steps)


def run_steps(
    ds: DataStore, steps: List[ComputeStep], run_config: Optional[RunConfig] = None
) -> None:
    for step in steps:
        with tracer.start_as_current_span(
            f"{step.get_name()} {[i.name for i in step.input_dts]} -> {[i.name for i in step.output_dts]}"
        ):
            logger.info(
                f"Running {step.get_name()} "
                f"{[i.name for i in step.input_dts]} -> {[i.name for i in step.output_dts]}"
            )

        step.run_full(ds, run_config)


def run_pipeline(
    ds: DataStore,
    catalog: Catalog,
    pipeline: Pipeline,
    run_config: Optional[RunConfig] = None,
) -> None:
    steps = build_compute(ds, catalog, pipeline)
    run_steps(ds, steps, run_config)


def run_changelist(
    ds: DataStore,
    catalog: Catalog,
    pipeline: Pipeline,
    changelist: ChangeList,
    run_config: Optional[RunConfig] = None,
) -> None:
    steps = build_compute(ds, catalog, pipeline)

    return run_steps_changelist(ds, steps, changelist, run_config)


def run_steps_changelist(
    ds: DataStore,
    steps: List[ComputeStep],
    changelist: ChangeList,
    run_config: Optional[RunConfig] = None,
) -> None:
    current_changes = changelist
    next_changes = ChangeList()
    iteration = 0

    with tracer.start_as_current_span("Start pipeline for changelist"):
        while not current_changes.empty() and iteration < 100:
            with tracer.start_as_current_span("run_steps"):
                for step in steps:
                    with tracer.start_as_current_span(
                        f"{step.get_name()} "
                        f"{[i.name for i in step.input_dts]} -> {[i.name for i in step.output_dts]}"
                    ):
                        logger.info(
                            f"Running {step.get_name()} "
                            f"{[i.name for i in step.input_dts]} -> {[i.name for i in step.output_dts]}"
                        )

                        try:
                            step_changes = step.run_changelist(
                                ds, current_changes, run_config
                            )
                            next_changes.extend(step_changes)
                        except NotImplementedError:
                            # Some steps do not implement `.run_changelist`, that's ok
                            pass

            current_changes = next_changes
            next_changes = ChangeList()
            iteration += 1

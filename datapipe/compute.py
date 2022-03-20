from typing import List, Dict, Callable, Optional, Any, cast
from dataclasses import dataclass, field
from abc import ABC

import logging
from opentelemetry import trace

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


# Пайплайн описывается через PipelineStep
# Для выполнения у каждого pipeline_step выполняется build_compute на основании которых создается граф вычислений
class PipelineStep(ABC):
    def build_compute(self, ds: DataStore, catalog: Catalog) -> List['DatatableTransformStep']:
        raise NotImplementedError


ComputeStepFunc = Callable[[DataStore, List[DataTable], List[DataTable], Optional[RunConfig]], None]


class DatatableTransform(PipelineStep):
    def __init__(
        self,
        func: ComputeStepFunc,
        inputs: List[str],
        outputs: List[str],
        check_for_changes: bool = True
    ) -> None:
        self.func = func
        self.inputs = inputs
        self.outputs = outputs
        self.check_for_changes = check_for_changes

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List['DatatableTransformStep']:
        return [
            DatatableTransformStep(
                name=self.func.__name__,
                input_dts=[catalog.get_datatable(ds, i) for i in self.inputs],
                output_dts=[catalog.get_datatable(ds, i) for i in self.outputs],
                func=self.func,
                check_for_changes=self.check_for_changes
            )
        ]


@dataclass
class DatatableTransformStep:
    name: str
    input_dts: List[DataTable]
    output_dts: List[DataTable]
    func: ComputeStepFunc
    kwargs: Dict[str, Any] = field(default_factory=lambda: cast(Dict[str, Any], {}))
    check_for_changes: bool = True

    def __post_init__(self):
        inp_p_keys = {key for inp in self.input_dts for key in inp.primary_keys}
        out_p_keys = {key for out in self.output_dts for key in out.primary_keys}
        join_keys = set.intersection(inp_p_keys, out_p_keys)

        key_to_column_type_inp = {
            column.name: type(column.type)
            for inp in self.input_dts
            for column in inp.primary_schema if column.name in join_keys
        }
        key_to_column_type_out = {
            column.name: type(column.type)
            for inp in self.output_dts
            for column in inp.primary_schema if column.name in join_keys
        }
        for key in join_keys:
            if key_to_column_type_inp[key] != key_to_column_type_out[key]:
                raise ValueError(
                    f'Primary key "{key}" in inputs and outputs must have same column\'s type: '
                    f'{key_to_column_type_inp[key]} != {key_to_column_type_out[key]}'
                )

    def run(self, ds: DataStore, run_config: RunConfig = None) -> None:
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


def build_compute(ds: DataStore, catalog: Catalog, pipeline: Pipeline) -> List[DatatableTransformStep]:
    with tracer.start_as_current_span("build_compute"):
        res: List[DatatableTransformStep] = []

        for step in pipeline.steps:
            res.extend(step.build_compute(ds, catalog))

        return res


def print_compute(steps: List[DatatableTransformStep]) -> None:
    import pprint
    pprint.pp(
        steps
    )


def run_steps(ds: DataStore, steps: List[DatatableTransformStep], run_config: RunConfig = None) -> None:
    with tracer.start_as_current_span("run_steps"):
        for step in steps:
            with tracer.start_as_current_span(
                f'{step.name} {[i.name for i in step.input_dts]} -> {[i.name for i in step.output_dts]}'
            ):
                logger.info(f'Running {step.name} {[i.name for i in step.input_dts]} -> {[i.name for i in step.output_dts]}')

                step.run(ds, run_config)


def run_pipeline(
    ds: DataStore,
    catalog: Catalog,
    pipeline: Pipeline,
    run_config: RunConfig = None,
) -> None:
    steps = build_compute(ds, catalog, pipeline)
    run_steps(ds, steps, run_config)

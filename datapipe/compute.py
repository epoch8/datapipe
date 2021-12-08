from typing import List, Dict, Callable, Optional, Any, cast, Union
from dataclasses import dataclass, field
from abc import ABC

import logging

from datapipe.types import ChangeList
from datapipe.datatable import DataTable, DataStore
from datapipe.run_config import RunConfig
from datapipe.store.table_store import TableStore

logger = logging.getLogger('datapipe.compute')


@dataclass
class Table:
    store: TableStore


class Catalog:
    def __init__(self, catalog: Dict[str, Table]):
        self.catalog = catalog
        self.data_tables: Dict[str, DataTable] = {}

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
    ) -> None:
        self.func = func
        self.inputs = inputs
        self.outputs = outputs

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List['DatatableTransformStep']:
        return [
            DatatableTransformStep(
                name=self.func.__name__,
                input_dts=[catalog.get_datatable(ds, i) for i in self.inputs],
                output_dts=[catalog.get_datatable(ds, i) for i in self.outputs],
                func=self.func,
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

    def _run(self, ds: DataStore, func: ComputeStepFunc, run_config: RunConfig = None) -> Any:
        if len(self.input_dts) > 0 and self.check_for_changes:
            changed_idx_count = ds.get_changed_idx_count(
                inputs=self.input_dts,
                outputs=self.output_dts,
                run_config=run_config
            )

            if changed_idx_count == 0:
                logger.debug(f'Skipping {self.name} execution - nothing to compute')

                return

        run_config = RunConfig.add_labels(run_config, {'step_name': self.name})

        return func(ds, self.input_dts, self.output_dts, run_config)

    def run(self, ds: DataStore, run_config: RunConfig = None) -> None:
        self._run(ds, self.func, run_config=run_config)


@dataclass
class PartialTransformStep(DatatableTransformStep):
    def run_changelist(self, ds: DataStore, changelist: ChangeList, run_config: RunConfig = None) -> ChangeList:
        raise NotImplementedError("You must implement run_changelist method in your step class")


def build_compute(ds: DataStore, catalog: Catalog, pipeline: Pipeline) -> List[DatatableTransformStep]:
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
    for step in steps:
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

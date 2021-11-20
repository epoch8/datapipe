from typing import List, Dict
from dataclasses import dataclass
from abc import ABC

import logging

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
    def build_compute(self, ds: DataStore, catalog: Catalog) -> List['ComputeStep']:
        raise NotImplementedError


@dataclass
class ComputeStep:
    name: str
    input_dts: List['DataTable']
    output_dts: List['DataTable']

    def run(self, ds: 'DataStore', run_config: RunConfig = None) -> None:
        raise NotImplementedError


def build_compute(ds: DataStore, catalog: Catalog, pipeline: Pipeline) -> List[ComputeStep]:
    res: List[ComputeStep] = []

    for step in pipeline.steps:
        res.extend(step.build_compute(ds, catalog))

    return res


def print_compute(steps: List[ComputeStep]) -> None:
    import pprint
    pprint.pp(
        steps
    )


def run_steps(ds: DataStore, steps: List[ComputeStep], run_config: RunConfig = None) -> None:
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

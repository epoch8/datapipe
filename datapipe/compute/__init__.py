from c12n_pipe.datatable import DataTable
from dataclasses import dataclass
from typing import List, Any

import logging

from c12n_pipe.metastore import MetaStore, DBConn

from ..dsl import ExternalTable, Catalog, TableStoreFiledir, Pipeline, Transform
from .filedir import ExternalFiledirUpdater
from .steps import ComputeStep, IncStep


logger = logging.getLogger('datapipe.compute')


def build_external_table_updater(ms: MetaStore, catalog: Catalog, name: str) -> ComputeStep:
    table = catalog.get_datatable(ms, name)

    if isinstance(table.table_store, TableStoreFiledir):
        return ExternalFiledirUpdater(
            name=f'update_{name}',
            table=table
        )
    else:
        raise NotImplemented


def build_compute(ms: MetaStore, catalog: Catalog, pipeline: Pipeline):
    res: List[ComputeStep] = []

    for name, tbl in catalog.catalog.items():
        if isinstance(tbl, ExternalTable):
            res.append(build_external_table_updater(ms, catalog, name))
    
    for step in pipeline.steps:
        if isinstance(step, Transform):
            input_dts = [catalog.get_datatable(ms, name) for name in step.inputs]
            output_dts = [catalog.get_datatable(ms, name) for name in step.outputs]

            res.append(IncStep(
                f'{step.func.__name__}', 
                input_dts=input_dts, 
                output_dts=output_dts,
                func=step.func,
                chunk_size=step.chunk_size
            ))
    
    return res
    

def print_compute(steps: List[ComputeStep]) -> None:
    import pprint
    pprint.pp(
        steps
    )


def run_steps(ms: MetaStore, steps: List[ComputeStep]) -> None:
    for step in steps:
        logger.info(f'Running {step.name} {[i.name for i in step.input_dts]} -> {[i.name for i in step.output_dts]}')

        step.run(ms)


def run_pipeline(ms: MetaStore, catalog: Catalog, pipeline: Pipeline) -> None:
    steps = build_compute(ms, catalog, pipeline)
    run_steps(ms, steps)
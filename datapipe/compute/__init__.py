from dataclasses import dataclass
from datapipe.compute.metastore import MetaStore
from typing import List, Any

import logging

from ..dsl import ExternalTable, Catalog, Filedir, Pipeline, Transform
from .filedir import FiledirUpdater
from .steps import ComputeStep, IncStep


logger = logging.getLogger('datapipe.compute')


def build_table_updater(name, tbl: ExternalTable) -> ComputeStep:
    if isinstance(tbl.store, Filedir):
        return FiledirUpdater(
            name=f'update_{name}',
            inputs=[],
            outputs=[name,],
            table_name=name,
            filedir=tbl.store
        )
    else:
        raise NotImplemented


def validate_tables(catalog: Catalog, tables: List[str]):
    for tbl in tables:
        assert(tbl in catalog.catalog)


def build_compute(catalog: Catalog, pipeline: Pipeline):
    res: List[ComputeStep] = []

    for name, tbl in catalog.catalog.items():
        if isinstance(tbl, ExternalTable):
            res.append(build_table_updater(name, tbl))
    
    for step in pipeline.steps:
        if isinstance(step, Transform):
            validate_tables(catalog, step.inputs)
            validate_tables(catalog, step.outputs)

            res.append(IncStep(
                f'{step.func.__name__}', 
                step.inputs, 
                step.outputs,
                step.func
            ))
    
    return res
    

def print_compute(steps: List[ComputeStep]) -> None:
    import pprint
    pprint.pp(
        steps
    )


def run_steps(ms: MetaStore, steps: List[ComputeStep]) -> None:
    for step in steps:
        logger.info(f'Running {step.name} {step.inputs} -> {step.outputs}')

        step.run(ms)


def run_pipeline(ms: MetaStore, catalog: Catalog, pipeline: Pipeline) -> None:
    steps = build_compute(catalog, pipeline)
    run_steps(ms, steps)
from dataclasses import dataclass
from typing import List, Any

import logging

from c12n_pipe.metastore import MetaStore, DBConn

from ..dsl import ExternalTable, Catalog, TableStoreFiledir, Pipeline, Transform
from .filedir import ExternalFiledirUpdater
from .steps import ComputeStep, IncStep


logger = logging.getLogger('datapipe.compute')


def build_external_table_updater(name, tbl: ExternalTable) -> ComputeStep:
    if isinstance(tbl.store, TableStoreFiledir):
        return ExternalFiledirUpdater(
            name=f'update_{name}',
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
            res.append(build_external_table_updater(name, tbl))
    
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
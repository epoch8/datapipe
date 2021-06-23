from typing import List, Callable, Union
from dataclasses import dataclass

import logging

from datapipe.metastore import MetaStore
from datapipe.datatable import gen_process_many, inc_process_many, ExternalTableUpdater

from .dsl import BatchGenerate, ExternalTable, Catalog, Pipeline, BatchTransform
from .step import ComputeStep

logger = logging.getLogger('datapipe.compute')


@dataclass
class BatchGenerateStep(ComputeStep):
    func: Callable

    def run(self, ms: MetaStore):
        return gen_process_many(
            self.output_dts,
            self.func
        )


@dataclass
class BatchTransformIncStep(ComputeStep):
    func: Callable
    chunk_size: int
    diff_chunk_size: Union[int, None] = None

    def run(self, ms: MetaStore):
        return inc_process_many(
            ms,
            self.input_dts,
            self.output_dts,
            self.func,
            self.chunk_size,
            self.diff_chunk_size
        )


def build_compute(ms: MetaStore, catalog: Catalog, pipeline: Pipeline) -> List[ComputeStep]:
    res: List[ComputeStep] = []

    for name, tbl in catalog.catalog.items():
        if isinstance(tbl, ExternalTable):
            res.append(ExternalTableUpdater(
                name=f'update_{name}',
                table=catalog.get_datatable(ms, name)
            ))

    for step in pipeline.steps:
        if isinstance(step, BatchGenerate):
            output_dts = [catalog.get_datatable(ms, name) for name in step.outputs]

            res.append(BatchGenerateStep(
                f'{step.func.__name__}',
                input_dts=[],
                output_dts=output_dts,
                func=step.func,
            ))

        if isinstance(step, BatchTransform):
            input_dts = [catalog.get_datatable(ms, name) for name in step.inputs]
            output_dts = [catalog.get_datatable(ms, name) for name in step.outputs]

            res.append(BatchTransformIncStep(
                f'{step.func.__name__}',
                input_dts=input_dts,
                output_dts=output_dts,
                func=step.func,
                chunk_size=step.chunk_size,
                diff_chunk_size=step.diff_chunk_size
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

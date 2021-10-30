from typing import List, Callable
from dataclasses import dataclass

import logging

from datapipe.datatable import DataStore, gen_process_many, inc_process_many, MetaTableUpdater

from datapipe.dsl import PipelineStep, ExternalTable, Catalog, Pipeline
from .step import ComputeStep, RunConfig


logger = logging.getLogger('datapipe.compute')


@dataclass
class BatchGenerate(PipelineStep):
    func: Callable
    outputs: List[str]

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        output_dts = [catalog.get_datatable(ds, name) for name in self.outputs]

        return [
            BatchGenerateStep(
                f'{self.func.__name__}',
                input_dts=[],
                output_dts=output_dts,
                func=self.func,
            )
        ]


@dataclass
class BatchGenerateStep(ComputeStep):
    func: Callable

    def run(self, ds: DataStore, run_config: RunConfig = None) -> None:
        gen_process_many(
            self.output_dts,
            self.func,
            run_config=RunConfig.add_labels(run_config, {'step_name': self.name}),
        )


@dataclass
class BatchTransform(PipelineStep):
    func: Callable
    inputs: List[str]
    outputs: List[str]
    chunk_size: int = 1000

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        input_dts = [catalog.get_datatable(ds, name) for name in self.inputs]
        output_dts = [catalog.get_datatable(ds, name) for name in self.outputs]

        return [
            BatchTransformStep(
                f'{self.func.__name__}',
                input_dts=input_dts,
                output_dts=output_dts,
                func=self.func,
                chunk_size=self.chunk_size
            )
        ]


@dataclass
class BatchTransformStep(ComputeStep):
    func: Callable
    chunk_size: int

    def run(self, ds: DataStore, run_config: RunConfig = None) -> None:
        inc_process_many(
            ds,
            self.input_dts,
            self.output_dts,
            self.func,
            self.chunk_size,
            run_config=RunConfig.add_labels(run_config, {'step_name': self.name})
        )


def build_compute(ds: DataStore, catalog: Catalog, pipeline: Pipeline) -> List[ComputeStep]:
    res: List[ComputeStep] = []

    for name, tbl in catalog.catalog.items():
        if isinstance(tbl, ExternalTable):
            res.append(MetaTableUpdater(
                name=f'update_{name}',
                table=catalog.get_datatable(ds, name)
            ))

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

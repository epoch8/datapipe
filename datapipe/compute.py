from typing import Any, List, Callable, Optional, Set, cast
from dataclasses import dataclass

import logging

from datapipe.datatable import DataStore, gen_process_many, inc_process_many, MetaTableUpdater

from .dsl import BatchGenerate, ExternalTable, Catalog, InternalTable, Pipeline, BatchTransform, UpdateMetaTable
from .step import ComputeStep, RunConfig


logger = logging.getLogger('datapipe.compute')


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
class BatchTransformIncStep(ComputeStep):
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

    internal_tables: Set[str] = set()
    for name, tbl in catalog.catalog.items():
        if isinstance(tbl, ExternalTable):
            res.append(MetaTableUpdater(
                name=f'update_{name}',
                table=catalog.get_datatable(ds, name)
            ))
        if isinstance(tbl, InternalTable):
            internal_tables.add(name)

    for step in pipeline.steps:
        if isinstance(step, BatchGenerate):
            output_dts = [catalog.get_datatable(ds, name) for name in step.outputs]

            res.append(BatchGenerateStep(
                f'{step.func.__name__}',
                input_dts=[],
                output_dts=output_dts,
                func=step.func,
            ))

        if isinstance(step, BatchTransform):
            input_dts = [catalog.get_datatable(ds, name) for name in step.inputs]
            output_dts = [catalog.get_datatable(ds, name) for name in step.outputs]

            res.append(BatchTransformIncStep(
                f'{step.func.__name__}',
                input_dts=input_dts,
                output_dts=output_dts,
                func=step.func,
                chunk_size=step.chunk_size
            ))

        outputs: Optional[str] = cast(Any, step).outputs if hasattr(step, 'outputs') else None
        if outputs is not None:
            res.extend([
                MetaTableUpdater(
                    name=f'update_{name}',
                    table=catalog.get_datatable(ds, name)
                )
                for name in outputs
                if name in internal_tables
            ])

        if isinstance(step, UpdateMetaTable):
            res.extend([
                MetaTableUpdater(
                    name=f'update_{name}',
                    table=catalog.get_datatable(ds, name)
                )
                for name in step.outputs
            ])

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

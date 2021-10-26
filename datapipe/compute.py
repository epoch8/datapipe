from typing import List, Callable
from dataclasses import dataclass

import logging
from datapipe.label_studio.session import LabelStudioModerationStep

from datapipe.datatable import DataStore, gen_process_many, inc_process_many, MetaTableUpdater

from .dsl import BatchGenerate, ExternalTable, Catalog, Pipeline, BatchTransform, LabelStudioModeration, UpdateMetaTable
from .step import ComputeStep, RunConfig

logger = logging.getLogger('datapipe.compute')


@dataclass
class BatchGenerateStep(ComputeStep):
    func: Callable

    def run(self, ds: DataStore, run_config: RunConfig = None) -> None:
        gen_process_many(
            self.output_dts,
            self.func,
            run_config=run_config,
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
            run_config=run_config
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

        if isinstance(step, LabelStudioModeration):
            input_dts = [catalog.get_datatable(ds, name) for name in step.inputs]
            output_dts = [catalog.get_datatable(ds, name) for name in step.outputs]

            res.append(LabelStudioModerationStep(
                name=f"LabelStudioModeration (Project {step.project_title})",
                input_dts=input_dts,
                output_dts=output_dts,
                ls_url=step.ls_url,
                auth=step.auth,
                project_title=step.project_title,
                project_description=step.project_description,
                project_label_config=step.project_label_config,
                data=step.data,
                annotations=step.annotations,
                predictions=step.predictions,
                chunk_size=step.chunk_size,
            ))

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


def run_pipeline(ds: DataStore, catalog: Catalog, pipeline: Pipeline, run_config: RunConfig = None) -> None:
    steps = build_compute(ds, catalog, pipeline)
    run_steps(ds, steps, run_config)

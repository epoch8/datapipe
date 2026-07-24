import logging
from dataclasses import dataclass
from collections.abc import Callable
from typing import Any, Protocol

from opentelemetry import trace

from datapipe.compute import (
    Catalog,
    ComputeInput,
    ComputeOutput,
    ComputeStep,
    PipelineStep,
    make_mungled_step_name,
    pipeline_input_to_compute_input,
    pipeline_output_to_compute_output,
)
from datapipe.datatable import DataStore, DataTable
from datapipe.executor import Executor
from datapipe.run_config import RunConfig
from datapipe.types import Labels, TableOrName

logger = logging.getLogger("datapipe.step.datatable_transform")
tracer = trace.get_tracer("datapipe.step.datatable_transform")


class DatatableTransformFunc(Protocol):
    __name__: str

    def __call__(
        self,
        ds: DataStore,
        input_dts: list[DataTable],
        output_dts: list[DataTable],
        run_config: RunConfig | None,
        kwargs: dict[str, Any] | None = None,
    ) -> None: ...


class DatatableTransformStep(ComputeStep):
    def __init__(
        self,
        name: str,
        input_dts: list[ComputeInput],
        output_dts: list[ComputeOutput],
        func: DatatableTransformFunc,
        kwargs: dict | None = None,
        check_for_changes: bool = True,
        labels: Labels | None = None,
    ) -> None:
        ComputeStep.__init__(self, name, input_dts, output_dts, labels)

        self.func = func
        self.kwargs = kwargs or {}
        self.check_for_changes = check_for_changes

    def run_full(
        self,
        ds: DataStore,
        run_config: RunConfig | None = None,
        executor: Executor | None = None,
        progress: Callable[[int, int | None], None] | None = None,
    ) -> None:
        logger.info(f"Running: {self.name} {self.format_io()}")

        # TODO implement "watermark" system for tracking computation status in DatatableTransform
        #
        # if len(self.input_dts) > 0 and self.check_for_changes:
        #     with tracer.start_as_current_span("check for changes"):
        #         changed_idx_count = ds.get_changed_idx_count(
        #             inputs=self.input_dts,
        #             outputs=self.output_dts,
        #             run_config=run_config,
        #         )

        #         if changed_idx_count == 0:
        #             logger.debug(
        #                 f"Skipping {self.get_name()} execution - nothing to compute"
        #             )

        #             return

        run_config = RunConfig.add_labels(run_config, {"step_name": self.get_name()})

        with tracer.start_as_current_span(f"Run {self.func}"):
            try:
                self.func(
                    ds=ds,
                    input_dts=[inp.dt for inp in self.input_dts],
                    output_dts=[out.dt for out in self.output_dts],
                    run_config=run_config,
                    kwargs=self.kwargs,
                )

                ds.event_logger.log_step_full_complete(self.name)
            except Exception as e:
                logger.error(f"Datatable transform ({self.func}) run failed: {str(e)}")
                ds.event_logger.log_exception(e, run_config=run_config)


@dataclass
class DatatableTransform(PipelineStep):
    func: DatatableTransformFunc
    inputs: list[TableOrName]
    outputs: list[TableOrName]
    check_for_changes: bool = True
    kwargs: dict[str, Any] | None = None
    labels: Labels | None = None

    def build_compute(self, ds: DataStore, catalog: Catalog) -> list["ComputeStep"]:
        input_dts = [pipeline_input_to_compute_input(ds, catalog, i) for i in self.inputs]
        output_dts = [pipeline_output_to_compute_output(ds, catalog, i) for i in self.outputs]

        step_name = make_mungled_step_name(DatatableTransformStep, self.func.__name__, input_dts, output_dts)

        return [
            DatatableTransformStep(
                name=step_name,
                input_dts=input_dts,
                output_dts=output_dts,
                func=self.func,
                kwargs=self.kwargs,
                check_for_changes=self.check_for_changes,
                labels=self.labels,
            )
        ]

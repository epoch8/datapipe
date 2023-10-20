import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Protocol

from opentelemetry import trace

from datapipe.compute import Catalog, ComputeStep, PipelineStep
from datapipe.datatable import DataStore, DataTable
from datapipe.executor import Executor
from datapipe.run_config import RunConfig
from datapipe.types import Labels

logger = logging.getLogger("datapipe.step.datatable_transform")
tracer = trace.get_tracer("datapipe.step.datatable_transform")


class DatatableTransformFunc(Protocol):
    __name__: str

    def __call__(
        self,
        ds: DataStore,
        input_dts: List[DataTable],
        output_dts: List[DataTable],
        run_config: Optional[RunConfig],
        # Возможно, лучше передавать как переменную, а не  **
        **kwargs,
    ) -> None:
        ...


class DatatableTransformStep(ComputeStep):
    def __init__(
        self,
        name: str,
        input_dts: List[DataTable],
        output_dts: List[DataTable],
        func: DatatableTransformFunc,
        kwargs: Optional[Dict] = None,
        check_for_changes: bool = True,
        labels: Optional[Labels] = None,
    ) -> None:
        ComputeStep.__init__(self, name, input_dts, output_dts, labels)

        self.func = func
        self.kwargs = kwargs or {}
        self.check_for_changes = check_for_changes

    def run_full(
        self,
        ds: DataStore,
        run_config: Optional[RunConfig] = None,
        executor: Optional[Executor] = None,
    ) -> None:
        logger.info(f"Running: {self.name}")

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
                    input_dts=self.input_dts,
                    output_dts=self.output_dts,
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
    inputs: List[str]
    outputs: List[str]
    check_for_changes: bool = True
    kwargs: Optional[Dict[str, Any]] = None
    labels: Optional[Labels] = None

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List["ComputeStep"]:
        return [
            DatatableTransformStep(
                name=self.func.__name__,
                input_dts=[catalog.get_datatable(ds, i) for i in self.inputs],
                output_dts=[catalog.get_datatable(ds, i) for i in self.outputs],
                func=self.func,
                kwargs=self.kwargs,
                check_for_changes=self.check_for_changes,
                labels=self.labels,
            )
        ]

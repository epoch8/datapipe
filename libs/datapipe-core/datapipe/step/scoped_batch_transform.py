from dataclasses import dataclass
from typing import Sequence

import pandas as pd
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
from datapipe.datatable import DataStore
from datapipe.run_config import RunConfig
from datapipe.step.batch_transform import (
    BaseBatchTransformStep,
    DatatableBatchTransformFunc,
)
from datapipe.step.scoped_batch_generate import ScopedOutputPolicy, apply_scoped_output
from datapipe.types import (
    ChangeList,
    IndexDF,
    Labels,
    PipelineInput,
    PipelineOutput,
    TransformResult,
    data_to_index,
)

tracer = trace.get_tracer("datapipe.step.scoped_batch_transform")


@dataclass
class ScopedDatatableBatchTransform(PipelineStep):
    func: DatatableBatchTransformFunc
    inputs: list[PipelineInput]
    outputs: list[PipelineOutput]
    policy: ScopedOutputPolicy
    name: str | None = None
    chunk_size: int = 1000
    transform_keys: list[str] | None = None
    kwargs: dict | None = None
    labels: Labels | None = None

    def build_compute(self, ds: DataStore, catalog: Catalog) -> list[ComputeStep]:
        if len(self.outputs) != 1:
            raise NotImplementedError(
                "ScopedDatatableBatchTransform supports a single DB output"
            )

        input_dts = [pipeline_input_to_compute_input(ds, catalog, input) for input in self.inputs]
        output_dts = [pipeline_output_to_compute_output(ds, catalog, output) for output in self.outputs]

        step_name = self.name or make_mungled_step_name(
            ScopedDatatableBatchTransformStep, self.func.__name__, input_dts, output_dts
        )

        return [
            ScopedDatatableBatchTransformStep(
                ds=ds,
                name=step_name,
                func=self.func,
                input_dts=input_dts,
                output_dts=output_dts,
                policy=self.policy,
                kwargs=self.kwargs,
                transform_keys=self.transform_keys,
                chunk_size=self.chunk_size,
                labels=self.labels,
            )
        ]


class ScopedDatatableBatchTransformStep(BaseBatchTransformStep):
    def __init__(
        self,
        ds: DataStore,
        name: str,
        func: DatatableBatchTransformFunc,
        input_dts: Sequence[ComputeInput],
        output_dts: Sequence[ComputeOutput],
        policy: ScopedOutputPolicy,
        kwargs: dict | None = None,
        transform_keys: list[str] | None = None,
        chunk_size: int = 1000,
        labels: Labels | None = None,
    ) -> None:
        if len(output_dts) != 1:
            raise NotImplementedError(
                "ScopedDatatableBatchTransform supports a single DB output"
            )
        if policy.missing_row_policy == "deactivate" and not policy.active_column:
            raise ValueError(
                'missing_row_policy="deactivate" requires active_column to be set'
            )

        super().__init__(
            ds=ds,
            name=name,
            input_dts=input_dts,
            output_dts=output_dts,
            transform_keys=transform_keys,
            chunk_size=chunk_size,
            labels=labels,
        )

        self.func = func
        self.kwargs = kwargs
        self.policy = policy

    def process_batch_dts(
        self,
        ds: DataStore,
        idx: IndexDF,
        run_config: RunConfig | None = None,
    ) -> TransformResult | None:
        return self.func(
            ds=ds,
            idx=idx,
            input_dts=[inp.dt for inp in self.input_dts],
            run_config=run_config,
            kwargs=self.kwargs,
        )

    def store_batch_result(
        self,
        ds: DataStore,
        idx: IndexDF,
        output_dfs: TransformResult | None,
        process_ts: float,
        run_config: RunConfig | None = None,
    ) -> ChangeList:
        run_config = self._apply_filters_to_run_config(run_config)
        changes = ChangeList()
        output_spec = self.output_specs[0]
        res_dt = output_spec.dt

        with tracer.start_as_current_span("scoped store output batch"):
            if output_dfs is None:
                generated_df = pd.DataFrame()
            elif isinstance(output_dfs, (list, tuple)):
                if len(output_dfs) != 1:
                    raise NotImplementedError(
                        "ScopedDatatableBatchTransform supports a single DB output"
                    )
                generated_df = output_dfs[0]
            else:
                generated_df = output_dfs

            if generated_df is None:
                generated_df = pd.DataFrame()

            output_idx = self._transform_idx_to_output_idx(idx, output_spec)

            apply_scoped_output(
                res_dt,
                generated_df,
                self.policy,
                run_config=run_config,
                consider_idx=idx,
            )

            if not generated_df.empty:
                changes.append(res_dt.name, data_to_index(generated_df, res_dt.primary_keys))
            elif output_idx is not None:
                # Report keys touched by missing-row reconciliation within scope.
                existing = res_dt.get_data(output_idx)
                if not existing.empty and self.policy.scope_column in existing.columns:
                    scoped = existing[
                        existing[self.policy.scope_column] == self.policy.scope_value
                    ]
                    if not scoped.empty:
                        changes.append(
                            res_dt.name, data_to_index(scoped, res_dt.primary_keys)
                        )

        self.meta.mark_rows_processed_success(idx, process_ts=process_ts, run_config=run_config)
        return changes

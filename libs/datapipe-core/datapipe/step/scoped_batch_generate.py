import inspect
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal

import pandas as pd
from opentelemetry import trace

from datapipe.compute import (
    Catalog,
    ComputeInput,
    ComputeStep,
    PipelineStep,
    make_mungled_step_name,
    pipeline_output_to_compute_output,
)
from datapipe.datatable import DataStore, DataTable
from datapipe.run_config import RunConfig
from datapipe.step.batch_generate import BatchGenerateFunc
from datapipe.step.datatable_transform import (
    DatatableTransformFunc,
    DatatableTransformStep,
)
from datapipe.types import (
    IndexDF,
    Labels,
    TableOrName,
    cast,
    data_to_index,
    index_difference,
)

logger = logging.getLogger("datapipe.step.scoped_batch_generate")
tracer = trace.get_tracer("datapipe.step.scoped_batch_generate")


@dataclass(frozen=True)
class ScopedOutputPolicy:
    scope_column: str
    scope_value: Any

    missing_row_policy: Literal[
        "keep",
        "delete",
        "deactivate",
    ] = "keep"

    active_column: str | None = None


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _schema_column_names(dt: DataTable) -> list[str]:
    return [col.name for col in dt.table_store.get_schema()]


def _find_timestamp_columns(dt: DataTable) -> tuple[str | None, str | None]:
    names = _schema_column_names(dt)
    created_at = next((n for n in names if n.endswith("__created_at") or n == "created_at"), None)
    updated_at = next((n for n in names if n.endswith("__updated_at") or n == "updated_at"), None)
    return created_at, updated_at


def _validate_scope(df: pd.DataFrame, policy: ScopedOutputPolicy) -> None:
    if df.empty:
        return
    if policy.scope_column not in df.columns:
        raise ValueError(
            f"Generated dataframe is missing scope column {policy.scope_column!r}"
        )
    mismatched = df[df[policy.scope_column] != policy.scope_value]
    if not mismatched.empty:
        raise ValueError(
            f"Generated rows must have {policy.scope_column}={policy.scope_value!r}; "
            f"found mismatched values: {mismatched[policy.scope_column].unique().tolist()}"
        )


def _apply_timestamps(
    generated_df: pd.DataFrame,
    existing_scoped: pd.DataFrame,
    dt: DataTable,
) -> pd.DataFrame:
    if generated_df.empty:
        return generated_df

    created_at_col, updated_at_col = _find_timestamp_columns(dt)
    if created_at_col is None and updated_at_col is None:
        return generated_df

    df = generated_df.copy()
    now = _utcnow()
    pk = dt.primary_keys

    existing_by_pk = (
        existing_scoped.set_index(pk)
        if not existing_scoped.empty
        else pd.DataFrame(columns=existing_scoped.columns).set_index(pk)
    )

    if created_at_col is not None and created_at_col not in df.columns:
        df[created_at_col] = None
    if updated_at_col is not None and updated_at_col not in df.columns:
        df[updated_at_col] = None

    compare_cols = [
        c
        for c in df.columns
        if c not in pk
        and c != created_at_col
        and c != updated_at_col
    ]

    created_values: list[Any] = []
    updated_values: list[Any] = []

    for _, row in df.iterrows():
        key = tuple(row[k] for k in pk) if len(pk) > 1 else row[pk[0]]
        if key in existing_by_pk.index:
            existing_row = existing_by_pk.loc[key]
            if isinstance(existing_row, pd.DataFrame):
                existing_row = existing_row.iloc[0]

            if created_at_col is not None:
                created_values.append(existing_row[created_at_col])
            else:
                created_values.append(None)

            changed = False
            for col in compare_cols:
                if col not in existing_by_pk.columns:
                    changed = True
                    break
                left = row[col]
                right = existing_row[col]
                if pd.isna(left) and pd.isna(right):
                    continue
                if left != right:
                    changed = True
                    break

            if updated_at_col is not None:
                updated_values.append(now if changed else existing_row[updated_at_col])
            else:
                updated_values.append(None)
        else:
            if created_at_col is not None:
                created_values.append(now)
            else:
                created_values.append(None)
            if updated_at_col is not None:
                updated_values.append(now)
            else:
                updated_values.append(None)

    if created_at_col is not None:
        df[created_at_col] = created_values
    if updated_at_col is not None:
        df[updated_at_col] = updated_values

    return df


def apply_scoped_output(
    dt: DataTable,
    generated_df: pd.DataFrame,
    policy: ScopedOutputPolicy,
    *,
    run_config: RunConfig | None = None,
    consider_idx: IndexDF | None = None,
) -> None:
    """Upsert generated rows and reconcile missing rows only within the policy scope."""
    if policy.missing_row_policy == "deactivate" and not policy.active_column:
        raise ValueError(
            'missing_row_policy="deactivate" requires active_column to be set'
        )

    _validate_scope(generated_df, policy)

    existing_all = dt.get_data()
    if existing_all.empty:
        existing_scoped = existing_all.copy()
    elif policy.scope_column not in existing_all.columns:
        existing_scoped = existing_all.iloc[0:0].copy()
    else:
        existing_scoped = existing_all[
            existing_all[policy.scope_column] == policy.scope_value
        ].copy()

    if consider_idx is not None and not consider_idx.empty and not existing_scoped.empty:
        # Restrict missing-row reconciliation to rows matching consider_idx columns
        # that exist on the output table (transform keys and/or primary keys).
        filter_cols = [c for c in consider_idx.columns if c in existing_scoped.columns]
        if filter_cols:
            existing_for_missing = existing_scoped.merge(
                consider_idx[filter_cols].drop_duplicates(),
                on=filter_cols,
                how="inner",
            )
        else:
            existing_for_missing = existing_scoped.iloc[0:0].copy()
    else:
        existing_for_missing = existing_scoped

    df = generated_df.copy()
    if policy.missing_row_policy == "deactivate" and policy.active_column and not df.empty:
        df[policy.active_column] = True

    df = _apply_timestamps(df, existing_scoped, dt)

    if not df.empty:
        dt.store_chunk(df, run_config=run_config)

    if policy.missing_row_policy == "keep" or existing_for_missing.empty:
        return

    generated_idx = (
        data_to_index(df, dt.primary_keys)
        if not df.empty
        else IndexDF(pd.DataFrame(columns=dt.primary_keys))
    )
    existing_idx = data_to_index(existing_for_missing, dt.primary_keys)
    missing_idx = index_difference(existing_idx, generated_idx)
    if missing_idx.empty:
        return

    if policy.missing_row_policy == "delete":
        dt.delete_by_idx(missing_idx, run_config=run_config)
        return

    # deactivate
    assert policy.active_column is not None
    to_deactivate = dt.get_data(missing_idx)
    if to_deactivate.empty:
        return
    to_deactivate = to_deactivate.copy()
    to_deactivate[policy.active_column] = False

    _, updated_at_col = _find_timestamp_columns(dt)
    if updated_at_col is not None:
        to_deactivate[updated_at_col] = _utcnow()

    dt.store_chunk(to_deactivate, run_config=run_config)


def _collect_generator_output(
    func: BatchGenerateFunc,
    ds: DataStore,
    kwargs: dict | None,
) -> pd.DataFrame:
    parameters = inspect.signature(func).parameters
    call_kwargs = {
        **({"ds": ds} if "ds" in parameters else {}),
        **(kwargs or {}),
    }

    assert inspect.isgeneratorfunction(func), "Starting v0.8.0 proc_func should be a generator"

    chunks: list[pd.DataFrame] = []
    with tracer.start_as_current_span("init generator"):
        iterable = func(**call_kwargs)

    while True:
        with tracer.start_as_current_span("get next batch"):
            try:
                chunk_dfs = next(iterable)
                if isinstance(chunk_dfs, pd.DataFrame):
                    chunk_dfs = (chunk_dfs,)
            except StopIteration:
                break

        if len(chunk_dfs) != 1:
            raise NotImplementedError(
                "ScopedBatchGenerate supports a single DB output"
            )
        chunks.append(chunk_dfs[0])

    if not chunks:
        return pd.DataFrame()
    return pd.concat(chunks, ignore_index=True)


def do_scoped_batch_generate(
    func: BatchGenerateFunc,
    ds: DataStore,
    output_dts: list[DataTable],
    policy: ScopedOutputPolicy,
    run_config: RunConfig | None = None,
    kwargs: dict | None = None,
) -> None:
    if len(output_dts) != 1:
        raise NotImplementedError("ScopedBatchGenerate supports a single DB output")

    if policy.missing_row_policy == "deactivate" and not policy.active_column:
        raise ValueError(
            'missing_row_policy="deactivate" requires active_column to be set'
        )

    try:
        generated_df = _collect_generator_output(func, ds, kwargs)
    except Exception as e:
        logger.exception(f"Generating failed ({func.__name__}): {str(e)}")
        ds.event_logger.log_exception(e, run_config=run_config)
        raise

    with tracer.start_as_current_span("scoped store results"):
        apply_scoped_output(
            output_dts[0],
            generated_df,
            policy,
            run_config=run_config,
        )


@dataclass
class ScopedBatchGenerate(PipelineStep):
    func: BatchGenerateFunc
    outputs: list[TableOrName]
    policy: ScopedOutputPolicy
    name: str | None = None
    kwargs: dict | None = None
    labels: Labels | None = None

    def build_compute(self, ds: DataStore, catalog: Catalog) -> list[ComputeStep]:
        if len(self.outputs) != 1:
            raise NotImplementedError("ScopedBatchGenerate supports a single DB output")

        input_dts: list[ComputeInput] = []
        output_dts = [
            pipeline_output_to_compute_output(ds, catalog, output) for output in self.outputs
        ]

        step_name = self.name or make_mungled_step_name(
            DatatableTransformStep, self.func.__name__, input_dts, output_dts
        )

        return [
            DatatableTransformStep(
                name=step_name,
                func=cast(
                    DatatableTransformFunc,
                    lambda ds, input_dts, output_dts, run_config, kwargs: do_scoped_batch_generate(
                        func=self.func,
                        ds=ds,
                        output_dts=output_dts,
                        policy=self.policy,
                        run_config=run_config,
                        kwargs=kwargs,
                    ),
                ),
                input_dts=input_dts,
                output_dts=output_dts,
                check_for_changes=False,
                kwargs=self.kwargs,
                labels=self.labels,
            )
        ]

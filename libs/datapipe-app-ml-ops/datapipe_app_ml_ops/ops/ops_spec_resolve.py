from __future__ import annotations

from datapipe_app_ml_ops.ops.ops_specs import DatapipeOpsSpec, OpsImageRecordViewSpec, OpsTextRecordViewSpec


def resolve_frozen_scope_column(
    spec: DatapipeOpsSpec,
    view: OpsImageRecordViewSpec | OpsTextRecordViewSpec,
) -> str:
    if view.scope_column:
        return view.scope_column
    frozen_dataset = spec.frozen_dataset
    if frozen_dataset is not None:
        return frozen_dataset.id_column
    raise ValueError("frozen_dataset.record_view.scope_column is not configured and frozen_dataset.id_column is missing")

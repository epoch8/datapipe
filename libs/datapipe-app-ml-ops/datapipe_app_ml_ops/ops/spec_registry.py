"""Re-export OpsSpecRegistry from datapipe-app and register ML validators."""

from __future__ import annotations

from datapipe_app.ops.spec_registry import (
    OpsSpecRegistry,
    TableSchema,
    _flatten_columns,
    register_spec_validator,
    register_table_id_extension,
)
from datapipe_app_ml_ops.ops.ops_spec_validation import collect_ml_table_ids, validate_ml_spec_extensions
from datapipe_app_ml_ops.ops.ops_specs import DatapipeOpsSpec


def _ml_spec_validator(spec, catalog, db, registry: OpsSpecRegistry) -> None:
    if not isinstance(spec, DatapipeOpsSpec):
        return
    if (
        spec.frozen_dataset is not None
        or spec.model is not None
        or spec.training is not None
        or spec.class_metrics
    ):
        validate_ml_spec_extensions(spec, catalog, db, registry=registry)


def _ml_table_ids(spec) -> set[str]:
    if isinstance(spec, DatapipeOpsSpec):
        return collect_ml_table_ids(spec)
    return set()


register_spec_validator(_ml_spec_validator)
register_table_id_extension(_ml_table_ids)

__all__ = ["OpsSpecRegistry", "TableSchema", "_flatten_columns"]

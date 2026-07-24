"""Re-export OpsSpecRegistry from datapipe-app and register ML validators."""

from __future__ import annotations

from typing import Sequence, cast

from datapipe_app.ops.spec_registry import (
    OpsSpecRegistry as _OpsSpecRegistry,
    TableSchema,
    _flatten_columns,
    register_spec_validator,
    register_table_id_extension,
)
from datapipe_app_ml_ops.ops.ops_spec_validation import collect_ml_table_ids, validate_ml_spec_extensions
from datapipe_app_ml_ops.ops.ops_specs import DatapipeOpsSpec


def _ml_spec_validator(spec, catalog, db, registry: _OpsSpecRegistry) -> None:
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


class OpsSpecRegistry(_OpsSpecRegistry):
    """ML ops registry: stores ``DatapipeOpsSpec`` instances with typed accessors."""

    def add_many(self, specs: Sequence[DatapipeOpsSpec]) -> None:  # type: ignore[override]
        super().add_many(specs)

    def get(self, spec_id: str) -> DatapipeOpsSpec:
        return cast(DatapipeOpsSpec, super().get(spec_id))

    def list(self) -> list[DatapipeOpsSpec]:  # type: ignore[override]
        return cast(list[DatapipeOpsSpec], super().list())


__all__ = ["OpsSpecRegistry", "TableSchema", "_flatten_columns", "DatapipeOpsSpec"]

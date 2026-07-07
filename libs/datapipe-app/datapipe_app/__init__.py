from datapipe.compute import DatapipeApp

from datapipe_app.datapipe_api import DatapipeAPI, setup_logging
from datapipe_app.db_schema import register_observability_tables_in_metadata
from datapipe_app.observability.tables import ObservabilityTableConfig
from datapipe_app.spec_registry import OpsSpecRegistry
from datapipe_app.specs import (
    DatapipeOpsSpec,
    OpsClassMetricTableSpec,
    OpsColumn,
    OpsColumnGroup,
    OpsDataSpec,
    OpsFrozenDatasetSpec,
    OpsMetricTableSpec,
    OpsModelSpec,
    OpsRelationSpec,
    OpsTableRef,
    OpsTrainingSpec,
)


def _ensure_ops_registry(app: DatapipeApp) -> OpsSpecRegistry:
    registry = getattr(app, "ops_specs", None)
    if registry is None:
        registry = OpsSpecRegistry()
        setattr(app, "ops_specs", registry)
    return registry


def _add_specs(self: DatapipeApp, specs) -> None:
    registry = _ensure_ops_registry(self)
    previous = dict(registry._specs)
    try:
        registry.add_many(specs)
        registry.validate(getattr(self, "catalog", None), getattr(self, "ds", None), strict=True)
    except Exception:
        registry._specs = previous
        raise


def _get_specs(self: DatapipeApp):
    return _ensure_ops_registry(self).list()


if not hasattr(DatapipeApp, "add_specs"):
    DatapipeApp.add_specs = _add_specs  # type: ignore[attr-defined,method-assign]
    DatapipeApp.get_specs = _get_specs  # type: ignore[attr-defined,method-assign]


__all__ = [
    "DatapipeAPI",
    "DatapipeApp",
    "DatapipeOpsSpec",
    "ObservabilityTableConfig",
    "OpsClassMetricTableSpec",
    "OpsColumn",
    "OpsColumnGroup",
    "OpsDataSpec",
    "OpsFrozenDatasetSpec",
    "OpsMetricTableSpec",
    "OpsModelSpec",
    "OpsRelationSpec",
    "OpsSpecRegistry",
    "OpsTableRef",
    "OpsTrainingSpec",
    "register_observability_tables_in_metadata",
    "setup_logging",
]

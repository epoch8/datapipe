"""Datapipe app package."""

_LAZY_EXPORTS = {
    "DatapipeAPI": ("datapipe_app.datapipe_api", "DatapipeAPI"),
    "DatapipeApp": ("datapipe.compute", "DatapipeApp"),
    "setup_logging": ("datapipe_app.datapipe_api", "setup_logging"),
    "ObservabilityTableConfig": ("datapipe_app.observability.tables", "ObservabilityTableConfig"),
    "OpsSpecRegistry": ("datapipe_app_ml_ops.spec_registry", "OpsSpecRegistry"),
    "OpsColumn": ("datapipe_app.specs", "OpsColumn"),
    "OpsColumnGroup": ("datapipe_app.specs", "OpsColumnGroup"),
    "OpsFilterRule": ("datapipe_app.specs", "OpsFilterRule"),
    "OpsMetricTableSpec": ("datapipe_app.specs", "OpsMetricTableSpec"),
    "OpsRelationSpec": ("datapipe_app.specs", "OpsRelationSpec"),
    "OpsTableRef": ("datapipe_app.specs", "OpsTableRef"),
    "register_observability_tables_in_metadata": (
        "datapipe_app.db_schema",
        "register_observability_tables_in_metadata",
    ),
}


def __getattr__(name: str):
    if name not in _LAZY_EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr_name = _LAZY_EXPORTS[name]
    module = __import__(module_name, fromlist=[attr_name])
    value = getattr(module, attr_name)
    if name == "DatapipeApp":
        from datapipe_app_ml_ops.ops_registry_hooks import ensure_datapipe_app_add_specs

        ensure_datapipe_app_add_specs()
    return value


__all__ = [
    "DatapipeAPI",
    "DatapipeApp",
    "ObservabilityTableConfig",
    "OpsColumn",
    "OpsColumnGroup",
    "OpsFilterRule",
    "OpsMetricTableSpec",
    "OpsRelationSpec",
    "OpsSpecRegistry",
    "OpsTableRef",
    "register_observability_tables_in_metadata",
    "setup_logging",
]

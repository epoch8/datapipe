"""Datapipe app package."""

_LAZY_EXPORTS = {
    "DatapipeAPI": ("datapipe_app.app.datapipe_api", "DatapipeAPI"),
    "DatapipeApp": ("datapipe.compute", "DatapipeApp"),
    "setup_logging": ("datapipe_app.app.datapipe_api", "setup_logging"),
    "RunLogsBackend": ("datapipe_app.observability.run_logs", "RunLogsBackend"),
    "ObservabilityTableConfig": ("datapipe_app.observability.config.tables", "ObservabilityTableConfig"),
    "OpsSpecRegistry": ("datapipe_app_ml_ops.ops.spec_registry", "OpsSpecRegistry"),
    "OpsColumn": ("datapipe_app.ops.specs", "OpsColumn"),
    "OpsColumnGroup": ("datapipe_app.ops.specs", "OpsColumnGroup"),
    "OpsFilterRule": ("datapipe_app.ops.specs", "OpsFilterRule"),
    "OpsMetricTableSpec": ("datapipe_app.ops.specs", "OpsMetricTableSpec"),
    "OpsRelationSpec": ("datapipe_app.ops.specs", "OpsRelationSpec"),
    "OpsTableRef": ("datapipe_app.ops.specs", "OpsTableRef"),
    "register_observability_tables_in_metadata": (
        "datapipe_app.app.db_schema",
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
        from datapipe_app_ml_ops.ops.ops_registry_hooks import ensure_datapipe_app_add_specs

        ensure_datapipe_app_add_specs()
    return value


__all__ = [
    "DatapipeAPI",
    "DatapipeApp",
    "ObservabilityTableConfig",
    "RunLogsBackend",
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

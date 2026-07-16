from __future__ import annotations

from typing import Any

from fastapi import FastAPI

from datapipe_app_ml_ops.observability.routes.routes import register_ml_observability_routes


def register_v1alpha3_extension(
    *,
    app: FastAPI,
    store: Any,
    registry: Any,
    ds: Any,
    catalog: Any,
    ops_spec_registry: Any,
    **_: Any,
) -> None:
    register_ml_observability_routes(
        app,
        store=store,
        registry=registry,
        ds=ds,
        catalog=catalog,
        ops_spec_registry=ops_spec_registry,
    )

from __future__ import annotations

from typing import Any, Callable

from fastapi import BackgroundTasks, FastAPI

from datapipe_app.observability.plugins.schemas import StartRunRequest, StartRunResponse
from datapipe_app_ml_ops.observability.routes.routes import register_ml_observability_routes


def register_v1alpha3_extension(
    *,
    app: FastAPI,
    store: Any,
    registry: Any,
    ds: Any,
    catalog: Any,
    ops_spec_registry: Any,
    recorder: Any,
    steps: Any,
    pipeline: Any,
    start_run_handler: Callable[[StartRunRequest, BackgroundTasks], StartRunResponse],
    require_pipeline: Callable[[str], None],
    **_: Any,
) -> None:
    register_ml_observability_routes(
        app,
        store=store,
        registry=registry,
        ds=ds,
        catalog=catalog,
        ops_spec_registry=ops_spec_registry,
        start_run_handler=start_run_handler,
        require_pipeline=require_pipeline,
    )

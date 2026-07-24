from __future__ import annotations

from typing import Any, Callable, Optional, TypeVar

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from datapipe_app_ml_ops.ops.frozen_datasets_launch_service import (
    FreezeLaunchResponse,
    FrozenDatasetsLaunchService,
)
from datapipe_app_ml_ops.ops.spec_registry import OpsSpecRegistry
from datapipe_app_ml_ops.ops.training_experiments_models import TrainingExperimentError
from datapipe_app_ml_ops.ops.training_experiments_service import RunStepsCallable

T = TypeVar("T")

_BASE = "/pipelines/{pipeline_id}/ops-specs/{spec_id}/frozen-datasets"


def _error_response(exc: TrainingExperimentError) -> JSONResponse:
    return JSONResponse(status_code=exc.status_code, content=exc.to_payload())


def _guard(fn: Callable[[], T]) -> Any:
    try:
        return fn()
    except TrainingExperimentError as exc:
        return _error_response(exc)


def register_frozen_datasets_launch_routes(
    app: FastAPI,
    *,
    registry: OpsSpecRegistry,
    run_steps: Optional[RunStepsCallable] = None,
) -> None:
    """Register freeze-launch endpoint for ops frozen-dataset specs."""

    def _service() -> FrozenDatasetsLaunchService:
        return FrozenDatasetsLaunchService(registry, run_steps=run_steps)

    @app.post(_BASE + "/launch", response_model=FreezeLaunchResponse)
    def launch_freeze(pipeline_id: str, spec_id: str) -> Any:
        return _guard(lambda: _service().launch_freeze(spec_id))


__all__ = ["register_frozen_datasets_launch_routes"]

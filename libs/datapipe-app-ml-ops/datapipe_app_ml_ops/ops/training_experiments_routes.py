from __future__ import annotations

from typing import Any, Callable, Optional, TypeVar

from datapipe.compute import Catalog
from datapipe.datatable import DataStore
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from datapipe_app_ml_ops.ops.spec_registry import OpsSpecRegistry
from datapipe_app_ml_ops.ops.training_experiments_models import (
    ConfigSchemaResponse,
    CreateTrainingExperimentRequest,
    CreateTrainingRequestRequest,
    CreateTrainingRequestResponse,
    DuplicateTrainingExperimentRequest,
    LaunchResponse,
    TrainingExperimentDetailResponse,
    TrainingExperimentError,
    TrainingExperimentModelsResponse,
    TrainingExperimentsListResponse,
    TrainingRequestsListResponse,
    UpdateTrainingExperimentRequest,
)
from datapipe_app_ml_ops.ops.training_experiments_service import (
    RunStepsCallable,
    TrainingExperimentsService,
)

T = TypeVar("T")

_BASE = "/pipelines/{pipeline_id}/ops-specs/{spec_id}/training"


def _error_response(exc: TrainingExperimentError) -> JSONResponse:
    return JSONResponse(status_code=exc.status_code, content=exc.to_payload())


def _guard(fn: Callable[[], T]) -> Any:
    try:
        return fn()
    except TrainingExperimentError as exc:
        return _error_response(exc)


def register_training_experiments_routes(
    app: FastAPI,
    *,
    registry: OpsSpecRegistry,
    ds: DataStore,
    catalog: Catalog,
    run_steps: Optional[RunStepsCallable] = None,
) -> None:
    """Register the custom training experiments API (spec §19-20)."""

    def _service() -> TrainingExperimentsService:
        return TrainingExperimentsService(
            registry, ds=ds, catalog=catalog, run_steps=run_steps
        )

    @app.get(_BASE + "/experiments", response_model=TrainingExperimentsListResponse)
    def list_experiments(
        pipeline_id: str,
        spec_id: str,
        search: Optional[str] = None,
        source: Optional[str] = None,
        state: Optional[str] = None,
        include_archived: bool = False,
        order: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Any:
        return _guard(
            lambda: _service().list_experiments(
                spec_id,
                search=search,
                source=source,
                state=state,
                include_archived=include_archived,
                order=order,
                limit=min(limit, 200),
                offset=offset,
            )
        )

    @app.get(_BASE + "/config-schema", response_model=ConfigSchemaResponse)
    def get_config_schema(
        pipeline_id: str,
        spec_id: str,
        config_type: Optional[str] = None,
    ) -> Any:
        return _guard(lambda: _service().get_config_schema(spec_id, config_type=config_type))

    @app.get(
        _BASE + "/experiments/{config_id}",
        response_model=TrainingExperimentDetailResponse,
    )
    def get_experiment(pipeline_id: str, spec_id: str, config_id: str) -> Any:
        return _guard(lambda: _service().get_experiment(spec_id, config_id))

    @app.get(
        _BASE + "/experiments/{config_id}/models",
        response_model=TrainingExperimentModelsResponse,
    )
    def list_experiment_models(pipeline_id: str, spec_id: str, config_id: str) -> Any:
        return _guard(lambda: _service().list_experiment_models(spec_id, config_id))

    @app.post(
        _BASE + "/experiments",
        response_model=TrainingExperimentDetailResponse,
        status_code=201,
    )
    def create_experiment(
        pipeline_id: str, spec_id: str, req: CreateTrainingExperimentRequest
    ) -> Any:
        return _guard(lambda: _service().create_experiment(spec_id, req))

    @app.patch(
        _BASE + "/experiments/{config_id}",
        response_model=TrainingExperimentDetailResponse,
    )
    def update_experiment(
        pipeline_id: str, spec_id: str, config_id: str, req: UpdateTrainingExperimentRequest
    ) -> Any:
        return _guard(lambda: _service().update_experiment(spec_id, config_id, req))

    @app.delete(_BASE + "/experiments/{config_id}")
    def delete_experiment(pipeline_id: str, spec_id: str, config_id: str) -> Any:
        def _run() -> JSONResponse:
            _service().delete_experiment(spec_id, config_id)
            return JSONResponse(status_code=200, content={"deleted": config_id})

        return _guard(_run)

    @app.post(
        _BASE + "/experiments/{config_id}/duplicate",
        response_model=TrainingExperimentDetailResponse,
        status_code=201,
    )
    def duplicate_experiment(
        pipeline_id: str,
        spec_id: str,
        config_id: str,
        req: DuplicateTrainingExperimentRequest,
    ) -> Any:
        return _guard(lambda: _service().duplicate_experiment(spec_id, config_id, req))

    @app.post(
        _BASE + "/experiments/{config_id}/archive",
        response_model=TrainingExperimentDetailResponse,
    )
    def archive_experiment(pipeline_id: str, spec_id: str, config_id: str) -> Any:
        return _guard(lambda: _service().archive_experiment(spec_id, config_id))

    @app.post(
        _BASE + "/experiments/{config_id}/unarchive",
        response_model=TrainingExperimentDetailResponse,
    )
    def unarchive_experiment(pipeline_id: str, spec_id: str, config_id: str) -> Any:
        return _guard(lambda: _service().unarchive_experiment(spec_id, config_id))

    @app.get(_BASE + "/requests", response_model=TrainingRequestsListResponse)
    def list_training_requests(
        pipeline_id: str,
        spec_id: str,
        limit: int = 50,
        offset: int = 0,
    ) -> Any:
        return _guard(
            lambda: _service().list_training_requests(
                spec_id,
                limit=min(limit, 200),
                offset=offset,
            )
        )

    @app.post(
        _BASE + "/requests",
        response_model=CreateTrainingRequestResponse,
        status_code=201,
    )
    def create_training_request(
        pipeline_id: str,
        spec_id: str,
        req: CreateTrainingRequestRequest,
    ) -> Any:
        return _guard(lambda: _service().create_training_request(spec_id, req))

    @app.post(
        _BASE + "/requests/{request_id}/launch",
        response_model=LaunchResponse,
    )
    def launch_training_request(
        pipeline_id: str, spec_id: str, request_id: str
    ) -> Any:
        return _guard(lambda: _service().launch_training_request(spec_id, request_id))

    @app.delete(_BASE + "/requests/{request_id}")
    def delete_training_request(
        pipeline_id: str, spec_id: str, request_id: str
    ) -> Any:
        def _run() -> JSONResponse:
            _service().delete_training_request(spec_id, request_id)
            return JSONResponse(status_code=200, content={"deleted": request_id})

        return _guard(_run)


__all__ = ["register_training_experiments_routes"]

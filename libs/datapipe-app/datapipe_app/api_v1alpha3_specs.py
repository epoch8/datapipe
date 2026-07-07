from __future__ import annotations

from typing import Any, Literal, Optional

from fastapi import FastAPI, HTTPException

from datapipe_app.ops_specs_service import OpsSpecsService
from datapipe_app.spec_registry import OpsSpecRegistry, OpsSpecValidationError


def register_ops_spec_routes(app: FastAPI, service: OpsSpecsService) -> None:
    @app.get("/pipelines/{pipeline_id}/ops-specs")
    def list_ops_specs(pipeline_id: str) -> dict[str, Any]:
        return service.list_specs(pipeline_id)

    @app.get("/pipelines/{pipeline_id}/ops-specs/{spec_id}")
    def get_ops_spec(pipeline_id: str, spec_id: str, debug: bool = False) -> dict[str, Any]:
        try:
            return service.get_spec(spec_id, debug=debug)
        except KeyError as exc:
            raise HTTPException(404, str(exc)) from exc

    @app.get("/pipelines/{pipeline_id}/ops-pages/frozen-datasets/overview")
    def frozen_overview(pipeline_id: str) -> dict[str, Any]:
        return service.frozen_overview()

    @app.get("/pipelines/{pipeline_id}/ops-pages/training/overview")
    def training_overview(pipeline_id: str) -> dict[str, Any]:
        return service.training_overview()

    @app.get("/pipelines/{pipeline_id}/ops-pages/metrics/overview")
    def metrics_overview(pipeline_id: str) -> dict[str, Any]:
        return service.metrics_overview()

    @app.get("/pipelines/{pipeline_id}/ops-pages/class-metrics/overview")
    def class_metrics_overview(pipeline_id: str) -> dict[str, Any]:
        return service.class_metrics_overview()

    @app.get("/pipelines/{pipeline_id}/ops-specs/{spec_id}/frozen-datasets")
    def frozen_rows(
        pipeline_id: str,
        spec_id: str,
        search: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_dir: Literal["asc", "desc"] = "desc",
        limit: int = 50,
        offset: int = 0,
    ) -> dict[str, Any]:
        try:
            return service.frozen_rows(
                spec_id,
                search=search,
                sort_by=sort_by,
                sort_dir=sort_dir,
                limit=limit,
                offset=offset,
            )
        except (KeyError, OpsSpecValidationError) as exc:
            raise HTTPException(400, str(exc)) from exc

    @app.get("/pipelines/{pipeline_id}/ops-specs/{spec_id}/training/runs")
    def training_rows(
        pipeline_id: str,
        spec_id: str,
        search: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_dir: Literal["asc", "desc"] = "desc",
        limit: int = 50,
        offset: int = 0,
    ) -> dict[str, Any]:
        try:
            return service.training_rows(
                spec_id,
                search=search,
                sort_by=sort_by,
                sort_dir=sort_dir,
                limit=limit,
                offset=offset,
            )
        except (KeyError, OpsSpecValidationError) as exc:
            raise HTTPException(400, str(exc)) from exc

    @app.get("/pipelines/{pipeline_id}/ops-specs/{spec_id}/metrics")
    def metrics_tables(pipeline_id: str, spec_id: str) -> dict[str, Any]:
        try:
            return {"tables": service.get_spec(spec_id)["metrics"]}
        except KeyError as exc:
            raise HTTPException(404, str(exc)) from exc

    @app.get("/pipelines/{pipeline_id}/ops-specs/{spec_id}/metrics/{table_id}/rows")
    def metric_rows(
        pipeline_id: str,
        spec_id: str,
        table_id: str,
        search: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_dir: Literal["asc", "desc"] = "desc",
        limit: int = 50,
        offset: int = 0,
    ) -> dict[str, Any]:
        try:
            return service.metric_table_rows(
                spec_id,
                table_id,
                search=search,
                sort_by=sort_by,
                sort_dir=sort_dir,
                limit=limit,
                offset=offset,
            )
        except (KeyError, OpsSpecValidationError) as exc:
            raise HTTPException(400, str(exc)) from exc

    @app.get("/pipelines/{pipeline_id}/ops-specs/{spec_id}/class-metrics")
    def class_metrics_tables(pipeline_id: str, spec_id: str) -> dict[str, Any]:
        try:
            return {"tables": service.get_spec(spec_id)["class_metrics"]}
        except KeyError as exc:
            raise HTTPException(404, str(exc)) from exc

    @app.get("/pipelines/{pipeline_id}/ops-specs/{spec_id}/class-metrics/{table_id}/rows")
    def class_metric_rows(
        pipeline_id: str,
        spec_id: str,
        table_id: str,
        search: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_dir: Literal["asc", "desc"] = "desc",
        limit: int = 50,
        offset: int = 0,
    ) -> dict[str, Any]:
        try:
            return service.metric_table_rows(
                spec_id,
                table_id,
                class_metrics=True,
                search=search,
                sort_by=sort_by,
                sort_dir=sort_dir,
                limit=limit,
                offset=offset,
            )
        except (KeyError, OpsSpecValidationError) as exc:
            raise HTTPException(400, str(exc)) from exc

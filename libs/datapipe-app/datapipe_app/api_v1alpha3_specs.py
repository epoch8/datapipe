from __future__ import annotations

from typing import Any, Literal, Optional

from fastapi import FastAPI, HTTPException, Query, Response

from datapipe_app.observability.schemas import OpsImageRecordDetailResponse, OpsImageRecordsCountResponse, OpsImageRecordsResponse

from datapipe_app.ops_filters import merge_filter_rules, parse_filter_rules
from datapipe_app.ops_specs_service import OpsSpecsService
from datapipe_app.spec_registry import OpsSpecValidationError


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
        filters: Optional[str] = None,
        filter_mode: Literal["or", "and"] = "or",
    ) -> dict[str, Any]:
        try:
            return service.frozen_rows(
                spec_id,
                search=search,
                sort_by=sort_by,
                sort_dir=sort_dir,
                limit=limit,
                offset=offset,
                filter_rules=parse_filter_rules(filters),
                filter_mode=filter_mode,
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
        filters: Optional[str] = None,
        filter_mode: Literal["or", "and"] = "or",
    ) -> dict[str, Any]:
        try:
            return service.training_rows(
                spec_id,
                search=search,
                sort_by=sort_by,
                sort_dir=sort_dir,
                limit=limit,
                offset=offset,
                filter_rules=parse_filter_rules(filters),
                filter_mode=filter_mode,
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
        filters: Optional[str] = None,
        filter_mode: Literal["or", "and"] = "or",
        model: list[str] = Query(default=[]),
        subset: list[str] = Query(default=[]),
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
                filter_rules=merge_filter_rules(
                    parse_filter_rules(filters),
                    model=model or None,
                    subset=subset or None,
                ),
                filter_mode=filter_mode,
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
        filters: Optional[str] = None,
        filter_mode: Literal["or", "and"] = "or",
        model: list[str] = Query(default=[]),
        subset: list[str] = Query(default=[]),
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
                filter_rules=merge_filter_rules(
                    parse_filter_rules(filters),
                    model=model or None,
                    subset=subset or None,
                ),
                filter_mode=filter_mode,
            )
        except (KeyError, OpsSpecValidationError) as exc:
            raise HTTPException(400, str(exc)) from exc

    @app.get("/pipelines/{pipeline_id}/ops-specs/{spec_id}/image/records", response_model=OpsImageRecordsResponse)
    def image_records(
        pipeline_id: str,
        spec_id: str,
        limit: int = 10,
        offset: int = 0,
        include_total: bool = False,
        search: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_dir: Literal["asc", "desc"] = "asc",
        filters: Optional[str] = None,
        filter_mode: Literal["or", "and"] = "or",
    ) -> OpsImageRecordsResponse:
        try:
            return service.image_records(
                pipeline_id=pipeline_id,
                spec_id=spec_id,
                limit=limit,
                offset=offset,
                include_total=include_total,
                search=search,
                sort_by=sort_by,
                sort_dir=sort_dir,
                filter_rules=parse_filter_rules(filters),
                filter_mode=filter_mode,
            )
        except (KeyError, OpsSpecValidationError) as exc:
            raise HTTPException(404, str(exc)) from exc

    @app.get(
        "/pipelines/{pipeline_id}/ops-specs/{spec_id}/image/records/count",
        response_model=OpsImageRecordsCountResponse,
    )
    def image_records_count(
        pipeline_id: str,
        spec_id: str,
        search: Optional[str] = None,
        filters: Optional[str] = None,
        filter_mode: Literal["or", "and"] = "or",
    ) -> OpsImageRecordsCountResponse:
        try:
            return service.image_records_count(
                pipeline_id=pipeline_id,
                spec_id=spec_id,
                search=search,
                filter_rules=parse_filter_rules(filters),
                filter_mode=filter_mode,
            )
        except (KeyError, OpsSpecValidationError) as exc:
            raise HTTPException(404, str(exc)) from exc

    @app.get(
        "/pipelines/{pipeline_id}/ops-specs/{spec_id}/image/records/{record_key}",
        response_model=OpsImageRecordDetailResponse,
    )
    def image_record_detail(pipeline_id: str, spec_id: str, record_key: str) -> OpsImageRecordDetailResponse:
        try:
            return service.image_record_detail(pipeline_id=pipeline_id, spec_id=spec_id, record_key=record_key)
        except KeyError as exc:
            raise HTTPException(404, str(exc)) from exc

    @app.get("/pipelines/{pipeline_id}/ops-specs/{spec_id}/image/records/{record_key}/preview")
    def image_record_preview(pipeline_id: str, spec_id: str, record_key: str) -> Response:
        return _image_record_response(
            service,
            pipeline_id=pipeline_id,
            spec_id=spec_id,
            scope="data",
            record_key=record_key,
            mode="preview",
        )

    @app.get("/pipelines/{pipeline_id}/ops-specs/{spec_id}/image/records/{record_key}/visualization")
    def image_record_visualization(
        pipeline_id: str,
        spec_id: str,
        record_key: str,
        overlay: Literal["record", "gt", "prediction", "both", "plain"] = "record",
        max_side: int | None = None,
    ) -> Response:
        return _image_record_response(
            service,
            pipeline_id=pipeline_id,
            spec_id=spec_id,
            scope="data",
            record_key=record_key,
            mode="visualization",
            overlay=overlay,
            max_side=max_side,
        )

    @app.get(
        "/pipelines/{pipeline_id}/ops-specs/{spec_id}/frozen-datasets/{dataset_id}/records",
        response_model=OpsImageRecordsResponse,
    )
    def frozen_dataset_records(
        pipeline_id: str,
        spec_id: str,
        dataset_id: str,
        limit: int = 10,
        offset: int = 0,
        include_total: bool = False,
        search: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_dir: Literal["asc", "desc"] = "asc",
        filters: Optional[str] = None,
        filter_mode: Literal["or", "and"] = "or",
    ) -> OpsImageRecordsResponse:
        try:
            return service.frozen_dataset_records(
                pipeline_id=pipeline_id,
                spec_id=spec_id,
                dataset_id=dataset_id,
                limit=limit,
                offset=offset,
                include_total=include_total,
                search=search,
                sort_by=sort_by,
                sort_dir=sort_dir,
                filter_rules=parse_filter_rules(filters),
                filter_mode=filter_mode,
            )
        except (KeyError, OpsSpecValidationError) as exc:
            raise HTTPException(404, str(exc)) from exc

    @app.get(
        "/pipelines/{pipeline_id}/ops-specs/{spec_id}/frozen-datasets/{dataset_id}/records/count",
        response_model=OpsImageRecordsCountResponse,
    )
    def frozen_dataset_records_count(
        pipeline_id: str,
        spec_id: str,
        dataset_id: str,
        search: Optional[str] = None,
        filters: Optional[str] = None,
        filter_mode: Literal["or", "and"] = "or",
    ) -> OpsImageRecordsCountResponse:
        try:
            return service.frozen_dataset_records_count(
                pipeline_id=pipeline_id,
                spec_id=spec_id,
                dataset_id=dataset_id,
                search=search,
                filter_rules=parse_filter_rules(filters),
                filter_mode=filter_mode,
            )
        except (KeyError, OpsSpecValidationError) as exc:
            raise HTTPException(404, str(exc)) from exc

    @app.get(
        "/pipelines/{pipeline_id}/ops-specs/{spec_id}/frozen-datasets/{dataset_id}/records/{record_key}",
        response_model=OpsImageRecordDetailResponse,
    )
    def frozen_dataset_record_detail(
        pipeline_id: str, spec_id: str, dataset_id: str, record_key: str
    ) -> OpsImageRecordDetailResponse:
        try:
            return service.frozen_dataset_record_detail(
                pipeline_id=pipeline_id, spec_id=spec_id, dataset_id=dataset_id, record_key=record_key
            )
        except KeyError as exc:
            raise HTTPException(404, str(exc)) from exc

    @app.get(
        "/pipelines/{pipeline_id}/ops-specs/{spec_id}/frozen-datasets/{dataset_id}/records/{record_key}/preview"
    )
    def frozen_dataset_record_preview(pipeline_id: str, spec_id: str, dataset_id: str, record_key: str) -> Response:
        return _image_record_response(
            service,
            pipeline_id=pipeline_id,
            spec_id=spec_id,
            scope="frozen_dataset",
            record_key=record_key,
            parent_id=dataset_id,
            mode="preview",
        )

    @app.get(
        "/pipelines/{pipeline_id}/ops-specs/{spec_id}/frozen-datasets/{dataset_id}/records/{record_key}/visualization"
    )
    def frozen_dataset_record_visualization(
        pipeline_id: str,
        spec_id: str,
        dataset_id: str,
        record_key: str,
        overlay: Literal["record", "gt", "prediction", "both", "plain"] = "record",
        max_side: int | None = None,
    ) -> Response:
        return _image_record_response(
            service,
            pipeline_id=pipeline_id,
            spec_id=spec_id,
            scope="frozen_dataset",
            record_key=record_key,
            parent_id=dataset_id,
            mode="visualization",
            overlay=overlay,
            max_side=max_side,
        )

    @app.get(
        "/pipelines/{pipeline_id}/ops-specs/{spec_id}/models/{model_id}/predictions",
        response_model=OpsImageRecordsResponse,
    )
    def model_prediction_records(
        pipeline_id: str,
        spec_id: str,
        model_id: str,
        limit: int = 10,
        offset: int = 0,
        include_total: bool = False,
        search: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_dir: Literal["asc", "desc"] = "asc",
        filters: Optional[str] = None,
        filter_mode: Literal["or", "and"] = "or",
    ) -> OpsImageRecordsResponse:
        try:
            return service.model_prediction_records(
                pipeline_id=pipeline_id,
                spec_id=spec_id,
                model_id=model_id,
                limit=limit,
                offset=offset,
                include_total=include_total,
                search=search,
                sort_by=sort_by,
                sort_dir=sort_dir,
                filter_rules=parse_filter_rules(filters),
                filter_mode=filter_mode,
            )
        except (KeyError, OpsSpecValidationError) as exc:
            raise HTTPException(404, str(exc)) from exc

    @app.get(
        "/pipelines/{pipeline_id}/ops-specs/{spec_id}/models/{model_id}/predictions/count",
        response_model=OpsImageRecordsCountResponse,
    )
    def model_prediction_records_count(
        pipeline_id: str,
        spec_id: str,
        model_id: str,
        search: Optional[str] = None,
        filters: Optional[str] = None,
        filter_mode: Literal["or", "and"] = "or",
    ) -> OpsImageRecordsCountResponse:
        try:
            return service.model_prediction_records_count(
                pipeline_id=pipeline_id,
                spec_id=spec_id,
                model_id=model_id,
                search=search,
                filter_rules=parse_filter_rules(filters),
                filter_mode=filter_mode,
            )
        except (KeyError, OpsSpecValidationError) as exc:
            raise HTTPException(404, str(exc)) from exc

    @app.get(
        "/pipelines/{pipeline_id}/ops-specs/{spec_id}/models/{model_id}/predictions/{record_key}",
        response_model=OpsImageRecordDetailResponse,
    )
    def model_prediction_record_detail(
        pipeline_id: str, spec_id: str, model_id: str, record_key: str
    ) -> OpsImageRecordDetailResponse:
        try:
            return service.model_prediction_record_detail(
                pipeline_id=pipeline_id, spec_id=spec_id, model_id=model_id, record_key=record_key
            )
        except KeyError as exc:
            raise HTTPException(404, str(exc)) from exc

    @app.get("/pipelines/{pipeline_id}/ops-specs/{spec_id}/models/{model_id}/predictions/{record_key}/preview")
    def model_prediction_record_preview(
        pipeline_id: str, spec_id: str, model_id: str, record_key: str
    ) -> Response:
        return _image_record_response(
            service,
            pipeline_id=pipeline_id,
            spec_id=spec_id,
            scope="model_prediction",
            record_key=record_key,
            parent_id=model_id,
            mode="preview",
        )

    @app.get(
        "/pipelines/{pipeline_id}/ops-specs/{spec_id}/models/{model_id}/predictions/{record_key}/visualization"
    )
    def model_prediction_record_visualization(
        pipeline_id: str,
        spec_id: str,
        model_id: str,
        record_key: str,
        overlay: Literal["record", "gt", "prediction", "both", "plain"] = "prediction",
        max_side: int | None = None,
    ) -> Response:
        return _image_record_response(
            service,
            pipeline_id=pipeline_id,
            spec_id=spec_id,
            scope="model_prediction",
            record_key=record_key,
            parent_id=model_id,
            mode="visualization",
            overlay=overlay,
            max_side=max_side,
        )


def _image_record_response(
    service: OpsSpecsService,
    *,
    pipeline_id: str,
    spec_id: str,
    scope: Literal["data", "frozen_dataset", "model_prediction"],
    record_key: str,
    parent_id: str | None = None,
    mode: Literal["preview", "visualization"] = "visualization",
    overlay: Literal["record", "gt", "prediction", "both", "plain"] = "record",
    max_side: int | None = None,
) -> Response:
    try:
        content, media_type = service.image_record_bytes(
            pipeline_id=pipeline_id,
            spec_id=spec_id,
            scope=scope,
            record_key=record_key,
            parent_id=parent_id,
            mode=mode,
            overlay=overlay,
            max_side=max_side,
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except OpsSpecValidationError as exc:
        raise HTTPException(400, str(exc)) from exc
    except Exception as exc:
        raise HTTPException(500, str(exc)) from exc
    headers = {"Cache-Control": "private, max-age=3600"} if mode == "preview" else None
    return Response(content=content, media_type=media_type, headers=headers)

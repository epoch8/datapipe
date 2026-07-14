from __future__ import annotations

import base64
import json
from typing import Any, Literal, Mapping, Sequence
from urllib.parse import quote

from datapipe_app.ops_filters import OpsFilterMode
from datapipe_app.ops_query import OpsQuery
from datapipe_ml.spec_registry import OpsSpecRegistry
from datapipe_app.specs import OpsColumn, OpsColumnGroup, OpsFilterRule, OpsMetricTableSpec
from datapipe_ml.image_visualization import OverlayMode, iter_bbox_fields_from_record, render_visualization_bytes
from datapipe_ml.metrics.common import METRICS_NULL_LABEL
from datapipe_ml.observability.schemas import (
    OpsBBoxRow,
    OpsImageRecordDetailResponse,
    OpsImageRecordListRow,
    OpsImageRecordsResponse,
)
from datapipe_ml.ops_spec_resolve import resolve_frozen_scope_column
from datapipe_ml.ops_specs import (
    OpsImageAnnotationSpec,
    OpsImageDataSpec,
    OpsImagePredictionViewSpec,
    OpsImageRecordViewSpec,
)

ImageScope = Literal["data", "frozen_dataset", "model_prediction"]


class OpsImageRecordsSupport:
    def __init__(self, registry: OpsSpecRegistry, query: OpsQuery) -> None:
        self.registry = registry
        self.query = query

    def image_records(
        self,
        *,
        pipeline_id: str,
        spec_id: str,
        limit: int = 10,
        offset: int = 0,
        include_total: bool = False,
        search: str | None = None,
        filter_rules: Sequence[OpsFilterRule] | None = None,
        filter_mode: OpsFilterMode = "or",
        sort_by: str | None = None,
        sort_dir: Literal["asc", "desc"] = "asc",
    ) -> OpsImageRecordsResponse:
        spec = self.registry.get(spec_id)
        if spec.data is None or spec.data.image_view is None:
            return self._empty_response(pipeline_id, spec_id, "data", tuple(), limit, offset, include_total=include_total)
        view = spec.data.image_view
        filter_columns = self._data_record_filter_columns(view)
        query_kwargs = self._data_image_query_kwargs(
            view,
            filter_columns=filter_columns,
            search=search,
            filter_rules=filter_rules,
            filter_mode=filter_mode,
            sort_by=sort_by,
        )
        resolved_sort_by, resolved_sort_dir = self._resolve_image_record_sort(
            filter_columns,
            sort_by=sort_by,
            sort_dir=sort_dir,
            default_source=view.image_primary_key_columns[0] if view.image_primary_key_columns else None,
        )
        rows, total = self.query.rows_scoped(
            view.image_table,
            {},
            limit=self._clamp_limit(limit),
            offset=offset,
            sort_by=resolved_sort_by,
            sort_dir=resolved_sort_dir,
            include_total=include_total,
            **query_kwargs,
        )
        list_rows = [
            self._data_list_row(
                pipeline_id=pipeline_id,
                spec_id=spec_id,
                view=view,
                record=row,
            )
            for row in rows
        ]
        return OpsImageRecordsResponse(
            pipeline_id=pipeline_id,
            spec_id=spec_id,
            scope="data",
            parent_id=None,
            primary_key_columns=list(view.image_primary_key_columns),
            list_columns=self._data_list_columns(view),
            rows=list_rows,
            total=total,
            limit=self._clamp_limit(limit),
            offset=offset,
        )

    def image_record_detail(
        self,
        *,
        pipeline_id: str,
        spec_id: str,
        record_key: str,
    ) -> OpsImageRecordDetailResponse:
        spec = self.registry.get(spec_id)
        if spec.data is None or spec.data.image_view is None:
            raise KeyError(f'Spec "{spec_id}" does not define data.image_view.')
        view = spec.data.image_view
        pk = self._decode_record_key(record_key)
        record = self._load_data_record(view, pk)
        return self._build_data_detail(pipeline_id, spec_id, record_key, pk, record, view)

    def image_records_count(
        self,
        *,
        pipeline_id: str,
        spec_id: str,
        search: str | None = None,
        filter_rules: Sequence[OpsFilterRule] | None = None,
        filter_mode: OpsFilterMode = "or",
    ) -> int:
        spec = self.registry.get(spec_id)
        if spec.data is None or spec.data.image_view is None:
            return 0
        view = spec.data.image_view
        filter_columns = self._data_record_filter_columns(view)
        query_kwargs = self._data_image_query_kwargs(
            view,
            filter_columns=filter_columns,
            search=search,
            filter_rules=filter_rules,
            filter_mode=filter_mode,
        )
        return self.query.count_scoped(view.image_table, {}, **query_kwargs)

    def frozen_dataset_records(
        self,
        *,
        pipeline_id: str,
        spec_id: str,
        dataset_id: str,
        limit: int = 10,
        offset: int = 0,
        include_total: bool = False,
        search: str | None = None,
        filter_rules: Sequence[OpsFilterRule] | None = None,
        filter_mode: OpsFilterMode = "or",
        sort_by: str | None = None,
        sort_dir: Literal["asc", "desc"] = "asc",
    ) -> OpsImageRecordsResponse:
        spec = self.registry.get(spec_id)
        frozen = spec.frozen_dataset
        if frozen is None or frozen.record_view is None or frozen.record_view.kind != "image":
            return self._empty_response(
                pipeline_id, spec_id, "frozen_dataset", tuple(), limit, offset, parent_id=dataset_id, include_total=include_total
            )
        view = frozen.record_view
        scope_column = resolve_frozen_scope_column(spec, view)
        filter_columns = self._frozen_record_filter_columns(view)
        resolved_sort_by, resolved_sort_dir = self._resolve_image_record_sort(
            filter_columns,
            sort_by=sort_by,
            sort_dir=sort_dir,
            default_source=view.primary_key_columns[0] if view.primary_key_columns else None,
        )
        rows, total = self.query.rows_scoped(
            view.table,
            {scope_column: dataset_id},
            allowed_columns=filter_columns,
            search=search,
            filter_rules=filter_rules,
            filter_mode=filter_mode,
            limit=self._clamp_limit(limit),
            offset=offset,
            sort_by=resolved_sort_by,
            sort_dir=resolved_sort_dir,
            include_total=include_total,
        )
        list_rows = [
            self._frozen_list_row(
                pipeline_id=pipeline_id,
                spec_id=spec_id,
                dataset_id=dataset_id,
                view=view,
                record=row,
            )
            for row in rows
        ]
        return OpsImageRecordsResponse(
            pipeline_id=pipeline_id,
            spec_id=spec_id,
            scope="frozen_dataset",
            parent_id=dataset_id,
            primary_key_columns=list(view.primary_key_columns),
            list_columns=["image_name", "subset_id", "bboxes"],
            rows=list_rows,
            total=total,
            limit=self._clamp_limit(limit),
            offset=offset,
        )

    def frozen_dataset_record_detail(
        self,
        *,
        pipeline_id: str,
        spec_id: str,
        dataset_id: str,
        record_key: str,
    ) -> OpsImageRecordDetailResponse:
        spec = self.registry.get(spec_id)
        frozen = spec.frozen_dataset
        if frozen is None or frozen.record_view is None or frozen.record_view.kind != "image":
            raise KeyError(f'Spec "{spec_id}" does not define frozen_dataset.record_view.')
        view = frozen.record_view
        pk = self._decode_record_key(record_key)
        scope_column = resolve_frozen_scope_column(spec, view)
        record = self.query.row_by_pk(view.table, {**pk, scope_column: dataset_id})
        if record is None:
            raise KeyError(f'Record "{record_key}" was not found.')
        return self._build_frozen_detail(pipeline_id, spec_id, dataset_id, record_key, pk, record, view)

    def frozen_dataset_records_count(
        self,
        *,
        spec_id: str,
        dataset_id: str,
        search: str | None = None,
        filter_rules: Sequence[OpsFilterRule] | None = None,
        filter_mode: OpsFilterMode = "or",
    ) -> int:
        spec = self.registry.get(spec_id)
        frozen = spec.frozen_dataset
        if frozen is None or frozen.record_view is None or frozen.record_view.kind != "image":
            return 0
        view = frozen.record_view
        scope_column = resolve_frozen_scope_column(spec, view)
        filter_columns = self._frozen_record_filter_columns(view)
        return self.query.count_scoped(
            view.table,
            {scope_column: dataset_id},
            allowed_columns=filter_columns,
            search=search,
            filter_rules=filter_rules,
            filter_mode=filter_mode,
        )

    def model_prediction_records(
        self,
        *,
        pipeline_id: str,
        spec_id: str,
        model_id: str,
        limit: int = 10,
        offset: int = 0,
        include_total: bool = False,
        search: str | None = None,
        filter_rules: Sequence[OpsFilterRule] | None = None,
        filter_mode: OpsFilterMode = "or",
        sort_by: str | None = None,
        sort_dir: Literal["asc", "desc"] = "asc",
    ) -> OpsImageRecordsResponse:
        spec = self.registry.get(spec_id)
        model = spec.model
        if model is None or model.prediction_view is None:
            return self._empty_response(
                pipeline_id, spec_id, "model_prediction", tuple(), limit, offset, parent_id=model_id, include_total=include_total
            )
        view = model.prediction_view
        filter_columns = self._prediction_record_filter_columns(view)
        query_kwargs = self._prediction_image_query_kwargs(
            view, search=search, filter_rules=filter_rules, filter_mode=filter_mode
        )
        resolved_sort_by, resolved_sort_dir = self._resolve_image_record_sort(
            filter_columns,
            sort_by=sort_by,
            sort_dir=sort_dir,
            default_source=view.image_primary_key_columns[0] if view.image_primary_key_columns else None,
        )
        rows, total = self.query.rows_scoped(
            view.table,
            {view.model_id_column: model_id},
            limit=self._clamp_limit(limit),
            offset=offset,
            sort_by=resolved_sort_by,
            sort_dir=resolved_sort_dir,
            include_total=include_total,
            **query_kwargs,
        )
        list_rows = [
            self._prediction_list_row(
                pipeline_id=pipeline_id,
                spec_id=spec_id,
                model_id=model_id,
                view=view,
                record=row,
            )
            for row in rows
        ]
        return OpsImageRecordsResponse(
            pipeline_id=pipeline_id,
            spec_id=spec_id,
            scope="model_prediction",
            parent_id=model_id,
            primary_key_columns=list(view.image_primary_key_columns),
            list_columns=["image_name", "subset", "bboxes"],
            rows=list_rows,
            total=total,
            limit=self._clamp_limit(limit),
            offset=offset,
        )

    def model_prediction_records_count(
        self,
        *,
        spec_id: str,
        model_id: str,
        search: str | None = None,
        filter_rules: Sequence[OpsFilterRule] | None = None,
        filter_mode: OpsFilterMode = "or",
    ) -> int:
        spec = self.registry.get(spec_id)
        model = spec.model
        if model is None or model.prediction_view is None:
            return 0
        view = model.prediction_view
        query_kwargs = self._prediction_image_query_kwargs(
            view, search=search, filter_rules=filter_rules, filter_mode=filter_mode
        )
        return self.query.count_scoped(view.table, {view.model_id_column: model_id}, **query_kwargs)

    def model_prediction_record_detail(
        self,
        *,
        pipeline_id: str,
        spec_id: str,
        model_id: str,
        record_key: str,
    ) -> OpsImageRecordDetailResponse:
        spec = self.registry.get(spec_id)
        model = spec.model
        if model is None or model.prediction_view is None:
            raise KeyError(f'Spec "{spec_id}" does not define model.prediction_view.')
        view = model.prediction_view
        pk = self._decode_record_key(record_key)
        prediction = self.query.row_by_pk(view.table, {**pk, view.model_id_column: model_id})
        if prediction is None:
            raise KeyError(f'Record "{record_key}" was not found.')
        return self._build_prediction_detail(pipeline_id, spec_id, model_id, record_key, pk, prediction, view)

    def image_record_bytes(
        self,
        *,
        pipeline_id: str,
        spec_id: str,
        scope: ImageScope,
        record_key: str,
        parent_id: str | None = None,
        mode: Literal["preview", "visualization"] = "visualization",
        overlay: OverlayMode = "record",
        annotations: bool = True,
        max_side: int | None = None,
    ) -> tuple[bytes, str]:
        del pipeline_id
        if mode == "preview":
            max_side = max_side or 56
            annotations = False
            overlay = "plain"
        detail = self._detail_for_scope(scope, spec_id=spec_id, record_key=record_key, parent_id=parent_id)
        image_url = detail.image_url
        if image_url is None:
            raise KeyError("Image URL is not available for this record.")
        gt_record = None
        prediction_record = None
        record = None
        if scope == "data":
            record = detail.record
            gt_record = self._gt_record_for_detail(detail, scope)
        elif scope == "frozen_dataset":
            record = detail.record
            gt_record = detail.record
        elif scope == "model_prediction":
            gt_record = self._joined_annotation_record(detail, "gt")
            prediction_record = self._joined_annotation_record(detail, "prediction")
        return render_visualization_bytes(
            image_url=image_url,
            record=record,
            gt_record=gt_record,
            prediction_record=prediction_record,
            overlay=overlay,
            scores_column=self._scores_column_for_scope(scope, spec_id),
            annotations=annotations,
            max_side=max_side,
        )

    def _detail_for_scope(
        self,
        scope: ImageScope,
        *,
        spec_id: str,
        record_key: str,
        parent_id: str | None,
    ) -> OpsImageRecordDetailResponse:
        if scope == "data":
            return self.image_record_detail(pipeline_id="", spec_id=spec_id, record_key=record_key)
        if scope == "frozen_dataset":
            if parent_id is None:
                raise KeyError("parent_id is required for frozen_dataset scope.")
            return self.frozen_dataset_record_detail(
                pipeline_id="",
                spec_id=spec_id,
                dataset_id=parent_id,
                record_key=record_key,
            )
        if parent_id is None:
            raise KeyError("parent_id is required for model_prediction scope.")
        return self.model_prediction_record_detail(
            pipeline_id="",
            spec_id=spec_id,
            model_id=parent_id,
            record_key=record_key,
        )

    def _gt_record_for_detail(self, detail: OpsImageRecordDetailResponse, scope: ImageScope) -> dict[str, Any] | None:
        if scope == "model_prediction":
            return self._joined_annotation_record(detail, "gt")
        return detail.record

    def _joined_annotation_record(self, detail: OpsImageRecordDetailResponse, role: str) -> dict[str, Any] | None:
        spec = self.registry.get(detail.spec_id)
        if spec.model is None or spec.model.prediction_view is None:
            return None
        view = spec.model.prediction_view
        annotation = view.ground_truth if role == "gt" else view.prediction
        if annotation is None:
            return None
        return self._load_annotation_record(
            annotation,
            detail.pk,
            parent_id=detail.parent_id,
            model_id_column=view.model_id_column,
        )

    def _scores_column_for_scope(self, scope: ImageScope, spec_id: str) -> str | None:
        spec = self.registry.get(spec_id)
        if scope == "model_prediction" and spec.model and spec.model.prediction_view and spec.model.prediction_view.prediction:
            return spec.model.prediction_view.prediction.scores_column
        if scope == "frozen_dataset" and spec.frozen_dataset and spec.frozen_dataset.record_view and spec.frozen_dataset.record_view.kind == "image":
            return spec.frozen_dataset.record_view.scores_column
        return None

    def _data_list_row(
        self,
        *,
        pipeline_id: str,
        spec_id: str,
        view: OpsImageDataSpec,
        record: dict[str, Any],
    ) -> OpsImageRecordListRow:
        pk = {key: record.get(key) for key in view.image_primary_key_columns}
        record_key = self._encode_record_key(pk)
        gt_record = self._load_ground_truth(view, pk) if view.records_show_ground_truth else None
        subset = self._load_subset(view, pk) if view.records_show_subset else None
        bbox_count = None
        if view.records_show_ground_truth:
            bbox_count = self._bbox_count(
                gt_record.get(view.ground_truth.bboxes_column or "bboxes") if gt_record and view.ground_truth else None
            )
        base = self._record_urls(pipeline_id, spec_id, "data", record_key)
        return OpsImageRecordListRow(
            record_key=record_key,
            pk=pk,
            preview_url=base + "/preview",
            visualization_url=base + "/visualization",
            detail_url=f"/image/{quote(spec_id, safe='')}/records/{quote(record_key, safe='')}",
            subset=subset,
            bbox_count=bbox_count,
        )

    def _frozen_list_row(
        self,
        *,
        pipeline_id: str,
        spec_id: str,
        dataset_id: str,
        view: OpsImageRecordViewSpec,
        record: dict[str, Any],
    ) -> OpsImageRecordListRow:
        pk = {key: record.get(key) for key in view.primary_key_columns}
        record_key = self._encode_record_key(pk)
        bbox_count = self._bbox_count(record.get(view.bboxes_column or "bboxes"))
        base = self._record_urls(pipeline_id, spec_id, "frozen_dataset", record_key, parent_id=dataset_id)
        return OpsImageRecordListRow(
            record_key=record_key,
            pk=pk,
            preview_url=base + "/preview",
            visualization_url=base + "/visualization",
            detail_url=(
                f"/frozen-datasets/{quote(spec_id, safe='')}/datasets/{quote(dataset_id, safe='')}"
                f"/records/{quote(record_key, safe='')}"
            ),
            subset=str(record.get("subset_id")) if record.get("subset_id") is not None else None,
            bbox_count=bbox_count,
        )

    def _prediction_list_row(
        self,
        *,
        pipeline_id: str,
        spec_id: str,
        model_id: str,
        view: OpsImagePredictionViewSpec,
        record: dict[str, Any],
    ) -> OpsImageRecordListRow:
        pk = {key: record.get(key) for key in view.image_primary_key_columns}
        record_key = self._encode_record_key(pk)
        pred_count = self._bbox_count(record.get(view.prediction.bboxes_column or "bboxes") if view.prediction else None)
        gt_record = (
            self._load_annotation_record(
                view.ground_truth,
                pk,
                parent_id=model_id,
                model_id_column=view.model_id_column,
            )
            if view.ground_truth
            else None
        )
        gt_count = self._bbox_count(gt_record.get(view.ground_truth.bboxes_column or "bboxes") if gt_record and view.ground_truth else None)
        metrics = self._prediction_metrics_values(view, model_id=model_id, pk=pk)
        base = self._record_urls(pipeline_id, spec_id, "model_prediction", record_key, parent_id=model_id)
        return OpsImageRecordListRow(
            record_key=record_key,
            pk=pk,
            preview_url=base + "/preview",
            visualization_url=base + "/visualization?overlay=both",
            detail_url=(
                f"/metrics/{quote(spec_id, safe='')}/models/{quote(model_id, safe='')}"
                f"/predictions/{quote(record_key, safe='')}"
            ),
            bbox_count=pred_count,
            gt_bbox_count=gt_count,
            prediction_bbox_count=pred_count,
            metrics=metrics,
        )

    def _build_data_detail(
        self,
        pipeline_id: str,
        spec_id: str,
        record_key: str,
        pk: dict[str, Any],
        record: dict[str, Any],
        view: OpsImageDataSpec,
    ) -> OpsImageRecordDetailResponse:
        gt_record = self._load_ground_truth(view, pk)
        subset = self._load_subset(view, pk)
        image_url = record.get(view.image_url_column)
        merged = {**record, **(gt_record or {})}
        bbox_rows = self._bbox_rows_from_record(gt_record or {}, view.ground_truth)
        base = self._record_urls(pipeline_id, spec_id, "data", record_key)
        return OpsImageRecordDetailResponse(
            pipeline_id=pipeline_id,
            spec_id=spec_id,
            scope="data",
            parent_id=None,
            record_key=record_key,
            pk=pk,
            record=merged,
            image_url=image_url,
            subset=subset,
            preview_url=base + "/preview",
            visualization_url=base + "/visualization",
            plain_image_url=base + "/visualization?overlay=plain",
            bbox_count=len(bbox_rows),
            gt_bbox_count=len(bbox_rows),
            bbox_rows=bbox_rows,
            gt_bbox_rows=bbox_rows,
        )

    def _build_frozen_detail(
        self,
        pipeline_id: str,
        spec_id: str,
        dataset_id: str,
        record_key: str,
        pk: dict[str, Any],
        record: dict[str, Any],
        view: OpsImageRecordViewSpec,
    ) -> OpsImageRecordDetailResponse:
        image_url = record.get(view.image_url_column or "image_url")
        bbox_rows = self._bbox_rows_from_record(record, None, view=view)
        base = self._record_urls(pipeline_id, spec_id, "frozen_dataset", record_key, parent_id=dataset_id)
        return OpsImageRecordDetailResponse(
            pipeline_id=pipeline_id,
            spec_id=spec_id,
            scope="frozen_dataset",
            parent_id=dataset_id,
            record_key=record_key,
            pk=pk,
            record=record,
            image_url=image_url,
            subset=str(record.get("subset_id")) if record.get("subset_id") is not None else None,
            preview_url=base + "/preview",
            visualization_url=base + "/visualization",
            plain_image_url=base + "/visualization?overlay=plain",
            bbox_count=len(bbox_rows),
            gt_bbox_count=len(bbox_rows),
            bbox_rows=bbox_rows,
            gt_bbox_rows=bbox_rows,
        )

    def _build_prediction_detail(
        self,
        pipeline_id: str,
        spec_id: str,
        model_id: str,
        record_key: str,
        pk: dict[str, Any],
        prediction: dict[str, Any],
        view: OpsImagePredictionViewSpec,
    ) -> OpsImageRecordDetailResponse:
        image_url = None
        if view.image_url_column and view.image_url_column in prediction:
            image_url = prediction.get(view.image_url_column)
        if not image_url:
            image_url = self._resolve_image_url(view, pk)
        gt_record = (
            self._load_annotation_record(
                view.ground_truth,
                pk,
                parent_id=model_id,
                model_id_column=view.model_id_column,
            )
            if view.ground_truth
            else None
        )
        subset = self._load_subset_for_prediction(view, pk)
        pred_rows = self._bbox_rows_from_record(prediction, view.prediction)
        gt_rows = self._bbox_rows_from_record(gt_record or {}, view.ground_truth)
        metrics = self._prediction_metrics_values(view, model_id=model_id, pk=pk)
        base = self._record_urls(pipeline_id, spec_id, "model_prediction", record_key, parent_id=model_id)
        return OpsImageRecordDetailResponse(
            pipeline_id=pipeline_id,
            spec_id=spec_id,
            scope="model_prediction",
            parent_id=model_id,
            record_key=record_key,
            pk=pk,
            record={**prediction, **(gt_record or {}), **(metrics or {})},
            image_url=image_url,
            subset=subset,
            preview_url=base + "/preview",
            visualization_url=base + "/visualization?overlay=prediction",
            plain_image_url=base + "/visualization?overlay=plain",
            gt_visualization_url=base + "/visualization?overlay=gt",
            prediction_visualization_url=base + "/visualization?overlay=prediction",
            plain_gt_image_url=base + "/visualization?overlay=plain",
            plain_prediction_image_url=base + "/visualization?overlay=plain",
            bbox_count=len(pred_rows),
            gt_bbox_count=len(gt_rows),
            prediction_bbox_count=len(pred_rows),
            bbox_rows=pred_rows,
            gt_bbox_rows=gt_rows,
            prediction_bbox_rows=pred_rows,
        )

    def _load_data_record(self, view: OpsImageDataSpec, pk: dict[str, Any]) -> dict[str, Any]:
        record = self.query.row_by_pk(view.image_table, pk)
        if record is None:
            raise KeyError(f'Record {pk} was not found in {view.image_table}.')
        return record

    def _load_ground_truth(self, view: OpsImageDataSpec, pk: dict[str, Any]) -> dict[str, Any] | None:
        if view.ground_truth is None:
            return None
        return self._load_annotation_record(view.ground_truth, pk)

    def _load_annotation_record(
        self,
        annotation: OpsImageAnnotationSpec | None,
        pk: dict[str, Any],
        *,
        parent_id: str | None = None,
        model_id_column: str | None = None,
    ) -> dict[str, Any] | None:
        if annotation is None:
            return None
        if annotation.join_columns:
            lookup = {dest: pk.get(src) for src, dest in annotation.join_columns.items()}
        else:
            lookup = {key: pk.get(key) for key in annotation.primary_key_columns}
        if parent_id is not None and model_id_column:
            try:
                table = self.query._table(annotation.table)  # type: ignore[attr-defined]
                if model_id_column in table.c:
                    lookup[model_id_column] = parent_id
            except Exception:
                pass
        if len(lookup) == 1:
            rows, _ = self.query.rows_scoped(annotation.table, lookup, limit=1, offset=0)
            return rows[0] if rows else None
        return self.query.row_by_pk(annotation.table, lookup)

    def _load_subset(self, view: OpsImageDataSpec, pk: dict[str, Any]) -> str | None:
        if view.subset_table is None or view.subset_column is None:
            return None
        if view.subset_join_columns:
            lookup = {dest: pk.get(src) for src, dest in view.subset_join_columns.items()}
        else:
            lookup = dict(pk)
        rows, _ = self.query.rows_scoped(view.subset_table, lookup, limit=1, offset=0)
        row = rows[0] if rows else None
        return str(row.get(view.subset_column)) if row else None

    def _load_subset_for_prediction(self, view: OpsImagePredictionViewSpec, pk: dict[str, Any]) -> str | None:
        if view.subset_table is None or view.subset_column is None:
            return None
        if view.subset_join_columns:
            lookup = {dest: pk.get(src) for src, dest in view.subset_join_columns.items()}
        else:
            lookup = dict(pk)
        rows, _ = self.query.rows_scoped(view.subset_table, lookup, limit=1, offset=0)
        row = rows[0] if rows else None
        return str(row.get(view.subset_column)) if row else None

    def _prediction_metrics_values(
        self,
        view: OpsImagePredictionViewSpec,
        *,
        model_id: str,
        pk: dict[str, Any],
    ) -> dict[str, Any] | None:
        table_spec = view.metrics_on_image
        if table_spec is None:
            return None
        metrics_row = self._load_prediction_metrics_on_image(view, model_id=model_id, pk=pk)
        if metrics_row is None:
            return None
        values: dict[str, Any] = {}
        for column in self._flatten_metric_columns(table_spec.metric_columns):
            values[column.source] = metrics_row.get(column.source)
            values[column.id] = metrics_row.get(column.source)
        return values

    def _load_prediction_metrics_on_image(
        self,
        view: OpsImagePredictionViewSpec,
        *,
        model_id: str,
        pk: dict[str, Any],
    ) -> dict[str, Any] | None:
        table_spec = view.metrics_on_image
        if table_spec is None:
            return None
        lookup = self._metrics_on_image_lookup(view, table_spec, model_id=model_id, pk=pk)
        return self.query.row_by_pk(table_spec.table, lookup)

    def _metrics_on_image_lookup(
        self,
        view: OpsImagePredictionViewSpec,
        table_spec: OpsMetricTableSpec,
        *,
        model_id: str,
        pk: dict[str, Any],
    ) -> dict[str, Any]:
        subset = self._load_subset_for_prediction(view, pk)
        lookup: dict[str, Any] = {}
        for column in table_spec.primary_key_columns:
            if column == "label":
                lookup[column] = view.metrics_on_image_label
            elif column == view.model_id_column:
                lookup[column] = model_id
            elif column in pk:
                lookup[column] = pk[column]
            elif column == view.subset_column or column == "subset_id":
                if subset is not None:
                    lookup[column] = subset
            else:
                value = pk.get(column)
                if value is not None:
                    lookup[column] = value
        return lookup

    @staticmethod
    def _flatten_metric_columns(columns: Sequence[OpsColumn | OpsColumnGroup]) -> list[OpsColumn]:
        flat: list[OpsColumn] = []
        for column in columns:
            if isinstance(column, OpsColumnGroup):
                flat.extend(column.columns)
            else:
                flat.append(column)
        return flat

    def _resolve_image_url(self, view: OpsImagePredictionViewSpec, pk: dict[str, Any]) -> str | None:
        if view.image_url_join_columns:
            lookup = {dest: pk.get(src) for src, dest in view.image_url_join_columns.items()}
        else:
            lookup = dict(pk)
        row = self.query.row_by_pk(view.image_url_table, lookup)
        if row is None and len(lookup) == 1:
            rows, _ = self.query.rows_scoped(view.image_url_table, lookup, limit=1, offset=0)
            row = rows[0] if rows else None
        return row.get(view.image_url_column) if row else None

    def _record_urls(
        self,
        pipeline_id: str,
        spec_id: str,
        scope: ImageScope,
        record_key: str,
        *,
        parent_id: str | None = None,
    ) -> str:
        encoded_pipeline = quote(pipeline_id, safe="")
        encoded_spec = quote(spec_id, safe="")
        encoded_key = quote(record_key, safe="")
        if scope == "data":
            return f"/api/v1alpha3/pipelines/{encoded_pipeline}/ops-specs/{encoded_spec}/image/records/{encoded_key}"
        if scope == "frozen_dataset":
            encoded_parent = quote(parent_id or "", safe="")
            return (
                f"/api/v1alpha3/pipelines/{encoded_pipeline}/ops-specs/{encoded_spec}"
                f"/frozen-datasets/{encoded_parent}/records/{encoded_key}"
            )
        encoded_parent = quote(parent_id or "", safe="")
        return (
            f"/api/v1alpha3/pipelines/{encoded_pipeline}/ops-specs/{encoded_spec}"
            f"/models/{encoded_parent}/predictions/{encoded_key}"
        )

    def _bbox_rows_from_record(
        self,
        record: dict[str, Any],
        annotation: OpsImageAnnotationSpec | None,
        *,
        view: OpsImageRecordViewSpec | None = None,
    ) -> list[OpsBBoxRow]:
        bboxes_column = (annotation.bboxes_column if annotation else None) or (view.bboxes_column if view else None) or "bboxes"
        labels_column = (annotation.labels_column if annotation else None) or (view.labels_column if view else None) or "labels"
        scores_column = (annotation.scores_column if annotation else None) or (view.scores_column if view else None)
        payload = {
            "bboxes": record.get(bboxes_column),
            "labels": record.get(labels_column),
            scores_column or "prediction__detection_scores": record.get(scores_column or "prediction__detection_scores"),
        }
        rows: list[OpsBBoxRow] = []
        for label, score, xmin, ymin, xmax, ymax in iter_bbox_fields_from_record(payload, scores_column=scores_column):
            rows.append(
                OpsBBoxRow(
                    label=label,
                    confidence=float(score) if score is not None else None,
                    x1=float(xmin) if xmin is not None else None,
                    y1=float(ymin) if ymin is not None else None,
                    x2=float(xmax) if xmax is not None else None,
                    y2=float(ymax) if ymax is not None else None,
                )
            )
        return rows

    def _bbox_count(self, value: Any) -> int:
        if value is None:
            return 0
        if isinstance(value, list):
            return len(value)
        return 0

    def _encode_record_key(self, pk: Mapping[str, Any]) -> str:
        raw = json.dumps(dict(pk), separators=(",", ":"), sort_keys=True).encode("utf-8")
        return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")

    def _decode_record_key(self, record_key: str) -> dict[str, Any]:
        padding = "=" * (-len(record_key) % 4)
        raw = base64.urlsafe_b64decode(record_key + padding)
        payload = json.loads(raw.decode("utf-8"))
        if not isinstance(payload, dict):
            raise KeyError(f'Invalid record key "{record_key}".')
        return payload

    def _clamp_limit(self, limit: int) -> int:
        return min(max(limit, 1), 100)

    def _resolve_image_record_sort(
        self,
        columns: Sequence[OpsColumn],
        *,
        sort_by: str | None,
        sort_dir: Literal["asc", "desc"],
        default_source: str | None,
    ) -> tuple[str | None, Literal["asc", "desc"]]:
        if sort_by:
            allowed_by_id = {col.id: col for col in columns}
            allowed_by_source = {col.source: col for col in columns}
            sort_col = allowed_by_id.get(sort_by) or allowed_by_source.get(sort_by)
            if sort_col is None or not sort_col.sortable:
                raise KeyError(f'Sort column "{sort_by}" is not available for image records.')
            return sort_col.id, sort_dir
        if default_source:
            for col in columns:
                if col.source == default_source:
                    return col.id, "asc"
        return None, "asc"

    def _data_image_query_kwargs(
        self,
        view: OpsImageDataSpec,
        *,
        filter_columns: Sequence[OpsColumn],
        search: str | None,
        filter_rules: Sequence[OpsFilterRule] | None,
        filter_mode: OpsFilterMode,
        sort_by: str | None = None,
    ) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "allowed_columns": list(filter_columns),
            "search": search,
            "filter_rules": filter_rules,
            "filter_mode": filter_mode,
        }
        if (
            view.records_show_subset
            and view.subset_table
            and view.subset_join_columns
            and self._needs_subset_join(view, filter_columns, filter_rules, sort_by)
        ):
            kwargs["join_table_name"] = view.subset_table
            kwargs["join_on"] = dict(view.subset_join_columns)
        return kwargs

    @staticmethod
    def _needs_subset_join(
        view: OpsImageDataSpec,
        filter_columns: Sequence[OpsColumn],
        filter_rules: Sequence[OpsFilterRule] | None,
        sort_by: str | None,
    ) -> bool:
        subset_column = view.subset_column or "subset_id"
        if sort_by:
            allowed_by_id = {col.id: col for col in filter_columns}
            sort_col = allowed_by_id.get(sort_by)
            if sort_col is not None and sort_col.source == subset_column:
                return True
        for rule in filter_rules or []:
            if rule.column_id in {"subset", "subset_id"}:
                return True
        return False

    @staticmethod
    def _data_list_columns(view: OpsImageDataSpec) -> list[str]:
        columns = ["image_name"]
        if view.records_show_subset:
            columns.append("subset")
        if view.records_show_ground_truth:
            columns.append("bboxes")
        return columns

    def _data_record_filter_columns(self, view: OpsImageDataSpec) -> list[OpsColumn]:
        pk_column = view.image_primary_key_columns[0] if view.image_primary_key_columns else "image_name"
        columns = [
            OpsColumn("image_name", "Image", pk_column, filterable=True, sortable=True),
            OpsColumn("image_url", "URL", view.image_url_column, filterable=True, sortable=True),
        ]
        if view.records_show_subset and view.subset_table and view.subset_column:
            columns.append(OpsColumn("subset", "Subset", view.subset_column, "chip", filterable=True, sortable=True))
        return columns

    def _frozen_record_filter_columns(self, view: OpsImageRecordViewSpec) -> list[OpsColumn]:
        pk_column = view.primary_key_columns[0] if view.primary_key_columns else "image_name"
        columns = [OpsColumn("image_name", "Image", pk_column, filterable=True, sortable=True)]
        if "subset_id" in view.primary_key_columns or view.primary_key_columns:
            columns.append(OpsColumn("subset_id", "Subset", "subset_id", "chip", filterable=True, sortable=True))
        if view.image_url_column:
            columns.append(OpsColumn("image_url", "URL", view.image_url_column, filterable=True, sortable=True))
        return columns

    def _prediction_record_filter_columns(self, view: OpsImagePredictionViewSpec) -> list[OpsColumn]:
        pk_column = view.image_primary_key_columns[0] if view.image_primary_key_columns else "image_name"
        columns = [OpsColumn("image_name", "Image", pk_column, filterable=True, sortable=True)]
        if view.subset_table and view.subset_column:
            columns.append(OpsColumn("subset", "Subset", view.subset_column, "chip", filterable=True, sortable=True))
        if view.image_url_column:
            columns.append(OpsColumn("image_url", "URL", view.image_url_column, filterable=True, sortable=True))
        return columns

    def _prediction_image_query_kwargs(
        self,
        view: OpsImagePredictionViewSpec,
        *,
        search: str | None,
        filter_rules: Sequence[OpsFilterRule] | None,
        filter_mode: OpsFilterMode,
    ) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "allowed_columns": self._prediction_record_filter_columns(view),
            "search": search,
            "filter_rules": filter_rules,
            "filter_mode": filter_mode,
        }
        if view.subset_table and view.subset_join_columns:
            kwargs["join_table_name"] = view.subset_table
            kwargs["join_on"] = dict(view.subset_join_columns)
        return kwargs

    def _empty_response(
        self,
        pipeline_id: str,
        spec_id: str,
        scope: ImageScope,
        primary_key_columns: tuple[str, ...],
        limit: int,
        offset: int,
        *,
        parent_id: str | None = None,
        include_total: bool = False,
    ) -> OpsImageRecordsResponse:
        return OpsImageRecordsResponse(
            pipeline_id=pipeline_id,
            spec_id=spec_id,
            scope=scope,
            parent_id=parent_id,
            primary_key_columns=list(primary_key_columns),
            list_columns=[],
            rows=[],
            total=0 if include_total else None,
            limit=self._clamp_limit(limit),
            offset=offset,
        )

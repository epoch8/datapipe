from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional

import fsspec
import pandas as pd

logger = logging.getLogger(__name__)

YOLO_DETECT_RENAME_MAP = {
    "epoch": "epoch",
    "train/box_loss": "train_box_loss",
    "train/cls_loss": "train_cls_loss",
    "train/dfl_loss": "train_dfl_loss",
    "metrics/precision(B)": "metrics_precision",
    "metrics/recall(B)": "metrics_recall",
    "metrics/mAP50(B)": "metrics_mAP_0_5",
    "metrics/mAP50-95(B)": "metrics_mAP_0_5_to_0_95",
    "val/box_loss": "val_box_loss",
    "val/cls_loss": "val_cls_loss",
    "val/dfl_loss": "val_dfl_loss",
    "lr/pg0": "lr_pg0",
    "lr/pg1": "lr_pg1",
    "lr/pg2": "lr_pg2",
}

YOLO_SEGMENT_RENAME_MAP = {
    "epoch": "epoch",
    "train/box_loss": "train_box_loss",
    "train/cls_loss": "train_cls_loss",
    "train/seg_loss": "train_seg_loss",
    "train/dfl_loss": "train_dfl_loss",
    "metrics/precision(B)": "metrics_precision_box",
    "metrics/recall(B)": "metrics_recall_box",
    "metrics/mAP50(B)": "metrics_mAP_0_5_box",
    "metrics/mAP50-95(B)": "metrics_mAP_0_5_to_0_95_box",
    "metrics/precision(M)": "metrics_precision_mask",
    "metrics/recall(M)": "metrics_recall_mask",
    "metrics/mAP50(M)": "metrics_mAP_0_5_mask",
    "metrics/mAP50-95(M)": "metrics_mAP_0_5_to_0_95_mask",
    "val/box_loss": "val_box_loss",
    "val/cls_loss": "val_cls_loss",
    "val/seg_loss": "val_seg_loss",
    "val/dfl_loss": "val_dfl_loss",
}

YOLO_POSE_RENAME_MAP = {
    "epoch": "epoch",
    "train/box_loss": "train_box_loss",
    "train/pose_loss": "train_pose_loss",
    "train/kobj_loss": "train_kobj_loss",
    "train/cls_loss": "train_cls_loss",
    "train/dfl_loss": "train_dfl_loss",
    "metrics/precision(P)": "metrics_precision_pose",
    "metrics/recall(P)": "metrics_recall_pose",
    "metrics/mAP50(P)": "metrics_mAP_0_5_pose",
    "metrics/mAP50-95(P)": "metrics_mAP_0_5_to_0_95_pose",
    "val/box_loss": "val_box_loss",
    "val/pose_loss": "val_pose_loss",
    "val/kobj_loss": "val_kobj_loss",
    "val/cls_loss": "val_cls_loss",
    "val/dfl_loss": "val_dfl_loss",
}

YOLO_V5_DETECT_RENAME_MAP = {
    "epoch": "epoch",
    "train/box_loss": "train_box_loss",
    "train/obj_loss": "train_obj_loss",
    "train/cls_loss": "train_cls_loss",
    "metrics/precision": "metrics_precision",
    "metrics/recall": "metrics_recall",
    "metrics/mAP_0.5": "metrics_mAP_0_5",
    "metrics/mAP_0.5:0.95": "metrics_mAP_0_5_to_0_95",
    "val/box_loss": "val_box_loss",
    "val/obj_loss": "val_obj_loss",
    "val/cls_loss": "val_cls_loss",
}

YOLO_RENAME_MAPS = [
    YOLO_DETECT_RENAME_MAP,
    YOLO_SEGMENT_RENAME_MAP,
    YOLO_POSE_RENAME_MAP,
    YOLO_V5_DETECT_RENAME_MAP,
]

TF_HISTORY_METRIC_COLUMNS = [
    "loss",
    "precision",
    "recall",
    "f1_score",
    "val_loss",
    "val_precision",
    "val_recall",
    "val_f1_score",
    "lr",
    "learning_rate",
]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _file_exists(path: str) -> bool:
    fs, stripped = fsspec.core.url_to_fs(path)
    return fs.exists(stripped)


def _read_csv(path: str) -> pd.DataFrame:
    with fsspec.open(path, "r") as src:
        return pd.read_csv(src, skipinitialspace=True)


def _parse_yolo_results(path: str) -> Optional[pd.DataFrame]:
    raw = _read_csv(path)
    if raw.empty:
        return None
    for rename_map in YOLO_RENAME_MAPS:
        df = raw.rename(columns=rename_map)
        if "epoch" in df.columns:
            return df
    return None


def _parse_tf_history(path: str) -> Optional[pd.DataFrame]:
    df = _read_csv(path)
    if df.empty:
        return None
    if "epoch" not in df.columns:
        return None
    return df


def _metric_rows_from_frame(
    df: pd.DataFrame,
    *,
    metric_columns: list[str],
) -> list[tuple[int, str, float]]:
    rows: list[tuple[int, str, float]] = []
    for _, record in df.iterrows():
        epoch_raw = record.get("epoch")
        if epoch_raw is None or pd.isna(epoch_raw):
            continue
        epoch = int(float(epoch_raw))
        for column in metric_columns:
            if column not in df.columns:
                continue
            value = record.get(column)
            if value is None or pd.isna(value):
                continue
            try:
                rows.append((epoch, column, float(value)))
            except (TypeError, ValueError):
                continue
    return rows


def _resolve_store_and_pipeline(
    store: Any,
    pipeline_id: Optional[str],
) -> tuple[Any, Optional[str]]:
    if store is not None:
        return store, pipeline_id
    try:
        from datapipe_app.observability.db import ObservabilityStore
        from datapipe_app.observability.settings import OPS_SETTINGS
    except ImportError:
        return None, pipeline_id
    url = OPS_SETTINGS.observability_db_url
    if not url:
        return None, OPS_SETTINGS.pipeline_id or pipeline_id
    return ObservabilityStore.from_url(url), OPS_SETTINGS.pipeline_id or pipeline_id


class TrainingCurvePublisher:
    def publish_hook(self, **kwargs: Any) -> None:
        self.publish(**kwargs)

    def publish(
        self,
        *,
        training_run_key: str,
        run_dir: str,
        pipeline_id: Optional[str] = None,
        total_epochs: Optional[int] = None,
        store: Any = None,
    ) -> None:
        if not training_run_key or not run_dir:
            return
        store, pipeline_id = _resolve_store_and_pipeline(store, pipeline_id)
        if store is None or not pipeline_id:
            return

        history_path = f"{run_dir.rstrip('/')}/history.csv"
        results_path = f"{run_dir.rstrip('/')}/results.csv"

        parsed: Optional[pd.DataFrame] = None
        metric_names: list[str] = []
        if _file_exists(history_path):
            parsed = _parse_tf_history(history_path)
            metric_names = [column for column in TF_HISTORY_METRIC_COLUMNS if parsed is not None and column in parsed.columns]
        elif _file_exists(results_path):
            parsed = _parse_yolo_results(results_path)
            if parsed is not None:
                metric_names = [
                    column
                    for column in parsed.columns
                    if column != "epoch" and pd.api.types.is_numeric_dtype(parsed[column])
                ]

        if parsed is None or not metric_names:
            return

        recorded_at = _utc_now()
        if total_epochs is None and "epoch" in parsed.columns:
            try:
                total_epochs = int(float(parsed["epoch"].max()))
            except (TypeError, ValueError):
                total_epochs = None

        for epoch, metric_name, metric_value in _metric_rows_from_frame(parsed, metric_columns=metric_names):
            store.upsert_training_epoch_metric(
                {
                    "pipeline_id": pipeline_id,
                    "training_run_key": training_run_key,
                    "epoch": epoch,
                    "total_epochs": total_epochs,
                    "metric_name": metric_name,
                    "metric_value": metric_value,
                    "recorded_at": recorded_at,
                }
            )

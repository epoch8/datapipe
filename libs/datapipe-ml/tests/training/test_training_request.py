from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from datapipe_ml.frameworks.yolo.dataset import (
    dedupe_size_for_resize,
    get_size_for_resize_from_training_request,
)
from datapipe_ml.training.train_config_id import (
    TrainingConfigPreset,
    build_auto_training_request_id,
    build_custom_train_config_id,
    build_manual_training_request_id,
    train_configs_to_dataframe,
)
from datapipe_ml.training.training_request import build_auto_training_request

FROZEN_DATASET_ID_COL = "detection_frozen_dataset_id"
TRAIN_CONFIG_ID_COL = "detection_train_config_id"
TRAIN_CONFIG_PARAMS_COL = "detection_train_config__params"


@dataclass
class _DummyConfig:
    model: str = "dummy.pt"
    imgsz: int = 640
    batch: int = 8
    epochs: int = 30


def _summary(params):
    return f"dummy-{params['imgsz']}"


def _builtin_config_row(config_id="cfg1", imgsz=640, config_hash="hash40", active=True, source="builtin"):
    return pd.DataFrame(
        [
            {
                TRAIN_CONFIG_ID_COL: config_id,
                TRAIN_CONFIG_PARAMS_COL: {"imgsz": imgsz, "epochs": 5},
                "train_config__source": source,
                "train_config__display_name": "Standard",
                "train_config__is_active": active,
                "train_config__config_hash": config_hash,
            }
        ]
    )


def test_builtin_config_has_registry_metadata():
    df = train_configs_to_dataframe(
        [_DummyConfig()],
        id_column=TRAIN_CONFIG_ID_COL,
        params_column=TRAIN_CONFIG_PARAMS_COL,
        summary_builder=_summary,
        config_type="dummy_detection",
    )
    row = df.iloc[0]
    assert row["train_config__source"] == "builtin"
    assert row["train_config__display_name"] == "dummy-640"
    assert row["train_config__description"] is None
    assert row["train_config__config_type"] == "dummy_detection"
    assert len(row["train_config__config_hash"]) == 40
    assert bool(row["train_config__is_active"]) is True
    assert row["train_config__revision"] == 1
    # timestamps are managed by ScopedBatchGenerate, not generated here.
    assert "train_config__created_at" not in df.columns
    assert "train_config__updated_at" not in df.columns


def test_preset_display_name_and_description():
    df = train_configs_to_dataframe(
        [TrainingConfigPreset(name="Large image test", config=_DummyConfig(), description="hi-res")],
        id_column=TRAIN_CONFIG_ID_COL,
        params_column=TRAIN_CONFIG_PARAMS_COL,
        summary_builder=_summary,
        config_type="dummy_detection",
    )
    row = df.iloc[0]
    assert row["train_config__display_name"] == "Large image test"
    assert row["train_config__description"] == "hi-res"


def test_custom_id_is_not_content_based():
    a = build_custom_train_config_id()
    b = build_custom_train_config_id()
    assert a != b
    assert a.startswith("custom_")


def test_manual_request_id_is_unique():
    a = build_manual_training_request_id()
    b = build_manual_training_request_id()
    assert a != b
    assert a.startswith("request_")


def test_auto_request_id_is_deterministic():
    id1 = build_auto_training_request_id(
        frozen_dataset_id="fd1", config_hash="h", config_type="yolov8_detection"
    )
    id2 = build_auto_training_request_id(
        frozen_dataset_id="fd1", config_hash="h", config_type="yolov8_detection"
    )
    id3 = build_auto_training_request_id(
        frozen_dataset_id="fd2", config_hash="h", config_type="yolov8_detection"
    )
    assert id1 == id2
    assert id1 != id3
    assert id1.startswith("auto_")


def test_build_auto_training_request_is_deterministic_and_snapshots():
    df_fd = pd.DataFrame([{FROZEN_DATASET_ID_COL: "fd1"}])
    df_tc = _builtin_config_row()
    kwargs = dict(
        frozen_dataset_id_col=FROZEN_DATASET_ID_COL,
        train_config_id_col=TRAIN_CONFIG_ID_COL,
        train_config_params_col=TRAIN_CONFIG_PARAMS_COL,
        train_config_type="yolov8_detection",
        max_within_time="1w",
    )
    req1 = build_auto_training_request(df_fd, df_tc, **kwargs)
    req2 = build_auto_training_request(df_fd, df_tc, **kwargs)
    assert len(req1) == 1
    assert req1.iloc[0]["training_request_id"] == req2.iloc[0]["training_request_id"]
    assert req1.iloc[0]["training_request__kind"] == "auto"
    assert bool(req1.iloc[0]["training_request__enabled"]) is True
    assert bool(req1.iloc[0]["training_request__force"]) is False
    assert req1.iloc[0]["training_request__max_within_time"] == "1w"
    assert req1.iloc[0]["training_request__config_params_snapshot"] == {"imgsz": 640, "epochs": 5}


def test_creating_config_does_not_prepare_data():
    # custom config -> no auto request -> nothing to resize
    df_fd = pd.DataFrame([{FROZEN_DATASET_ID_COL: "fd1"}])
    df_custom = _builtin_config_row(config_id="custom_x", source="custom")
    req = build_auto_training_request(
        df_fd,
        df_custom,
        frozen_dataset_id_col=FROZEN_DATASET_ID_COL,
        train_config_id_col=TRAIN_CONFIG_ID_COL,
        train_config_params_col=TRAIN_CONFIG_PARAMS_COL,
        train_config_type="yolov8_detection",
        max_within_time="1w",
    )
    assert req.empty

    sizes = get_size_for_resize_from_training_request(
        req,
        request_id_col="training_request_id",
        frozen_dataset_id_col=FROZEN_DATASET_ID_COL,
        params_snapshot_col="training_request__config_params_snapshot",
        request_enabled_col="training_request__enabled",
        resize_images=True,
    )
    assert sizes.empty


def test_inactive_builtin_does_not_produce_request():
    df_fd = pd.DataFrame([{FROZEN_DATASET_ID_COL: "fd1"}])
    df_inactive = _builtin_config_row(active=False)
    req = build_auto_training_request(
        df_fd,
        df_inactive,
        frozen_dataset_id_col=FROZEN_DATASET_ID_COL,
        train_config_id_col=TRAIN_CONFIG_ID_COL,
        train_config_params_col=TRAIN_CONFIG_PARAMS_COL,
        train_config_type="yolov8_detection",
        max_within_time="1w",
    )
    assert req.empty


def _request_df(enabled=True, imgsz=640, request_id="auto_1", frozen_dataset_id="fd1"):
    return pd.DataFrame(
        [
            {
                "training_request_id": request_id,
                FROZEN_DATASET_ID_COL: frozen_dataset_id,
                "training_request__config_params_snapshot": {"imgsz": imgsz},
                "training_request__enabled": enabled,
            }
        ]
    )


def test_inactive_request_does_not_prepare_data():
    sizes = get_size_for_resize_from_training_request(
        _request_df(enabled=False),
        request_id_col="training_request_id",
        frozen_dataset_id_col=FROZEN_DATASET_ID_COL,
        params_snapshot_col="training_request__config_params_snapshot",
        request_enabled_col="training_request__enabled",
        resize_images=True,
    )
    assert sizes.empty


def test_enabled_request_prepares_size_scoped_to_dataset():
    sizes = get_size_for_resize_from_training_request(
        _request_df(enabled=True, imgsz=1280),
        request_id_col="training_request_id",
        frozen_dataset_id_col=FROZEN_DATASET_ID_COL,
        params_snapshot_col="training_request__config_params_snapshot",
        request_enabled_col="training_request__enabled",
        resize_images=True,
    )
    assert len(sizes) == 1
    row = sizes.iloc[0]
    assert row["width"] == 1280
    assert row["height"] == 1280
    assert row[FROZEN_DATASET_ID_COL] == "fd1"
    assert list(sizes.columns) == ["training_request_id", FROZEN_DATASET_ID_COL, "width", "height"]


def test_resize_sizes_are_scoped_by_dataset():
    df = pd.DataFrame(
        [
            {FROZEN_DATASET_ID_COL: "fd1", "width": 640, "height": 640},
            {FROZEN_DATASET_ID_COL: "fd2", "width": 640, "height": 640},
            {FROZEN_DATASET_ID_COL: "fd1", "width": 640, "height": 640},
        ]
    )
    out = dedupe_size_for_resize(df, frozen_dataset_id_col=FROZEN_DATASET_ID_COL)
    # same (width, height) for two datasets must NOT collapse into a single row.
    assert len(out) == 2
    assert set(out[FROZEN_DATASET_ID_COL]) == {"fd1", "fd2"}

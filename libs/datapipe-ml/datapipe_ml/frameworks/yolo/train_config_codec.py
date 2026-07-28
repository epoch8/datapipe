from __future__ import annotations

from dataclasses import MISSING, asdict, fields
from typing import Any, Mapping, Optional, Type, Union, get_args, get_origin

from datapipe_ml.frameworks.yolo.yolov8.runner import (
    YoloV8DetectionTrainingConfig,
    YoloV8KeypointsTrainingConfig,
    YoloV8SegmentationTrainingConfig,
    YoloV8_TrainingConfig,
)
from datapipe_ml.training.config_codec import (
    TrainConfigValidationError,
    register_train_config_codec,
)

# Pipeline / orchestrator owns these; exposing them in the experiment form is
# misleading and can break training paths.
_EXCLUDED_FIELDS = frozenset(
    {
        "tmp_folder",
        "initial_weights_path",
        "persisted_project_dir",
        "data",
        "project",
        "name",
        "exist_ok",
        "resume",
    }
)

# Extra JSON-Schema metadata (titles, groups, enums, bounds) layered on top of
# types/defaults inferred from the task config dataclass.
_FIELD_UI: dict[str, dict[str, Any]] = {
    "model": {"title": "Model checkpoint", "ui_group": "model", "ui_order": 10},
    "imgsz": {"title": "Image size", "ui_group": "model", "ui_order": 20, "minimum": 32},
    "batch": {"title": "Batch size", "ui_group": "model", "ui_order": 30, "minimum": 1},
    "epochs": {"title": "Epochs", "ui_group": "model", "ui_order": 40, "minimum": 1},
    "time": {"title": "Max hours", "ui_group": "model", "ui_order": 50},
    "pretrained": {"title": "Pretrained", "ui_group": "model", "ui_order": 60},
    "freeze": {"title": "Freeze layers", "ui_group": "model", "ui_order": 70},
    "device": {"title": "Device", "ui_group": "model", "ui_order": 80},
    "workers": {"title": "Workers", "ui_group": "model", "ui_order": 90, "minimum": 0},
    "optimizer": {
        "title": "Optimizer",
        "ui_group": "training",
        "ui_order": 10,
        "enum": ["auto", "SGD", "Adam", "AdamW", "NAdam", "RAdam", "RMSProp"],
    },
    "lr0": {"title": "Learning rate", "ui_group": "training", "ui_order": 20, "exclusiveMinimum": 0},
    "lrf": {"title": "Final LR factor", "ui_group": "training", "ui_order": 30},
    "momentum": {"title": "Momentum", "ui_group": "training", "ui_order": 40},
    "weight_decay": {"title": "Weight decay", "ui_group": "training", "ui_order": 50, "minimum": 0},
    "warmup_epochs": {"title": "Warmup epochs", "ui_group": "training", "ui_order": 60},
    "warmup_momentum": {"title": "Warmup momentum", "ui_group": "training", "ui_order": 70},
    "warmup_bias_lr": {"title": "Warmup bias LR", "ui_group": "training", "ui_order": 80},
    "patience": {"title": "Early-stop patience", "ui_group": "training", "ui_order": 90, "minimum": 0},
    "cos_lr": {"title": "Cosine LR", "ui_group": "training", "ui_order": 100},
    "close_mosaic": {"title": "Close mosaic (epochs)", "ui_group": "training", "ui_order": 110},
    "amp": {"title": "AMP", "ui_group": "training", "ui_order": 120},
    "fraction": {"title": "Dataset fraction", "ui_group": "training", "ui_order": 130},
    "seed": {"title": "Seed", "ui_group": "training", "ui_order": 140},
    "deterministic": {"title": "Deterministic", "ui_group": "training", "ui_order": 150},
    "single_cls": {"title": "Single class", "ui_group": "training", "ui_order": 160},
    "rect": {"title": "Rectangular training", "ui_group": "training", "ui_order": 170},
    "save": {"title": "Save checkpoints", "ui_group": "training", "ui_order": 180},
    "save_period": {"title": "Save period", "ui_group": "training", "ui_order": 190},
    "cache": {"title": "Cache images", "ui_group": "training", "ui_order": 200},
    "verbose": {"title": "Verbose", "ui_group": "training", "ui_order": 210},
    "profile": {"title": "Profile", "ui_group": "training", "ui_order": 220},
    "val": {"title": "Validate", "ui_group": "training", "ui_order": 230},
    "plots": {"title": "Plots", "ui_group": "training", "ui_order": 240},
    "nbs": {"title": "Nominal batch size", "ui_group": "training", "ui_order": 250},
    "box": {"title": "Box loss weight", "ui_group": "loss", "ui_order": 10},
    "cls": {"title": "Cls loss weight", "ui_group": "loss", "ui_order": 20},
    "dfl": {"title": "DFL loss weight", "ui_group": "loss", "ui_order": 30},
    "pose": {"title": "Pose loss weight", "ui_group": "loss", "ui_order": 40},
    "kobj": {"title": "Keypoint obj loss", "ui_group": "loss", "ui_order": 50},
    "label_smoothing": {"title": "Label smoothing", "ui_group": "loss", "ui_order": 60},
    "dropout": {"title": "Dropout", "ui_group": "loss", "ui_order": 70},
    "overlap_mask": {"title": "Overlap masks", "ui_group": "loss", "ui_order": 80},
    "mask_ratio": {"title": "Mask ratio", "ui_group": "loss", "ui_order": 90},
    "multi_scale": {"title": "Multi-scale", "ui_group": "augmentation", "ui_order": 10},
    "degrees": {"title": "Rotation degrees", "ui_group": "augmentation", "ui_order": 20},
    "translate": {"title": "Translate", "ui_group": "augmentation", "ui_order": 30},
    "scale": {"title": "Scale", "ui_group": "augmentation", "ui_order": 40},
    "shear": {"title": "Shear", "ui_group": "augmentation", "ui_order": 50},
    "perspective": {"title": "Perspective", "ui_group": "augmentation", "ui_order": 60},
    "fliplr": {"title": "Flip LR", "ui_group": "augmentation", "ui_order": 70},
    "flipud": {"title": "Flip UD", "ui_group": "augmentation", "ui_order": 80},
    "mosaic": {"title": "Mosaic", "ui_group": "augmentation", "ui_order": 90},
    "mixup": {"title": "MixUp", "ui_group": "augmentation", "ui_order": 100},
    "copy_paste": {"title": "Copy-paste", "ui_group": "augmentation", "ui_order": 110},
    "copy_paste_mode": {
        "title": "Copy-paste mode",
        "ui_group": "augmentation",
        "ui_order": 120,
        "enum": ["flip", "mixup"],
    },
    "hsv_h": {"title": "HSV hue", "ui_group": "augmentation", "ui_order": 130},
    "hsv_s": {"title": "HSV saturation", "ui_group": "augmentation", "ui_order": 140},
    "hsv_v": {"title": "HSV value", "ui_group": "augmentation", "ui_order": 150},
    "bgr": {"title": "BGR swap", "ui_group": "augmentation", "ui_order": 160},
    "cutmix": {"title": "CutMix", "ui_group": "augmentation", "ui_order": 170},
    "erasing": {"title": "Random erasing", "ui_group": "augmentation", "ui_order": 180},
    "auto_augment": {"title": "Auto augment", "ui_group": "augmentation", "ui_order": 190},
}

_TYPE_MAP = {
    str: "string",
    int: "integer",
    float: "number",
    bool: "boolean",
}


def _unwrap_optional(annotation: Any) -> tuple[Any, bool]:
    origin = get_origin(annotation)
    if origin is Union:
        args = [a for a in get_args(annotation) if a is not type(None)]
        if len(args) == 1:
            return args[0], True
    return annotation, False


def _json_type(annotation: Any) -> Optional[str]:
    annotation, _ = _unwrap_optional(annotation)
    return _TYPE_MAP.get(annotation)


def _field_default(field) -> Any:
    if field.default is not MISSING:
        return field.default
    if field.default_factory is not MISSING:  # type: ignore[unreachable]
        return field.default_factory()
    return None


def build_yolov8_training_config_schema(
    config_cls: Type[YoloV8_TrainingConfig],
    *,
    excluded: frozenset[str] = _EXCLUDED_FIELDS,
    ui_meta: Mapping[str, Mapping[str, Any]] = _FIELD_UI,
) -> dict[str, Any]:
    """Build a JSON Schema from a task config dataclass (defaults included)."""
    properties: dict[str, Any] = {}
    for field in fields(config_cls):
        if field.name in excluded:
            continue
        prop: dict[str, Any] = {}
        json_type = _json_type(field.type)
        if json_type is not None:
            prop["type"] = json_type
        _, is_optional = _unwrap_optional(field.type)
        if is_optional:
            prop["nullable"] = True
        prop["default"] = _field_default(field)
        for key, value in ui_meta.get(field.name, {}).items():
            prop[key] = value
        if "ui_group" not in prop:
            prop["ui_group"] = "general"
        if "title" not in prop:
            prop["title"] = field.name.replace("_", " ").capitalize()
        properties[field.name] = prop
    return {"type": "object", "properties": properties}


class YoloV8TrainConfigCodec:
    """Codec backed by a task-specific ``YoloV8_*TrainingConfig`` dataclass."""

    config_type: str
    config_cls: Type[YoloV8_TrainingConfig]

    def _allowed_fields(self) -> set[str]:
        return {f.name for f in fields(self.config_cls)}

    def validate(self, params: Mapping[str, Any]) -> dict[str, Any]:
        allowed = self._allowed_fields()
        errors: list[dict[str, Any]] = []
        for key in params:
            if key not in allowed:
                errors.append({"field": key, "message": f"Unknown field: {key!r}"})
        if errors:
            raise TrainConfigValidationError("Invalid train config parameters", errors=errors)
        try:
            config = self.config_cls(**dict(params))
        except TypeError as exc:
            raise TrainConfigValidationError(
                str(exc), errors=[{"field": None, "message": str(exc)}]
            ) from exc
        return asdict(config)

    def normalize(self, params: Mapping[str, Any]) -> dict[str, Any]:
        return self.validate(params)

    def json_schema(self) -> dict[str, Any]:
        return build_yolov8_training_config_schema(self.config_cls)

    def default_params(self) -> dict[str, Any]:
        """Defaults for UI prefill (editable fields only), from the dataclass."""
        schema = self.json_schema()
        properties = schema.get("properties") or {}
        return {key: prop.get("default") for key, prop in properties.items()}

    def summarize(self, params: Mapping[str, Any]) -> dict[str, Any]:
        model = params.get("model")
        image_size = params.get("imgsz")
        batch_size = params.get("batch")
        epochs = params.get("epochs")
        optimizer = params.get("optimizer")
        display = f"{model} · {image_size}px · batch {batch_size} · {epochs} epochs"
        return {
            "model": model,
            "image_size": image_size,
            "batch_size": batch_size,
            "epochs": epochs,
            "optimizer": optimizer,
            "display": display,
        }


class YoloV8DetectionTrainConfigCodec(YoloV8TrainConfigCodec):
    config_type = "yolov8_detection"
    config_cls = YoloV8DetectionTrainingConfig


class YoloV8SegmentationTrainConfigCodec(YoloV8TrainConfigCodec):
    config_type = "yolov8_segmentation"
    config_cls = YoloV8SegmentationTrainingConfig


class YoloV8KeypointsTrainConfigCodec(YoloV8TrainConfigCodec):
    config_type = "yolov8_keypoints"
    config_cls = YoloV8KeypointsTrainingConfig


register_train_config_codec(YoloV8DetectionTrainConfigCodec())
register_train_config_codec(YoloV8SegmentationTrainConfigCodec())
register_train_config_codec(YoloV8KeypointsTrainConfigCodec())

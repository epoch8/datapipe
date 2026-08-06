from __future__ import annotations

from dataclasses import MISSING, asdict, fields
from typing import Any, Mapping, Optional, Tuple, Union, get_args, get_origin

from datapipe_ml.frameworks.tensorflow.classification_runner import TF_ClassificationTrainingConfig
from datapipe_ml.training.config_codec import (
    TrainConfigValidationError,
    register_train_config_codec,
)

_EXCLUDED_FIELDS: frozenset[str] = frozenset()

_FIELD_UI: dict[str, dict[str, Any]] = {
    "arch": {
        "title": "Architecture",
        "ui_group": "model",
        "ui_order": 10,
        "enum": [
            "mobilenet",
            "mobilenetv2",
            "mobilenetv3",
            "tiny_cnn",
            "coatnet",
            "beit",
            "caformer",
            "gcvit",
        ],
    },
    "image_size": {"title": "Image size (W×H)", "ui_group": "model", "ui_order": 20},
    "batch_size": {"title": "Batch size", "ui_group": "model", "ui_order": 30, "minimum": 1},
    "epochs": {"title": "Epochs", "ui_group": "model", "ui_order": 40, "minimum": 1},
    "seed": {"title": "Seed", "ui_group": "model", "ui_order": 50},
    "model_spec": {"title": "Custom model spec", "ui_group": "model", "ui_order": 60},
    "init_lr": {"title": "Initial LR", "ui_group": "training", "ui_order": 10, "exclusiveMinimum": 0},
    "reduce_lr_patience": {
        "title": "Reduce LR patience",
        "ui_group": "training",
        "ui_order": 20,
        "minimum": 0,
    },
    "reduce_lr_factor": {"title": "Reduce LR factor", "ui_group": "training", "ui_order": 30},
    "early_stopping_patience": {
        "title": "Early-stop patience",
        "ui_group": "training",
        "ui_order": 40,
        "minimum": 0,
    },
    "label_smoothing": {"title": "Label smoothing", "ui_group": "training", "ui_order": 50, "minimum": 0},
    "class_weight": {"title": "Class weights", "ui_group": "training", "ui_order": 60},
    "augmentations": {"title": "Augmentations", "ui_group": "augmentation", "ui_order": 10},
    "augment_func_file": {"title": "Custom augment script", "ui_group": "augmentation", "ui_order": 20},
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


def _json_type(annotation: Any) -> Optional[str | dict[str, Any]]:
    annotation, _ = _unwrap_optional(annotation)
    if annotation in _TYPE_MAP:
        return _TYPE_MAP[annotation]
    origin = get_origin(annotation)
    if origin in (tuple, Tuple, list):
        args = get_args(annotation)
        item_type = _TYPE_MAP.get(args[0]) if args else None
        prop: dict[str, Any] = {"type": "array"}
        if item_type is not None:
            prop["items"] = {"type": item_type}
        if origin in (tuple, Tuple) and args:
            prop["minItems"] = len(args)
            prop["maxItems"] = len(args)
        return prop
    return None


def _field_default(field) -> Any:
    if field.default is not MISSING:
        return field.default
    if field.default_factory is not MISSING:  # type: ignore[unreachable]
        return field.default_factory()
    return None


def build_tf_classification_training_config_schema() -> dict[str, Any]:
    properties: dict[str, Any] = {}
    for field in fields(TF_ClassificationTrainingConfig):
        if field.name in _EXCLUDED_FIELDS:
            continue
        prop: dict[str, Any] = {}
        json_type = _json_type(field.type)
        if isinstance(json_type, str):
            prop["type"] = json_type
        elif isinstance(json_type, dict):
            prop.update(json_type)
        _, is_optional = _unwrap_optional(field.type)
        if is_optional:
            prop["nullable"] = True
        prop["default"] = _field_default(field)
        for key, value in _FIELD_UI.get(field.name, {}).items():
            prop[key] = value
        if "ui_group" not in prop:
            prop["ui_group"] = "general"
        if "title" not in prop:
            prop["title"] = field.name.replace("_", " ").capitalize()
        properties[field.name] = prop
    return {"type": "object", "properties": properties}


class TFClassificationTrainConfigCodec:
    config_type = "tf_classification"
    config_cls = TF_ClassificationTrainingConfig

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
        except (TypeError, ValueError) as exc:
            raise TrainConfigValidationError(
                str(exc), errors=[{"field": None, "message": str(exc)}]
            ) from exc
        return asdict(config)

    def normalize(self, params: Mapping[str, Any]) -> dict[str, Any]:
        return self.validate(params)

    def json_schema(self) -> dict[str, Any]:
        return build_tf_classification_training_config_schema()

    def default_params(self) -> dict[str, Any]:
        schema = self.json_schema()
        properties = schema.get("properties") or {}
        return {key: prop.get("default") for key, prop in properties.items()}

    def summarize(self, params: Mapping[str, Any]) -> dict[str, Any]:
        arch = params.get("arch")
        if arch is None and isinstance(params.get("model_spec"), Mapping):
            factory = params["model_spec"].get("factory")
            arch = str(factory).rsplit(":", maxsplit=1)[-1] if factory else "custom"
        image_size = params.get("image_size")
        if isinstance(image_size, (list, tuple)) and len(image_size) >= 2:
            size_label = f"{image_size[0]}x{image_size[1]}"
        elif image_size is None:
            size_label = "?"
        else:
            size_label = str(image_size)
        batch_size = params.get("batch_size")
        epochs = params.get("epochs")
        display = f"{arch} · {size_label} · batch {batch_size} · {epochs} epochs"
        return {
            "model": arch,
            "image_size": image_size,
            "batch_size": batch_size,
            "epochs": epochs,
            "display": display,
        }


register_train_config_codec(TFClassificationTrainConfigCodec())

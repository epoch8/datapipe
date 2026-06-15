from __future__ import annotations

import io
import re
from pathlib import Path
from typing import Any, Mapping, Protocol, runtime_checkable

import fsspec

_ROOT_YAML_PATH_RE = re.compile(r"""^\s*[\w.]+\.ROOT\s*/\s*['"]([^'"]+)['"]\s*$""")


def _path_stem(value: str) -> str:
    from datapipe_ml.training.train_config_id import short_path_label

    return short_path_label(value)


def _is_checkpoint_path(value: str) -> bool:
    return "/" in value or "://" in value


@runtime_checkable
class _CheckpointModelWithYamlFile(Protocol):
    yaml_file: str


@runtime_checkable
class _CheckpointModelWithYamlMapping(Protocol):
    yaml: Mapping[str, Any]


def _architecture_from_checkpoint(ckpt: Mapping[str, Any]) -> str | None:
    train_args = ckpt.get("train_args")
    if isinstance(train_args, Mapping) and train_args.get("model"):
        return Path(str(train_args["model"])).stem

    model = ckpt.get("ema") or ckpt.get("model")
    yaml_file: str | None = None
    if isinstance(model, _CheckpointModelWithYamlFile):
        yaml_file = model.yaml_file
    elif isinstance(model, _CheckpointModelWithYamlMapping):
        yaml_file = model.yaml.get("yaml_file")
    if yaml_file:
        return Path(str(yaml_file)).stem
    return None


def _read_checkpoint_architecture_label(path: str) -> str | None:
    import torch

    from datapipe_ml.utils.fsspec_storage import fsspec_storage_options

    with fsspec.open(path, "rb", **fsspec_storage_options(path)) as src:
        ckpt = torch.load(io.BytesIO(src.read()), map_location="cpu", weights_only=False)
    if isinstance(ckpt, Mapping):
        return _architecture_from_checkpoint(ckpt)
    return None


def resolve_yolo_model_label(value: str) -> str:
    normalized = str(value).strip()
    if not normalized:
        raise ValueError("Cannot resolve YOLO model label from an empty path.")

    if normalized.endswith((".pt", ".pth")):
        if _is_checkpoint_path(normalized):
            try:
                label = _read_checkpoint_architecture_label(normalized)
            except Exception as exc:
                raise ValueError(
                    f"Cannot resolve YOLO model architecture from checkpoint {normalized!r}: {exc}"
                ) from exc
            if not label:
                raise ValueError(
                    f"Cannot resolve YOLO model architecture from checkpoint {normalized!r}: "
                    "checkpoint metadata does not contain train_args.model or model yaml."
                )
            return label.replace(" ", "_")
        return _path_stem(normalized)

    if ".yaml" in normalized or ".yml" in normalized:
        root_path_match = _ROOT_YAML_PATH_RE.match(normalized)
        if root_path_match is not None:
            normalized = root_path_match.group(1)
        return _path_stem(normalized)

    if _is_checkpoint_path(normalized):
        raise ValueError(f"Cannot resolve YOLO model label from path {normalized!r}.")

    return _path_stem(normalized)


def build_yolo_train_config_summary(
    params: Mapping[str, Any],
    *,
    model_key: str = "model",
    batch_key: str | None = None,
) -> str:
    if batch_key is None:
        batch_key = "batch_size" if "batch_size" in params else "batch"
    model_label = resolve_yolo_model_label_from_params(params, model_key=model_key)
    return f"{model_label}-{params['imgsz']}-batch{params[batch_key]}-epochs{params['epochs']}"


def resolve_yolo_model_label_from_params(params: Mapping[str, Any], *, model_key: str = "model") -> str:
    for key in (model_key, "cfg" if model_key == "weights" else None, "initial_weights_path"):
        if key is None:
            continue
        value = str(params.get(key) or "").strip()
        if value:
            return resolve_yolo_model_label(value)
    raise ValueError(
        "Cannot resolve YOLO model label: training config has no model, cfg, or initial_weights_path."
    )

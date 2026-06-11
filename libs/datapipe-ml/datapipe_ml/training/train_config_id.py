from __future__ import annotations

import hashlib
import json
from dataclasses import fields, is_dataclass
from typing import Any, Callable, Iterable, Mapping, Optional

import pandas as pd
from pathy import Pathy


def short_path_label(value: str) -> str:
    """Short label for a preset name or checkpoint path in human-readable summaries."""
    name = Pathy.fluid(value.rstrip("/")).name
    for suffix in (".pt", ".pth", ".yaml", ".yml"):
        if name.endswith(suffix):
            name = name[: -len(suffix)]
            break
    return name.replace(" ", "_")


def hash_train_config_params(params: Mapping[str, Any], *, length: int = 10) -> str:
    canonical = json.dumps(params, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha1(canonical.encode("utf-8")).hexdigest()[:length]


def build_train_config_id(
    params: Mapping[str, Any],
    *,
    summary: str,
    hash_length: int = 10,
) -> str:
    digest = hash_train_config_params(params, length=hash_length)
    return f"{summary}-cfg{digest}"


def _dataclass_params(instance: object) -> dict[str, Any]:
    if isinstance(instance, type) or not is_dataclass(instance):
        raise TypeError(f"Expected dataclass config instance, got {type(instance)!r}")
    return {field.name: getattr(instance, field.name) for field in fields(instance)}


def build_yolo_train_config_summary(
    params: Mapping[str, Any],
    *,
    model_key: str = "model",
    batch_key: Optional[str] = None,
) -> str:
    from datapipe_ml.frameworks.yolo.checkpoint_label import resolve_yolo_model_label_from_params

    if batch_key is None:
        batch_key = "batch_size" if "batch_size" in params else "batch"
    model_label = resolve_yolo_model_label_from_params(params, model_key=model_key)
    return f"{model_label}-{params['imgsz']}-batch{params[batch_key]}-epochs{params['epochs']}"


def train_configs_to_dataframe(
    configs: Iterable[Any],
    *,
    id_column: str,
    params_column: str,
    summary_builder: Callable[[dict[str, Any]], str],
    extra_columns: Optional[Callable[[Any, dict[str, Any], str], dict[str, Any]]] = None,
) -> pd.DataFrame:
    rows = []
    for config in configs:
        params = _dataclass_params(config)
        summary = summary_builder(params)
        config_id = build_train_config_id(params, summary=summary)
        row = {id_column: config_id, params_column: params}
        if extra_columns is not None:
            row.update(extra_columns(config, params, config_id))
        rows.append(row)
    return pd.DataFrame(rows)

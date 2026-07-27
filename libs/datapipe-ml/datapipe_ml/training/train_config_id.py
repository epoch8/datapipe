from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, is_dataclass
from typing import Any, Callable, Iterable, Mapping, Optional, Union, cast
from uuid import uuid4

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


def build_custom_train_config_id() -> str:
    """Custom experiments use an opaque, non-content-based id (spec §6)."""
    return f"custom_{uuid4().hex}"


def build_manual_training_request_id() -> str:
    """Manual training requests are unique per user action (spec §6)."""
    return f"request_{uuid4().hex}"


def build_auto_training_request_id(
    *,
    frozen_dataset_id: str,
    config_hash: str,
    config_type: str,
) -> str:
    """Deterministic id for the automatic (frozen_dataset + built-in config) policy."""
    raw = f"{frozen_dataset_id}:{config_hash}:{config_type}"
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:20]
    return f"auto_{digest}"


@dataclass(frozen=True)
class TrainingConfigPreset:
    """Wrapper for a built-in config carrying a human-readable name/description."""

    name: str
    config: Any
    description: Optional[str] = None


def _dataclass_params(instance: object) -> dict[str, Any]:
    if isinstance(instance, type) or not is_dataclass(instance):
        raise TypeError(f"Expected dataclass config instance, got {type(instance)!r}")
    return asdict(cast(Any, instance))


def train_configs_to_dataframe(
    configs: Iterable[Union[Any, TrainingConfigPreset]],
    *,
    id_column: str,
    params_column: str,
    summary_builder: Callable[[dict[str, Any]], str],
    config_type: Optional[str] = None,
    extra_columns: Optional[Callable[[Any, dict[str, Any], str], dict[str, Any]]] = None,
) -> pd.DataFrame:
    """Build rows for the pipeline-owned ``default_train_config`` table.

    Accepts plain config dataclasses (backward compatible) and
    :class:`TrainingConfigPreset` wrappers. When ``config_type`` is provided the
    default-config metadata columns (display_name/description/config_type/
    config_hash/revision) are attached.
    """
    rows = []
    for item in configs:
        if isinstance(item, TrainingConfigPreset):
            config = item.config
            display_name: Optional[str] = item.name
            description: Optional[str] = item.description
        else:
            config = item
            display_name = None
            description = None

        params = _dataclass_params(config)
        summary = summary_builder(params)
        config_id = build_train_config_id(params, summary=summary)
        if display_name is None:
            display_name = summary

        row: dict[str, Any] = {id_column: config_id, params_column: params}
        if config_type is not None:
            row.update(
                {
                    "train_config__display_name": display_name,
                    "train_config__description": description,
                    "train_config__config_type": config_type,
                    "train_config__config_hash": hash_train_config_params(params, length=40),
                    "train_config__revision": 1,
                }
            )
        if extra_columns is not None:
            row.update(extra_columns(config, params, config_id))
        rows.append(row)
    return pd.DataFrame(rows)

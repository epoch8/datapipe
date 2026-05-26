from __future__ import annotations

import base64
import pickle
from dataclasses import replace
from typing import Any

from datapipe_ml.training.specs import TrainingLaunchRequest


def rewrite_value(value: Any, rewrites: tuple[tuple[str, str], ...]) -> Any:
    if isinstance(value, str):
        out = value
        for src, dst in sorted(rewrites, key=lambda item: len(item[0]), reverse=True):
            out = out.replace(src, dst)
        return out
    if isinstance(value, tuple):
        return tuple(rewrite_value(item, rewrites) for item in value)
    if isinstance(value, list):
        return [rewrite_value(item, rewrites) for item in value]
    if isinstance(value, dict):
        return {rewrite_value(key, rewrites): rewrite_value(item, rewrites) for key, item in value.items()}
    if hasattr(value, "__dataclass_fields__"):
        updates = {
            field_name: rewrite_value(getattr(value, field_name), rewrites) for field_name in value.__dataclass_fields__
        }
        return replace(value, **updates)
    return value


def to_remote_request(request: TrainingLaunchRequest) -> TrainingLaunchRequest:
    return replace(request, args=rewrite_value(request.args, request.path_rewrites))


def dumps_to_text(value: Any) -> str:
    return base64.b64encode(pickle.dumps(value)).decode("ascii")


def loads_from_text(value: str) -> Any:
    return pickle.loads(base64.b64decode(value.encode("ascii")))

from __future__ import annotations

import json
from typing import Any, Literal, Optional

from datapipe.types import Labels

RunScope = Literal["full_pipeline", "stage_run", "label_run"]


def labels_to_json(labels: Labels) -> Optional[str]:
    if not labels:
        return None
    return json.dumps(list(labels))


def labels_from_json(raw: Optional[str]) -> Labels:
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return []
    if not isinstance(parsed, list):
        return []
    result: Labels = []
    for item in parsed:
        if isinstance(item, (list, tuple)) and len(item) == 2:
            result.append((str(item[0]), str(item[1])))
    return result


def trigger_from_labels(labels: Labels) -> str:
    if not labels:
        return "api:pipeline"
    stage = next((value for key, value in labels if key == "stage"), None)
    if stage:
        return f"api:stage:{stage}"
    return "api"


def derive_run_scope(
    *,
    labels: Labels,
    trigger: Optional[str] = None,
) -> dict[str, Any]:
    if labels:
        stage = next((value for key, value in labels if key == "stage"), None)
        if stage:
            return {
                "run_scope": "stage_run",
                "target_labels": [[key, value] for key, value in labels],
                "target_label_display": stage,
            }
        return {
            "run_scope": "label_run",
            "target_labels": [[key, value] for key, value in labels],
            "target_label_display": labels[0][1] if labels else "unknown",
        }

    if trigger == "api:pipeline" or trigger is None:
        return {
            "run_scope": "full_pipeline",
            "target_labels": [],
            "target_label_display": "all labels",
        }

    if trigger and trigger.startswith("api:stage:"):
        stage = trigger[len("api:stage:") :]
        return {
            "run_scope": "stage_run",
            "target_labels": [["stage", stage]],
            "target_label_display": stage,
        }

    return {
        "run_scope": "full_pipeline",
        "target_labels": [],
        "target_label_display": "all labels",
    }

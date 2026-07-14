from __future__ import annotations

from typing import Optional

from datapipe.types import Labels

API_STAGE_PREFIX = "api:stage:"
CLI_STAGE_PREFIX = "cli:stage:"
API_PIPELINE_TRIGGER = "api:pipeline"
CLI_PIPELINE_TRIGGER = "cli:pipeline"


def cli_trigger_from_labels(labels: Labels) -> str:
    if not labels:
        return CLI_PIPELINE_TRIGGER
    stage = next((value for key, value in labels if key == "stage"), None)
    if stage:
        return f"{CLI_STAGE_PREFIX}{stage}"
    return "cli"


def stage_from_trigger(trigger: Optional[str]) -> Optional[str]:
    if not trigger:
        return None
    for prefix in (API_STAGE_PREFIX, CLI_STAGE_PREFIX):
        if trigger.startswith(prefix):
            return trigger[len(prefix) :]
    return None


def stage_trigger_values(stage: str) -> list[str]:
    return [f"{API_STAGE_PREFIX}{stage}", f"{CLI_STAGE_PREFIX}{stage}"]


def pipeline_trigger_values() -> list[str]:
    return [API_PIPELINE_TRIGGER, CLI_PIPELINE_TRIGGER]

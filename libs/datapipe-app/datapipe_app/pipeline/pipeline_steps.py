from __future__ import annotations

from typing import Protocol, runtime_checkable

from datapipe.compute import PipelineStep
from datapipe.types import Labels


@runtime_checkable
class LabeledPipelineStep(Protocol):
    labels: Labels | None


def pipeline_step_labels(step: PipelineStep) -> Labels:
    if isinstance(step, LabeledPipelineStep):
        return step.labels or []
    return []

"""Launch freeze / snapshot pipeline steps from an ops frozen-dataset spec."""

from __future__ import annotations

from typing import Any, Dict, Optional

from pydantic import BaseModel

from datapipe_app_ml_ops.ops.ops_specs import DatapipeOpsSpec, OpsFrozenDatasetSpec
from datapipe_app_ml_ops.ops.spec_registry import OpsSpecRegistry
from datapipe_app_ml_ops.ops.training_experiments_models import TrainingExperimentError
from datapipe_app_ml_ops.ops.training_experiments_service import RunStepsCallable

RunStepsResult = Dict[str, Any]


class FreezeLaunchResponse(BaseModel):
    started: bool
    run_id: Optional[str] = None


class FrozenDatasetsLaunchService:
    def __init__(
        self,
        registry: OpsSpecRegistry,
        *,
        run_steps: Optional[RunStepsCallable] = None,
    ) -> None:
        self.registry = registry
        self.run_steps = run_steps

    def _spec(self, spec_id: str) -> DatapipeOpsSpec:
        try:
            return self.registry.get(spec_id)
        except KeyError as exc:
            raise TrainingExperimentError(
                "not_found",
                f'Ops spec "{spec_id}" was not found.',
                status_code=404,
            ) from exc

    def _frozen_spec(self, spec_id: str) -> OpsFrozenDatasetSpec:
        spec = self._spec(spec_id)
        if spec.frozen_dataset is None:
            raise TrainingExperimentError(
                "frozen_dataset_not_configured",
                f'Ops spec "{spec_id}" has no frozen_dataset section.',
                status_code=400,
            )
        return spec.frozen_dataset

    def _ensure_launch_configured(self, frozen_spec: OpsFrozenDatasetSpec) -> None:
        if frozen_spec.run_labels:
            return
        raise TrainingExperimentError(
            "freeze_launch_not_configured",
            "Freezing a new dataset is not configured for this ops spec "
            "(set OpsFrozenDatasetSpec.run_labels to a pipeline stage such as "
            "stage=train-prepare).",
            status_code=400,
        )

    def launch_freeze(self, spec_id: str) -> FreezeLaunchResponse:
        frozen_spec = self._frozen_spec(spec_id)
        self._ensure_launch_configured(frozen_spec)
        if self.run_steps is None:
            raise TrainingExperimentError(
                "freeze_launch_failed",
                "Launching freeze steps is not available on this instance.",
                status_code=503,
            )
        result = self.run_steps("freeze", list(frozen_spec.run_labels), {})
        return FreezeLaunchResponse(
            started=bool(result.get("started", False)),
            run_id=result.get("run_id"),
        )


__all__ = ["FreezeLaunchResponse", "FrozenDatasetsLaunchService"]

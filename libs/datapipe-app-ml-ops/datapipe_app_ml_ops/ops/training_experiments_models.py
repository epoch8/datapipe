from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field, computed_field


class TrainingExperimentError(Exception):
    """Domain error for training experiments (spec §19).

    Rendered by the routes as::

        {"error": {"code": "...", "message": "...", "details": {}}}
    """

    def __init__(
        self,
        code: str,
        message: str,
        *,
        status_code: int = 400,
        details: Optional[dict[str, Any]] = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code
        self.details: dict[str, Any] = details or {}

    def to_payload(self) -> dict[str, Any]:
        return {"error": {"code": self.code, "message": self.message, "details": self.details}}


class TrainingExperimentCapabilities(BaseModel):
    """Backend-computed action flags for one experiment (spec §16).

    The backend is the sole source of these flags; the UI must not re-derive
    them from other fields.
    """

    can_edit: bool
    can_delete: bool
    can_duplicate: bool
    can_launch: bool
    can_archive: bool
    lock_reason: Optional[str] = None


class TrainingExperimentRow(BaseModel):
    """A row in the training experiment registry (builtin or custom)."""

    id: str
    source: str
    display_name: Optional[str] = None
    description: Optional[str] = None
    config_type: str
    params: dict[str, Any] = Field(default_factory=dict)
    config_hash: Optional[str] = None
    active: bool = True
    revision: int = 1
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    summary: dict[str, Any] = Field(default_factory=dict)
    requests_count: int = 0
    runs_count: int = 0
    last_used_at: Optional[str] = None
    capabilities: TrainingExperimentCapabilities

    @computed_field  # type: ignore[prop-decorator]
    @property
    def is_active(self) -> bool:
        """Deprecated alias for ``active``, kept for older clients/tests."""
        return self.active

    @computed_field  # type: ignore[prop-decorator]
    @property
    def archived(self) -> bool:
        """Convenience alias: an experiment is archived when not ``active``."""
        return not self.active


# Backward-compatible alias for the previous class name.
TrainingExperiment = TrainingExperimentRow


class TrainingExperimentsSummary(BaseModel):
    total: int = 0
    builtin: int = 0
    custom: int = 0
    editable: int = 0
    locked: int = 0
    archived: int = 0


class TrainingExperimentsListResponse(BaseModel):
    rows: list[TrainingExperimentRow] = Field(default_factory=list)
    total: int = 0
    summary: TrainingExperimentsSummary = Field(default_factory=TrainingExperimentsSummary)
    filters: dict[str, Any] = Field(default_factory=dict)


class TrainingExperimentModelRow(BaseModel):
    """A model produced by training with a given experiment config."""

    model_id: str
    frozen_dataset_id: str
    frozen_dataset_display_name: Optional[str] = None
    created_at: Optional[str] = None
    run_key: Optional[str] = None
    link_table: Optional[str] = None


class TrainingExperimentDetailResponse(BaseModel):
    experiment: TrainingExperimentRow
    models: list[TrainingExperimentModelRow] = Field(default_factory=list)


class TrainingExperimentModelsResponse(BaseModel):
    experiment: TrainingExperimentRow
    models: list[TrainingExperimentModelRow] = Field(default_factory=list)
    total: int = 0


class ConfigSchemaResponse(BaseModel):
    config_type: str
    json_schema: dict[str, Any] = Field(default_factory=dict)


class TrainingRequest(BaseModel):
    """A manual training request (spec §17).

    ``id``, ``kind``, ``state``, ``frozen_dataset_id``, ``train_config_id``,
    ``config_name`` and ``config_hash`` are the normative fields. The
    remaining fields carry the full snapshot for debugging/detail views.
    """

    id: str
    kind: str = "manual"
    state: str = "queued"
    frozen_dataset_id: Optional[str] = None
    train_config_id: str
    config_name: Optional[str] = None
    config_hash: Optional[str] = None
    enabled: bool = True
    force: bool = False
    max_within_time: Optional[str] = None
    config_source: Optional[str] = None
    config_params_snapshot: dict[str, Any] = Field(default_factory=dict)
    requested_at: Optional[str] = None
    requested_by: Optional[str] = None
    client_request_id: Optional[str] = None


class LaunchInfo(BaseModel):
    started: bool
    run_id: Optional[str] = None


class CreateTrainingRequestResponse(BaseModel):
    request: TrainingRequest
    launch: Optional[LaunchInfo] = None


class LaunchResponse(BaseModel):
    training_request_id: str
    started: bool
    run_id: Optional[str] = None


# ------------------------------------------------------------------ requests


class CreateTrainingExperimentRequest(BaseModel):
    display_name: str
    description: Optional[str] = None
    params: dict[str, Any] = Field(default_factory=dict)


class UpdateTrainingExperimentRequest(BaseModel):
    display_name: str
    description: Optional[str] = None
    params: dict[str, Any] = Field(default_factory=dict)
    expected_revision: int


class DuplicateTrainingExperimentRequest(BaseModel):
    display_name: Optional[str] = None
    description: Optional[str] = None


class CreateTrainingRequestRequest(BaseModel):
    frozen_dataset_id: str
    train_config_id: str
    client_request_id: str
    launch: bool = False
    """When True, skip ``max_within_time`` freshness and train on a stale dataset."""
    force: bool = False


# Backward-compatible aliases for the previous request class names.
CreateExperimentRequest = CreateTrainingExperimentRequest
UpdateExperimentRequest = UpdateTrainingExperimentRequest
DuplicateExperimentRequest = DuplicateTrainingExperimentRequest


__all__ = [
    "TrainingExperimentError",
    "TrainingExperimentCapabilities",
    "TrainingExperimentRow",
    "TrainingExperiment",
    "TrainingExperimentsSummary",
    "TrainingExperimentsListResponse",
    "TrainingExperimentDetailResponse",
    "TrainingExperimentModelRow",
    "TrainingExperimentModelsResponse",
    "ConfigSchemaResponse",
    "TrainingRequest",
    "LaunchInfo",
    "CreateTrainingRequestResponse",
    "LaunchResponse",
    "CreateTrainingExperimentRequest",
    "UpdateTrainingExperimentRequest",
    "DuplicateTrainingExperimentRequest",
    "CreateTrainingRequestRequest",
    "CreateExperimentRequest",
    "UpdateExperimentRequest",
    "DuplicateExperimentRequest",
]

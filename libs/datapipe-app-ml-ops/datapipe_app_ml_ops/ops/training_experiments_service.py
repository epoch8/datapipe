from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import pandas as pd
from datapipe.compute import Catalog
from datapipe.datatable import DataStore
from datapipe.types import IndexDF

# Importing codecs registers yolov8_* / tf_classification (spec §15). Side-effect
# imports are guarded because they pull optional extras (torch/ultralytics or
# tensorflow); when an extra is absent the codec simply is not registered and
# codec-dependent endpoints report an ``unknown_config_type`` error instead of
# failing to import.
try:  # pragma: no cover - depends on optional extra being installed
    import datapipe_ml.frameworks.yolo.train_config_codec  # noqa: F401
except Exception:  # noqa: BLE001 - optional dependency may be missing
    pass
try:  # pragma: no cover - depends on optional extra being installed
    import datapipe_ml.frameworks.tensorflow.train_config_codec  # noqa: F401
except Exception:  # noqa: BLE001 - optional dependency may be missing
    pass
from datapipe_ml.training.config_codec import (
    TrainConfigValidationError,
    get_train_config_codec,
)
from datapipe_ml.training.train_config_id import (
    build_custom_train_config_id,
    build_manual_training_request_id,
    hash_train_config_params,
)

from datapipe_app_ml_ops.ops.ops_specs import (
    DatapipeOpsSpec,
    OpsFrozenDatasetSpec,
    OpsTrainConfigRegistrySpec,
    OpsTrainingRequestSpec,
)
from datapipe_app_ml_ops.ops.spec_registry import OpsSpecRegistry
from datapipe_app_ml_ops.ops.training_experiments_models import (
    ConfigSchemaResponse,
    CreateTrainingExperimentRequest,
    CreateTrainingRequestRequest,
    CreateTrainingRequestResponse,
    DuplicateTrainingExperimentRequest,
    LaunchInfo,
    LaunchResponse,
    TrainingExperimentCapabilities,
    TrainingExperimentDetailResponse,
    TrainingExperimentError,
    TrainingExperimentModelRow,
    TrainingExperimentModelsResponse,
    TrainingExperimentRow,
    TrainingExperimentsListResponse,
    TrainingExperimentsSummary,
    TrainingRequest,
    TrainingRequestListRow,
    TrainingRequestsListResponse,
    UpdateTrainingExperimentRequest,
)

# ------------------------------------------------------------------ run_steps

# The result of launching a training request (spec §18).
RunStepsResult = Dict[str, Any]  # {"started": bool, "run_id": Optional[str]}

# Callable injected by datapipe-app that filters the pipeline steps by
# ``run_labels`` and runs them with a primary-key filter on the request id
# (spec §18). It is synchronous to match the existing v1alpha3 API style.
RunStepsCallable = Callable[
    [str, Sequence[Tuple[str, str]], Dict[str, List[Any]]],
    RunStepsResult,
]


def build_capabilities(
    *,
    source: str,
    active: bool,
    requests_count: int,
) -> TrainingExperimentCapabilities:
    """Compute action flags for an experiment (spec §16).

    The backend is the sole source of these flags. ``active`` means the
    experiment is enabled (not archived); ``source`` is ``"builtin"`` or
    ``"custom"``. Built-in experiments are always read-only. A custom
    experiment's parameters become locked (read-only) as soon as it has been
    used by at least one training request.
    """

    if source == "builtin":
        return TrainingExperimentCapabilities(
            can_edit=False,
            can_delete=False,
            can_duplicate=True,
            can_launch=active,
            can_archive=False,
            lock_reason="Built-in experiments are managed by pipeline code",
        )
    if requests_count == 0:
        return TrainingExperimentCapabilities(
            can_edit=True,
            can_delete=True,
            can_duplicate=True,
            can_launch=active,
            can_archive=True,
            lock_reason=None,
        )
    return TrainingExperimentCapabilities(
        can_edit=False,
        can_delete=False,
        can_duplicate=True,
        can_launch=active,
        can_archive=True,
        lock_reason=(
            "Experiment parameters are locked because "
            "the experiment has already been used by a training request"
        ),
    )


# ------------------------------------------------------------------ helpers


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_nan(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    try:
        if pd.isna(value):
            return True
    except (TypeError, ValueError):
        pass
    return False


def _clean_str(value: Any) -> Optional[str]:
    if _is_nan(value):
        return None
    text = str(value)
    return text


def _clean_bool(value: Any, *, default: bool = False) -> bool:
    if _is_nan(value):
        return default
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(value)


def _clean_int(value: Any, *, default: int = 0) -> int:
    if _is_nan(value):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _clean_dict(value: Any) -> dict[str, Any]:
    if _is_nan(value):
        return {}
    if isinstance(value, dict):
        return dict(value)
    return {}


def _guess_column(
    df: pd.DataFrame,
    *,
    suffixes: Sequence[str] = (),
    exact: Sequence[str] = (),
) -> Optional[str]:
    columns = list(df.columns)
    for name in exact:
        if name in columns:
            return name
    for col in columns:
        for suffix in suffixes:
            if str(col).endswith(suffix):
                return str(col)
    return None


def _record_created_at(record: dict[str, Any]) -> Optional[str]:
    for key, value in record.items():
        key_l = str(key).lower()
        if key_l.endswith("created_at") or key_l.endswith("started_at") or key_l == "created_at":
            text = _clean_str(value)
            if text:
                return text
    return None


def _request_can_delete(
    *,
    kind: str,
    run_key: Optional[str],
    model_id: Optional[str],
    status: Optional[str],
    started_at: Optional[str],
) -> bool:
    """Manual requests may be deleted only before launch / training starts."""
    if kind != "manual":
        return False
    if run_key or model_id or started_at:
        return False
    if status is None:
        return True
    return status.strip().lower() in {"", "queued", "pending"}


class TrainingExperimentsService:
    """Backend for the custom training experiments API (spec §14-18)."""

    def __init__(
        self,
        registry: OpsSpecRegistry,
        *,
        ds: DataStore,
        catalog: Catalog,
        run_steps: Optional[RunStepsCallable] = None,
    ) -> None:
        self.registry = registry
        self.ds = ds
        self.catalog = catalog
        self.run_steps = run_steps

    # ----------------------------------------------------------- spec resolve

    def _spec(self, spec_id: str) -> DatapipeOpsSpec:
        try:
            return self.registry.get(spec_id)
        except KeyError as exc:
            raise TrainingExperimentError(
                "not_found",
                f'Ops spec "{spec_id}" was not found.',
                status_code=404,
            ) from exc

    def _experiments_spec(self, spec_id: str) -> OpsTrainConfigRegistrySpec:
        spec = self._spec(spec_id)
        training = spec.training
        experiments = training.experiments if training is not None else None
        if experiments is None:
            raise TrainingExperimentError(
                "not_configured",
                f'Ops spec "{spec_id}" has no training experiments registry configured.',
                status_code=404,
            )
        return experiments

    def _requests_spec(self, spec_id: str) -> OpsTrainingRequestSpec:
        spec = self._spec(spec_id)
        training = spec.training
        requests = training.requests if training is not None else None
        if requests is None:
            raise TrainingExperimentError(
                "not_configured",
                f'Ops spec "{spec_id}" has no training requests table configured.',
                status_code=404,
            )
        return requests

    def _frozen_spec(self, spec_id: str) -> Optional[OpsFrozenDatasetSpec]:
        return self._spec(spec_id).frozen_dataset

    # ----------------------------------------------------------- table access

    def _read_df(self, table: str) -> pd.DataFrame:
        dt = self.catalog.get_datatable(self.ds, table)
        df = dt.get_data()
        if df is None:
            return pd.DataFrame()
        return df

    def _write_row(self, table: str, row: dict[str, Any]) -> None:
        dt = self.catalog.get_datatable(self.ds, table)
        dt.store_chunk(pd.DataFrame([row]))

    def _delete_id(self, table: str, id_column: str, value: Any) -> None:
        dt = self.catalog.get_datatable(self.ds, table)
        idx = IndexDF(pd.DataFrame([{id_column: value}]))
        dt.delete_by_idx(idx)

    # --------------------------------------------------------- codec helpers

    def _codec(self, config_type: str):
        try:
            return get_train_config_codec(config_type)
        except ValueError as exc:
            raise TrainingExperimentError(
                "unknown_config_type",
                str(exc),
                status_code=400,
            ) from exc

    def _validate_params(self, config_type: str, params: dict[str, Any]) -> dict[str, Any]:
        codec = self._codec(config_type)
        try:
            return codec.normalize(params)
        except TrainConfigValidationError as exc:
            raise TrainingExperimentError(
                "train_config_validation_failed",
                str(exc),
                status_code=422,
                details={"errors": exc.errors},
            ) from exc

    def _summarize(self, config_type: str, params: dict[str, Any]) -> dict[str, Any]:
        codec = self._codec(config_type)
        try:
            return codec.summarize(params)
        except Exception:  # summarize is best-effort display metadata
            return {}

    # ----------------------------------------------------------- row -> model

    def _experiment_from_row(
        self,
        registry_spec: OpsTrainConfigRegistrySpec,
        row: pd.Series,
        *,
        requests_count: int,
        runs_count: int,
        last_used_at: Optional[str] = None,
        source: Optional[str] = None,
    ) -> TrainingExperimentRow:
        if source is None:
            source = _clean_str(row.get(registry_spec.source_column)) or "builtin"
        # Default/builtin table has no is_active column; treat as always active.
        if source == "builtin":
            active = True
        else:
            active = _clean_bool(row.get(registry_spec.active_column), default=True)
        params = _clean_dict(row.get(registry_spec.params_column))
        config_type = (
            _clean_str(row.get(registry_spec.config_type_column))
            or registry_spec.config_type
        )
        return TrainingExperimentRow(
            id=str(row.get(registry_spec.id_column)),
            source=source,
            display_name=_clean_str(row.get(registry_spec.display_name_column)),
            description=_clean_str(row.get(registry_spec.description_column)),
            config_type=config_type,
            params=params,
            config_hash=_clean_str(row.get(registry_spec.hash_column)),
            active=active,
            revision=_clean_int(row.get(registry_spec.revision_column), default=1),
            created_at=_clean_str(row.get(registry_spec.created_at_column)),
            updated_at=_clean_str(row.get(registry_spec.updated_at_column)),
            summary=self._summarize(config_type, params),
            requests_count=requests_count,
            runs_count=runs_count,
            last_used_at=last_used_at,
            capabilities=build_capabilities(
                source=source,
                active=active,
                requests_count=requests_count,
            ),
        )

    def _iter_registry_rows(
        self, registry_spec: OpsTrainConfigRegistrySpec
    ) -> list[tuple[pd.Series, str]]:
        """Return ``(row, source)`` pairs from default + custom registry tables."""
        rows: list[tuple[pd.Series, str]] = []
        if registry_spec.default_table:
            df_default = self._read_df(registry_spec.default_table)
            if not df_default.empty and registry_spec.id_column in df_default.columns:
                for _, row in df_default.iterrows():
                    rows.append((row, "builtin"))
        df_custom = self._read_df(registry_spec.table)
        if not df_custom.empty and registry_spec.id_column in df_custom.columns:
            for _, row in df_custom.iterrows():
                # Legacy single-table mode: honour stored source when no default_table.
                if registry_spec.default_table is None:
                    source = _clean_str(row.get(registry_spec.source_column)) or "custom"
                else:
                    source = "custom"
                rows.append((row, source))
        return rows

    # ----------------------------------------------------------- counts

    def _requests_df(self, spec_id: str) -> pd.DataFrame:
        spec = self._spec(spec_id)
        training = spec.training
        requests = training.requests if training is not None else None
        if requests is None:
            return pd.DataFrame()
        return self._read_df(requests.table)

    def _counts_by_config(
        self, spec_id: str
    ) -> Tuple[dict[str, int], dict[str, int], dict[str, str]]:
        """Return ``(requests_count, runs_count, last_used_at)`` keyed by
        ``train_config_id``."""
        spec = self._spec(spec_id)
        training = spec.training
        requests = training.requests if training is not None else None
        if requests is None:
            return {}, {}, {}
        df_req = self._read_df(requests.table)
        requests_count: dict[str, int] = {}
        last_used_at: dict[str, str] = {}
        request_to_config: dict[str, str] = {}
        if not df_req.empty and requests.train_config_id_column in df_req.columns:
            for _, r in df_req.iterrows():
                config_id = _clean_str(r.get(requests.train_config_id_column))
                request_id = _clean_str(r.get(requests.id_column))
                if config_id is None:
                    continue
                requests_count[config_id] = requests_count.get(config_id, 0) + 1
                if request_id is not None:
                    request_to_config[request_id] = config_id
                requested_at = _clean_str(r.get(requests.requested_at_column))
                if requested_at is not None:
                    current = last_used_at.get(config_id)
                    if current is None or requested_at > current:
                        last_used_at[config_id] = requested_at

        runs_count: dict[str, int] = {}
        if (
            requests.status_table
            and requests.status_request_id_column
            and request_to_config
        ):
            df_status = self._read_df(requests.status_table)
            if (
                not df_status.empty
                and requests.status_request_id_column in df_status.columns
            ):
                for _, r in df_status.iterrows():
                    request_id = _clean_str(r.get(requests.status_request_id_column))
                    config_id = request_to_config.get(request_id or "")
                    if config_id is None:
                        continue
                    runs_count[config_id] = runs_count.get(config_id, 0) + 1
        return requests_count, runs_count, last_used_at

    def list_training_requests(
        self,
        spec_id: str,
        *,
        limit: int = 50,
        offset: int = 0,
    ) -> TrainingRequestsListResponse:
        """List training requests joined with status run/model when available."""
        requests_spec = self._requests_spec(spec_id)
        spec = self._spec(spec_id)
        df_req = self._read_df(requests_spec.table)
        if df_req.empty or requests_spec.id_column not in df_req.columns:
            return TrainingRequestsListResponse(rows=[], total=0)

        status_by_request = self._status_by_request_id(spec, requests_spec)

        rows: list[TrainingRequestListRow] = []
        for _, row in df_req.iterrows():
            request_id = _clean_str(row.get(requests_spec.id_column))
            if request_id is None:
                continue
            train_config_id = _clean_str(row.get(requests_spec.train_config_id_column)) or ""
            status = status_by_request.get(request_id)
            status_value = status.get("status") if status else None
            run_key = status.get("run_key") if status else None
            model_id = status.get("model_id") if status else None
            started_at = status.get("started_at") if status else None
            kind = _clean_str(row.get(requests_spec.kind_column)) or "manual"
            rows.append(
                TrainingRequestListRow(
                    id=request_id,
                    kind=kind,
                    state=status_value or "queued",
                    frozen_dataset_id=_clean_str(row.get(requests_spec.frozen_dataset_id_column)),
                    train_config_id=train_config_id,
                    config_name=_clean_str(row.get(requests_spec.config_name_snapshot_column)),
                    requested_at=_clean_str(row.get(requests_spec.requested_at_column)),
                    force=_clean_bool(row.get(requests_spec.force_column), default=False),
                    enabled=_clean_bool(row.get(requests_spec.enabled_column), default=True),
                    run_key=run_key,
                    model_id=model_id,
                    status=status_value,
                    started_at=started_at,
                    can_delete=_request_can_delete(
                        kind=kind,
                        run_key=run_key,
                        model_id=model_id,
                        status=status_value,
                        started_at=started_at,
                    ),
                )
            )

        rows.sort(key=lambda r: r.requested_at or "", reverse=True)
        total = len(rows)
        page = rows[offset : offset + limit]
        return TrainingRequestsListResponse(rows=page, total=total)

    def _status_by_request_id(
        self,
        spec: DatapipeOpsSpec,
        requests_spec: OpsTrainingRequestSpec,
    ) -> dict[str, dict[str, Optional[str]]]:
        """Map ``training_request_id`` → latest status fields (run/model/status)."""
        status_table = requests_spec.status_table or (
            spec.training.status_table if spec.training is not None else None
        )
        request_col = requests_spec.status_request_id_column
        if not status_table or not request_col:
            return {}

        df = self._read_df(status_table)
        if df.empty or request_col not in df.columns:
            return {}

        model_id_col = (
            spec.model.id_column
            if spec.model is not None
            else _guess_column(df, suffixes=("_model_id",), exact=("model_id",))
        )
        run_key_col = _guess_column(
            df,
            suffixes=("__run_key",),
            exact=("run_key", "training_status__run_key"),
        )
        status_col = _guess_column(
            df,
            suffixes=("__status",),
            exact=("status", "training_status__status"),
        )
        started_col = _guess_column(
            df,
            suffixes=("__started_at", "__created_at"),
            exact=("started_at", "created_at"),
        )

        # Prefer the chronologically latest status row per request.
        if started_col and started_col in df.columns:
            df = df.sort_values(by=started_col, ascending=True, na_position="first")

        out: dict[str, dict[str, Optional[str]]] = {}
        for _, r in df.iterrows():
            request_id = _clean_str(r.get(request_col))
            if request_id is None:
                continue
            out[request_id] = {
                "run_key": _clean_str(r.get(run_key_col)) if run_key_col else None,
                "model_id": _clean_str(r.get(model_id_col)) if model_id_col else None,
                "status": _clean_str(r.get(status_col)) if status_col else None,
                "started_at": _clean_str(r.get(started_col)) if started_col else None,
            }
        return out

    # ================================================================= read

    def list_experiments(
        self,
        spec_id: str,
        *,
        search: Optional[str] = None,
        source: Optional[str] = None,
        state: Optional[str] = None,
        include_archived: bool = False,
        order: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> TrainingExperimentsListResponse:
        registry_spec = self._experiments_spec(spec_id)
        requests_count, runs_count, last_used_at = self._counts_by_config(spec_id)

        experiments: list[TrainingExperimentRow] = []
        for row, row_source in self._iter_registry_rows(registry_spec):
            config_id = str(row.get(registry_spec.id_column))
            experiments.append(
                self._experiment_from_row(
                    registry_spec,
                    row,
                    requests_count=requests_count.get(config_id, 0),
                    runs_count=runs_count.get(config_id, 0),
                    last_used_at=last_used_at.get(config_id),
                    source=row_source,
                )
            )

        summary = TrainingExperimentsSummary(
            total=len(experiments),
            builtin=sum(1 for e in experiments if e.source == "builtin"),
            custom=sum(1 for e in experiments if e.source == "custom"),
            editable=sum(
                1 for e in experiments if e.source == "custom" and e.capabilities.can_edit
            ),
            locked=sum(
                1
                for e in experiments
                if e.source == "custom" and e.requests_count > 0 and e.active
            ),
            archived=sum(1 for e in experiments if not e.active),
        )

        rows = experiments
        if not include_archived and state != "archived":
            rows = [e for e in rows if e.active]
        if source:
            rows = [e for e in rows if e.source == source]
        if state == "active":
            rows = [e for e in rows if e.active]
        elif state == "archived":
            rows = [e for e in rows if not e.active]
        if search:
            q = search.lower()
            rows = [
                e
                for e in rows
                if q in e.id.lower()
                or q in (e.display_name or "").lower()
                or q in (e.description or "").lower()
            ]

        rows = self._order_experiments(rows, order)

        total = len(rows)
        page = rows[offset : offset + limit]
        return TrainingExperimentsListResponse(
            rows=page,
            total=total,
            summary=summary,
            filters={
                "sources": sorted({e.source for e in experiments}),
                "states": ["active", "archived"],
            },
        )

    def _order_experiments(
        self, rows: list[TrainingExperimentRow], order: Optional[str]
    ) -> list[TrainingExperimentRow]:
        field = order or "-updated_at"
        reverse = field.startswith("-")
        key = field[1:] if reverse else field
        allowed = {"updated_at", "created_at", "display_name", "id", "revision"}
        if key not in allowed:
            key = "updated_at"
            reverse = True

        def _sort_key(e: TrainingExperimentRow) -> str:
            value = getattr(e, key, None)
            return "" if value is None else str(value)

        return sorted(rows, key=_sort_key, reverse=reverse)

    def get_experiment(self, spec_id: str, config_id: str) -> TrainingExperimentDetailResponse:
        experiment = self._load_experiment(spec_id, config_id)
        models = self._models_for_config(spec_id, config_id)
        return TrainingExperimentDetailResponse(experiment=experiment, models=models)

    def list_experiment_models(
        self, spec_id: str, config_id: str
    ) -> TrainingExperimentModelsResponse:
        experiment = self._load_experiment(spec_id, config_id)
        models = self._models_for_config(spec_id, config_id)
        return TrainingExperimentModelsResponse(
            experiment=experiment,
            models=models,
            total=len(models),
        )

    def _models_for_config(
        self, spec_id: str, config_id: str
    ) -> list[TrainingExperimentModelRow]:
        """Models trained with ``config_id``, including the frozen dataset used."""
        spec = self._spec(spec_id)
        try:
            requests_spec = self._requests_spec(spec_id)
        except TrainingExperimentError:
            return []

        train_config_col = requests_spec.train_config_id_column
        dataset_names = self._frozen_dataset_names(spec_id)
        rows: list[TrainingExperimentModelRow] = []
        seen: set[tuple[str, str]] = set()

        relation = self._trained_on_relation(spec)
        if relation is not None:
            df = self._read_df(relation.table)
            if (
                not df.empty
                and train_config_col in df.columns
                and relation.from_column in df.columns
                and relation.to_column in df.columns
            ):
                match = df[df[train_config_col].astype(str) == str(config_id)]
                for _, r in match.iterrows():
                    model_id = _clean_str(r.get(relation.from_column))
                    dataset_id = _clean_str(r.get(relation.to_column))
                    if model_id is None or dataset_id is None:
                        continue
                    key = (model_id, dataset_id)
                    if key in seen:
                        continue
                    seen.add(key)
                    rows.append(
                        TrainingExperimentModelRow(
                            model_id=model_id,
                            frozen_dataset_id=dataset_id,
                            frozen_dataset_display_name=dataset_names.get(dataset_id),
                            created_at=_record_created_at({str(k): v for k, v in r.to_dict().items()}),
                            link_table=relation.table,
                        )
                    )

        if not rows:
            rows.extend(
                self._models_from_status_table(
                    spec,
                    requests_spec,
                    config_id=config_id,
                    dataset_names=dataset_names,
                    seen=seen,
                )
            )

        rows.sort(key=lambda row: row.created_at or "", reverse=True)
        return rows

    def _trained_on_relation(self, spec: DatapipeOpsSpec):
        relation_id = (
            spec.frozen_dataset.models_count_relation_id
            if spec.frozen_dataset is not None
            else "model_trained_on_frozen_dataset"
        )
        if not relation_id:
            return None
        return next((rel for rel in spec.relations if rel.id == relation_id), None)

    def _frozen_dataset_names(self, spec_id: str) -> dict[str, str]:
        frozen = self._frozen_spec(spec_id)
        if frozen is None or not frozen.display_name_column:
            return {}
        df = self._read_df(frozen.table)
        if df.empty or frozen.id_column not in df.columns:
            return {}
        names: dict[str, str] = {}
        for _, r in df.iterrows():
            dataset_id = _clean_str(r.get(frozen.id_column))
            display = _clean_str(r.get(frozen.display_name_column))
            if dataset_id and display:
                names[dataset_id] = display
        return names

    def _models_from_status_table(
        self,
        spec: DatapipeOpsSpec,
        requests_spec: OpsTrainingRequestSpec,
        *,
        config_id: str,
        dataset_names: dict[str, str],
        seen: set[tuple[str, str]],
    ) -> list[TrainingExperimentModelRow]:
        status_table = requests_spec.status_table or (
            spec.training.status_table if spec.training is not None else None
        )
        if not status_table:
            return []
        df = self._read_df(status_table)
        train_config_col = requests_spec.train_config_id_column
        frozen_id_col = requests_spec.frozen_dataset_id_column
        model_id_col = (
            spec.model.id_column
            if spec.model is not None
            else _guess_column(df, suffixes=("_model_id",), exact=("model_id",))
        )
        if (
            df.empty
            or train_config_col not in df.columns
            or frozen_id_col not in df.columns
            or not model_id_col
            or model_id_col not in df.columns
        ):
            return []

        run_key_col = _guess_column(
            df,
            suffixes=("__run_key",),
            exact=("run_key", "training_status__run_key"),
        )
        started_col = _guess_column(
            df,
            suffixes=("__started_at", "__created_at"),
            exact=("started_at", "created_at"),
        )

        rows: list[TrainingExperimentModelRow] = []
        match = df[df[train_config_col].astype(str) == str(config_id)]
        for _, r in match.iterrows():
            model_id = _clean_str(r.get(model_id_col))
            dataset_id = _clean_str(r.get(frozen_id_col))
            if model_id is None or dataset_id is None:
                continue
            key = (model_id, dataset_id)
            if key in seen:
                continue
            seen.add(key)
            created_at = _clean_str(r.get(started_col)) if started_col else None
            rows.append(
                TrainingExperimentModelRow(
                    model_id=model_id,
                    frozen_dataset_id=dataset_id,
                    frozen_dataset_display_name=dataset_names.get(dataset_id),
                    created_at=created_at,
                    run_key=_clean_str(r.get(run_key_col)) if run_key_col else None,
                    link_table=status_table,
                )
            )
        return rows

    def _load_experiment(self, spec_id: str, config_id: str) -> TrainingExperimentRow:
        registry_spec = self._experiments_spec(spec_id)
        requests_count, runs_count, last_used_at = self._counts_by_config(spec_id)
        # Prefer the custom table so API-owned rows win over builtins on id clash.
        candidates = list(self._iter_registry_rows(registry_spec))
        candidates.sort(key=lambda item: 0 if item[1] == "custom" else 1)
        for row, source in candidates:
            if str(row.get(registry_spec.id_column)) != str(config_id):
                continue
            return self._experiment_from_row(
                registry_spec,
                row,
                requests_count=requests_count.get(config_id, 0),
                runs_count=runs_count.get(config_id, 0),
                last_used_at=last_used_at.get(config_id),
                source=source,
            )
        raise self._not_found(config_id)

    def _not_found(self, config_id: str) -> TrainingExperimentError:
        return TrainingExperimentError(
            "experiment_not_found",
            f'Training experiment "{config_id}" was not found.',
            status_code=404,
        )

    def get_config_schema(
        self, spec_id: str, *, config_type: Optional[str] = None
    ) -> ConfigSchemaResponse:
        registry_spec = self._experiments_spec(spec_id)
        resolved = config_type or registry_spec.config_type
        codec = self._codec(resolved)
        return ConfigSchemaResponse(config_type=resolved, json_schema=codec.json_schema())

    # ================================================================ write

    def create_experiment(
        self, spec_id: str, req: CreateTrainingExperimentRequest
    ) -> TrainingExperimentDetailResponse:
        registry_spec = self._experiments_spec(spec_id)
        config_type = registry_spec.config_type
        params = self._validate_params(config_type, req.params)
        config_id = build_custom_train_config_id()
        now = _now_iso()
        row = self._registry_row(
            registry_spec,
            config_id=config_id,
            config_type=config_type,
            params=params,
            display_name=req.display_name,
            description=req.description,
            source="custom",
            active=True,
            revision=1,
            created_at=now,
            updated_at=now,
        )
        self._write_row(registry_spec.table, row)
        return self.get_experiment(spec_id, config_id)

    def update_experiment(
        self, spec_id: str, config_id: str, req: UpdateTrainingExperimentRequest
    ) -> TrainingExperimentDetailResponse:
        registry_spec = self._experiments_spec(spec_id)
        current = self._load_experiment(spec_id, config_id)
        if current.source != "custom":
            raise TrainingExperimentError(
                "builtin_experiment_read_only",
                "Built-in experiments are managed by pipeline code and cannot be edited.",
                status_code=403,
            )
        if current.requests_count > 0:
            raise TrainingExperimentError(
                "experiment_locked",
                "Experiment parameters are locked because the experiment has "
                "already been used by a training request.",
                status_code=409,
            )
        # Optimistic concurrency: caller-provided revision must match current
        # (spec §14). DataTable has no multi-row transaction, so this check and
        # the write below are best-effort but ordered.
        if req.expected_revision != current.revision:
            raise TrainingExperimentError(
                "experiment_revision_conflict",
                "Experiment was modified by someone else.",
                status_code=409,
                details={"expected": req.expected_revision, "actual": current.revision},
            )

        params = self._validate_params(current.config_type, req.params)
        now = _now_iso()
        row = self._registry_row(
            registry_spec,
            config_id=config_id,
            config_type=current.config_type,
            params=params,
            display_name=req.display_name,
            description=req.description,
            source="custom",
            active=current.active,
            revision=current.revision + 1,
            created_at=current.created_at or now,
            updated_at=now,
        )
        self._write_row(registry_spec.table, row)
        return self.get_experiment(spec_id, config_id)

    def delete_experiment(self, spec_id: str, config_id: str) -> None:
        registry_spec = self._experiments_spec(spec_id)
        current = self._load_experiment(spec_id, config_id)
        if current.source != "custom":
            raise TrainingExperimentError(
                "builtin_experiment_read_only",
                "Built-in experiments cannot be deleted.",
                status_code=403,
            )
        if current.requests_count > 0:
            raise TrainingExperimentError(
                "experiment_locked",
                "Experiment has training requests and cannot be deleted.",
                status_code=409,
                details={"requests_count": current.requests_count},
            )
        self._delete_id(registry_spec.table, registry_spec.id_column, config_id)

    def duplicate_experiment(
        self, spec_id: str, config_id: str, req: DuplicateTrainingExperimentRequest
    ) -> TrainingExperimentDetailResponse:
        registry_spec = self._experiments_spec(spec_id)
        source_exp = self._load_experiment(spec_id, config_id)
        new_id = build_custom_train_config_id()
        now = _now_iso()
        display_name = req.display_name or self._copy_name(source_exp.display_name)
        description = source_exp.description if req.description is None else req.description
        row = self._registry_row(
            registry_spec,
            config_id=new_id,
            config_type=source_exp.config_type,
            params=source_exp.params,
            display_name=display_name,
            description=description,
            source="custom",
            active=True,
            revision=1,
            created_at=now,
            updated_at=now,
        )
        self._write_row(registry_spec.table, row)
        return self.get_experiment(spec_id, new_id)

    def _copy_name(self, display_name: Optional[str]) -> str:
        base = display_name or "Experiment"
        return f"{base} (copy)"

    def archive_experiment(self, spec_id: str, config_id: str) -> TrainingExperimentDetailResponse:
        return self._set_active(spec_id, config_id, active=False)

    def unarchive_experiment(self, spec_id: str, config_id: str) -> TrainingExperimentDetailResponse:
        return self._set_active(spec_id, config_id, active=True)

    def _set_active(
        self, spec_id: str, config_id: str, *, active: bool
    ) -> TrainingExperimentDetailResponse:
        registry_spec = self._experiments_spec(spec_id)
        current = self._load_experiment(spec_id, config_id)
        if current.source != "custom":
            raise TrainingExperimentError(
                "builtin_experiment_read_only",
                "Built-in experiments cannot be archived.",
                status_code=403,
            )
        if current.active == active:
            return TrainingExperimentDetailResponse(experiment=current)
        now = _now_iso()
        row = self._registry_row(
            registry_spec,
            config_id=config_id,
            config_type=current.config_type,
            params=current.params,
            display_name=current.display_name,
            description=current.description,
            source="custom",
            active=active,
            revision=current.revision + 1,
            created_at=current.created_at or now,
            updated_at=now,
        )
        self._write_row(registry_spec.table, row)
        return self.get_experiment(spec_id, config_id)

    def _registry_row(
        self,
        registry_spec: OpsTrainConfigRegistrySpec,
        *,
        config_id: str,
        config_type: str,
        params: dict[str, Any],
        display_name: Optional[str],
        description: Optional[str],
        source: str,
        active: bool,
        revision: int,
        created_at: str,
        updated_at: str,
    ) -> dict[str, Any]:
        # ``source`` is derived from which table the row lives in (custom vs
        # default); the custom table schema does not store a source column.
        _ = source
        return {
            registry_spec.id_column: config_id,
            registry_spec.params_column: params,
            registry_spec.display_name_column: display_name,
            registry_spec.description_column: description,
            registry_spec.config_type_column: config_type,
            registry_spec.hash_column: hash_train_config_params(params, length=40),
            registry_spec.active_column: active,
            registry_spec.revision_column: revision,
            registry_spec.created_at_column: created_at,
            registry_spec.updated_at_column: updated_at,
        }

    # =========================================================== requests

    def _frozen_dataset_exists(self, spec_id: str, frozen_dataset_id: str) -> bool:
        frozen = self._frozen_spec(spec_id)
        if frozen is None:
            # No frozen dataset spec configured: cannot verify, treat as present.
            return True
        df = self._read_df(frozen.table)
        if df.empty or frozen.id_column not in df.columns:
            return False
        return bool((df[frozen.id_column].astype(str) == str(frozen_dataset_id)).any())

    def create_training_request(
        self, spec_id: str, req: CreateTrainingRequestRequest
    ) -> CreateTrainingRequestResponse:
        """Create a manual training request for an experiment (spec §17)."""
        requests_spec = self._requests_spec(spec_id)
        experiment = self._load_experiment(spec_id, req.train_config_id)

        # 1. Experiment must be usable.
        if not experiment.active:
            raise TrainingExperimentError(
                "experiment_inactive",
                "Cannot create a request for an inactive (archived) experiment.",
                status_code=409,
            )

        # 2. Frozen dataset must exist.
        if not self._frozen_dataset_exists(spec_id, req.frozen_dataset_id):
            raise TrainingExperimentError(
                "frozen_dataset_not_found",
                f'Frozen dataset "{req.frozen_dataset_id}" was not found.',
                status_code=404,
            )

        # 3. Idempotency by client_request_id.
        df_req = self._read_df(requests_spec.table)
        existing = self._find_by_client_request_id(
            df_req, requests_spec, req.client_request_id
        )
        if existing is not None:
            return CreateTrainingRequestResponse(
                request=self._request_from_row(requests_spec, existing),
                launch=None,
            )

        # 4. Snapshot config + build request.
        # ``force`` bypasses max_within_time at train time (stale datasets).
        # Without force, stamp the ops-spec freshness window onto the request.
        request_id = build_manual_training_request_id()
        now = _now_iso()
        max_within_time = None if req.force else requests_spec.max_within_time
        row = {
            requests_spec.id_column: request_id,
            requests_spec.train_config_id_column: req.train_config_id,
            requests_spec.frozen_dataset_id_column: req.frozen_dataset_id,
            requests_spec.kind_column: "manual",
            requests_spec.enabled_column: True,
            requests_spec.force_column: bool(req.force),
            requests_spec.max_within_time_column: max_within_time,
            requests_spec.config_source_column: experiment.source,
            requests_spec.config_name_snapshot_column: experiment.display_name,
            requests_spec.config_params_snapshot_column: experiment.params,
            requests_spec.config_hash_column: experiment.config_hash,
            requests_spec.requested_at_column: now,
            requests_spec.requested_by_column: None,
            requests_spec.client_request_id_column: req.client_request_id,
        }
        self._write_row(requests_spec.table, row)
        request_info = self._request_from_row(requests_spec, pd.Series(row))

        launch_info: Optional[LaunchInfo] = None
        if req.launch:
            self._ensure_launch_configured(requests_spec)
            launch_info = self._launch(requests_spec, request_id)

        return CreateTrainingRequestResponse(request=request_info, launch=launch_info)

    def _find_by_client_request_id(
        self,
        df_req: pd.DataFrame,
        requests_spec: OpsTrainingRequestSpec,
        client_request_id: str,
    ) -> Optional[pd.Series]:
        if df_req.empty or requests_spec.client_request_id_column not in df_req.columns:
            return None
        match = df_req[
            df_req[requests_spec.client_request_id_column].astype(str)
            == str(client_request_id)
        ]
        if match.empty:
            return None
        return match.iloc[0]

    def _request_from_row(
        self, requests_spec: OpsTrainingRequestSpec, row: pd.Series
    ) -> TrainingRequest:
        return TrainingRequest(
            id=str(row.get(requests_spec.id_column)),
            kind=_clean_str(row.get(requests_spec.kind_column)) or "manual",
            state="queued",
            frozen_dataset_id=_clean_str(row.get(requests_spec.frozen_dataset_id_column)),
            train_config_id=_clean_str(row.get(requests_spec.train_config_id_column)) or "",
            config_name=_clean_str(row.get(requests_spec.config_name_snapshot_column)),
            config_hash=_clean_str(row.get(requests_spec.config_hash_column)),
            enabled=_clean_bool(row.get(requests_spec.enabled_column), default=True),
            force=_clean_bool(row.get(requests_spec.force_column), default=False),
            max_within_time=_clean_str(row.get(requests_spec.max_within_time_column)),
            config_source=_clean_str(row.get(requests_spec.config_source_column)),
            config_params_snapshot=_clean_dict(row.get(requests_spec.config_params_snapshot_column)),
            requested_at=_clean_str(row.get(requests_spec.requested_at_column)),
            requested_by=_clean_str(row.get(requests_spec.requested_by_column)),
            client_request_id=_clean_str(row.get(requests_spec.client_request_id_column)),
        )

    def _ensure_launch_configured(self, requests_spec: OpsTrainingRequestSpec) -> None:
        if requests_spec.run_labels:
            return
        raise TrainingExperimentError(
            "training_launch_not_configured",
            "Immediate training launch is not configured for this ops spec "
            "(set OpsTrainingRequestSpec.run_labels to a pipeline stage such as "
            'stage=train-without-freeze).',
            status_code=400,
        )

    def _launch(
        self, requests_spec: OpsTrainingRequestSpec, request_id: str
    ) -> LaunchInfo:
        if self.run_steps is None:
            raise TrainingExperimentError(
                "training_launch_failed",
                "Launching training requests is not available on this instance.",
                status_code=503,
            )
        self._ensure_launch_configured(requests_spec)
        # Never HTTP-call our own API: run the pipeline steps in-process, scoped
        # by run_labels and a primary-key filter on the request id (spec §18).
        filters: Dict[str, List[Any]] = {requests_spec.id_column: [request_id]}
        result = self.run_steps(request_id, list(requests_spec.run_labels), filters)
        return LaunchInfo(
            started=bool(result.get("started", False)),
            run_id=result.get("run_id"),
        )

    def launch_training_request(self, spec_id: str, request_id: str) -> LaunchResponse:
        """Launch the pipeline for a single training request (spec §18)."""
        requests_spec = self._requests_spec(spec_id)
        df_req = self._read_df(requests_spec.table)
        if df_req.empty or requests_spec.id_column not in df_req.columns:
            raise self._request_not_found(request_id)
        match = df_req[df_req[requests_spec.id_column].astype(str) == str(request_id)]
        if match.empty:
            raise self._request_not_found(request_id)

        launch_info = self._launch(requests_spec, request_id)
        return LaunchResponse(
            training_request_id=request_id,
            started=launch_info.started,
            run_id=launch_info.run_id,
        )

    def delete_training_request(self, spec_id: str, request_id: str) -> None:
        """Delete a manual request that has not been launched or trained yet."""
        requests_spec = self._requests_spec(spec_id)
        spec = self._spec(spec_id)
        df_req = self._read_df(requests_spec.table)
        if df_req.empty or requests_spec.id_column not in df_req.columns:
            raise self._request_not_found(request_id)
        match = df_req[df_req[requests_spec.id_column].astype(str) == str(request_id)]
        if match.empty:
            raise self._request_not_found(request_id)

        row = match.iloc[0]
        kind = _clean_str(row.get(requests_spec.kind_column)) or "manual"
        status = self._status_by_request_id(spec, requests_spec).get(request_id)
        run_key = status.get("run_key") if status else None
        model_id = status.get("model_id") if status else None
        status_value = status.get("status") if status else None
        started_at = status.get("started_at") if status else None
        if not _request_can_delete(
            kind=kind,
            run_key=run_key,
            model_id=model_id,
            status=status_value,
            started_at=started_at,
        ):
            raise TrainingExperimentError(
                "request_not_deletable",
                "Only manual training requests that have not been launched or "
                "trained can be deleted.",
                status_code=409,
                details={
                    "kind": kind,
                    "run_key": run_key,
                    "model_id": model_id,
                    "status": status_value,
                    "started_at": started_at,
                },
            )
        self._delete_id(requests_spec.table, requests_spec.id_column, request_id)

    def _request_not_found(self, request_id: str) -> TrainingExperimentError:
        return TrainingExperimentError(
            "not_found",
            f'Training request "{request_id}" was not found.',
            status_code=404,
        )


__all__ = [
    "TrainingExperimentsService",
    "RunStepsCallable",
    "build_capabilities",
]

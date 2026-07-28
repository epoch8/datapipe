from __future__ import annotations

from typing import Any, Callable, Optional, Tuple

import pandas as pd

from datapipe_ml.training.training_request import (
    REQUEST_ENABLED_COL,
    REQUEST_FORCE_COL,
    REQUEST_ID_COL,
    REQUEST_KIND_COL,
    REQUEST_MAX_WITHIN_TIME_COL,
    REQUEST_CONFIG_PARAMS_SNAPSHOT_COL,
)

# Status values that mean a manual request has already been handled and must not
# be launched again (one-shot semantics, spec §11.1 step 4).
_ONE_SHOT_TERMINAL_OR_ACTIVE = frozenset({"success", "completed", "running", "queued"})


def _is_truthy(value: Any) -> bool:
    if value is None:
        return False
    try:
        if bool(pd.isna(value)):
            return False
    except (TypeError, ValueError):
        pass
    return bool(value)


def _empty_outputs(n: int) -> Tuple[pd.DataFrame, ...]:
    return tuple(pd.DataFrame() for _ in range(n))


def _add_request_id(df: pd.DataFrame, request_id: str, request_id_col: str) -> pd.DataFrame:
    if df is None or df.empty:
        return df if df is not None else pd.DataFrame()
    df = df.copy()
    df[request_id_col] = request_id
    return df


def _filter_by_frozen_dataset_id(
    df: pd.DataFrame,
    *,
    frozen_dataset_id_col: str,
    frozen_dataset_id: Any,
) -> pd.DataFrame:
    if df is None or df.empty or frozen_dataset_id_col not in df.columns:
        return df if df is not None else pd.DataFrame()
    return df[df[frozen_dataset_id_col] == frozen_dataset_id].reset_index(drop=True)


def run_training_request(
    df_frozen_dataset: pd.DataFrame,
    df_training_request: pd.DataFrame,
    df_class_names: pd.DataFrame,
    df_resized_images: pd.DataFrame,
    df_yolo_txt: pd.DataFrame,
    *,
    train_callable: Callable[..., Tuple[pd.DataFrame, ...]],
    train_config_id_col: str,
    train_config_params_col: str,
    request_id_col: str = REQUEST_ID_COL,
    request_params_col: str = REQUEST_CONFIG_PARAMS_SNAPSHOT_COL,
    request_kind_col: str = REQUEST_KIND_COL,
    request_enabled_col: str = REQUEST_ENABLED_COL,
    request_force_col: str = REQUEST_FORCE_COL,
    request_max_within_time_col: str = REQUEST_MAX_WITHIN_TIME_COL,
    frozen_dataset_id_col: Optional[str] = None,
    dt_training_status: Any = None,
    num_outputs: int = 3,
    **train_kwargs: Any,
) -> Tuple[pd.DataFrame, ...]:
    """Adapter that runs a single training request through an existing trainer.

    Implements spec §11.1. The trainer is invoked with a compatibility
    ``df_train_config`` built from the immutable request snapshot, and
    ``training_request_id`` is stamped onto every output.
    """
    if df_training_request is None or df_training_request.empty:
        return _empty_outputs(num_outputs)
    if len(df_training_request) != 1:
        raise ValueError(
            f"run_training_request expects exactly one request row, got {len(df_training_request)}"
        )

    request = df_training_request.iloc[0]

    # 2. Disabled request -> no outputs.
    if request_enabled_col in df_training_request.columns and not _is_truthy(
        request[request_enabled_col]
    ):
        return _empty_outputs(num_outputs)

    # Scope related frames to this request's frozen dataset (request-keyed
    # transforms may otherwise join every frozen dataset into the batch).
    if frozen_dataset_id_col and frozen_dataset_id_col in df_training_request.columns:
        frozen_dataset_id = request[frozen_dataset_id_col]
        df_frozen_dataset = _filter_by_frozen_dataset_id(
            df_frozen_dataset,
            frozen_dataset_id_col=frozen_dataset_id_col,
            frozen_dataset_id=frozen_dataset_id,
        )
        df_class_names = _filter_by_frozen_dataset_id(
            df_class_names,
            frozen_dataset_id_col=frozen_dataset_id_col,
            frozen_dataset_id=frozen_dataset_id,
        )
        df_resized_images = _filter_by_frozen_dataset_id(
            df_resized_images,
            frozen_dataset_id_col=frozen_dataset_id_col,
            frozen_dataset_id=frozen_dataset_id,
        )
        df_yolo_txt = _filter_by_frozen_dataset_id(
            df_yolo_txt,
            frozen_dataset_id_col=frozen_dataset_id_col,
            frozen_dataset_id=frozen_dataset_id,
        )

    # 3. Request id.
    request_id = request[request_id_col]
    kind = request.get(request_kind_col)

    # 4. Manual request is one-shot: skip if already handled.
    if kind == "manual" and dt_training_status is not None:
        df_status = dt_training_status.get_data()
        if (
            df_status is not None
            and not df_status.empty
            and request_id_col in df_status.columns
            and "training_status__status" in df_status.columns
        ):
            prior = df_status[df_status[request_id_col] == request_id]
            statuses = {
                str(s).lower() for s in prior["training_status__status"].dropna().tolist()
            }
            if statuses & _ONE_SHOT_TERMINAL_OR_ACTIVE:
                return _empty_outputs(num_outputs)

    # 5. Compatibility train config dataframe from the immutable snapshot.
    df_train_config = pd.DataFrame(
        [
            {
                train_config_id_col: request[train_config_id_col],
                train_config_params_col: request[request_params_col],
            }
        ]
    )

    # 6-8. Determine force / max_within_time from the request row.
    # Manual UI requests set ``force=True`` to bypass dataset freshness
    # (``max_within_time``); otherwise the age check applies like auto requests.
    force_training = _is_truthy(request.get(request_force_col))
    raw_max = request.get(request_max_within_time_col)
    max_within_time: Optional[str] = None if _is_missing_scalar(raw_max) or force_training else raw_max

    outputs = train_callable(
        df_frozen_dataset=df_frozen_dataset,
        df_training_request=df_training_request,
        df_train_config=df_train_config,
        df_class_names=df_class_names,
        df_resized_images=df_resized_images,
        df_yolo_txt=df_yolo_txt,
        max_within_time=max_within_time,
        force_training=force_training,
        **train_kwargs,
    )

    if not isinstance(outputs, (list, tuple)):
        outputs = (outputs,)

    # 9. Stamp request id onto all outputs.
    stamped = tuple(_add_request_id(df, request_id, request_id_col) for df in outputs)
    return stamped


def _is_missing_scalar(value: Any) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False

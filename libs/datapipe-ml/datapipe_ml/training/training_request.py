from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

import pandas as pd
from datapipe.datatable import DataStore, DataTable
from datapipe.run_config import RunConfig
from datapipe.types import IndexDF

from datapipe_ml.training.train_config_id import (
    build_auto_training_request_id,
    hash_train_config_params,
)

# Standard column names for a training request row (spec §3.2).
REQUEST_ID_COL = "training_request_id"
REQUEST_KIND_COL = "training_request__kind"
REQUEST_ENABLED_COL = "training_request__enabled"
REQUEST_FORCE_COL = "training_request__force"
REQUEST_MAX_WITHIN_TIME_COL = "training_request__max_within_time"
REQUEST_CONFIG_SOURCE_COL = "training_request__config_source"
REQUEST_CONFIG_NAME_SNAPSHOT_COL = "training_request__config_name_snapshot"
REQUEST_CONFIG_PARAMS_SNAPSHOT_COL = "training_request__config_params_snapshot"
REQUEST_CONFIG_HASH_COL = "training_request__config_hash"
REQUEST_REQUESTED_AT_COL = "training_request__requested_at"
REQUEST_REQUESTED_BY_COL = "training_request__requested_by"
REQUEST_CLIENT_REQUEST_ID_COL = "training_request__client_request_id"


def _auto_request_columns(frozen_dataset_id_col: str, train_config_id_col: str) -> List[str]:
    return [
        REQUEST_ID_COL,
        frozen_dataset_id_col,
        train_config_id_col,
        REQUEST_KIND_COL,
        REQUEST_ENABLED_COL,
        REQUEST_FORCE_COL,
        REQUEST_MAX_WITHIN_TIME_COL,
        REQUEST_CONFIG_SOURCE_COL,
        REQUEST_CONFIG_NAME_SNAPSHOT_COL,
        REQUEST_CONFIG_PARAMS_SNAPSHOT_COL,
        REQUEST_CONFIG_HASH_COL,
    ]


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def build_auto_training_request(
    df_frozen_dataset: pd.DataFrame,
    df_train_config: pd.DataFrame,
    *,
    frozen_dataset_id_col: str,
    train_config_id_col: str,
    train_config_params_col: str,
    train_config_type: str,
    max_within_time: str,
    config_source: str = "default",
) -> pd.DataFrame:
    """Materialize the automatic training request for one dataset/default-config pair.

    The caller is expected to feed rows from ``default_train_config`` only.
    Custom configs live in a separate table and are requested via the Ops API.
    """
    columns = _auto_request_columns(frozen_dataset_id_col, train_config_id_col)
    empty = pd.DataFrame(columns=columns)
    if df_frozen_dataset.empty or df_train_config.empty:
        return empty

    config_row = df_train_config.iloc[0]
    fd_row = df_frozen_dataset.iloc[0]
    frozen_dataset_id = fd_row[frozen_dataset_id_col]

    params = config_row[train_config_params_col]
    config_hash = config_row.get("train_config__config_hash")
    if _is_missing(config_hash):
        config_hash = hash_train_config_params(params, length=40)

    request_id = build_auto_training_request_id(
        frozen_dataset_id=str(frozen_dataset_id),
        config_hash=str(config_hash),
        config_type=train_config_type,
    )
    row = {
        REQUEST_ID_COL: request_id,
        frozen_dataset_id_col: frozen_dataset_id,
        train_config_id_col: config_row[train_config_id_col],
        REQUEST_KIND_COL: "auto",
        REQUEST_ENABLED_COL: True,
        REQUEST_FORCE_COL: False,
        REQUEST_MAX_WITHIN_TIME_COL: max_within_time,
        REQUEST_CONFIG_SOURCE_COL: config_source,
        REQUEST_CONFIG_NAME_SNAPSHOT_COL: config_row.get("train_config__display_name"),
        REQUEST_CONFIG_PARAMS_SNAPSHOT_COL: params,
        REQUEST_CONFIG_HASH_COL: config_hash,
    }
    return pd.DataFrame([row], columns=columns)


def make_build_auto_training_request(
    *,
    frozen_dataset_id_col: str,
    train_config_id_col: str,
    train_config_params_col: str,
    train_config_type: str,
    max_within_time: str,
    config_source: str = "default",
) -> Callable[..., pd.DataFrame]:
    """Wrap :func:`build_auto_training_request` as a datatable-transform func.

    The returned callable matches ``DatatableBatchTransformFunc``:
    ``func(ds, idx, input_dts, run_config, kwargs)`` where
    ``input_dts == [frozen_dataset, default_train_config]``.
    """

    def build_auto_training_request_step(
        ds: DataStore,
        idx: IndexDF,
        input_dts: List[DataTable],
        run_config: Optional[RunConfig] = None,
        kwargs: Optional[Dict[str, Any]] = None,
    ) -> pd.DataFrame:
        dt__frozen_dataset, dt__train_config = input_dts
        df_frozen_dataset = dt__frozen_dataset.get_data(idx)
        df_train_config = dt__train_config.get_data(idx)
        return build_auto_training_request(
            df_frozen_dataset,
            df_train_config,
            frozen_dataset_id_col=frozen_dataset_id_col,
            train_config_id_col=train_config_id_col,
            train_config_params_col=train_config_params_col,
            train_config_type=train_config_type,
            max_within_time=max_within_time,
            config_source=config_source,
        )

    return build_auto_training_request_step

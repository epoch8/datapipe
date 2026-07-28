"""Shared catalog schemas and pipeline steps for train experiments.

Architecture (all training backends share this layout):

* ``default_train_config`` — owned by the pipeline via ``BatchGenerate``
  (code-listed presets). Full-table ownership is fine: ``delete_stale`` may
  remove removed presets.
* ``custom_train_config`` — owned by the Ops API / external ``store_chunk``.
  The pipeline never writes this table.
* ``train_experiment_request`` — one row per ``(frozen_dataset_id, train_config_id)``
  that should train. Auto rows are materialized from ``default_train_config``
  (with ``max_within_time`` stamped on the request). Manual / custom rows are
  inserted by the API. Because auto materialization joins only the *default*
  config table, custom request rows are never deleted by the pipeline.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Sequence

from datapipe.compute import Catalog, Table
from datapipe.datatable import DataStore
from datapipe.step.batch_generate import BatchGenerate
from datapipe.step.batch_transform import DatatableBatchTransform
from datapipe.store.database import TableStoreDB
from datapipe.types import Labels
from sqlalchemy import JSON, Boolean, Column, DateTime
from sqlalchemy.sql.sqltypes import Integer, String

from datapipe_ml.training.training_request import make_build_auto_training_request


@dataclass(frozen=True)
class TrainExperimentTableNames:
    """Logical table names for one training architecture."""

    default_train_config: str
    custom_train_config: str
    train_experiment_request: str


def default_train_config_schema(
    *,
    train_config_id_col: str,
    train_config_params_col: str,
    params_type=JSON,
) -> List[Column]:
    """Schema for pipeline-owned default / built-in train configs."""
    return [
        Column(train_config_id_col, String, primary_key=True),
        Column(train_config_params_col, params_type),
        Column("train_config__display_name", String, nullable=False),
        Column("train_config__description", String, nullable=True),
        Column("train_config__config_type", String, nullable=False),
        Column("train_config__config_hash", String, nullable=False),
        Column("train_config__revision", Integer, nullable=False),
    ]


def custom_train_config_schema(
    *,
    train_config_id_col: str,
    train_config_params_col: str,
    params_type=JSON,
) -> List[Column]:
    """Schema for API-owned custom train configs / experiments."""
    return [
        Column(train_config_id_col, String, primary_key=True),
        Column(train_config_params_col, params_type),
        Column("train_config__display_name", String, nullable=False),
        Column("train_config__description", String, nullable=True),
        Column("train_config__config_type", String, nullable=False),
        Column("train_config__config_hash", String, nullable=False),
        Column("train_config__is_active", Boolean, nullable=False),
        Column("train_config__created_at", DateTime, nullable=False),
        Column("train_config__updated_at", DateTime, nullable=False),
        Column("train_config__revision", Integer, nullable=False),
    ]


def train_experiment_request_schema(
    *,
    frozen_dataset_id_col: str,
    train_config_id_col: str,
    params_type=JSON,
) -> List[Column]:
    """Schema for auto + manual training experiment requests."""
    return [
        Column("training_request_id", String, primary_key=True),
        Column(frozen_dataset_id_col, String),
        Column(train_config_id_col, String),
        Column("training_request__kind", String),
        Column("training_request__enabled", Boolean),
        Column("training_request__force", Boolean),
        Column("training_request__max_within_time", String, nullable=True),
        Column("training_request__config_source", String),
        Column("training_request__config_name_snapshot", String),
        Column("training_request__config_params_snapshot", params_type),
        Column("training_request__config_hash", String),
        Column("training_request__requested_at", DateTime, nullable=True),
        Column("training_request__requested_by", String, nullable=True),
        Column("training_request__client_request_id", String, nullable=True),
    ]


def register_train_experiment_tables(
    *,
    ds: DataStore,
    catalog: Catalog,
    tables: TrainExperimentTableNames,
    train_config_id_col: str,
    train_config_params_col: str,
    frozen_dataset_id_col: str,
    create_table: bool,
    params_type=JSON,
) -> None:
    """Register default / custom config + experiment-request tables on ``catalog``."""
    for name, schema in (
        (
            tables.default_train_config,
            default_train_config_schema(
                train_config_id_col=train_config_id_col,
                train_config_params_col=train_config_params_col,
                params_type=params_type,
            ),
        ),
        (
            tables.custom_train_config,
            custom_train_config_schema(
                train_config_id_col=train_config_id_col,
                train_config_params_col=train_config_params_col,
                params_type=params_type,
            ),
        ),
        (
            tables.train_experiment_request,
            train_experiment_request_schema(
                frozen_dataset_id_col=frozen_dataset_id_col,
                train_config_id_col=train_config_id_col,
                params_type=params_type,
            ),
        ),
    ):
        catalog.add_datatable(
            name,
            Table(
                ds.get_or_create_table(
                    name,
                    TableStoreDB(
                        dbconn=ds.meta_dbconn,
                        name=name,
                        data_sql_schema=schema,
                        create_table=create_table,
                    ),
                ).table_store
            ),
        )


def build_default_train_config_generate(
    *,
    func: Callable[..., Any],
    default_train_config_table: str,
    kwargs: Optional[Dict[str, Any]] = None,
    labels: Optional[Labels] = None,
    name: Optional[str] = None,
) -> BatchGenerate:
    """Pipeline step that fully owns ``default_train_config``."""
    return BatchGenerate(
        func=func,
        outputs=[default_train_config_table],
        kwargs=kwargs,
        labels=labels,
        name=name,
        delete_stale=True,
    )


def build_auto_experiment_request_transform(
    *,
    frozen_dataset_table: str,
    default_train_config_table: str,
    train_experiment_request_table: str,
    frozen_dataset_id_col: str,
    train_config_id_col: str,
    train_config_params_col: str,
    train_config_type: str,
    max_within_time: str,
    labels: Optional[Labels] = None,
    name: Optional[str] = None,
    transform_keys: Optional[Sequence[str]] = None,
) -> DatatableBatchTransform:
    """Materialize auto requests from frozen datasets × default configs only.

    Custom configs live in a separate table and are never inputs here, so
    API-owned request rows are left untouched.
    """
    return DatatableBatchTransform(
        func=make_build_auto_training_request(
            frozen_dataset_id_col=frozen_dataset_id_col,
            train_config_id_col=train_config_id_col,
            train_config_params_col=train_config_params_col,
            train_config_type=train_config_type,
            max_within_time=max_within_time,
            config_source="default",
        ),
        inputs=[frozen_dataset_table, default_train_config_table],
        outputs=[train_experiment_request_table],
        transform_keys=list(
            transform_keys
            if transform_keys is not None
            else [frozen_dataset_id_col, train_config_id_col]
        ),
        labels=labels,
        name=name,
    )

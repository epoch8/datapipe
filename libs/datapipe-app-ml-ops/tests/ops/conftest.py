from __future__ import annotations

import tempfile
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd
import pytest
from datapipe.compute import Catalog, Table
from datapipe.datatable import DataStore
from datapipe.store.database import DBConn, TableStoreDB
from sqlalchemy import JSON, Boolean, Column, Integer, String

from datapipe_app_ml_ops.ops.ops_specs import (
    DatapipeOpsSpec,
    OpsColumn,
    OpsDataSpec,
    OpsFrozenDatasetSpec,
    OpsModelSpec,
    OpsTrainConfigRegistrySpec,
    OpsTrainingRequestSpec,
    OpsTrainingSpec,
)
from datapipe_app.ops.specs import OpsRelationSpec
from datapipe_app_ml_ops.ops.spec_registry import OpsSpecRegistry
from datapipe_ml.training import config_codec

REGISTRY_TABLE = "train_config"
REQUESTS_TABLE = "training_request"
FROZEN_TABLE = "frozen_dataset"
STATUS_TABLE = "training_status"
LINK_TABLE = "model_is_trained_on_frozen_dataset"

SPEC_ID = "detection"

# The real yolov8 codec pulls the optional torch/ultralytics extra, which is not
# installed in CI. Register a lightweight stand-in with the same public contract
# when the real codec is unavailable so codec-dependent paths can be tested.
_YOLO_FIELDS = {
    "model",
    "imgsz",
    "batch",
    "epochs",
    "optimizer",
    "lr0",
    "weight_decay",
    "seed",
    "patience",
}


class _StubYoloV8Codec:
    config_type = "yolov8_detection"

    def validate(self, params):
        errors = [
            {"field": key, "message": f"Unknown field: {key!r}"}
            for key in params
            if key not in _YOLO_FIELDS
        ]
        if errors:
            raise config_codec.TrainConfigValidationError(
                "Invalid train config parameters", errors=errors
            )
        return dict(params)

    def normalize(self, params):
        return self.validate(params)

    def json_schema(self):
        return {"type": "object", "properties": {f: {"type": "string"} for f in _YOLO_FIELDS}}

    def summarize(self, params):
        return {"display": f"{params.get('model')} · {params.get('epochs')} epochs"}


def _ensure_yolo_codec() -> None:
    try:
        config_codec.get_train_config_codec("yolov8_detection")
    except ValueError:
        config_codec.register_train_config_codec(_StubYoloV8Codec())


_ensure_yolo_codec()


class RunStepsSpy:
    """Mock ``run_steps`` callable that records its arguments (spec §35)."""

    def __init__(self, result: Optional[Dict[str, Any]] = None) -> None:
        self.calls: List[Tuple[str, Sequence[Tuple[str, str]], Dict[str, List[Any]]]] = []
        self.result = result or {"started": True, "run_id": "run-123"}

    def __call__(
        self,
        request_id: str,
        run_labels: Sequence[Tuple[str, str]],
        filters: Dict[str, List[Any]],
    ) -> Dict[str, Any]:
        self.calls.append((request_id, list(run_labels), dict(filters)))
        return self.result


@dataclass
class Env:
    ds: DataStore
    catalog: Catalog
    registry: OpsSpecRegistry
    run_steps: RunStepsSpy
    spec_id: str = SPEC_ID

    def write(self, table: str, rows: List[Dict[str, Any]]) -> None:
        dt = self.catalog.get_datatable(self.ds, table)
        dt.store_chunk(pd.DataFrame(rows))

    def read(self, table: str) -> pd.DataFrame:
        return self.catalog.get_datatable(self.ds, table).get_data()

    def write_builtin_config(
        self,
        config_id: str = "builtin-1",
        *,
        display_name: str = "Standard",
        params: Optional[Dict[str, Any]] = None,
        active: bool = True,
    ) -> str:
        self.write(
            REGISTRY_TABLE,
            [
                {
                    "train_config_id": config_id,
                    "train_config__params": params or {"model": "yolov8n.pt", "epochs": 5},
                    "train_config__source": "builtin",
                    "train_config__display_name": display_name,
                    "train_config__description": None,
                    "train_config__config_type": "yolov8_detection",
                    "train_config__config_hash": "builtinhash",
                    "train_config__is_active": active,
                    "train_config__revision": 1,
                    "train_config__created_at": "2026-01-01T00:00:00+00:00",
                    "train_config__updated_at": "2026-01-01T00:00:00+00:00",
                }
            ],
        )
        return config_id

    def write_frozen_dataset(self, frozen_dataset_id: str = "fd-1") -> str:
        self.write(
            FROZEN_TABLE,
            [
                {
                    "frozen_dataset_id": frozen_dataset_id,
                    "frozen_dataset__created_at": "2026-01-01T00:00:00+00:00",
                }
            ],
        )
        return frozen_dataset_id


def _build_catalog(dbconn: DBConn) -> Catalog:
    return Catalog(
        {
            REGISTRY_TABLE: Table(
                TableStoreDB(
                    name=REGISTRY_TABLE,
                    dbconn=dbconn,
                    data_sql_schema=[
                        Column("train_config_id", String(), primary_key=True),
                        Column("train_config__params", JSON()),
                        Column("train_config__source", String()),
                        Column("train_config__display_name", String()),
                        Column("train_config__description", String()),
                        Column("train_config__config_type", String()),
                        Column("train_config__config_hash", String()),
                        Column("train_config__is_active", Boolean()),
                        Column("train_config__revision", Integer()),
                        Column("train_config__created_at", String()),
                        Column("train_config__updated_at", String()),
                    ],
                    create_table=True,
                )
            ),
            REQUESTS_TABLE: Table(
                TableStoreDB(
                    name=REQUESTS_TABLE,
                    dbconn=dbconn,
                    data_sql_schema=[
                        Column("training_request_id", String(), primary_key=True),
                        Column("train_config_id", String()),
                        Column("frozen_dataset_id", String()),
                        Column("training_request__kind", String()),
                        Column("training_request__enabled", Boolean()),
                        Column("training_request__force", Boolean()),
                        Column("training_request__max_within_time", String()),
                        Column("training_request__config_source", String()),
                        Column("training_request__config_name_snapshot", String()),
                        Column("training_request__config_params_snapshot", JSON()),
                        Column("training_request__config_hash", String()),
                        Column("training_request__requested_at", String()),
                        Column("training_request__requested_by", String()),
                        Column("training_request__client_request_id", String()),
                    ],
                    create_table=True,
                )
            ),
            FROZEN_TABLE: Table(
                TableStoreDB(
                    name=FROZEN_TABLE,
                    dbconn=dbconn,
                    data_sql_schema=[
                        Column("frozen_dataset_id", String(), primary_key=True),
                        Column("frozen_dataset__created_at", String()),
                    ],
                    create_table=True,
                )
            ),
            STATUS_TABLE: Table(
                TableStoreDB(
                    name=STATUS_TABLE,
                    dbconn=dbconn,
                    data_sql_schema=[
                        Column("run_key", String(), primary_key=True),
                        Column("training_request_id", String()),
                    ],
                    create_table=True,
                )
            ),
            LINK_TABLE: Table(
                TableStoreDB(
                    name=LINK_TABLE,
                    dbconn=dbconn,
                    data_sql_schema=[
                        Column("model_id", String(), primary_key=True),
                        Column("frozen_dataset_id", String(), primary_key=True),
                        Column("train_config_id", String()),
                        Column("created_at", String()),
                    ],
                    create_table=True,
                )
            ),
        }
    )


def _build_spec() -> DatapipeOpsSpec:
    return DatapipeOpsSpec(
        id=SPEC_ID,
        title="Detection",
        description="Detection training experiments",
        data=OpsDataSpec(
            tables=[REGISTRY_TABLE, REQUESTS_TABLE, FROZEN_TABLE, STATUS_TABLE, LINK_TABLE]
        ),
        frozen_dataset=OpsFrozenDatasetSpec(
            table=FROZEN_TABLE,
            id_column="frozen_dataset_id",
            created_at_column="frozen_dataset__created_at",
            display_name_column=None,
            columns=[OpsColumn("frozen_dataset_id", "Dataset", "frozen_dataset_id")],
            models_count_relation_id="model_trained_on_frozen_dataset",
        ),
        model=OpsModelSpec(
            table=LINK_TABLE,
            id_column="model_id",
        ),
        relations=[
            OpsRelationSpec(
                id="model_trained_on_frozen_dataset",
                table=LINK_TABLE,
                from_entity="model",
                from_column="model_id",
                to_entity="frozen_dataset",
                to_column="frozen_dataset_id",
            )
        ],
        training=OpsTrainingSpec(
            status_table=STATUS_TABLE,
            columns=[OpsColumn("run_key", "Run", "run_key")],
            experiments=OpsTrainConfigRegistrySpec(
                table=REGISTRY_TABLE,
                id_column="train_config_id",
                params_column="train_config__params",
                config_type="yolov8_detection",
            ),
            requests=OpsTrainingRequestSpec(
                table=REQUESTS_TABLE,
                id_column="training_request_id",
                train_config_id_column="train_config_id",
                frozen_dataset_id_column="frozen_dataset_id",
                run_labels=[("stage", "train")],
                status_table=STATUS_TABLE,
                status_request_id_column="training_request_id",
            ),
        ),
    )


@pytest.fixture
def env():
    with tempfile.TemporaryDirectory() as tmpdir:
        dbconn = DBConn(f"sqlite+pysqlite3:///{tmpdir}/store.sqlite")
        ds = DataStore(dbconn, create_meta_table=True)
        catalog = _build_catalog(dbconn)
        # Materialize datatables / meta.
        for name in (REGISTRY_TABLE, REQUESTS_TABLE, FROZEN_TABLE, STATUS_TABLE, LINK_TABLE):
            catalog.get_datatable(ds, name)
        registry = OpsSpecRegistry()
        registry.add_many([_build_spec()])
        yield Env(ds=ds, catalog=catalog, registry=registry, run_steps=RunStepsSpy())


@dataclass
class FullApiEnv:
    api: Any
    ds: DataStore
    catalog: Catalog
    client: Any
    spec_id: str = SPEC_ID

    def write(self, table: str, rows: List[Dict[str, Any]]) -> None:
        self.catalog.get_datatable(self.ds, table).store_chunk(pd.DataFrame(rows))


@pytest.fixture
def full_api_env(agent_env):
    """A real ``DatapipeAPI`` with the training spec registered end-to-end.

    Exercises the v1alpha3 entry-point extension wiring (spec §20), including
    passing the real ``run_steps`` callable.
    """
    from fastapi.testclient import TestClient

    from datapipe.compute import Pipeline
    from datapipe_app import DatapipeAPI

    with tempfile.TemporaryDirectory() as tmpdir:
        dbconn = DBConn(f"sqlite+pysqlite3:///{tmpdir}/store.sqlite")
        ds = DataStore(dbconn, create_meta_table=True)
        catalog = _build_catalog(dbconn)
        for name in (REGISTRY_TABLE, REQUESTS_TABLE, FROZEN_TABLE, STATUS_TABLE, LINK_TABLE):
            catalog.get_datatable(ds, name)
        api = DatapipeAPI(ds, catalog, Pipeline([]), pipeline_id="test_pipeline")
        api.add_specs([_build_spec()])
        yield FullApiEnv(api=api, ds=ds, catalog=catalog, client=TestClient(api))

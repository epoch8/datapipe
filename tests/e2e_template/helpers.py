from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path
from typing import Literal

import pytest
from datapipe.compute import DatapipeApp, run_steps
from datapipe.cli import filter_steps_by_labels_and_name
from datapipe_ml.utils.image_data_stores import _reset_fiftyone_dataset_store_registry
from sqlalchemy import create_engine, text
_TEMPLATE_DIRS = {
    "detection": "image_detection",
    "keypoints": "image_keypoints",
}
_SCHEMA_ENV_VARS = {
    "detection": "DB_SCHEMA_DETECTION",
    "keypoints": "DB_SCHEMA_KEYPOINTS",
}
_SCHEMA_DEFAULTS = {
    "detection": "datapipe_e2e_detection",
    "keypoints": "datapipe_e2e_keypoints",
}


def pipeline_db_schema(pipeline_name: Literal["detection", "keypoints"]) -> str:
    env_var = _SCHEMA_ENV_VARS[pipeline_name]
    return os.environ.get(env_var, _SCHEMA_DEFAULTS[pipeline_name])


def template_dir(pipeline_name: Literal["detection", "keypoints"]) -> Path:
    return Path(__file__).resolve().parents[2] / "examples" / "e2e_template" / _TEMPLATE_DIRS[pipeline_name]


def require_postgres() -> None:
    os.environ.setdefault("DB_URL", "postgresql+psycopg://postgres:password@localhost:5432/postgres")
    try:
        with create_engine(os.environ["DB_URL"]).connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception:
        pytest.skip("Postgres is not available")


def load_template_module(pipeline_name: Literal["detection", "keypoints"], module: str):
    os.environ.setdefault("DB_URL", "postgresql+psycopg://postgres:password@localhost:5432/postgres")
    if module in ("app", "data"):
        require_postgres()
        ensure_postgres_schema(pipeline_name)

    path_str = str(template_dir(pipeline_name))
    for stale in ("app", "steps", "config", "data"):
        sys.modules.pop(stale, None)
    if module in ("app", "data"):
        _reset_fiftyone_dataset_store_registry()
    sys.path.insert(0, path_str)
    try:
        return importlib.import_module(module)
    finally:
        sys.path.remove(path_str)


def load_template_app(pipeline_name: Literal["detection", "keypoints"]) -> DatapipeApp:
    return load_template_module(pipeline_name, "app").app


def ensure_postgres_schema(pipeline_name: Literal["detection", "keypoints"]) -> None:
    schema = pipeline_db_schema(pipeline_name)
    with create_engine(os.environ["DB_URL"]).begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))


def ensure_template_db(pipeline_name: Literal["detection", "keypoints"]) -> DatapipeApp:
    require_postgres()
    ensure_postgres_schema(pipeline_name)
    app = load_template_app(pipeline_name)
    app.ds.meta_dbconn.sqla_metadata.create_all(app.ds.meta_dbconn.con)
    return app


def run_template_stage(pipeline_name: Literal["detection", "keypoints"], stage: str) -> DatapipeApp:
    app = ensure_template_db(pipeline_name)
    steps = filter_steps_by_labels_and_name(app, labels=[("stage", stage)])
    if not steps:
        raise AssertionError(f"No steps with label stage={stage} in {pipeline_name} template")
    run_steps(app.ds, steps)
    return app

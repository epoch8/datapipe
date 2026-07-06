from types import ModuleType
from unittest.mock import MagicMock

from datapipe.compute import Catalog, DataStore, Pipeline
from datapipe.store.database import DBConn

from datapipe_app.observability.settings import (
    pipeline_id_from_module,
    pipeline_id_from_spec,
    resolve_ops_settings,
)


def test_pipeline_id_from_spec_qualified_module():
    assert pipeline_id_from_spec("image_detection.app:app") == "image_detection_app"
    assert pipeline_id_from_spec("my.pipeline.module:app") == "my_pipeline_module"


def test_pipeline_id_from_spec_simple_module():
    assert pipeline_id_from_spec("app:app") == "app"
    assert pipeline_id_from_spec("app") == "app"


def test_pipeline_id_from_module():
    module = ModuleType("app")
    module.__file__ = "/srv/image_detection/app.py"
    assert pipeline_id_from_module(module) == "app"


def test_resolve_ops_settings_from_pipeline_module(monkeypatch):
    monkeypatch.delenv("DATAPIPE_APP_PIPELINE_ID", raising=False)
    monkeypatch.delenv("DATAPIPE_APP_OBSERVABILITY_DB_URL", raising=False)

    dbconn = DBConn("sqlite+pysqlite3:///:memory:", "my_schema")
    ds = DataStore(dbconn, create_meta_table=False)
    module = ModuleType("pipeline")
    module.__file__ = "/srv/examples/my_pipeline/app.py"

    settings = resolve_ops_settings(ds=ds, pipeline_module=module)

    assert settings.mode == "agent"
    assert settings.pipeline_id == "app"
    assert settings.observability_db_url == dbconn.connstr


def test_resolve_ops_settings_env_overrides(monkeypatch):
    monkeypatch.setenv("DATAPIPE_APP_PIPELINE_ID", "from_env")
    monkeypatch.setenv("DATAPIPE_APP_OBSERVABILITY_DB_URL", "sqlite:///override.db")

    dbconn = DBConn("sqlite+pysqlite3:///:memory:", "my_schema")
    ds = DataStore(dbconn, create_meta_table=False)
    module = MagicMock()
    module.__file__ = "/srv/examples/my_pipeline/app.py"

    settings = resolve_ops_settings(
        ds=ds,
        pipeline_module=module,
        pipeline_spec="image_detection.app:app",
    )

    assert settings.pipeline_id == "from_env"
    assert settings.observability_db_url == "sqlite:///override.db"

import pytest
from types import ModuleType
from unittest.mock import MagicMock

from datapipe.compute import Catalog, DataStore, Pipeline
from datapipe.store.database import DBConn

from datapipe_app.observability.config.settings import (
    pipeline_id_from_module,
    pipeline_id_from_spec,
    pipeline_module_from_caller,
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

    module = ModuleType("pipeline")
    module.__file__ = "/srv/examples/my_pipeline/app.py"

    settings = resolve_ops_settings(pipeline_module=module)

    assert settings.pipeline_id == "app"


def test_resolve_ops_settings_env_overrides(monkeypatch):
    monkeypatch.setenv("DATAPIPE_APP_PIPELINE_ID", "from_env")

    module = MagicMock()
    module.__file__ = "/srv/examples/my_pipeline/app.py"

    settings = resolve_ops_settings(
        pipeline_module=module,
        pipeline_spec="image_detection.app:app",
    )

    assert settings.pipeline_id == "from_env"


def test_resolve_ops_settings_constructor_overrides_env(monkeypatch):
    monkeypatch.setenv("DATAPIPE_APP_PIPELINE_ID", "from_env")

    settings = resolve_ops_settings(pipeline_id="from-constructor")

    assert settings.pipeline_id == "from-constructor"


def test_datapipe_api_accepts_run_logs_backend_constructor(tmp_path, monkeypatch):
    from test_clickhouse_run_logs import FakeClickHouseClient

    monkeypatch.setattr(
        "datapipe_app.observability.run_logs.store._create_clickhouse_client",
        lambda _url: FakeClickHouseClient(),
    )

    from datapipe.compute import Catalog, Pipeline
    from datapipe_app import DatapipeAPI, RunLogsBackend

    dbconn = DBConn(f"sqlite+pysqlite3:///{tmp_path / 'pipeline.db'}", None)
    ds = DataStore(dbconn, create_meta_table=False)
    catalog = Catalog({})
    pipeline = Pipeline([])
    run_logs_backend = RunLogsBackend.clickhouse("clickhouse://default:@localhost:8123/default")

    api = DatapipeAPI(
        ds,
        catalog,
        pipeline,
        pipeline_id="test_pipeline",
        run_logs_backend=run_logs_backend,
    )

    assert api.run_logs_backend is run_logs_backend
    assert api.observability_dbconn is ds.meta_dbconn
    assert api.observability_store.use_external_run_logs is True


def test_datapipe_api_defaults_observability_dbconn_to_ds(tmp_path):
    from datapipe.compute import Catalog, Pipeline
    from datapipe_app import DatapipeAPI

    dbconn = DBConn(f"sqlite+pysqlite3:///{tmp_path / 'pipeline.db'}", None)
    ds = DataStore(dbconn, create_meta_table=False)
    catalog = Catalog({})
    pipeline = Pipeline([])

    api = DatapipeAPI(ds, catalog, pipeline, pipeline_id="test_pipeline")

    assert api.observability_dbconn.connstr == ds.meta_dbconn.connstr
    assert api.observability_dbconn.schema == ds.meta_dbconn.schema


def test_datapipe_api_rejects_wrapping_datapipe_api(tmp_path):
    from datapipe.compute import Catalog, Pipeline
    from datapipe_app import DatapipeAPI

    dbconn = DBConn(f"sqlite+pysqlite3:///{tmp_path / 'pipeline.db'}", None)
    ds = DataStore(dbconn, create_meta_table=False)
    catalog = Catalog({})
    pipeline = Pipeline([])

    existing = DatapipeAPI(ds, catalog, pipeline, pipeline_id="test_pipeline")

    with pytest.raises(TypeError, match="Cannot wrap DatapipeAPI"):
        DatapipeAPI(app=existing, pipeline_spec="app:app")


def test_pipeline_module_from_caller_skips_datapipe_package(monkeypatch):
    datapipe_pkg = ModuleType("datapipe")
    datapipe_pkg.__file__ = "/venv/lib/python3.12/site-packages/datapipe/__init__.py"
    app_module = ModuleType("app")
    app_module.__file__ = "/srv/examples/detection_tags/detection/app.py"

    frames = [
        MagicMock(frame=MagicMock(f_globals={"__name__": "datapipe_app.observability.config.settings"})),
        MagicMock(frame=MagicMock(f_globals={"__name__": "datapipe.cli"})),
        MagicMock(frame=MagicMock(f_globals={"__name__": "app"})),
    ]

    def fake_getmodule(frame):
        name = frame.f_globals["__name__"]
        if name == "datapipe.cli":
            return datapipe_pkg
        if name == "app":
            return app_module
        return ModuleType("datapipe_app.observability.config.settings")

    monkeypatch.setattr("datapipe_app.observability.config.settings.inspect.stack", lambda: frames)
    monkeypatch.setattr("datapipe_app.observability.config.settings.inspect.getmodule", fake_getmodule)

    found = pipeline_module_from_caller()
    assert found is app_module
    assert pipeline_id_from_module(found) == "app"

from __future__ import annotations

import importlib
import sys
from typing import Any

import pytest
from click.testing import CliRunner
from sqlalchemy import Column, Integer

from datapipe.cli import cli, run_cli
from datapipe.compute import Catalog, DatapipeApp, Pipeline, Table
from datapipe.datatable import DataStore
from datapipe.store.database import DBConn, TableStoreDB

TEST_SCHEMA = [
    Column("id", Integer, primary_key=True),
    Column("value", Integer),
]


@pytest.fixture(autouse=True)
def no_global_tracing(monkeypatch: pytest.MonkeyPatch) -> None:
    """The `cli` group calls `setup_tracing`, which sets the global tracer
    provider and instruments SQLAlchemy for the whole process. That is right
    for a real CLI process and wrong for a test session -- it leaks into every
    later test (see tests/test_database_tracing.py). None of these tests are
    about tracing.
    """
    monkeypatch.setattr("datapipe.cli.setup_tracing", lambda **kwargs: None)


@pytest.fixture
def app() -> DatapipeApp:
    # Deliberately not the `dbconn` fixture from conftest: that one talks to
    # Postgres unless TEST_DB_ENV=sqlite, and these tests never read any data.
    if sys.platform == "darwin":
        dbconn = DBConn("sqlite:///:memory:")
    else:
        dbconn = DBConn("sqlite+pysqlite3:///:memory:")

    ds = DataStore(dbconn, create_meta_table=True)
    catalog = Catalog(
        {"bound_table": Table(store=TableStoreDB(dbconn, "bound_table_data", TEST_SCHEMA, True))},
    )

    return DatapipeApp(ds, catalog, Pipeline([]))


def test_run_cli_uses_the_bound_app(app: DatapipeApp) -> None:
    result = CliRunner().invoke(cli, ["table", "list"], obj={"pipeline": app})

    assert result.exit_code == 0, result.output
    assert "bound_table" in result.output


def test_run_cli_does_not_touch_sys_path(app: DatapipeApp) -> None:
    before = list(sys.path)

    result = CliRunner().invoke(cli, ["table", "list"], obj={"pipeline": app})

    assert result.exit_code == 0, result.output
    assert sys.path == before


def test_pipeline_option_is_rejected_when_an_app_is_bound(app: DatapipeApp) -> None:
    result = CliRunner().invoke(
        cli,
        ["--pipeline", "somewhere:app", "table", "list"],
        obj={"pipeline": app},
    )

    assert result.exit_code == 2
    assert "--pipeline cannot be used" in result.output


def test_pipeline_envvar_is_ignored_when_an_app_is_bound(
    app: DatapipeApp,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATAPIPE_PIPELINE", "somewhere:app")

    result = CliRunner().invoke(
        cli,
        ["table", "list"],
        obj={"pipeline": app},
        auto_envvar_prefix="DATAPIPE",
    )

    assert result.exit_code == 0, result.output
    assert "bound_table" in result.output


class _FakeEntryPoint:
    def __init__(self, fn):
        self._fn = fn

    def load(self):
        return self._fn


def _fake_entry_points(monkeypatch: pytest.MonkeyPatch, **groups: Any) -> None:
    """Make `datapipe.cli` see only the given entry points."""
    monkeypatch.setattr(
        "datapipe.cli._entry_points",
        lambda group: [_FakeEntryPoint(groups[group])] if group in groups else [],
    )


def _recording_init(calls: list[dict[str, Any]]):
    def init_fn(app: DatapipeApp, **kwargs: Any) -> None:
        calls.append(kwargs)

    return init_fn


def _recording_callback_factory(calls: list[dict[str, Any]]):
    def factory(app: DatapipeApp, steps: Any, **kwargs: Any) -> None:
        # Returning None opts the run out of a callback, and still exercises
        # the factory -- which is all these tests need.
        calls.append(kwargs)

    return factory


_PIPELINE_MODULE = """
import pandas as pd
from sqlalchemy import Column, Integer

from datapipe.compute import Catalog, DatapipeApp, Pipeline, Table
from datapipe.datatable import DataStore
from datapipe.step.batch_generate import BatchGenerate
from datapipe.store.database import DBConn, TableStoreDB

dbconn = DBConn("sqlite:///:memory:")
schema = [Column("id", Integer, primary_key=True)]
catalog = Catalog({"out": Table(store=TableStoreDB(dbconn, "out_data", schema, True))})


def gen():
    yield pd.DataFrame({"id": [1]})


app = DatapipeApp(
    DataStore(dbconn, create_meta_table=True),
    catalog,
    Pipeline([BatchGenerate(gen, outputs=["out"])]),
)
"""


def _write_pipeline_module(tmp_path, monkeypatch: pytest.MonkeyPatch, name: str) -> None:
    """A real importable pipeline module, for exercising the legacy path."""
    (tmp_path / f"{name}.py").write_text(_PIPELINE_MODULE)
    monkeypatch.syspath_prepend(str(tmp_path))


def test_pipeline_init_runs_once_for_a_bound_app(
    app: DatapipeApp,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []
    _fake_entry_points(monkeypatch, **{"datapipe.pipeline_init": _recording_init(calls)})

    result = CliRunner().invoke(cli, ["table", "list"], obj={"pipeline": app})

    assert result.exit_code == 0, result.output
    assert calls == [{"pipeline_module": None, "pipeline_spec": None}]


def test_pipeline_init_gets_the_module_on_the_legacy_path(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_pipeline_module(tmp_path, monkeypatch, "init_hook_pipeline")
    calls: list[dict[str, Any]] = []
    _fake_entry_points(monkeypatch, **{"datapipe.pipeline_init": _recording_init(calls)})

    result = CliRunner().invoke(cli, ["--pipeline", "init_hook_pipeline:app", "table", "list"])

    assert result.exit_code == 0, result.output
    assert len(calls) == 1
    assert calls[0]["pipeline_spec"] == "init_hook_pipeline:app"
    assert calls[0]["pipeline_module"].__name__ == "init_hook_pipeline"


def test_run_reports_the_real_pipeline_spec(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression: this used to be the literal "app" on every invocation."""
    _write_pipeline_module(tmp_path, monkeypatch, "spec_pipeline")
    calls: list[dict[str, Any]] = []
    _fake_entry_points(
        monkeypatch,
        **{"datapipe.run_callbacks": _recording_callback_factory(calls)},
    )

    result = CliRunner().invoke(cli, ["--pipeline", "spec_pipeline:app", "run"])

    assert result.exit_code == 0, result.output
    assert [c["pipeline_spec"] for c in calls] == ["spec_pipeline:app"]


def test_step_run_reports_the_real_pipeline_spec(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The `step run` path read ctx.parent.params, which is the `step` group."""
    _write_pipeline_module(tmp_path, monkeypatch, "step_spec_pipeline")
    calls: list[dict[str, Any]] = []
    _fake_entry_points(
        monkeypatch,
        **{"datapipe.run_callbacks": _recording_callback_factory(calls)},
    )

    result = CliRunner().invoke(cli, ["--pipeline", "step_spec_pipeline:app", "step", "run"])

    assert result.exit_code == 0, result.output
    assert [c["pipeline_spec"] for c in calls] == ["step_spec_pipeline:app"]


def test_bound_run_reports_no_pipeline_spec(
    app: DatapipeApp,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []
    _fake_entry_points(
        monkeypatch,
        **{"datapipe.run_callbacks": _recording_callback_factory(calls)},
    )

    result = CliRunner().invoke(cli, ["run"], obj={"pipeline": app})

    assert result.exit_code == 0, result.output
    assert [c["pipeline_spec"] for c in calls] == [None]


def test_binding_avoids_a_second_copy_of_the_pipeline_module(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The double import this whole change exists to prevent.

    The legacy path would import the module again under whatever name the
    --pipeline spec used, leaving two copies with two DataStores.
    """
    _write_pipeline_module(tmp_path, monkeypatch, "dup_check_pipeline")
    module = importlib.import_module("dup_check_pipeline")

    result = CliRunner().invoke(cli, ["table", "list"], obj={"pipeline": module.app})

    assert result.exit_code == 0, result.output

    holders = [name for name, mod in list(sys.modules.items()) if getattr(mod, "__file__", None) == module.__file__]
    assert holders == ["dup_check_pipeline"]


def test_run_cli_passes_the_app_through_to_click(
    app: DatapipeApp,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: dict[str, Any] = {}

    def fake_cli(**kwargs: Any) -> None:
        seen.update(kwargs)

    monkeypatch.setattr("datapipe.cli.cli", fake_cli)

    run_cli(app)

    assert seen == {"auto_envvar_prefix": "DATAPIPE", "obj": {"pipeline": app}}

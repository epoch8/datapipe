from __future__ import annotations

from datetime import datetime, timezone

import pytest

from datapipe_app.observability.run_logs.clickhouse_schema import (
    clickhouse_max_run_log_seq_statement,
    clickhouse_run_log_column_names,
    clickhouse_run_log_create_sql,
    clickhouse_select_run_logs_statement,
    compile_clickhouse_query,
)
from datapipe_app.observability.run_logs.store import (
    ClickHouseRunLogStore,
    RunLogStore,
    MISSING_RUN_LOGS_BACKEND_WARNING,
    SqlAlchemyRunLogStore,
    warn_if_run_logs_backend_missing,
)
from datapipe_app.observability.run_logs import DEFAULT_RUN_LOGS_TABLE_NAME, RunLogsBackend


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.commands: list[str] = []
        self.rows: list[dict[str, object]] = []

    def command(self, sql: str) -> None:
        self.commands.append(sql)

    def insert(self, table: str, data: list[list[object]], *, column_names: list[str]) -> None:
        for row in data:
            self.rows.append(dict(zip(column_names, row)))

    def query(self, sql: str, *, parameters: dict[str, object] | None = None):
        params = parameters or {}
        if "max(" in sql.lower():
            run_id = str(params.get("run_id", ""))
            seqs = [int(row["seq"]) for row in self.rows if row["run_id"] == run_id]
            return type("QueryResult", (), {"result_rows": [[max(seqs) if seqs else 0]]})()

        run_id = str(params.get("run_id", ""))
        after = int(params.get("after", 0))
        limit = int(params.get("limit", 500))
        matched = [
            row
            for row in self.rows
            if row["run_id"] == run_id and int(row["seq"]) > after
        ]
        matched.sort(key=lambda row: int(row["seq"]))
        matched = matched[:limit]
        return type(
            "QueryResult",
            (),
            {
                "result_rows": [
                    [row["run_id"], row["seq"], row["logged_at"], row["level"], row["message"]]
                    for row in matched
                ]
            },
        )()


@pytest.fixture
def fake_clickhouse_client():
    return FakeClickHouseClient()


def test_clickhouse_run_log_create_sql_uses_sqlalchemy_schema():
    sql = clickhouse_run_log_create_sql("datapipe_api__run_logs")
    assert sql.startswith("CREATE TABLE IF NOT EXISTS datapipe_api__run_logs")
    assert "run_id String" in sql
    assert "seq UInt32" in sql
    assert "logged_at DateTime64(6, 'UTC')" in sql
    assert "level LowCardinality(String)" in sql
    assert "ENGINE = MergeTree()" in sql
    assert "ORDER BY (run_id, seq)" in sql
    assert clickhouse_run_log_column_names("datapipe_api__run_logs") == [
        "run_id",
        "seq",
        "logged_at",
        "level",
        "message",
    ]


def test_clickhouse_run_log_select_queries_use_sqlalchemy():
    table_name = "datapipe_api__run_logs"
    select_sql = compile_clickhouse_query(clickhouse_select_run_logs_statement(table_name))
    max_sql = compile_clickhouse_query(clickhouse_max_run_log_seq_statement(table_name))

    assert f"FROM {table_name}" in select_sql
    assert "run_id = %(run_id)s" in select_sql
    assert "seq > %(after)s" in select_sql
    assert "LIMIT %(limit)s" in select_sql
    assert f"FROM {table_name}" in max_sql
    assert f"max({table_name}.seq)" in max_sql.replace("`", "")


def test_run_log_store_implementations_are_subclasses():
    assert issubclass(SqlAlchemyRunLogStore, RunLogStore)
    assert issubclass(ClickHouseRunLogStore, RunLogStore)


def test_clickhouse_run_log_store_roundtrip(fake_clickhouse_client):
    store = ClickHouseRunLogStore(
        fake_clickhouse_client,
        table_name=DEFAULT_RUN_LOGS_TABLE_NAME,
    )
    store.ensure_table()

    logged_at = datetime(2026, 7, 15, 12, 0, 0, 123456, tzinfo=timezone.utc)
    store.append_run_logs(
        [
            {
                "run_id": "run-1",
                "seq": 1,
                "logged_at": logged_at,
                "level": "INFO",
                "message": "hello",
            },
            {
                "run_id": "run-1",
                "seq": 2,
                "logged_at": logged_at,
                "level": "ERROR",
                "message": "boom",
            },
        ]
    )

    lines = store.get_run_logs("run-1")
    assert len(lines) == 2
    assert lines[0].message == "hello"
    assert lines[1].level == "ERROR"
    assert store.get_last_log_seq("run-1") == 2
    assert store.get_run_logs("run-1", after=1, limit=10)[0].message == "boom"


def test_clickhouse_run_log_store_concurrent_append_and_read(fake_clickhouse_client):
    import threading

    store = ClickHouseRunLogStore(
        fake_clickhouse_client,
        table_name=DEFAULT_RUN_LOGS_TABLE_NAME,
    )
    store.ensure_table()
    logged_at = datetime(2026, 7, 15, 12, 0, 0, tzinfo=timezone.utc)
    errors: list[BaseException] = []

    def writer() -> None:
        try:
            for seq in range(1, 51):
                store.append_run_logs(
                    [
                        {
                            "run_id": "run-concurrent",
                            "seq": seq,
                            "logged_at": logged_at,
                            "level": "INFO",
                            "message": f"line {seq}",
                        }
                    ]
                )
        except BaseException as exc:
            errors.append(exc)

    def reader() -> None:
        try:
            for _ in range(50):
                store.get_run_logs("run-concurrent")
                store.get_last_log_seq("run-concurrent")
        except BaseException as exc:
            errors.append(exc)

    threads = [
        threading.Thread(target=writer),
        threading.Thread(target=reader),
        threading.Thread(target=reader),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert not errors
    assert store.get_last_log_seq("run-concurrent") == 50


def test_warn_if_run_logs_backend_missing(caplog):
    caplog.set_level("WARNING")

    warn_if_run_logs_backend_missing(None)

    assert any(MISSING_RUN_LOGS_BACKEND_WARNING[:40] in record.message for record in caplog.records)


def test_warn_if_run_logs_backend_missing_skips_with_clickhouse(caplog, monkeypatch):
    caplog.set_level("WARNING")
    monkeypatch.setattr(
        "datapipe_app.observability.run_logs.store._create_clickhouse_client",
        lambda _url: FakeClickHouseClient(),
    )

    run_logs_backend = RunLogsBackend.clickhouse("clickhouse://default:@localhost:8123/default")

    warn_if_run_logs_backend_missing(run_logs_backend)

    assert not caplog.records


def test_observability_store_uses_clickhouse_for_logs(tmp_path, fake_clickhouse_client, monkeypatch):
    monkeypatch.setattr(
        "datapipe_app.observability.run_logs.store._create_clickhouse_client",
        lambda _url: fake_clickhouse_client,
    )

    from datapipe_app.observability.store.db import ObservabilityStore
    from datapipe_app.observability.run_logs import RunLogsBackend

    store = ObservabilityStore.from_url(
        f"sqlite:///{tmp_path / 'obs.db'}",
        run_logs_backend=RunLogsBackend.clickhouse("clickhouse://default:@localhost:8123/default"),
    )

    assert store.run_logs_configured is True
    assert store.use_external_run_logs is True
    store.append_run_logs(
        [
            {
                "run_id": "run-ch",
                "seq": 1,
                "logged_at": datetime.now(timezone.utc),
                "level": "INFO",
                "message": "from clickhouse",
            }
        ]
    )
    lines = store.get_run_logs("run-ch")
    assert len(lines) == 1
    assert lines[0].message == "from clickhouse"


def test_observability_store_without_backend_does_not_persist_logs(tmp_path):
    from datapipe_app.observability.store.db import ObservabilityStore

    store = ObservabilityStore.from_url(f"sqlite:///{tmp_path / 'obs.db'}")
    assert store.run_logs_configured is False
    store.append_run_logs(
        [
            {
                "run_id": "run-null",
                "seq": 1,
                "logged_at": datetime.now(timezone.utc),
                "level": "INFO",
                "message": "ignored",
            }
        ]
    )
    assert store.get_run_logs("run-null") == []


def test_sqlalchemy_run_log_store_delegates_to_observability_store(tmp_path):
    from datapipe_app.observability.store.db import ObservabilityStore

    store = ObservabilityStore.from_url(f"sqlite:///{tmp_path / 'obs.db'}")
    backend = SqlAlchemyRunLogStore(store)
    backend.append_run_logs(
        [
            {
                "run_id": "run-sql",
                "seq": 1,
                "logged_at": datetime.now(timezone.utc),
                "level": "INFO",
                "message": "sql backend",
            }
        ]
    )

    lines = backend.get_run_logs("run-sql")
    assert len(lines) == 1
    assert lines[0].message == "sql backend"
    assert backend.get_last_log_seq("run-sql") == 1

from __future__ import annotations

import logging
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Optional
from urllib.parse import urlparse

from datapipe_app.observability.run_logs.clickhouse_schema import (
    clickhouse_max_run_log_seq_statement,
    clickhouse_run_log_column_names,
    clickhouse_run_log_create_sql,
    clickhouse_select_run_logs_statement,
    compile_clickhouse_query,
)
from datapipe_app.observability.config.tables import ObservabilityTableConfig

logger = logging.getLogger(__name__)

SHARED_RUN_LOGS_WARNING = (
    "Run logs are stored in the same SQL database as pipeline metadata (%s). "
    "This does not scale: the database will grow quickly as log volume increases. "
    "Pass a dedicated ClickHouse RunLogsBackend to DatapipeAPI "
    "(for example RunLogsBackend.clickhouse('clickhouse://default:@localhost:8123/default'))."
)

if TYPE_CHECKING:
    from datapipe.store.database import DBConn

    from datapipe_app.observability.run_logs.backend import RunLogsBackend


@dataclass(frozen=True)
class RunLogRecord:
    run_id: str
    seq: int
    logged_at: datetime
    level: str
    message: str


class RunLogStore(ABC):
    @abstractmethod
    def append_run_logs(self, rows: list[dict[str, Any]]) -> None:
        """Persist a batch of run log rows."""

    @abstractmethod
    def get_run_logs(
        self,
        run_id: str,
        *,
        after: int = 0,
        limit: int = 500,
    ) -> list[RunLogRecord]:
        """Return log lines for a run after the given sequence number."""

    @abstractmethod
    def get_last_log_seq(self, run_id: str) -> int:
        """Return the highest persisted sequence number for a run."""


def is_clickhouse_url(url: str) -> bool:
    scheme = urlparse(url).scheme.lower()
    return scheme in {"clickhouse", "clickhouse+http", "clickhouse+native"}


def warn_if_run_logs_share_pipeline_db(
    *,
    pipeline_dbconn: Optional[DBConn],
    observability_dbconn: Optional[DBConn],
    run_logs_backend: Optional[RunLogsBackend],
) -> None:
    from datapipe_app.observability.connections.dbconn import dbconn_same_target

    if run_logs_backend is not None:
        return
    if pipeline_dbconn is None or observability_dbconn is None:
        return
    if not dbconn_same_target(pipeline_dbconn, observability_dbconn):
        return
    logger.warning(SHARED_RUN_LOGS_WARNING, observability_dbconn.connstr)


def resolve_run_log_store(
    *,
    observability_store: Any,
    run_logs_backend: Optional[RunLogsBackend],
    table_name: str,
) -> tuple[RunLogStore, bool]:
    """Return (store, use_external_run_logs)."""
    if run_logs_backend is not None:
        return run_logs_backend.store, True
    return SqlAlchemyRunLogStore(observability_store), False


class SqlAlchemyRunLogStore(RunLogStore):
    def __init__(self, observability_store: Any):
        self._store = observability_store

    def append_run_logs(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        from datapipe_app.observability.store.db import PipelineRunLogRow

        with self._store.session() as session:
            for row in rows:
                session.add(PipelineRunLogRow(**row))
            session.commit()

    def get_run_logs(
        self,
        run_id: str,
        *,
        after: int = 0,
        limit: int = 500,
    ) -> list[RunLogRecord]:
        from datapipe_app.observability.store.db import PipelineRunLogRow
        from sqlalchemy import select

        with self._store.session() as session:
            rows = list(
                session.scalars(
                    select(PipelineRunLogRow)
                    .where(
                        PipelineRunLogRow.run_id == run_id,
                        PipelineRunLogRow.seq > after,
                    )
                    .order_by(PipelineRunLogRow.seq)
                    .limit(limit)
                ).all()
            )
        return [_record_from_sql_row(row) for row in rows]

    def get_last_log_seq(self, run_id: str) -> int:
        from datapipe_app.observability.store.db import PipelineRunLogRow
        from sqlalchemy import select

        with self._store.session() as session:
            row = session.scalars(
                select(PipelineRunLogRow.seq)
                .where(PipelineRunLogRow.run_id == run_id)
                .order_by(PipelineRunLogRow.seq.desc())
                .limit(1)
            ).first()
            return row or 0


class ClickHouseRunLogStore(RunLogStore):
    def __init__(self, client: Any, *, table_name: str):
        self._client = client
        self._table_name = table_name
        # clickhouse_connect clients are not thread-safe; pipeline log flushes and
        # API log polling can overlap on the same store instance.
        self._lock = threading.Lock()

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        table_name: str = ObservabilityTableConfig().pipeline_run_logs,
    ) -> ClickHouseRunLogStore:
        client = _create_clickhouse_client(url)
        store = cls(client, table_name=table_name)
        store.ensure_table()
        return store

    def ensure_table(self) -> None:
        with self._lock:
            self._client.command(clickhouse_run_log_create_sql(self._table_name))

    def append_run_logs(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        column_names = clickhouse_run_log_column_names(self._table_name)
        payload = [[row[column] for column in column_names] for row in rows]
        with self._lock:
            self._client.insert(
                self._table_name,
                payload,
                column_names=column_names,
            )

    def _query(self, statement: Any, *, parameters: dict[str, Any]) -> Any:
        with self._lock:
            return self._client.query(
                compile_clickhouse_query(statement),
                parameters=parameters,
            )

    def get_run_logs(
        self,
        run_id: str,
        *,
        after: int = 0,
        limit: int = 500,
    ) -> list[RunLogRecord]:
        result = self._query(
            clickhouse_select_run_logs_statement(self._table_name),
            parameters={"run_id": run_id, "after": after, "limit": limit},
        )
        records: list[RunLogRecord] = []
        for row in result.result_rows:
            logged_at = row[2]
            if not isinstance(logged_at, datetime):
                logged_at = datetime.fromisoformat(str(logged_at))
            records.append(
                RunLogRecord(
                    run_id=str(row[0]),
                    seq=int(row[1]),
                    logged_at=logged_at,
                    level=str(row[3]),
                    message=str(row[4]),
                )
            )
        return records

    def get_last_log_seq(self, run_id: str) -> int:
        result = self._query(
            clickhouse_max_run_log_seq_statement(self._table_name),
            parameters={"run_id": run_id},
        )
        if not result.result_rows:
            return 0
        value = result.result_rows[0][0]
        return int(value or 0)


def _record_from_sql_row(row: Any) -> RunLogRecord:
    logged_at = row.logged_at or datetime.now(timezone.utc)
    return RunLogRecord(
        run_id=row.run_id,
        seq=row.seq,
        logged_at=logged_at,
        level=row.level,
        message=row.message,
    )


def _create_clickhouse_client(url: str) -> Any:
    try:
        import clickhouse_connect
    except ImportError as exc:
        raise ImportError(
            "ClickHouse run logs require clickhouse-connect. "
            "Install with: pip install 'datapipe-app[clickhouse]'"
        ) from exc

    parsed = urlparse(url)
    scheme = parsed.scheme.lower()
    if not is_clickhouse_url(url):
        raise ValueError(f"Unsupported ClickHouse URL: {url!r}")

    database = parsed.path.lstrip("/") or "default"
    secure = scheme.endswith("s") or scheme in {"clickhouse+https"}
    port = parsed.port
    if port is None:
        if scheme.endswith("native"):
            port = 9000
        else:
            port = 8443 if secure else 8123

    return clickhouse_connect.get_client(
        host=parsed.hostname or "localhost",
        port=port,
        username=parsed.username or "default",
        password=parsed.password or "",
        database=database,
        secure=secure,
    )

from __future__ import annotations

from typing import TYPE_CHECKING

from datapipe_app.observability.run_logs.store import (
    ClickHouseRunLogStore,
    InMemoryRunLogStore,
    RunLogStore,
    SqlAlchemyRunLogStore,
    is_clickhouse_url,
)

if TYPE_CHECKING:
    from datapipe_app.observability.store.db import ObservabilityStore

DEFAULT_RUN_LOGS_TABLE_NAME = "datapipe_api__run_logs"


class RunLogsBackend:
    """Dedicated storage for pipeline run logs (ClickHouse, in-memory, or SQL)."""

    def __init__(self, store: RunLogStore):
        self._store = store

    @classmethod
    def clickhouse(
        cls,
        url: str,
        *,
        table_name: str = DEFAULT_RUN_LOGS_TABLE_NAME,
        create_table: bool = False,
    ) -> RunLogsBackend:
        """Connect to ClickHouse for run logs.

        ``create_table`` defaults to False — create the table via Alembic
        (or call ``store.ensure_table()`` / pass ``create_table=True``).
        """
        if not is_clickhouse_url(url):
            raise ValueError(
                f"Unsupported run logs URL scheme: {url!r}. "
                "Only ClickHouse URLs are supported (clickhouse://...)."
            )
        return cls(
            ClickHouseRunLogStore.from_url(
                url,
                table_name=table_name,
                create_table=create_table,
            )
        )

    @classmethod
    def memory(cls) -> RunLogsBackend:
        """Ephemeral in-process log store (tests / local smoke)."""
        return cls(InMemoryRunLogStore())

    @classmethod
    def sqlalchemy(
        cls,
        observability_store: ObservabilityStore,
        *,
        table_name: str = DEFAULT_RUN_LOGS_TABLE_NAME,
    ) -> RunLogsBackend:
        """Persist run logs in the observability SQL database (explicit opt-in)."""
        return cls(SqlAlchemyRunLogStore(observability_store, table_name=table_name))

    @property
    def store(self) -> RunLogStore:
        return self._store

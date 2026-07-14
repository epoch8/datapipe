from __future__ import annotations

from datapipe_app.observability.run_logs.store import (
    ClickHouseRunLogStore,
    RunLogStore,
    is_clickhouse_url,
)
from datapipe_app.observability.config.tables import ObservabilityTableConfig


class RunLogsBackend:
    """Dedicated storage for pipeline run logs (for example ClickHouse)."""

    def __init__(self, store: RunLogStore):
        self._store = store

    @classmethod
    def clickhouse(
        cls,
        url: str,
        *,
        table_name: str = ObservabilityTableConfig().pipeline_run_logs,
    ) -> RunLogsBackend:
        if not is_clickhouse_url(url):
            raise ValueError(
                f"Unsupported run logs URL scheme: {url!r}. "
                "Only ClickHouse URLs are supported (clickhouse://...)."
            )
        return cls(ClickHouseRunLogStore.from_url(url, table_name=table_name))

    @property
    def store(self) -> RunLogStore:
        return self._store

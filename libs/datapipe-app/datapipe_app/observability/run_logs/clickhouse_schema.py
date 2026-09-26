from __future__ import annotations

from functools import lru_cache
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sqlalchemy.sql import Select
    from sqlalchemy.sql.schema import Table

_CLICKHOUSE_SQLALCHEMY_IMPORT_ERROR = (
    "ClickHouse run logs require clickhouse-sqlalchemy. "
    "Install with: pip install 'datapipe-app[clickhouse]'"
)


def _require_clickhouse_sqlalchemy():
    try:
        import clickhouse_sqlalchemy  # noqa: F401
    except ImportError as exc:
        raise ImportError(_CLICKHOUSE_SQLALCHEMY_IMPORT_ERROR) from exc


def _clickhouse_dialect():
    _require_clickhouse_sqlalchemy()
    from clickhouse_sqlalchemy.drivers.base import ClickHouseDialect

    return ClickHouseDialect()


@lru_cache(maxsize=1)
def _clickhouse_dialect_singleton():
    return _clickhouse_dialect()


def compile_clickhouse_query(statement: Any) -> str:
    """Compile a SQLAlchemy statement to ClickHouse SQL."""
    return str(statement.compile(dialect=_clickhouse_dialect_singleton()))


@lru_cache(maxsize=32)
def clickhouse_run_log_table(table_name: str) -> Table:
    """SQLAlchemy Core table for ClickHouse run logs (MergeTree)."""
    _require_clickhouse_sqlalchemy()
    from clickhouse_sqlalchemy import Table, engines, types
    from sqlalchemy import Column, MetaData

    metadata = MetaData()
    return Table(
        table_name,
        metadata,
        Column("run_id", types.String, nullable=False),
        Column("seq", types.UInt32, nullable=False),
        Column("logged_at", types.DateTime64(6, "UTC"), nullable=False),
        Column("level", types.LowCardinality(types.String), nullable=False),
        Column("message", types.String, nullable=False),
        engines.MergeTree(order_by=("run_id", "seq")),
    )


def clickhouse_run_log_column_names(table_name: str) -> list[str]:
    return [column.name for column in clickhouse_run_log_table(table_name).c]


def clickhouse_select_run_logs_statement(table_name: str) -> Select:
    _require_clickhouse_sqlalchemy()
    from sqlalchemy import bindparam, select

    table = clickhouse_run_log_table(table_name)
    return (
        select(table.c.run_id, table.c.seq, table.c.logged_at, table.c.level, table.c.message)
        .where(
            table.c.run_id == bindparam("run_id"),
            table.c.seq > bindparam("after"),
        )
        .order_by(table.c.seq)
        .limit(bindparam("limit"))
    )


def clickhouse_max_run_log_seq_statement(table_name: str) -> Select:
    _require_clickhouse_sqlalchemy()
    from sqlalchemy import bindparam, func, select

    table = clickhouse_run_log_table(table_name)
    return select(func.max(table.c.seq)).where(table.c.run_id == bindparam("run_id"))


def clickhouse_run_log_create_sql(table_name: str) -> str:
    """Compile CREATE TABLE IF NOT EXISTS from the SQLAlchemy schema."""
    _require_clickhouse_sqlalchemy()
    from sqlalchemy.schema import CreateTable

    ddl = str(
        CreateTable(clickhouse_run_log_table(table_name)).compile(
            dialect=_clickhouse_dialect_singleton()
        )
    ).strip()
    if ddl.upper().startswith("CREATE TABLE"):
        ddl = ddl.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1)
    return ddl

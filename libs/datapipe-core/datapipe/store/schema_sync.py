"""Sync SQLAlchemy MetaData onto an existing database (ALTER / ADD COLUMN).

Uses Alembic's documented programmatic API (``produce_migrations`` +
``Operations.invoke``) so we do not maintain a hand-rolled DDL differ.
See Alembic cookbook: "Run Alembic Operation Objects Directly"
(https://alembic.sqlalchemy.org/en/latest/cookbook.html).

Requires optional extra: ``pip install datapipe-core[alembic]``.
"""

from __future__ import annotations

from typing import Any, Iterable

from sqlalchemy import MetaData
from sqlalchemy.engine import Connection, Engine

_ALEMBIC_INSTALL_HINT = (
    "Schema sync requires the optional Alembic extra. "
    "Install with: pip install 'datapipe-core[alembic]' "
    "(or: uv add --optional alembic / uv sync --extra alembic)."
)


class AlembicNotInstalledError(ImportError):
    """Raised when schema sync is requested but Alembic is not installed."""


def _require_alembic() -> None:
    try:
        import alembic  # noqa: F401
    except ImportError as exc:
        raise AlembicNotInstalledError(_ALEMBIC_INSTALL_HINT) from exc


def _schemas_compatible(a: str | None, b: str | None, *, default: str | None) -> bool:
    """Match MetaData / reflected schema names for the active DBConn schema."""
    if a == b:
        return True

    def effective(value: str | None) -> str | None:
        if value is not None:
            return value
        if default is not None:
            return default
        # Unqualified objects on Postgres land in ``public``.
        return "public"

    return effective(a) == effective(b)


def sync_sqla_metadata(
    bind: Engine | Connection,
    metadata: MetaData,
    *,
    schema: str | None = None,
) -> int:
    """Bring ``bind`` in line with ``metadata`` without writing migration files.

    Intended to run after ``MetaData.create_all``: applies ADD COLUMN / ALTER
    (and similar) when definitions drifted. Table create/drop is left to
    ``create_all`` / ``--force-recreate`` so we never re-CREATE existing tables.

    Honours custom Postgres schemas from ``DBConn(..., schema=...)`` (e.g.
    ``datapipe_e2e_detection`` in e2e_template): only that schema is scanned.

    Tables present in the DB but absent from ``metadata`` are left alone.

    Returns the number of leaf Alembic operations applied.

    Raises:
        AlembicNotInstalledError: if the optional ``alembic`` extra is missing.
    """
    _require_alembic()
    from alembic.autogenerate import produce_migrations
    from alembic.migration import MigrationContext
    from alembic.operations import Operations
    from alembic.operations.ops import CreateTableOp, DropTableOp, ModifyTableOps

    # Autogenerate is very chatty at INFO; keep create-all output readable.
    import logging

    logging.getLogger("alembic").setLevel(logging.WARNING)

    known_by_name = {table.name: table for table in metadata.tables.values()}

    def include_object(
        object_: Any,
        name: str | None,
        type_: str,
        reflected: bool,
        compare_to: Any,
    ) -> bool:
        # Ignore DB tables that are not part of pipeline metadata (no DROP TABLE).
        if type_ == "table" and reflected:
            if name is None or name not in known_by_name:
                return False
            meta_table = known_by_name[name]
            reflected_schema = getattr(object_, "schema", None)
            return _schemas_compatible(reflected_schema, meta_table.schema, default=schema)
        return True

    def include_name(name: str | None, type_: str, parent_names: dict[str, Any]) -> bool:
        if type_ == "schema":
            if schema is None:
                # Default DB schema: Alembic reports it as None; Postgres as public.
                return name is None or name == "public"
            # Custom schema only (e.g. datapipe_e2e_detection) — do not scan public.
            return name == schema
        return True

    opts: dict[str, Any] = {
        "compare_type": True,
        "include_object": include_object,
        "version_table": False,
    }
    # Always scan schemas we care about so Postgres ``public`` matches MetaData(schema=None).
    opts["include_schemas"] = True
    opts["include_name"] = include_name

    def _should_apply(elem: Any) -> bool:
        # create_all already created missing tables; never drop extras here.
        return not isinstance(elem, (CreateTableOp, DropTableOp))

    def _apply(conn: Connection) -> int:
        context = MigrationContext.configure(conn, opts=opts)
        migrations = produce_migrations(context, metadata)
        operations = Operations(context)
        use_batch = conn.dialect.name == "sqlite"
        applied = 0

        stack: list[Any] = [migrations.upgrade_ops]
        while stack:
            elem = stack.pop(0)
            if use_batch and isinstance(elem, ModifyTableOps):
                with operations.batch_alter_table(elem.table_name, schema=elem.schema) as batch_ops:
                    for table_elem in elem.ops:
                        if not _should_apply(table_elem):
                            continue
                        batch_ops.invoke(table_elem)
                        applied += 1
            elif hasattr(elem, "ops"):
                stack.extend(list(elem.ops))
            elif _should_apply(elem):
                operations.invoke(elem)
                applied += 1
        return applied

    if isinstance(bind, Engine):
        with bind.begin() as conn:
            return _apply(conn)
    return _apply(bind)


def iter_schema_diffs(
    bind: Engine | Connection,
    metadata: MetaData,
    *,
    schema: str | None = None,
) -> Iterable[Any]:
    """Yield Alembic compare_metadata diffs (for logging / dry-run)."""
    _require_alembic()
    from alembic.autogenerate import compare_metadata
    from alembic.migration import MigrationContext

    opts: dict[str, Any] = {
        "compare_type": True,
        "version_table": False,
        "include_schemas": True,
    }

    if isinstance(bind, Engine):
        with bind.connect() as conn:
            context = MigrationContext.configure(conn, opts=opts)
            return list(compare_metadata(context, metadata))

    context = MigrationContext.configure(bind, opts=opts)
    return list(compare_metadata(context, metadata))


__all__ = [
    "AlembicNotInstalledError",
    "sync_sqla_metadata",
    "iter_schema_diffs",
    "_schemas_compatible",
]

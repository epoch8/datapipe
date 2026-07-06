from __future__ import annotations

import logging
import os
from typing import Optional

from sqlalchemy import text

from datapipe.datatable import DataStore
from datapipe.store.database import DBConn

logger = logging.getLogger(__name__)

DATAPIPE_TABLE_PREFIXES = ("pipeline_", "analytics_", "datapipe_")


def resolve_datapipe_schema(
    ds: Optional[DataStore] = None,
    explicit_schema: Optional[str] = None,
) -> Optional[str]:
    if explicit_schema:
        return explicit_schema

    env_schema = os.environ.get("DATAPIPE_DB_SCHEMA") or os.environ.get("DATAPIPE_APP_DB_SCHEMA")
    if env_schema:
        return env_schema

    if ds is not None and ds.meta_dbconn.schema:
        return ds.meta_dbconn.schema

    if ds is not None:
        dbconn = ds.meta_dbconn
        if not dbconn.connstr.startswith("sqlite"):
            try:
                with dbconn.con.begin() as con:
                    current = con.execute(text("SELECT current_schema()")).scalar()
                if current and current != "public":
                    return str(current)
            except Exception:
                logger.debug("Could not read current_schema()", exc_info=True)

    logger.warning(
        "Datapipe schema not configured explicitly; observability tables may use default schema"
    )
    return None


def ensure_db_schema(dbconn: DBConn) -> None:
    if not dbconn.schema:
        return
    if dbconn.connstr.startswith("sqlite"):
        return
    with dbconn.con.begin() as con:
        con.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{dbconn.schema}"'))


def is_datapipe_owned_table(table_name: str) -> bool:
    return table_name.startswith(DATAPIPE_TABLE_PREFIXES)


def assert_safe_drop_schema(schema: Optional[str], *, allow_cascade: bool = False) -> None:
    if allow_cascade:
        return
    if schema in (None, "public"):
        raise RuntimeError(
            "Refusing destructive CASCADE on default/public schema; set an isolated datapipe schema"
        )

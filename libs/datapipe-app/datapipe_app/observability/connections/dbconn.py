from __future__ import annotations

from urllib.parse import urlparse

from datapipe.store.database import DBConn


def normalize_db_url(url: str) -> str:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    port = parsed.port
    if port is None:
        port = 5432 if parsed.scheme.startswith("postgres") else 0
    path = parsed.path.rstrip("/") or ""
    user = parsed.username or ""
    return f"{parsed.scheme}://{user}@{host}:{port}{path}"


def dbconn_same_target(left: DBConn, right: DBConn) -> bool:
    """True when both connections point at the same database schema."""
    return (
        normalize_db_url(left.connstr) == normalize_db_url(right.connstr)
        and left.schema == right.schema
    )

from __future__ import annotations

import re
import time
from typing import Any, Optional

from sqlalchemy import text

FORBIDDEN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|ATTACH|DETACH|PRAGMA|VACUUM|REPLACE|TRUNCATE|GRANT|REVOKE)\b",
    re.IGNORECASE,
)

# Qualified names first (longest). Unqualified bare names use word boundaries
# with a negative lookbehind so `analytics_metrics_on_subset` is not rewritten twice.
QUALIFIED_TABLE_MAP = {
    "datapipe_analytics.metrics_on_subset": "analytics_metrics_on_subset",
    "datapipe_analytics.metrics_by_class": "analytics_metrics_by_class",
    "datapipe_analytics.training_runs": "analytics_training_runs",
    "datapipe_analytics.artifacts": "analytics_training_runs",
    "datapipe_analytics.predictions": "analytics_metrics_on_subset",
}

BARE_TABLE_MAP = {
    "metrics_on_subset": "analytics_metrics_on_subset",
    "metrics_by_class": "analytics_metrics_by_class",
    "training_runs": "analytics_training_runs",
}


def _validate_sql(sql: str) -> None:
    stripped = sql.strip().rstrip(";")
    upper = stripped.upper()
    if not (upper.startswith("SELECT") or upper.startswith("WITH")):
        raise ValueError("Only SELECT / WITH queries are allowed")
    if FORBIDDEN.search(stripped):
        raise ValueError("Forbidden SQL statement detected")
    if ";" in stripped:
        raise ValueError("Multiple statements are not allowed")


def _rewrite_sql(sql: str) -> str:
    result = sql
    for src, dst in sorted(QUALIFIED_TABLE_MAP.items(), key=lambda item: len(item[0]), reverse=True):
        result = re.sub(re.escape(src), dst, result, flags=re.IGNORECASE)
    for src, dst in BARE_TABLE_MAP.items():
        # Avoid matching the suffix of analytics_<table> after qualified rewrite.
        pattern = rf"(?<![a-zA-Z0-9_]){re.escape(src)}\b"
        result = re.sub(pattern, dst, result, flags=re.IGNORECASE)
    if "LIMIT" not in result.upper():
        result = f"{result.rstrip(';')} LIMIT 1000"
    return result


def execute_readonly_query(
    engine: Any,
    sql: str,
    *,
    limit: int = 1000,
    offset: int = 0,
    timeout_s: float = 10.0,
) -> dict[str, Any]:
    _validate_sql(sql)
    rewritten = _rewrite_sql(sql)
    if "LIMIT" in rewritten.upper():
        rewritten = re.sub(r"LIMIT\s+\d+", f"LIMIT {min(limit, 5000)}", rewritten, flags=re.IGNORECASE)
    else:
        rewritten = f"{rewritten} LIMIT {min(limit, 5000)}"
    if offset and "OFFSET" not in rewritten.upper():
        rewritten = f"{rewritten} OFFSET {offset}"

    start = time.perf_counter()
    with engine.connect() as conn:
        conn = conn.execution_options(timeout=timeout_s)
        result = conn.execute(text(rewritten))
        rows = [dict(row._mapping) for row in result.fetchmany(min(limit, 5000))]
        columns = [{"name": col, "type": "unknown"} for col in (rows[0].keys() if rows else [])]
    elapsed_ms = (time.perf_counter() - start) * 1000
    return {
        "columns": columns,
        "rows": rows,
        "total": len(rows),
        "execution_ms": round(elapsed_ms, 2),
        "truncated": len(rows) >= limit,
    }

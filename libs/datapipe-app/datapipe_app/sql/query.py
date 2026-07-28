from __future__ import annotations

from typing import Literal, Optional

from sqlalchemy import Table
from sqlalchemy.sql import Select
from sqlalchemy.sql.expression import asc, desc


def apply_table_order_by(
    sql: Select,
    table: Table,
    order_by: Optional[str],
    order: Optional[str] = "asc",
) -> Select:
    if not order_by:
        return sql
    if order_by not in table.c:
        raise ValueError(f"Unknown order_by column: {order_by}")

    direction: Literal["asc", "desc"] = "asc" if (order or "asc").lower() != "desc" else "desc"
    column = table.c[order_by]
    sql = sql.where(column.is_not(None))
    return sql.order_by(asc(column) if direction == "asc" else desc(column))

from datapipe.meta.base import TableMeta, TransformMeta
from datapipe.meta.sql_meta import SQLTableMeta, SQLTransformMeta


def require_sql_table_meta(meta: TableMeta) -> SQLTableMeta:
    if not isinstance(meta, SQLTableMeta):
        raise RuntimeError("SQL-backed table metadata is required")
    return meta


def require_sql_transform_meta(meta: TransformMeta) -> SQLTransformMeta:
    if not isinstance(meta, SQLTransformMeta):
        raise RuntimeError("SQL-backed transform metadata is required")
    return meta

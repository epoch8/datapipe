from __future__ import annotations

from datetime import date, datetime
from typing import Any, Iterable, Literal, Sequence

from datapipe.compute import Catalog
from datapipe.datatable import DataStore
from sqlalchemy import MetaData, String, Table, asc, desc, func, inspect, select
from sqlalchemy.exc import OperationalError

from datapipe_app.spec_registry import OpsSpecValidationError
from datapipe_app.specs import OpsColumn, OpsColumnGroup, OpsMetricTableSpec


def format_snapshot_label(value: Any, *, mode: str = "timestamp") -> str:
    if value is None:
        return ""
    if mode == "id":
        return str(value)
    if mode == "short_id":
        raw = str(value)
        return raw if len(raw) <= 12 else f"{raw[:7]}...{raw[-3:]}"
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M")
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


class OpsQuery:
    def __init__(self, ds: DataStore | None, catalog: Catalog | None):
        self.ds = ds
        self.catalog = catalog

    def count_rows(self, table_name: str) -> int:
        table = self._table(table_name)
        if not self._physical_table_exists(table):
            return 0
        try:
            with self._engine().connect() as conn:
                return int(conn.execute(select(func.count()).select_from(table)).scalar() or 0)
        except OperationalError:
            return 0

    def latest_value(self, table_name: str, column_name: str) -> Any:
        table = self._table(table_name)
        if not self._physical_table_exists(table):
            return None
        column = table.c[column_name]
        try:
            with self._engine().connect() as conn:
                return conn.execute(select(column).order_by(desc(column)).limit(1)).scalar()
        except OperationalError:
            return None


    def value_counts(self, table_name: str, key_column: str) -> dict[Any, int]:
        table = self._table(table_name)
        if key_column not in table.c or not self._physical_table_exists(table):
            return {}
        try:
            with self._engine().connect() as conn:
                result = conn.execute(
                    select(table.c[key_column], func.count().label("count")).group_by(table.c[key_column])
                )
                return {row[0]: int(row[1] or 0) for row in result}
        except OperationalError:
            return {}

    def rows(
        self,
        table_name: str,
        *,
        allowed_columns: Sequence[OpsColumn],
        sort_by: str | None = None,
        sort_dir: Literal["asc", "desc"] = "desc",
        search: str | None = None,
        filters: dict[str, str | Sequence[str]] | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[dict[str, Any]], int]:
        table = self._table(table_name)
        allowed_by_id = {col.id: col for col in allowed_columns}
        allowed_by_source = {col.source: col for col in allowed_columns}
        selected_sources = list(dict.fromkeys(col.source for col in allowed_columns if col.source in table.c))
        if not selected_sources:
            selected_sources = [col.name for col in table.c][:20]

        query = select(*(table.c[source] for source in selected_sources))
        count_query = select(func.count()).select_from(table)
        conditions = []

        for key, value in (filters or {}).items():
            spec_col = allowed_by_id.get(key) or allowed_by_source.get(key)
            if spec_col is None or not spec_col.filterable:
                raise OpsSpecValidationError(f'Filter "{key}" is not configured for table "{table_name}".')
            values = value if isinstance(value, Sequence) and not isinstance(value, (str, bytes)) else [value]
            values = [item for item in values if item not in {None, ""}]
            if not values:
                continue
            if len(values) == 1:
                conditions.append(table.c[spec_col.source] == values[0])
            else:
                conditions.append(table.c[spec_col.source].in_(list(values)))

        if search:
            search_conditions = [
                table.c[col.source].cast(String).like(f"%{search}%")
                for col in allowed_columns
                if col.kind in {"text", "link", "chip", "status"} and col.source in table.c
            ]
            if search_conditions:
                from sqlalchemy import or_

                conditions.append(or_(*search_conditions))

        for condition in conditions:
            query = query.where(condition)
            count_query = count_query.where(condition)

        if sort_by:
            sort_col = allowed_by_id.get(sort_by) or allowed_by_source.get(sort_by)
            if sort_col is None or not sort_col.sortable:
                raise OpsSpecValidationError(f'Sort "{sort_by}" is not configured for table "{table_name}".')
            ordering = desc(table.c[sort_col.source]) if sort_dir == "desc" else asc(table.c[sort_col.source])
            query = query.order_by(ordering)

        limit = max(1, min(limit, 500))
        offset = max(0, offset)
        query = query.limit(limit).offset(offset)

        if not self._physical_table_exists(table):
            return [], 0

        try:
            with self._engine().connect() as conn:
                rows = [dict(row._mapping) for row in conn.execute(query)]
                total = int(conn.execute(count_query).scalar() or 0)
        except OperationalError:
            return [], 0
        return rows, total

    def _engine(self):
        if self.ds is None:
            raise OpsSpecValidationError("Local datastore is not available for Ops spec queries.")
        return self.ds.meta_dbconn.con

    def _table(self, table_name: str) -> Table:
        if self.catalog is not None and table_name in self.catalog.catalog:
            data_table = getattr(self.catalog.catalog[table_name].store, "data_table", None)
            if isinstance(data_table, Table):
                return data_table
        if self.ds is not None and table_name in self.ds.tables:
            data_table = getattr(self.ds.get_table(table_name).table_store, "data_table", None)
            if isinstance(data_table, Table):
                return data_table
        engine = self._engine()
        schema = self.ds.meta_dbconn.schema if self.ds is not None else None
        if inspect(engine).has_table(table_name, schema=schema):
            return Table(table_name, MetaData(), schema=schema, autoload_with=engine)
        raise OpsSpecValidationError(f'Table "{table_name}" is not configured in the datastore or catalog.')

    def _physical_table_exists(self, table: Table) -> bool:
        return inspect(self._engine()).has_table(table.name, schema=table.schema)


def metric_table_columns(table_spec: OpsMetricTableSpec) -> list[OpsColumn]:
    columns: list[OpsColumn] = []
    columns.extend(table_spec.primary_columns)
    for column in table_spec.metric_columns:
        if isinstance(column, OpsColumnGroup):
            columns.extend(column.columns)
        else:
            columns.append(column)
    columns.extend(table_spec.filters)
    return list({column.id: column for column in columns}.values())

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any, Iterable, Literal, Sequence

from datapipe.compute import Catalog
from datapipe.datatable import DataStore
from sqlalchemy import MetaData, String, Table, and_, asc, desc, event, func, inspect, not_, or_, select
from sqlalchemy.exc import OperationalError

from datapipe_app.ops_filters import OpsFilterMode, OpsFilterRule, compile_regex_pattern
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
        filter_rules: Sequence[OpsFilterRule] | None = None,
        filter_mode: OpsFilterMode = "or",
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
        where_parts: list[Any] = []

        rule_conditions = self._build_filter_rule_conditions(
            table,
            table_name,
            allowed_by_id,
            allowed_by_source,
            filter_rules or [],
        )
        if rule_conditions:
            combined = or_(*rule_conditions) if filter_mode == "or" else and_(*rule_conditions)
            where_parts.append(combined)

        legacy_conditions = self._build_legacy_filter_conditions(
            table,
            table_name,
            allowed_by_id,
            allowed_by_source,
            filters or {},
        )
        where_parts.extend(legacy_conditions)

        if search:
            search_conditions = [
                table.c[col.source].cast(String).ilike(f"%{search}%")
                for col in allowed_columns
                if col.kind in {"text", "link", "chip", "status"} and col.source in table.c
            ]
            if search_conditions:
                where_parts.append(or_(*search_conditions))

        for condition in where_parts:
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

    def _build_filter_rule_conditions(
        self,
        table: Table,
        table_name: str,
        allowed_by_id: dict[str, OpsColumn],
        allowed_by_source: dict[str, OpsColumn],
        filter_rules: Sequence[OpsFilterRule],
    ) -> list[Any]:
        conditions: list[Any] = []
        for rule in filter_rules:
            spec_col = allowed_by_id.get(rule.column_id) or allowed_by_source.get(rule.column_id)
            if spec_col is None:
                continue
            if not spec_col.filterable:
                raise OpsSpecValidationError(
                    f'Filter "{rule.column_id}" is not configured for table "{table_name}".'
                )
            condition = self._filter_condition(table, spec_col, rule.operator, rule.value)
            if condition is not None:
                conditions.append(condition)
        return conditions

    def _build_legacy_filter_conditions(
        self,
        table: Table,
        table_name: str,
        allowed_by_id: dict[str, OpsColumn],
        allowed_by_source: dict[str, OpsColumn],
        filters: dict[str, str | Sequence[str]],
    ) -> list[Any]:
        conditions: list[Any] = []
        for key, value in filters.items():
            spec_col = allowed_by_id.get(key) or allowed_by_source.get(key)
            if spec_col is None:
                continue
            if not spec_col.filterable:
                raise OpsSpecValidationError(f'Filter "{key}" is not configured for table "{table_name}".')
            values = value if isinstance(value, Sequence) and not isinstance(value, (str, bytes)) else [value]
            values = [item for item in values if item not in {None, ""}]
            if not values:
                continue
            if len(values) == 1:
                conditions.append(table.c[spec_col.source] == values[0])
            else:
                conditions.append(table.c[spec_col.source].in_(list(values)))
        return conditions

    def _filter_condition(
        self,
        table: Table,
        col: OpsColumn,
        operator: str,
        value: str | None,
    ) -> Any | None:
        raw = table.c[col.source]
        expr = raw.cast(String)
        if operator == "contains":
            return expr.ilike(f"%{value}%")
        if operator == "not_contains":
            return not_(expr.ilike(f"%{value}%"))
        if operator == "equal":
            return expr == str(value)
        if operator == "not_equal":
            return expr != str(value)
        if operator == "is_empty":
            return or_(raw.is_(None), expr == "")
        if operator == "regex":
            return self._regex_condition(expr, str(value))
        raise OpsSpecValidationError(f'Unknown filter operator "{operator}"')

    def _regex_condition(self, expr: Any, value: str) -> Any:
        compile_regex_pattern(value)
        dialect = self._engine().dialect.name
        if dialect == "postgresql":
            return expr.op("~")(value)
        if dialect == "sqlite":
            return expr.op("REGEXP")(value)
        raise OpsSpecValidationError(f'Regex filters are not supported for dialect "{dialect}"')

    def _engine(self):
        if self.ds is None:
            raise OpsSpecValidationError("Local datastore is not available for Ops spec queries.")
        engine = self.ds.meta_dbconn.con
        self._ensure_sqlite_regexp(engine)
        return engine

    @staticmethod
    def _ensure_sqlite_regexp(engine) -> None:
        if engine.dialect.name != "sqlite":
            return
        if getattr(engine, "_ops_regexp_registered", False):
            return

        @event.listens_for(engine, "connect")
        def _register_regexp(dbapi_connection, _connection_record) -> None:
            def regexp(pattern: str, value: object) -> int:
                if value is None:
                    return 0
                try:
                    return 1 if re.search(pattern, str(value)) else 0
                except re.error:
                    return 0

            dbapi_connection.create_function("REGEXP", 2, regexp)

        engine._ops_regexp_registered = True  # type: ignore[attr-defined]

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

import copy
import logging
import math
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Tuple, cast

import pandas as pd
from cityhash import CityHash32
from opentelemetry import trace
from sqlalchemy import Column, Float, Integer, Table, column
from sqlalchemy.schema import SchemaItem
from sqlalchemy.sql.expression import and_, func, or_, select

from datapipe.event_logger import EventLogger
from datapipe.run_config import RunConfig
from datapipe.sql_util import (
    sql_apply_filters_idx_to_subquery,
    sql_apply_idx_filter_to_table,
    sql_apply_runconfig_filter,
)
from datapipe.store.database import DBConn, MetaKey
from datapipe.store.table_store import TableStore
from datapipe.types import (
    DataDF,
    DataSchema,
    IndexDF,
    MetadataDF,
    MetaSchema,
    TAnyDF,
    data_to_index,
    index_difference,
)

if TYPE_CHECKING:
    try:
        # works only with sqlalchemy>=2
        from sqlalchemy.sql.expression import ColumnClause
    except ImportError:
        ColumnClause = Any  # type: ignore

logger = logging.getLogger("datapipe.datatable")
tracer = trace.get_tracer("datapipe.datatable")


TABLE_META_SCHEMA: List[Column] = [
    Column("hash", Integer),
    Column("create_ts", Float),  # Время создания строки
    Column("update_ts", Float),  # Время последнего изменения
    Column("process_ts", Float),  # Время последней успешной обработки
    Column("delete_ts", Float),  # Время удаления
]


@dataclass
class TableDebugInfo:
    name: str
    size: int


class MetaTable:
    def __init__(
        self,
        dbconn: DBConn,
        name: str,
        primary_schema: DataSchema,
        meta_schema: MetaSchema = [],
        create_table: bool = False,
    ):
        self.dbconn = dbconn
        self.name = name

        self.primary_schema = primary_schema
        self.primary_keys: List[str] = [column.name for column in primary_schema]

        for item in primary_schema:
            item.primary_key = True

        self.meta_schema = meta_schema
        self.meta_keys = {}

        meta_key_prop = MetaKey.get_property_name()

        for column in meta_schema:
            target_name = (
                column.meta_key.target_name
                if hasattr(column, meta_key_prop)
                else column.name
            )
            self.meta_keys[target_name] = column.name

        self.sql_schema: List[SchemaItem] = [
            i._copy() for i in primary_schema + meta_schema + TABLE_META_SCHEMA
        ]

        self.sql_table = Table(
            f"{self.name}_meta",
            self.dbconn.sqla_metadata,
            *self.sql_schema,
        )

        if create_table:
            self.sql_table.create(self.dbconn.con, checkfirst=True)

    def __reduce__(self) -> Tuple[Any, ...]:
        return self.__class__, (
            self.dbconn,
            self.name,
            self.primary_schema,
            self.meta_schema,
            False,
        )

    def _chunk_size(self):
        # Magic number derived empirically. See
        # https://github.com/epoch8/datapipe/issues/178 for details.
        #
        # TODO Investigate deeper how does stack in Postgres work
        return 5000 // len(self.primary_keys)

    def _chunk_idx_df(self, idx: TAnyDF) -> Iterator[TAnyDF]:
        """
        Split IndexDF to chunks acceptable for typical Postgres configuration.
        See `_chunk_size` for detatils.
        """

        CHUNK_SIZE = self._chunk_size()

        for chunk_no in range(int(math.ceil(len(idx) / CHUNK_SIZE))):
            chunk_idx = idx.iloc[chunk_no * CHUNK_SIZE : (chunk_no + 1) * CHUNK_SIZE, :]

            yield cast(TAnyDF, chunk_idx)

    def _build_metadata_query(
        self, sql, idx: Optional[IndexDF] = None, include_deleted: bool = False
    ):
        if idx is not None:
            if len(self.primary_keys) == 0:
                # Когда ключей нет - не делаем ничего
                pass

            else:
                sql = sql_apply_idx_filter_to_table(
                    sql, self.sql_table, self.primary_keys, idx
                )

        if not include_deleted:
            sql = sql.where(self.sql_table.c.delete_ts.is_(None))

        return sql

    def get_metadata(
        self, idx: Optional[IndexDF] = None, include_deleted: bool = False
    ) -> MetadataDF:
        """
        Получить датафрейм с метаданными.

        idx - опциональный фильтр по целевым строкам
        include_deleted - флаг, возвращать ли удаленные строки, по умолчанию = False
        """

        res = []
        sql = select(*self.sql_schema)  # type: ignore

        with self.dbconn.con.begin() as con:
            if idx is None:
                sql = self._build_metadata_query(sql, idx, include_deleted)
                return cast(MetadataDF, pd.read_sql_query(sql, con=con))

            for chunk_idx in self._chunk_idx_df(idx):
                chunk_sql = self._build_metadata_query(sql, chunk_idx, include_deleted)

                res.append(pd.read_sql_query(chunk_sql, con=con))

            if len(res) > 0:
                return cast(MetadataDF, pd.concat(res))
            else:
                return cast(
                    MetadataDF,
                    pd.DataFrame(columns=[column.name for column in self.sql_schema]),  # type: ignore
                )

    def get_metadata_size(
        self, idx: Optional[IndexDF] = None, include_deleted: bool = False
    ) -> int:
        """
        Получить количество строк метаданных.

        idx - опциональный фильтр по целевым строкам
        include_deleted - флаг, возвращать ли удаленные строки, по умолчанию = False
        """

        sql = select(func.count()).select_from(self.sql_table)
        sql = self._build_metadata_query(sql, idx, include_deleted)

        with self.dbconn.con.begin() as con:
            res = con.execute(sql).fetchone()

            assert res is not None and len(res) == 1
            return res[0]

    def _make_new_metadata_df(self, now: float, df: DataDF) -> MetadataDF:
        meta_keys = self.primary_keys + list(self.meta_keys.values())
        res_df = df[meta_keys]

        res_df = res_df.assign(
            hash=self._get_hash_for_df(df),
            create_ts=now,
            update_ts=now,
            process_ts=now,
            delete_ts=None,
        )

        return cast(MetadataDF, res_df)

    def _get_meta_data_columns(self):
        return (
            self.primary_keys
            + list(self.meta_keys.values())
            + [column.name for column in TABLE_META_SCHEMA]
        )

    def _get_hash_for_df(self, df) -> pd.DataFrame:
        return df.apply(lambda x: str(list(x)), axis=1).apply(
            lambda x: int.from_bytes(
                CityHash32(x).to_bytes(4, "little"), "little", signed=True
            )
        )

    # Fix numpy types in Index
    # FIXME разобраться, что это за грязный хак
    def _get_sql_param(self, param):
        return param.item() if hasattr(param, "item") else param

    def get_existing_idx(self, idx: Optional[IndexDF] = None) -> IndexDF:
        sql = select(*self.sql_schema)  # type: ignore

        if idx is not None:
            if len(idx.index) == 0:
                # Empty index -> empty result
                return cast(
                    IndexDF,
                    pd.DataFrame(columns=[column.name for column in self.sql_schema]),  # type: ignore
                )
            idx_cols = list(set(idx.columns.tolist()) & set(self.primary_keys))
        else:
            idx_cols = []

        if len(idx_cols) > 0:
            row_queries = []

            # FIXME поправить на сравнение кортежей
            assert idx is not None
            for _, row in idx.iterrows():
                and_params = [
                    self.sql_table.c[key] == self._get_sql_param(row[key])  # type: ignore
                    for key in idx_cols
                    if key in self.primary_keys
                ]
                and_query = and_(*and_params)
                row_queries.append(and_query)

            sql = sql.where(or_(*row_queries))

        sql = sql.where(self.sql_table.c.delete_ts.is_(None))

        with self.dbconn.con.begin() as con:
            res_df: DataDF = pd.read_sql_query(
                sql,
                con=con,
            )

        return data_to_index(res_df, self.primary_keys)

    def get_table_debug_info(self) -> TableDebugInfo:
        with self.dbconn.con.begin() as con:
            res = con.execute(
                select(func.count()).select_from(self.sql_table)
            ).fetchone()

            assert res is not None and len(res) == 1
            return TableDebugInfo(
                name=self.name,
                size=res[0],
            )

    # TODO Может быть переделать работу с метадатой на контекстный менеджер?
    # FIXME поправить возвращаемые структуры данных, _meta_df должны содержать только _meta колонки
    def get_changes_for_store_chunk(
        self, data_df: DataDF, now: Optional[float] = None
    ) -> Tuple[DataDF, DataDF, MetadataDF, MetadataDF]:
        """
        Анализирует блок данных data_df, выделяет строки new_ которые нужно добавить и строки changed_ которые нужно обновить

        Returns tuple:
            new_data_df     - строки данных, которые нужно добавить
            changed_data_df - строки данных, которые нужно изменить
            new_meta_df     - строки метаданных, которые нужно добавить
            changed_meta_df - строки метаданных, которые нужно изменить
        """

        if now is None:
            now = time.time()

        # получить meta по чанку
        existing_meta_df = self.get_metadata(
            data_to_index(data_df, self.primary_keys), include_deleted=True
        )
        data_cols = list(data_df.columns)
        meta_cols = self._get_meta_data_columns()

        # Дополняем данные методанными
        merged_df = pd.merge(
            data_df.assign(data_hash=self._get_hash_for_df(data_df)),
            existing_meta_df,
            how="left",
            left_on=self.primary_keys,
            right_on=self.primary_keys,
            suffixes=("", "_exist"),
        )

        new_idx = merged_df["hash"].isna() | merged_df["delete_ts"].notnull()

        # Ищем новые записи
        new_df = data_df.loc[new_idx.values, data_cols]  # type: ignore

        # Создаем мета данные для новых записей
        new_meta_data_df = merged_df.loc[merged_df["hash"].isna().values, data_cols]  # type: ignore
        new_meta_df = self._make_new_metadata_df(now, new_meta_data_df)

        # Ищем изменившиеся записи
        changed_idx = (
            (merged_df["hash"].notna())
            & (merged_df["delete_ts"].isnull())
            & (merged_df["hash"] != merged_df["data_hash"])
        )
        changed_df = merged_df.loc[changed_idx.values, data_cols]  # type: ignore

        # Меняем мета данные для существующих записей
        changed_meta_idx = (merged_df["hash"].notna()) & (
            merged_df["hash"] != merged_df["data_hash"]
        ) | (merged_df["delete_ts"].notnull())
        changed_meta_df = merged_df.loc[merged_df["hash"].notna(), :].copy()

        changed_meta_df.loc[changed_meta_idx, "update_ts"] = now
        changed_meta_df["process_ts"] = now
        changed_meta_df["delete_ts"] = None
        changed_meta_df["hash"] = changed_meta_df["data_hash"]

        return (
            cast(DataDF, new_df),
            cast(DataDF, changed_df),
            cast(MetadataDF, new_meta_df),
            cast(MetadataDF, changed_meta_df[meta_cols]),
        )

    def update_rows(self, df: MetadataDF) -> None:
        if df.empty:
            return

        insert_sql = self.dbconn.insert(self.sql_table).values(
            df.to_dict(orient="records")
        )

        sql = insert_sql.on_conflict_do_update(
            index_elements=self.primary_keys,
            set_={
                "hash": insert_sql.excluded.hash,
                "update_ts": insert_sql.excluded.update_ts,
                "process_ts": insert_sql.excluded.process_ts,
                "delete_ts": insert_sql.excluded.delete_ts,
            },
        )

        with self.dbconn.con.begin() as con:
            con.execute(sql)

    def mark_rows_deleted(
        self,
        deleted_idx: IndexDF,
        now: Optional[float] = None,
    ) -> None:
        if len(deleted_idx) > 0:
            if now is None:
                now = time.time()

            meta_df = self.get_metadata(deleted_idx)

            meta_df["hash"] = 0
            meta_df["delete_ts"] = now
            meta_df["update_ts"] = now
            meta_df["process_ts"] = now

            self.update_rows(meta_df)

    def get_stale_idx(
        self,
        process_ts: float,
        run_config: Optional[RunConfig] = None,
    ) -> Iterator[IndexDF]:
        idx_cols = [self.sql_table.c[key] for key in self.primary_keys]
        sql = select(*idx_cols).where(
            and_(
                self.sql_table.c.process_ts < process_ts,
                self.sql_table.c.delete_ts.is_(None),
            )
        )

        sql = sql_apply_runconfig_filter(
            sql, self.sql_table, self.primary_keys, run_config
        )

        with self.dbconn.con.begin() as con:
            return cast(
                Iterator[IndexDF],
                list(pd.read_sql_query(sql, con=con, chunksize=1000)),
            )

    def get_changed_rows_count_after_timestamp(
        self,
        ts: float,
    ) -> int:
        sql = select(func.count()).where(
            and_(
                self.sql_table.c.process_ts > ts,
                self.sql_table.c.delete_ts.is_(None),
            )
        )

        with self.dbconn.con.begin() as con:
            res = con.execute(sql).fetchone()
            assert res is not None and len(res) == 1

            return res[0]


class DataTable:
    def __init__(
        self,
        name: str,
        meta_dbconn: DBConn,
        meta_table: MetaTable,
        table_store: TableStore,
        event_logger: EventLogger,
    ):
        self.name = name
        self.meta_dbconn = meta_dbconn
        self.meta_table = meta_table
        self.table_store = table_store
        self.event_logger = event_logger

        self.primary_schema = meta_table.primary_schema
        self.primary_keys = meta_table.primary_keys

    def get_metadata(self, idx: Optional[IndexDF] = None) -> MetadataDF:
        return self.meta_table.get_metadata(idx)

    def get_data(self, idx: Optional[IndexDF] = None) -> DataDF:
        return self.table_store.read_rows(self.meta_table.get_existing_idx(idx))

    def reset_metadata(self):
        with self.meta_dbconn.con.begin() as con:
            con.execute(
                self.meta_table.sql_table.update().values(process_ts=0, update_ts=0)
            )

    def get_size(self) -> int:
        """
        Get the number of non-deleted rows in the DataTable
        """
        return self.meta_table.get_metadata_size(idx=None, include_deleted=False)

    def store_chunk(
        self,
        data_df: DataDF,
        processed_idx: Optional[IndexDF] = None,
        now: Optional[float] = None,
        run_config: Optional[RunConfig] = None,
    ) -> IndexDF:
        """
        Записать новые данные в таблицу.

        При указанном `processed_idx` удалить те строки, которые находятся
        внутри `processed_idx`, но отсутствуют в `data_df`.
        """

        # In case `processed_idx` is not None, check that it contains any of
        # relevant columns
        if processed_idx is not None:
            relevant_keys = set(self.primary_keys) & set(processed_idx.columns)
            if not relevant_keys:
                processed_idx = None

        # Check that all index values in `data_df` is unique
        if data_df.duplicated(self.primary_keys).any():
            raise ValueError("`data_df` index values should be unique")

        changes = [IndexDF(pd.DataFrame(columns=self.primary_keys))]

        with tracer.start_as_current_span(f"{self.name} store_chunk"):
            if not data_df.empty:
                logger.debug(
                    f"Inserting chunk {len(data_df.index)} rows into {self.name}"
                )

                with tracer.start_as_current_span("get_changes_for_store_chunk"):
                    (
                        new_df,
                        changed_df,
                        new_meta_df,
                        changed_meta_df,
                    ) = self.meta_table.get_changes_for_store_chunk(data_df, now)

                # TODO implement transaction meckanism
                with tracer.start_as_current_span("store data"):
                    self.table_store.insert_rows(new_df)
                    self.table_store.update_rows(changed_df)

                with tracer.start_as_current_span("store metadata"):
                    self.meta_table.update_rows(
                        cast(MetadataDF, pd.concat([new_meta_df, changed_meta_df]))
                    )

                    if not new_df.empty:
                        changes.append(data_to_index(new_df, self.primary_keys))
                    if not changed_df.empty:
                        changes.append(data_to_index(changed_df, self.primary_keys))
            else:
                data_df = pd.DataFrame(columns=self.primary_keys)

            with tracer.start_as_current_span("cleanup deleted rows"):
                data_idx = data_to_index(data_df, self.primary_keys)

                if processed_idx is not None:
                    existing_idx = self.meta_table.get_existing_idx(processed_idx)
                    deleted_idx = index_difference(existing_idx, data_idx)

                    self.delete_by_idx(deleted_idx, now=now, run_config=run_config)

                    if not deleted_idx.empty:
                        changes.append(deleted_idx)

        return cast(IndexDF, pd.concat(changes))

    def delete_by_idx(
        self,
        idx: IndexDF,
        now: Optional[float] = None,
        run_config: Optional[RunConfig] = None,
    ) -> None:
        if len(idx) > 0:
            logger.debug(f"Deleting {len(idx.index)} rows from {self.name} data")

            self.table_store.delete_rows(idx)
            self.meta_table.mark_rows_deleted(idx, now=now)

    def delete_stale_by_process_ts(
        self,
        process_ts: float,
        now: Optional[float] = None,
        run_config: Optional[RunConfig] = None,
    ) -> None:
        for deleted_df in self.meta_table.get_stale_idx(
            process_ts, run_config=run_config
        ):
            deleted_idx = data_to_index(deleted_df, self.primary_keys)

            self.delete_by_idx(deleted_idx, now=now, run_config=run_config)

    def get_agg_cte(
        self,
        transform_keys: List[str],
        filters_idx: Optional[IndexDF] = None,
        run_config: Optional[RunConfig] = None,
    ) -> Tuple[List[str], Any]:
        """
        Create a CTE that aggregates the table by transform keys and returns the
        maximum update_ts for each group.

        CTE has the following columns:

        * transform keys which are present in primary keys
        * update_ts

        Returns a tuple of (keys, CTE).
        """

        tbl = self.meta_table.sql_table

        keys = [k for k in transform_keys if k in self.primary_keys]
        key_cols: List["ColumnClause"] = [column(k) for k in keys]

        sql: Any = (
            select(*key_cols + [func.max(tbl.c["update_ts"]).label("update_ts")])
            .select_from(tbl)
            .group_by(*key_cols)
        )

        sql = sql_apply_filters_idx_to_subquery(sql, keys, filters_idx)
        sql = sql_apply_runconfig_filter(sql, tbl, self.primary_keys, run_config)

        return (keys, sql.cte(name=f"{tbl.name}__update"))


class DataStore:
    def __init__(
        self,
        meta_dbconn: DBConn,
        create_meta_table: bool = False,
    ) -> None:
        self.meta_dbconn = meta_dbconn
        self.event_logger = EventLogger()
        self.tables: Dict[str, DataTable] = {}

        self.create_meta_table = create_meta_table

    def create_table(self, name: str, table_store: TableStore) -> DataTable:
        assert name not in self.tables

        primary_schema = table_store.get_primary_schema()
        meta_schema = table_store.get_meta_schema()

        res = DataTable(
            name=name,
            meta_dbconn=self.meta_dbconn,
            meta_table=MetaTable(
                dbconn=self.meta_dbconn,
                name=name,
                primary_schema=primary_schema,
                meta_schema=meta_schema,
                create_table=self.create_meta_table,
            ),
            table_store=table_store,
            event_logger=self.event_logger,
        )

        self.tables[name] = res

        return res

    def get_or_create_table(self, name: str, table_store: TableStore) -> DataTable:
        if name in self.tables:
            return self.tables[name]
        else:
            return self.create_table(name, table_store)

    def get_table(self, name: str) -> DataTable:
        return self.tables[name]

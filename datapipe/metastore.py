import copy
import logging
import math
import time
from dataclasses import dataclass
from typing import Any, Iterator, List, Optional, Tuple, cast

import pandas as pd
from cityhash import CityHash32
from sqlalchemy import Boolean, Column, Float, Integer, String, Table, column, func
from sqlalchemy.sql.expression import and_, delete, or_, select, text, tuple_

from datapipe.datatable import DataStore
from datapipe.run_config import RunConfig
from datapipe.store.database import (
    DBConn,
    MetaKey,
    sql_apply_idx_filter,
    sql_apply_runconfig_filter,
    sql_schema_to_sqltype,
)
from datapipe.types import (
    DataDF,
    DataSchema,
    IndexDF,
    MetadataDF,
    MetaSchema,
    TAnyDF,
    data_to_index,
)

logger = logging.getLogger("datapipe.metastore")

TABLE_META_SCHEMA = [
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
        ds: DataStore,
        name: str,
        primary_schema: DataSchema,
        meta_schema: MetaSchema = [],
    ):
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

        self.sql_schema = [
            copy.copy(i) for i in primary_schema + meta_schema + TABLE_META_SCHEMA
        ]

        self.sql_table = Table(
            f"{self.name}_meta",
            ds.sqla_metadata,
            *self.sql_schema,
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
                sql = sql_apply_idx_filter(sql, self.sql_table, self.primary_keys, idx)

        if not include_deleted:
            sql = sql.where(self.sql_table.c.delete_ts.is_(None))

        return sql

    def get_metadata(
        self,
        dbconn: DBConn,
        idx: Optional[IndexDF] = None,
        include_deleted: bool = False,
    ) -> MetadataDF:
        """
        Получить датафрейм с метаданными.

        idx - опциональный фильтр по целевым строкам
        include_deleted - флаг, возвращать ли удаленные строки, по умолчанию = False
        """

        res = []
        sql = select(self.sql_schema)

        if idx is None:
            sql = self._build_metadata_query(sql, idx, include_deleted)
            return cast(MetadataDF, pd.read_sql_query(sql, con=dbconn.con))

        for chunk_idx in self._chunk_idx_df(idx):
            chunk_sql = self._build_metadata_query(sql, chunk_idx, include_deleted)
            res.append(pd.read_sql_query(chunk_sql, con=dbconn.con))

        if len(res) > 0:
            return cast(MetadataDF, pd.concat(res))
        else:
            return cast(
                MetadataDF,
                pd.DataFrame(columns=[column.name for column in self.sql_schema]),
            )

    def get_metadata_size(
        self,
        dbconn: DBConn,
        idx: Optional[IndexDF] = None,
        include_deleted: bool = False,
    ) -> int:
        """
        Получить количество строк метаданных.

        idx - опциональный фильтр по целевым строкам
        include_deleted - флаг, возвращать ли удаленные строки, по умолчанию = False
        """

        sql = select([func.count()]).select_from(self.sql_table)
        sql = self._build_metadata_query(sql, idx, include_deleted)

        res = dbconn.con.execute(sql).fetchone()
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

    def get_existing_idx(
        self,
        dbconn: DBConn,
        idx: Optional[IndexDF] = None,
    ) -> IndexDF:
        sql = select(self.sql_schema)

        if idx is not None:
            if len(idx.index) == 0:
                # Empty index -> empty result
                return cast(
                    IndexDF,
                    pd.DataFrame(columns=[column.name for column in self.sql_schema]),
                )
            idx_cols = list(set(idx.columns.tolist()) & set(self.primary_keys))

            if not idx_cols:
                raise ValueError("Index does not contain any primary key ")

            row_queries = []

            # FIXME поправить на сравнение кортежей
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

        res_df: DataDF = pd.read_sql_query(
            sql,
            con=dbconn.con,
        )

        return data_to_index(res_df, self.primary_keys)

    def get_table_debug_info(self, dbconn: DBConn) -> TableDebugInfo:
        return TableDebugInfo(
            name=self.name,
            size=dbconn.con.execute(
                select([func.count()]).select_from(self.sql_table)
            ).fetchone()[0],
        )

    # TODO Может быть переделать работу с метадатой на контекстный менеджер?
    # FIXME поправить возвращаемые структуры данных, _meta_df должны содержать только _meta колонки
    def get_changes_for_store_chunk(
        self,
        dbconn: DBConn,
        data_df: DataDF,
        now: Optional[float] = None,
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
            dbconn, data_to_index(data_df, self.primary_keys), include_deleted=True
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

    def _insert_rows(self, dbconn: DBConn, df: MetadataDF) -> None:
        if len(df) > 0:
            logger.debug(f"Inserting {len(df)} rows into {self.name} data")

            df.to_sql(
                name=self.sql_table.name,
                con=dbconn.con,
                schema=dbconn.schema,
                if_exists="append",
                index=False,
                chunksize=1000,
                method="multi",
                dtype=sql_schema_to_sqltype(self.sql_schema),
            )

    def _delete_rows(self, dbconn: DBConn, df: MetadataDF) -> None:
        if len(df) == 0:
            return

        idx = df[self.primary_keys]
        sql = delete(self.sql_table)

        for chunk_idx in self._chunk_idx_df(idx):
            if len(self.primary_keys) == 1:
                # Когда ключ один - сравниваем напрямую
                key = self.primary_keys[0]
                chunk_sql = sql.where(
                    self.sql_table.c[key].in_(chunk_idx[key].to_list())
                )

            else:
                # Когда ключей много - сравниваем через tuple
                keys = tuple_(*[self.sql_table.c[key] for key in self.primary_keys])

                chunk_sql = sql.where(
                    keys.in_(
                        [
                            tuple([r[key] for key in self.primary_keys])  # type: ignore
                            for r in chunk_idx.to_dict(orient="records")
                        ]
                    )
                )

            dbconn.con.execute(chunk_sql)

    def _update_existing_metadata_rows(self, dbconn: DBConn, df: MetadataDF) -> None:
        if len(df) == 0:
            return

        table = (
            f"{dbconn.schema}.{self.sql_table.name}"
            if dbconn.schema
            else self.sql_table.name
        )
        values_table = f"{self.sql_table.name}_values"
        columns = [column.name for column in self.sql_schema]
        update_columns = set(columns) - set(self.primary_keys)

        update_expression = ", ".join(
            [f"{column}={values_table}.{column}" for column in update_columns]
        )

        where_expressiom = " AND ".join(
            [f"{table}.{key} = {values_table}.{key}" for key in self.primary_keys]
        )

        for chunk_df in self._chunk_idx_df(df):
            params_df = chunk_df.reset_index()[columns]
            values_params = []
            params = {}

            for index, row in params_df.iterrows():
                row_values = [
                    f"CAST(:{column.name}_{index} AS {column.type})"
                    for column in self.sql_schema
                ]
                row_params = {f"{key}_{index}": row[key] for key in row.keys()}

                values_params.append(f'({", ".join(row_values)})')
                params.update(row_params)

            stmt = text(
                f"""
                UPDATE {table}
                SET {update_expression}
                FROM (
                    VALUES {", ".join(values_params)}
                ) AS {values_table} ({', '.join(columns)})
                WHERE {where_expressiom}
            """
            )

            dbconn.con.execution_options(compiled_cache=None).execute(stmt, params)

    # TODO объединить
    def insert_meta_for_store_chunk(
        self,
        dbconn: DBConn,
        new_meta_df: MetadataDF,
    ) -> None:
        if len(new_meta_df) > 0:
            self._insert_rows(dbconn, new_meta_df)

    def update_meta_for_store_chunk(
        self,
        dbconn: DBConn,
        changed_meta_df: MetadataDF,
    ) -> None:
        if len(changed_meta_df) > 0:
            if dbconn.supports_update_from:
                self._update_existing_metadata_rows(dbconn, changed_meta_df)
            else:
                self._delete_rows(dbconn, changed_meta_df)
                self._insert_rows(dbconn, changed_meta_df)

    def mark_rows_deleted(
        self,
        dbconn: DBConn,
        deleted_idx: IndexDF,
        now: Optional[float] = None,
    ) -> None:
        if len(deleted_idx) > 0:
            if now is None:
                now = time.time()

            meta_df = self.get_metadata(dbconn, deleted_idx)

            meta_df["hash"] = 0
            meta_df["delete_ts"] = now
            meta_df["update_ts"] = now
            meta_df["process_ts"] = now

            self.update_meta_for_store_chunk(dbconn, meta_df)

    def get_stale_idx(
        self,
        dbconn: DBConn,
        process_ts: float,
        run_config: Optional[RunConfig] = None,
    ) -> Iterator[IndexDF]:
        idx_cols = [self.sql_table.c[key] for key in self.primary_keys]
        sql = select(idx_cols).where(
            and_(
                self.sql_table.c.process_ts < process_ts,
                self.sql_table.c.delete_ts.is_(None),
            )
        )

        sql = sql_apply_runconfig_filter(
            sql, self.sql_table, self.primary_keys, run_config
        )

        return cast(
            Iterator[IndexDF],
            pd.read_sql_query(sql, con=dbconn.con, chunksize=1000),
        )

    def make_agg_update_sql(
        self,
        transform_keys: List[str],
        run_config: Optional[RunConfig] = None,
    ) -> Tuple[List[str], Any]:
        """
        Prepare SQL subquery for update_ts aggregation in relation to transform_keys

        Returns tuple:
            keys - list of keys for aggregation
            sql - SQLAlchemy select with keys columns and max(update_ts) as update_ts
        """

        tbl = self.sql_table

        keys = [k for k in transform_keys if k in self.primary_keys]
        key_cols = [column(k) for k in keys]

        sql = (
            select(*key_cols + [func.max(tbl.c["update_ts"]).label("update_ts")])
            .select_from(tbl)
            .group_by(*key_cols)
        )

        sql = sql_apply_runconfig_filter(sql, tbl, self.primary_keys, run_config)

        return (keys, sql.cte(name=f"{tbl.name}__update_ts"))


TRANSFORM_META_SCHEMA = [
    Column("process_ts", Float),  # Время последней успешной обработки
    Column("is_success", Boolean),  # Успешно ли обработана строка
    Column("error", String),  # Текст ошибки
]


class TransformMetaTable:
    def __init__(
        self,
        ds: DataStore,
        name: str,
        primary_schema: DataSchema,
    ) -> None:
        self.name = name
        self.primary_schema = primary_schema
        self.primary_keys = [i.name for i in primary_schema]

        self.sql_schema = [i.copy() for i in primary_schema + TRANSFORM_META_SCHEMA]

        self.sql_table = Table(
            name,
            ds.sqla_metadata,
            *self.sql_schema,
        )

    def mark_rows_processed_success(
        self,
        dbconn: DBConn,
        idx: IndexDF,
        process_ts: float,
        run_config: Optional[RunConfig] = None,
    ) -> None:
        idx = cast(IndexDF, idx[self.primary_keys])

        insert_sql = dbconn.insert(self.sql_table).values(
            [
                {
                    "process_ts": process_ts,
                    "is_success": True,
                    "error": None,
                    **idx_dict,  # type: ignore
                }
                for idx_dict in idx.to_dict(orient="records")
            ]
        )

        sql = insert_sql.on_conflict_do_update(
            index_elements=self.primary_keys,
            set_={
                "process_ts": process_ts,
                "is_success": True,
                "error": None,
            },
        )

        # execute
        dbconn.con.execute(sql)

    def mark_rows_processed_error(
        self,
        dbconn: DBConn,
        idx: IndexDF,
        process_ts: float,
        error: str,
        run_config: Optional[RunConfig] = None,
    ) -> None:
        idx = cast(IndexDF, idx[self.primary_keys])

        insert_sql = dbconn.insert(self.sql_table).values(
            [
                {
                    "process_ts": process_ts,
                    "is_success": False,
                    "error": error,
                    **idx_dict,  # type: ignore
                }
                for idx_dict in idx.to_dict(orient="records")
            ]
        )

        sql = insert_sql.on_conflict_do_update(
            index_elements=self.primary_keys,
            set_={
                "process_ts": process_ts,
                "is_success": False,
                "error": error,
            },
        )

        # execute
        dbconn.con.execute(sql)

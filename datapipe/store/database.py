import copy
import logging
import math
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, Union, cast

import numpy as np
import pandas as pd
from opentelemetry import trace
from sqlalchemy import Column, MetaData, Table, create_engine, func, text
from sqlalchemy.pool import QueuePool, SingletonThreadPool
from sqlalchemy.schema import SchemaItem
from sqlalchemy.sql.base import SchemaEventTarget
from sqlalchemy.sql.expression import delete, select

from datapipe.run_config import RunConfig
from datapipe.sql_util import sql_apply_idx_filter_to_table, sql_apply_runconfig_filter
from datapipe.store.table_store import TableStore, TableStoreCaps
from datapipe.types import DataDF, DataSchema, IndexDF, MetaSchema, OrmTable, TAnyDF

logger = logging.getLogger("datapipe.store.database")
tracer = trace.get_tracer("datapipe.store.database")


class DBConn:
    def __init__(
        self,
        connstr: str,
        schema: Optional[str] = None,
        create_engine_kwargs: Optional[Dict[str, Any]] = None,
        sqla_metadata: Optional[MetaData] = None,
    ):
        create_engine_kwargs = create_engine_kwargs or {}
        self._init(connstr, schema, create_engine_kwargs, sqla_metadata)

    def _init(
        self,
        connstr: str,
        schema: Optional[str],
        create_engine_kwargs: Dict[str, Any],
        sqla_metadata: Optional[MetaData] = None,
    ) -> None:
        self.connstr = connstr
        self.schema = schema
        self.create_engine_kwargs = create_engine_kwargs

        self.insert: Callable
        self.func_greatest: Callable
        if connstr.startswith("sqlite") or connstr.startswith("pysqlite"):
            self.supports_update_from = False

            from sqlalchemy.dialects.sqlite import insert

            self.insert = insert
            self.func_greatest = func.max

            self.con = create_engine(
                connstr,
                poolclass=SingletonThreadPool,
                **create_engine_kwargs,
            )

            # WAL mode is required for concurrent reads and writes
            # https://www.sqlite.org/wal.html
            with self.con.begin() as con:
                con.execute(text("PRAGMA journal_mode=WAL"))
        else:
            # Assume relatively new Postgres
            self.supports_update_from = True

            from sqlalchemy.dialects.postgresql import insert as pg_insert

            self.insert = pg_insert
            self.func_greatest = func.greatest

            self.con = create_engine(
                connstr,
                poolclass=QueuePool,
                pool_pre_ping=True,
                pool_recycle=3600,
                **create_engine_kwargs,
                # pool_size=25,
            )

        if sqla_metadata is None:
            self.sqla_metadata = MetaData(schema=schema)
        else:
            self.sqla_metadata = sqla_metadata

    def __reduce__(self) -> Tuple[Any, ...]:
        return self.__class__, (
            self.connstr,
            self.schema,
            self.create_engine_kwargs,
        )

    def __getstate__(self):
        return {
            "connstr": self.connstr,
            "schema": self.schema,
            "create_engine_kwargs": self.create_engine_kwargs,
        }

    def __setstate__(self, state):
        self._init(state["connstr"], state["schema"], state["create_engine_kwargs"])


class MetaKey(SchemaItem):
    def __init__(self, target_name: Optional[str] = None) -> None:
        self.target_name = target_name

    def _set_parent(self, parent: SchemaEventTarget, **kw: Any) -> None:
        self.parent = parent
        self.parent.meta_key = self  # type: ignore

        if not self.target_name:
            self.target_name = parent.name  # type: ignore

    @classmethod
    def get_property_name(cls) -> str:
        return "meta_key"


class TableStoreDB(TableStore):
    caps = TableStoreCaps(
        supports_delete=True,
        supports_get_schema=True,
        supports_read_all_rows=True,
        supports_read_nonexistent_rows=True,
        supports_read_meta_pseudo_df=True,
    )

    def __init__(
        self,
        dbconn: Union["DBConn", str],
        name: Optional[str] = None,
        data_sql_schema: Optional[List[Column]] = None,
        create_table: bool = False,
        orm_table: Optional[OrmTable] = None,
    ) -> None:
        if isinstance(dbconn, str):
            self.dbconn = DBConn(dbconn)
        else:
            self.dbconn = dbconn

        if orm_table is not None:
            assert name is None, "name should be None if orm_table is provided"
            assert data_sql_schema is None, "data_sql_schema should be None if orm_table is provided"

            orm_table__table = orm_table.__table__  # type: ignore
            self.data_table = cast(Table, orm_table__table)

            self.name = self.data_table.name

            self.data_sql_schema = [
                Column(
                    column.name,
                    column.type,
                    primary_key=column.primary_key,
                    nullable=column.nullable,
                    unique=column.unique,
                    *column.constraints,
                )
                for column in self.data_table.columns
            ]
            self.data_keys = [column.name for column in self.data_sql_schema if not column.primary_key]

        else:
            assert name is not None, "name should be provided if data_table is not provided"
            assert data_sql_schema is not None, "data_sql_schema should be provided if data_table is not provided"

            self.name = name

            self.data_sql_schema = data_sql_schema

            self.data_keys = [column.name for column in self.data_sql_schema if not column.primary_key]

            self.data_table = Table(
                self.name,
                self.dbconn.sqla_metadata,
                *[copy.copy(i) for i in self.data_sql_schema],
                extend_existing=True,
            )

        if create_table:
            self.data_table.create(self.dbconn.con, checkfirst=True)

    def __reduce__(self) -> Tuple[Any, ...]:
        return self.__class__, (
            self.dbconn,
            self.name,
            self.data_sql_schema,
        )

    def get_schema(self) -> DataSchema:
        return self.data_sql_schema

    def get_primary_schema(self) -> DataSchema:
        return [column for column in self.data_sql_schema if column.primary_key]

    def get_meta_schema(self) -> MetaSchema:
        meta_key_prop = MetaKey.get_property_name()
        return [column for column in self.data_sql_schema if hasattr(column, meta_key_prop)]

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

    def delete_rows(self, idx: IndexDF) -> None:
        if idx is None or len(idx.index) == 0:
            return

        logger.debug(f"Deleting {len(idx.index)} rows from {self.name} data")

        for chunk_idx in self._chunk_idx_df(idx):
            sql = sql_apply_idx_filter_to_table(delete(self.data_table), self.data_table, self.primary_keys, chunk_idx)
            with self.dbconn.con.begin() as con:
                con.execute(sql)

    def insert_rows(self, df: DataDF) -> None:
        self.update_rows(df)

    def update_rows(self, df: DataDF) -> None:
        if df.empty:
            return

        # TODO разобраться, нужен ли этот блок кода, написать тесты на заполнение None/NULL
        insert_sql = self.dbconn.insert(self.data_table).values(
            df.fillna(np.nan).replace({np.nan: None}).to_dict(orient="records")
        )

        if len(self.data_keys) > 0:
            sql = insert_sql.on_conflict_do_update(
                index_elements=self.primary_keys,
                set_={col.name: insert_sql.excluded[col.name] for col in self.data_sql_schema if not col.primary_key},
            )
        else:
            sql = insert_sql.on_conflict_do_nothing(index_elements=self.primary_keys)

        with self.dbconn.con.begin() as con:
            con.execute(sql)

    # Fix numpy types in IndexDF
    def _get_sql_param(self, param):
        return param.item() if hasattr(param, "item") else param

    def read_rows(self, idx: Optional[IndexDF] = None) -> pd.DataFrame:
        sql = select(*self.data_table.c)

        if idx is not None:
            if len(idx.index) == 0:
                # Empty index -> empty result
                return pd.DataFrame(columns=[column.name for column in self.data_sql_schema])

            res = []

            with self.dbconn.con.begin() as con:
                for chunk_idx in self._chunk_idx_df(idx):
                    chunk_sql = sql_apply_idx_filter_to_table(sql, self.data_table, self.primary_keys, chunk_idx)
                    chunk_df = pd.read_sql_query(chunk_sql, con=con)

                    res.append(chunk_df)

            return pd.concat(res, ignore_index=True)

        else:
            with self.dbconn.con.begin() as con:
                return pd.read_sql_query(sql, con=con)

    def read_rows_meta_pseudo_df(
        self,
        chunksize: int = 1000,
        run_config: Optional[RunConfig] = None,
    ) -> Iterator[DataDF]:
        sql = select(*self.data_table.c)

        sql = sql_apply_runconfig_filter(sql, self.data_table, self.primary_keys, run_config)

        with self.dbconn.con.execution_options(stream_results=True).begin() as con:
            yield from pd.read_sql_query(
                sql,
                con=con,
                chunksize=chunksize,
            )

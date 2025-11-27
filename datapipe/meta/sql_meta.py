import itertools
import math
import time
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Literal,
    Optional,
    Set,
    Tuple,
    cast,
)

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import or_

from datapipe.run_config import RunConfig
from datapipe.sql_util import sql_apply_idx_filter_to_table, sql_apply_runconfig_filter
from datapipe.store.database import DBConn, MetaKey, TableStoreDB
from datapipe.types import (
    DataDF,
    DataSchema,
    HashDF,
    IndexDF,
    MetadataDF,
    MetaSchema,
    TAnyDF,
    data_to_index,
    hash_to_index,
)

if TYPE_CHECKING:
    from datapipe.compute import ComputeInput
    from datapipe.step.batch_transform import BaseBatchTransformStep
    from datapipe.datatable import DataStore


TABLE_META_SCHEMA: List[sa.Column] = [
    sa.Column("hash", sa.Integer),
    sa.Column("create_ts", sa.Float),  # Время создания строки
    sa.Column("update_ts", sa.Float),  # Время последнего изменения
    sa.Column("process_ts", sa.Float),  # Время последней успешной обработки
    sa.Column("delete_ts", sa.Float),  # Время удаления
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
    ) -> None:
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
            target_name = column.meta_key.target_name if hasattr(column, meta_key_prop) else column.name
            self.meta_keys[target_name] = column.name

        self.sql_schema: List[sa.schema.SchemaItem] = [
            i._copy() for i in primary_schema + meta_schema + TABLE_META_SCHEMA
        ]

        self.sql_table = sa.Table(
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

    def _build_metadata_query(self, sql, idx: Optional[IndexDF] = None, include_deleted: bool = False):
        if idx is not None:
            if len(self.primary_keys) == 0:
                # Когда ключей нет - не делаем ничего
                pass

            else:
                sql = sql_apply_idx_filter_to_table(sql, self.sql_table, self.primary_keys, idx)

        if not include_deleted:
            sql = sql.where(self.sql_table.c.delete_ts.is_(None))

        return sql

    def get_metadata(self, idx: Optional[IndexDF] = None, include_deleted: bool = False) -> MetadataDF:
        """
        Получить датафрейм с метаданными.

        idx - опциональный фильтр по целевым строкам
        include_deleted - флаг, возвращать ли удаленные строки, по умолчанию = False
        """

        res = []
        sql = sa.select(*self.sql_schema)  # type: ignore

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

    def get_metadata_size(self, idx: Optional[IndexDF] = None, include_deleted: bool = False) -> int:
        """
        Получить количество строк метаданных.

        idx - опциональный фильтр по целевым строкам
        include_deleted - флаг, возвращать ли удаленные строки, по умолчанию = False
        """

        sql = sa.select(sa.func.count()).select_from(self.sql_table)
        sql = self._build_metadata_query(sql, idx, include_deleted)

        with self.dbconn.con.begin() as con:
            res = con.execute(sql).fetchone()

            assert res is not None and len(res) == 1
            return res[0]

    def _make_new_metadata_df(self, now: float, df: HashDF) -> MetadataDF:
        res_df = df.assign(
            create_ts=now,
            update_ts=now,
            process_ts=now,
            delete_ts=None,
        )

        return cast(MetadataDF, res_df)

    def _get_meta_data_columns(self):
        return self.primary_keys + list(self.meta_keys.values()) + [column.name for column in TABLE_META_SCHEMA]

    # Fix numpy types in Index
    # FIXME разобраться, что это за грязный хак
    def _get_sql_param(self, param):
        return param.item() if hasattr(param, "item") else param

    def get_existing_idx(self, idx: Optional[IndexDF] = None) -> IndexDF:
        sql = sa.select(*self.sql_schema)  # type: ignore

        if idx is not None:
            if len(idx.index) == 0:
                # Empty index -> empty result
                return cast(
                    IndexDF,
                    pd.DataFrame(columns=self.primary_keys),  # type: ignore
                )
            idx_cols = list(set(idx.columns.tolist()) & set(self.primary_keys))
        else:
            idx_cols = []

        if len(idx_cols) > 0 and idx is not None and len(idx) > 0:
            sql = sql_apply_idx_filter_to_table(sql=sql, table=self.sql_table, primary_keys=idx_cols, idx=idx)

        sql = sql.where(self.sql_table.c.delete_ts.is_(None))

        with self.dbconn.con.begin() as con:
            res_df: DataDF = pd.read_sql_query(
                sql,
                con=con,
            )

        return data_to_index(res_df, self.primary_keys)

    def get_table_debug_info(self) -> TableDebugInfo:
        with self.dbconn.con.begin() as con:
            res = con.execute(sa.select(sa.func.count()).select_from(self.sql_table)).fetchone()

            assert res is not None and len(res) == 1
            return TableDebugInfo(
                name=self.name,
                size=res[0],
            )

    # TODO Может быть переделать работу с метадатой на контекстный менеджер?
    def get_changes_for_store_chunk(
        self, hash_df: HashDF, now: Optional[float] = None
    ) -> Tuple[IndexDF, IndexDF, MetadataDF, MetadataDF]:
        """
        Анализирует блок hash_df, выделяет строки new_ которые нужно добавить и строки changed_ которые нужно обновить

        Returns tuple:
            new_index_df     - индекс данных, которые нужно добавить
            changed_index_df - индекс данных, которые нужно изменить
            new_meta_df     - строки метаданных, которые нужно добавить
            changed_meta_df - строки метаданных, которые нужно изменить
        """

        if now is None:
            now = time.time()

        # получить meta по чанку
        existing_meta_df = self.get_metadata(hash_to_index(hash_df, self.primary_keys), include_deleted=True)
        hash_cols = list(hash_df.columns)
        meta_cols = self._get_meta_data_columns()

        # Дополняем данные методанными
        merged_df = pd.merge(
            hash_df,
            existing_meta_df,
            how="left",
            left_on=self.primary_keys,
            right_on=self.primary_keys,
            suffixes=("", "_exist"),
        )

        new_idx = merged_df["hash_exist"].isna() | merged_df["delete_ts"].notnull()

        # Ищем новые записи
        new_index_df = hash_df.loc[new_idx.values, self.primary_keys]  # type: ignore

        # Создаем мета данные для новых записей
        new_meta_data_df = merged_df.loc[merged_df["hash_exist"].isna().values, hash_cols]  # type: ignore
        new_meta_df = self._make_new_metadata_df(now, cast(HashDF, new_meta_data_df))

        # Ищем изменившиеся записи
        changed_idx = (
            (merged_df["hash_exist"].notna())
            & (merged_df["delete_ts"].isnull())
            & (merged_df["hash_exist"] != merged_df["hash"])
        )
        changed_index_df = merged_df.loc[changed_idx.values, self.primary_keys]  # type: ignore

        # Меняем мета данные для существующих записей
        changed_meta_idx = (merged_df["hash_exist"].notna()) & (merged_df["hash_exist"] != merged_df["hash"]) | (
            merged_df["delete_ts"].notnull()
        )
        changed_meta_df = merged_df.loc[merged_df["hash_exist"].notna(), meta_cols].copy()

        changed_meta_df.loc[changed_meta_idx, "update_ts"] = now
        changed_meta_df["process_ts"] = now
        changed_meta_df["delete_ts"] = None

        return (
            cast(IndexDF, new_index_df),
            cast(IndexDF, changed_index_df),
            cast(MetadataDF, new_meta_df),
            cast(MetadataDF, changed_meta_df[meta_cols]),
        )

    def update_rows(self, df: MetadataDF) -> None:
        if df.empty:
            return

        insert_sql = self.dbconn.insert(self.sql_table).values(df.to_dict(orient="records"))

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
        sql = sa.select(*idx_cols).where(
            sa.and_(
                self.sql_table.c.process_ts < process_ts,
                self.sql_table.c.delete_ts.is_(None),
            )
        )

        sql = sql_apply_runconfig_filter(sql, self.sql_table, self.primary_keys, run_config)

        with self.dbconn.con.begin() as con:
            return cast(
                Iterator[IndexDF],
                list(pd.read_sql_query(sql, con=con, chunksize=1000)),
            )

    def get_changed_rows_count_after_timestamp(
        self,
        ts: float,
    ) -> int:
        sql = sa.select(sa.func.count()).where(
            sa.and_(
                self.sql_table.c.process_ts > ts,
                self.sql_table.c.delete_ts.is_(None),
            )
        )

        with self.dbconn.con.begin() as con:
            res = con.execute(sql).fetchone()
            assert res is not None and len(res) == 1

            return res[0]

    def get_agg_cte(
        self,
        transform_keys: List[str],
        filters_idx: Optional[IndexDF] = None,
        run_config: Optional[RunConfig] = None,
    ) -> Tuple[List[str], Any]:
        """
        Create a CTE that aggregates the meta table by transform keys and returns the
        maximum update_ts for each group.

        CTE has the following columns:

        * transform keys which are present in primary keys
        * update_ts

        Returns a tuple of (keys, CTE).
        """

        tbl = self.sql_table

        keys = [k for k in transform_keys if k in self.primary_keys]
        key_cols: List[Any] = [sa.column(k) for k in keys]

        sql: Any = sa.select(*key_cols + [sa.func.max(tbl.c["update_ts"]).label("update_ts")]).select_from(tbl)

        if len(key_cols) > 0:
            sql = sql.group_by(*key_cols)

        sql = sql_apply_filters_idx_to_subquery(sql, keys, filters_idx)
        sql = sql_apply_runconfig_filter(sql, tbl, self.primary_keys, run_config)

        return (keys, sql.cte(name=f"{tbl.name}__update"))


TRANSFORM_META_SCHEMA: DataSchema = [
    sa.Column("process_ts", sa.Float),  # Время последней успешной обработки
    sa.Column("priority", sa.Integer),  # Приоритет обработки (чем больше, тем выше)
    sa.Column("status", sa.String), # Статус исполнения трансформации
    sa.Column("error", sa.String),  # Текст ошибки
]


class TransformStatus(Enum):
    PENDING = "pending"
    COMPLETED = "completed"
    ERROR = "error"


class TransformMetaTable:
    def __init__(
        self,
        dbconn: DBConn,
        name: str,
        primary_schema: DataSchema,
        create_table: bool = False,
    ) -> None:
        self.dbconn = dbconn
        self.name = name
        self.primary_schema = primary_schema
        self.primary_keys = [i.name for i in primary_schema]

        self.sql_schema = [i._copy() for i in primary_schema + TRANSFORM_META_SCHEMA]

        self.sql_table = sa.Table(
            name,
            dbconn.sqla_metadata,
            *self.sql_schema,
            # TODO remove in 0.15 release
            keep_existing=True,
        )

        if create_table:
            self.sql_table.create(self.dbconn.con, checkfirst=True)

    def __reduce__(self) -> Tuple[Any, ...]:
        return self.__class__, (
            self.dbconn,
            self.name,
            self.primary_schema,
        )

    def insert_rows(
        self,
        idx: IndexDF,
    ) -> None:
        """
        Создает строки в таблице метаданных для указанных индексов. Если строки
        уже существуют - не делает ничего.
        """

        idx = cast(IndexDF, idx[self.primary_keys])

        insert_sql = self.dbconn.insert(self.sql_table).values(
            [
                {
                    "process_ts": 0,
                    "priority": 0,
                    "error": None,
                    "status": TransformStatus.PENDING.value,
                    **idx_dict,  # type: ignore
                }
                for idx_dict in idx.to_dict(orient="records")
            ]
        )

        sql = insert_sql.on_conflict_do_nothing(index_elements=self.primary_keys)

        with self.dbconn.con.begin() as con:
            con.execute(sql)

    def insert_rows_by_sql(self, sql) -> None:
        sql = sql.add_columns(
            sa.literal(0).label("process_ts"),
            sa.literal(0).label("priority"),
            sa.literal(None).label("error"),
            sa.literal(TransformStatus.PENDING.value).label("status")
        )
        insert_sql = self.dbconn.insert(self.sql_table).from_select(
            sorted(self.primary_keys) + ["process_ts", "priority", "error", "status"],
            sql
        ).on_conflict_do_nothing(index_elements=self.primary_keys)

        with self.dbconn.con.begin() as con:
            con.execute(insert_sql)

    def reset_rows(self, idx: IndexDF) -> None:
        idx = cast(IndexDF, idx[self.primary_keys])
        if len(idx) == 0:
            return

        colname_bind_mapping = {
            col: col + "_val"
            for col in idx.columns
        }
        primary_key_conditions = [
            getattr(self.sql_table.c, col) == sa.bindparam(col_bind)
            for col, col_bind in colname_bind_mapping.items()
        ]

        update_sql = (
            sa.update(self.sql_table)
            .values({
                "process_ts": 0,
                "status": TransformStatus.PENDING.value,
                "error": None,
            })
            .where(sa.and_(*primary_key_conditions))
        )
        update_data = [
            {
                colname_bind_mapping[str(key)]: value
                for key, value in value_dict.items()
            }
            for value_dict in idx.to_dict(orient="records")
        ]

        with self.dbconn.con.begin() as con:
            con.execute(update_sql, update_data)

    def reset_rows_by_sql(self, sql) -> None:
        primary_cols = [getattr(self.sql_table.c, col) for col in self.primary_keys]
        update_sql = sa.update(self.sql_table).values({
            "process_ts": 0,
            "status": TransformStatus.PENDING.value,
            "error": None,
        }).where(
            sa.tuple_(*primary_cols).in_(sql)
        )

        with self.dbconn.con.begin() as con:
            con.execute(update_sql)

    def reset_all_rows(self) -> None:
        """
        Difference from mark_all_rows_unprocessed: mark_all_rows_unporocessed checks status
        Here what happens is all rows are reset to original state
        """
        update_sql = (
            sa.update(self.sql_table)
            .values({
                "process_ts": 0,
                "status": TransformStatus.PENDING.value,
                "error": None,
            })
        )
        with self.dbconn.con.begin() as con:
            con.execute(update_sql)

    def mark_rows_processed_success(
        self,
        idx: IndexDF,
        process_ts: float,
        run_config: Optional[RunConfig] = None,
    ) -> None:
        idx = cast(
            IndexDF, idx[self.primary_keys].drop_duplicates().dropna()
        )  # FIXME: сделать в основном запросе distinct
        if len(idx) == 0:
            return

        if idx.empty:
            # DataFrame считает, что он пустой, если в нем нет колонок
            # При этом мы хотим создать строки в БД

            # Мы можем обработать только случай с одной строкой
            assert len(idx) == 1

            with self.dbconn.con.begin() as con:
                insert_sql = self.dbconn.insert(self.sql_table).values(
                    [
                        {
                            "process_ts": process_ts,
                            "priority": 0,
                            "status": TransformStatus.COMPLETED.value,
                            "error": None,
                        }
                    ]
                )

                # удалить все из таблицы
                con.execute(self.sql_table.delete())
                con.execute(insert_sql)

        else:
            insert_sql = self.dbconn.insert(self.sql_table).values(
                [
                    {
                        "process_ts": process_ts,
                        "priority": 0,
                        "status": TransformStatus.COMPLETED.value,
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
                    "status": TransformStatus.COMPLETED.value,
                    "error": None,
                },
            )

            # execute
            with self.dbconn.con.begin() as con:
                con.execute(sql)

    def mark_rows_processed_error(
        self,
        idx: IndexDF,
        process_ts: float,
        error: str,
        run_config: Optional[RunConfig] = None,
    ) -> None:
        idx = cast(
            IndexDF, idx[self.primary_keys].drop_duplicates().dropna()
        )  # FIXME: сделать в основном запросе distinct
        if len(idx) == 0:
            return

        insert_sql = self.dbconn.insert(self.sql_table).values(
            [
                {
                    "process_ts": process_ts,
                    "priority": 0,
                    "status": TransformStatus.ERROR.value,
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
                "status": TransformStatus.ERROR.value,
                "error": error,
            },
        )

        # execute
        with self.dbconn.con.begin() as con:
            con.execute(sql)

    def get_metadata_size(self) -> int:
        """
        Получить количество строк метаданных трансформации.
        """

        sql = sa.select(sa.func.count()).select_from(self.sql_table)
        with self.dbconn.con.begin() as con:
            res = con.execute(sql).fetchone()

        assert res is not None and len(res) == 1
        return res[0]

    def mark_all_rows_unprocessed(
        self,
        run_config: Optional[RunConfig] = None,
    ) -> None:
        update_sql = (
            sa.update(self.sql_table)
            .values(
                {
                    "process_ts": 0,
                    "status": TransformStatus.PENDING.value,
                    "error": None,
                }
            )
            .where(self.sql_table.c.status == TransformStatus.COMPLETED.value)  # noqa: E712
        )

        sql = sql_apply_runconfig_filter(update_sql, self.sql_table, self.primary_keys, run_config)

        # execute
        with self.dbconn.con.begin() as con:
            con.execute(sql)


def sql_apply_filters_idx_to_subquery(
    sql: Any,
    keys: List[str],
    filters_idx: Optional[pd.DataFrame],
) -> Any:
    if filters_idx is None:
        return sql

    applicable_filter_keys = [i for i in filters_idx.columns if i in keys]
    if len(applicable_filter_keys) > 0:
        sql = sql.where(
            sa.tuple_(*[sa.column(i) for i in applicable_filter_keys]).in_(
                [sa.tuple_(*[r[k] for k in applicable_filter_keys]) for r in filters_idx.to_dict(orient="records")]
            )
        )

    return sql


@dataclass
class ComputeInputCTE:
    cte: Any
    keys: List[str]
    join_type: Literal["inner", "full"]


def _make_agg_of_agg(
    ds: "DataStore",
    transform_keys: List[str],
    agg_col: str,
    ctes: List[ComputeInputCTE],
) -> Any:
    assert len(ctes) > 0

    if len(ctes) == 1:
        return ctes[0].cte

    coalesce_keys = []

    for key in transform_keys:
        ctes_with_key = [cte.cte for cte in ctes if key in cte.keys]

        if len(ctes_with_key) == 0:
            raise ValueError(f"Key {key} not found in any of the input tables")

        if len(ctes_with_key) == 1:
            coalesce_keys.append(ctes_with_key[0].c[key])
        else:
            coalesce_keys.append(sa.func.coalesce(*[cte.c[key] for cte in ctes_with_key]).label(key))

    agg = sa.func.max(ds.meta_dbconn.func_greatest(*[cte.cte.c[agg_col] for cte in ctes])).label(agg_col)

    first_cte = ctes[0].cte

    sql = sa.select(*coalesce_keys + [agg]).select_from(first_cte)

    prev_ctes = [ctes[0]]

    for cte in ctes[1:]:
        onclause = []

        for prev_cte in prev_ctes:
            for key in cte.keys:
                if key in prev_cte.keys:
                    onclause.append(prev_cte.cte.c[key] == cte.cte.c[key])

        if len(onclause) > 0:
            sql = sql.outerjoin(
                cte.cte,
                onclause=sa.and_(*onclause),
                full=True,
            )
        else:
            sql = sql.outerjoin(
                cte.cte,
                onclause=sa.literal(True),
                full=True,
            )

        if cte.join_type == "inner":
            sql = sql.where(sa.and_(*[cte.cte.c[key].isnot(None) for key in cte.keys]))

        prev_ctes.append(cte)

    sql = sql.group_by(*coalesce_keys)

    return sql.cte(name=f"all__{agg_col}")


def _all_input_tables_from_same_sql_db(input_tables):
    table_stores = []
    is_all_table_store_db = True
    for input_table in input_tables:
        if isinstance(input_table.dt.table_store, TableStoreDB):
            table_stores.append(input_table.dt.table_store.dbconn.connstr)
        else:
            is_all_table_store_db = False
            table_stores.append(type(input_table.dt.table_store))

    # all tables are from the same database and it is sql database
    return len(set(table_stores)) == 1 and is_all_table_store_db


def _all_input_tables_have_meta_table(input_tables):
    pass


def _split_tables_into_connected_groups(
    input_dts: List["ComputeInput"]
) -> Dict[Tuple[str], List["ComputeInput"]]:
    """
    This function calculates groups of tables that are connected with primary keys
    """

    all_input_keyset = set([])
    all_input_keys = []

    # this is simple way of determining graph connectivity among tables and keys
    # tables are vertices, primary keys are edges
    adjacency_keys_dict: Dict[str, Set[str]] = {}
    for input_dt in input_dts:
        current_keys = input_dt.dt.primary_keys
        for key in current_keys:
            if adjacency_keys_dict.get(key) is None:
                adjacency_keys_dict[key] = set()

            for another_key in current_keys:
                adjacency_keys_dict[key].add(another_key)

        all_input_keyset.update(input_dt.dt.primary_keys)
        if input_dt.join_type != 'inner':
            all_input_keys.append(tuple(sorted(input_dt.dt.primary_keys)))

    max_sets: List[Set[str]] = []  # each set contains all connected keys
    # all connected keys can be inner joined, cross join for inter-group joins
    for key_set in adjacency_keys_dict.values():
        found_related_set = False
        for max_set in max_sets:
            for key in key_set:
                if key in max_set:
                    max_set.update(key_set)
                    found_related_set = True

        if not found_related_set:
            max_sets.append(key_set)

    table_groups = defaultdict(list)
    for key_clique in max_sets:
        for input_dt in input_dts:
            if input_dt.dt.primary_keys[0] in key_clique:
                key_clique_tuple = tuple(sorted(key_clique))
                if input_dt.join_type == 'inner':
                    # all inner joins are at the tail of the list
                    table_groups[key_clique_tuple].append(input_dt)
                else:
                    table_groups[key_clique_tuple].insert(0, input_dt)

    return cast(dict, table_groups)


def _join_delta(
    result_df: IndexDF, table_df: IndexDF, result_pkeys: Set[str], table_pkeys: Set[str],
    result_is_required_table: bool, table_is_required_table: bool
):
    """
    delta_df - this is df for changes (chunk that is stored currently) + pkeys
    table_df - this is data from existing table + pkeys
    result_df - this is aggregator of final result + pkeys
    delta_is_required_table - this is flag that says that delta change is applied to Required table
    table_is_required_table - this is flag that says that the table_df is Required table
    """
    if result_df.empty:
        return result_df, result_pkeys

    key_intersection = list(result_pkeys.intersection(set(table_pkeys)))
    if len(key_intersection) > 0:
        if result_is_required_table:
            # the difference is that delta data from required tables is not included
            # into the results, it is only used for filtering
            result = pd.merge(
                result_df, table_df,
                how="inner", on=key_intersection
            )
            result_pkeys_copy = result_pkeys.copy()
            # result_keys are not changed since the result doesn't extend
        elif table_is_required_table and table_pkeys.issubset(result_pkeys):
            result = pd.merge(
                result_df, table_df.drop_duplicates(),
                how="inner", on=key_intersection
            )
            result_pkeys_copy = result_pkeys.copy()
            # again result_pkeys don't change
        else:
            result = pd.merge(
                result_df, table_df, how='inner', on=key_intersection
            )
            result_pkeys_copy = result_pkeys.copy()
            result_pkeys_copy.update(set(table_pkeys))
    else:
        result = pd.merge(result_df, table_df, how='cross')
        result_pkeys_copy = result_pkeys.copy()
        result_pkeys_copy.update(set(table_pkeys))

    return result, result_pkeys_copy


def _join_input_tables_in_sql(all_input_tables, transform_keys):
    """
    This function calculates sql query that joins all input tables
    """
    select_columns_dict = {}
    for input_table in all_input_tables:
        for key in input_table.dt.primary_keys:
            if key in transform_keys:
                select_columns_dict[key] = input_table.dt.meta_table.sql_table.c[key]
    # sort keys because the order of selected cols here and in
    # TransformMetaTable.insert_rows_by_sql must be the same
    sorted_keys = sorted(select_columns_dict.keys())
    select_columns = [select_columns_dict[key].label(key) for key in sorted_keys]

    first_table = all_input_tables[0]
    sql = sa.select(*select_columns).select_from(first_table.dt.meta_table.sql_table)
    prev_tables = [first_table]
    for table in all_input_tables[1:]:
        onclause = []

        for prev_table in prev_tables:
            for key in table.dt.primary_keys:
                if key in prev_table.dt.primary_keys:
                    onclause.append(
                        prev_table.dt.meta_table.sql_table.c[key] == table.dt.meta_table.sql_table.c[key]
                    )

        if len(onclause) > 0:
            sql = sql.join(table.dt.meta_table.sql_table, onclause=sa.and_(*onclause))
        else:
            sql = sql.join(table.dt.meta_table.sql_table, onclause=sa.literal(True))

    return sql


def _calculate_changes_in_sql(
    transf, current_table, all_input_tables, new_df, changed_df
):
    sql = _join_input_tables_in_sql(all_input_tables, transf.transform_keys)

    primary_key_columns = [
        current_table.dt.meta_table.sql_table.c[pk]
        for pk in current_table.dt.primary_keys
    ]

    if not new_df.empty:
        new_df_sql = sql.where(
            sa.tuple_(*primary_key_columns).in_(
                new_df[current_table.dt.primary_keys].values.tolist()
            )
        )
        transf.meta_table.insert_rows_by_sql(new_df_sql)

    if not changed_df.empty:
        changed_df_sql = sql.where(
            sa.tuple_(*primary_key_columns).in_(
                changed_df[current_table.dt.primary_keys].values.tolist()
            )
        )
        transf.meta_table.reset_rows_by_sql(changed_df_sql)


def _calculate_changes_in_pandas(
    transf, current_table_name, all_input_tables, primary_keys,
    new_df, changed_df
):
    """
    See note for extract_transformation_meta function
    """
    table_groups = _split_tables_into_connected_groups(all_input_tables)
    filtered_groups = {
        key_tuple: table_group
        for key_tuple, table_group in table_groups.items()
        if any(transf_key in set(key_tuple) for transf_key in transf.transform_keys)
    }

    current_table_is_required = any([
        dt.dt for dt in all_input_tables
        if dt.dt.name == current_table_name
            and dt.join_type == 'inner'
    ])
    remaining_tables = [
        dt.dt for dt in itertools.chain(*filtered_groups.values())
        if dt.dt.name != current_table_name
    ]
    required_tables = set([
        dt.dt.name for dt in itertools.chain(*filtered_groups.values())
        if dt.dt.name != current_table_name
            and dt.join_type == 'inner'
    ])

    new_result, changed_result = new_df[primary_keys], changed_df[primary_keys]
    result_keys = set(primary_keys)

    for remaining_table in remaining_tables:
        remaining_table_df = remaining_table.get_index_data()
        result_keys_cache = result_keys.copy()
        new_result, result_keys = _join_delta(
            new_result, remaining_table_df[remaining_table.primary_keys],
            result_keys_cache, set(remaining_table.primary_keys),
            current_table_is_required, remaining_table.name in required_tables
        )
        changed_result, _ = _join_delta(
            changed_result, remaining_table_df[remaining_table.primary_keys],
            result_keys_cache, set(remaining_table.primary_keys),
            current_table_is_required, remaining_table.name in required_tables
        )

    if not new_result.empty:
        transf.meta_table.insert_rows(new_result)
    if not changed_result.empty:
        transf.meta_table.reset_rows(changed_result)


def _initial_transformation_meta_extract_in_pandas(
    transf: "BaseBatchTransformStep", input_dts: List["ComputeInput"], transform_keys: List[str]
) -> None:
    """
    This function takes all the input tables to the transformation
    and creates index of rows to be created in transformation meta table
    to initialize it if transformation was added after the input tables were already filled in

    Note: (very important) This method does the join between multiple tables in some cases
    - if tables are in the same database, then method uses in-database join
    - if tables reside in different stores, then data is fetched to python code and joined manually
    this can cause out-of memory errors if data is too large
    """
    table_groups = _split_tables_into_connected_groups(input_dts)

    # within each group join the tables how=inner by intersecting keys
    within_group_results = {}
    for group_key, table_group in table_groups.items():
        first_table = table_group[0].dt
        first_table_index = first_table.get_index_data()
        first_table_keys = set(first_table.primary_keys)
        for table in table_group[1:]:
            key_intersection = list(first_table_keys.intersection(set(table.dt.primary_keys)))
            current_table_index = table.dt.get_index_data()
            first_table_index = pd.merge(
                first_table_index, current_table_index, how='inner', on=key_intersection
            )
            first_table_keys = set(list(first_table_keys) + list(table.dt.primary_keys))
        within_group_results[group_key] = first_table_index

    # cross join as final step
    within_group_indexes = list(within_group_results.values())
    final_result = within_group_indexes[0]
    for table_data in within_group_indexes[1:]:
        final_result = pd.merge(final_result, table_data, how="cross")

    idx_to_write = cast(IndexDF, final_result[transform_keys].drop_duplicates())
    transf.meta_table.insert_rows(idx_to_write)


def _initial_transformation_meta_extract_in_sql(
    transf: "BaseBatchTransformStep", all_input_tables, transform_keys
) -> None:
    sql = _join_input_tables_in_sql(all_input_tables, transform_keys)
    sql = sql.where(sa.text('TRUE'))
    transf.meta_table.insert_rows_by_sql(sql)


def create_transformation_meta_for_changes(
    transformations: List["BaseBatchTransformStep"], current_table_name: str, primary_keys,
    new_df: pd.DataFrame, changed_df: pd.DataFrame
) -> None:
    for transf in transformations:
        output_tables = set([dt.name for dt in transf.output_dts])
        all_input_tables = [dt for dt in transf.input_dts if dt.dt.name not in output_tables]
        current_table = [dt for dt in transf.input_dts if dt.dt.name == current_table_name][0]

        if not set(primary_keys).issubset(set(transf.transform_keys)):
            # what happens here:
            # update to a table that participates in a join and later in aggregation
            # since values from current table participate in all aggregations, aggs are no longer correct
            # so I reset all the rows - this causes the aggregation to be recomputed
            # this is the same for both required and normal tables
            transf.meta_table.reset_all_rows()
            changed_df = pd.DataFrame(columns=changed_df.columns)  # don't process it

        _calculate_changes_in_sql(transf, current_table, all_input_tables, new_df, changed_df)


def init_transformation_meta(transf: "BaseBatchTransformStep") -> None:
    output_tables = set([dt.name for dt in transf.output_dts])
    all_input_tables = [dt for dt in transf.input_dts if dt.dt.name not in output_tables]
    all_input_tables_from_sql_db = _all_input_tables_from_same_sql_db(all_input_tables)

    if all_input_tables_from_sql_db:
        _initial_transformation_meta_extract_in_sql(transf, all_input_tables, transf.transform_keys)
    else:
        # this case is required for test_chunked_processing_pipeline.py::test_table_store_json_line_reading
        _initial_transformation_meta_extract_in_pandas(transf, all_input_tables, transf.transform_keys)


def build_changed_idx_sql(
    ds: "DataStore",
    meta_table: "TransformMetaTable",
    input_dts: List["ComputeInput"],
    transform_keys: List[str],
    filters_idx: Optional[IndexDF] = None,
    order_by: Optional[List[str]] = None,
    order: Literal["asc", "desc"] = "asc",
    run_config: Optional[RunConfig] = None,  # TODO remove
):
    sql = (
        sa.select(
            # Нам нужно выбирать хотя бы что-то, чтобы не было ошибки при
            # пустом transform_keys
            sa.literal(1).label("_datapipe_dummy"),
            *[meta_table.sql_table.c[key] for key in transform_keys],
        )
        .select_from(meta_table.sql_table)
        .where(sa.or_(
            meta_table.sql_table.c.status == TransformStatus.PENDING.value,
            meta_table.sql_table.c.status == TransformStatus.ERROR.value
        ))
    )
    sql = sql_apply_runconfig_filter(sql, meta_table.sql_table, transform_keys, run_config)

    if order_by is None:
        sql = sql.order_by(
            meta_table.sql_table.c.priority.desc().nullslast(),
            *[sa.column(k) for k in transform_keys],
        )
    else:
        if order == "desc":
            sql = sql.order_by(
                *[sa.desc(sa.column(k)) for k in order_by],
                meta_table.sql_table.c.priority.desc().nullslast(),
            )
        elif order == "asc":
            sql = sql.order_by(
                *[sa.asc(sa.column(k)) for k in order_by],
                meta_table.sql_table.c.priority.desc().nullslast(),
            )
    return transform_keys, sql


def build_changed_idx_sql_deprecated(
    ds: "DataStore",
    meta_table: "TransformMetaTable",
    input_dts: List["ComputeInput"],
    transform_keys: List[str],
    filters_idx: Optional[IndexDF] = None,
    order_by: Optional[List[str]] = None,
    order: Literal["asc", "desc"] = "asc",
    run_config: Optional[RunConfig] = None,  # TODO remove
) -> Tuple[Iterable[str], Any]:
    """
    Function is not working, is_success field is removed
    """
    all_input_keys_counts: Dict[str, int] = {}
    for col in itertools.chain(*[inp.dt.primary_schema for inp in input_dts]):
        all_input_keys_counts[col.name] = all_input_keys_counts.get(col.name, 0) + 1

    inp_ctes = []
    for inp in input_dts:
        keys, cte = inp.dt.meta_table.get_agg_cte(
            transform_keys=transform_keys,
            filters_idx=filters_idx,
            run_config=run_config,
        )
        inp_ctes.append(ComputeInputCTE(cte=cte, keys=keys, join_type=inp.join_type))

    agg_of_aggs = _make_agg_of_agg(
        ds=ds,
        transform_keys=transform_keys,
        ctes=inp_ctes,
        agg_col="update_ts",
    )

    tr_tbl = meta_table.sql_table
    out: Any = (
        sa.select(
            *[sa.column(k) for k in transform_keys] + [tr_tbl.c.process_ts, tr_tbl.c.priority, tr_tbl.c.is_success]
        )
        .select_from(tr_tbl)
        .group_by(*[sa.column(k) for k in transform_keys])
    )

    out = sql_apply_filters_idx_to_subquery(out, transform_keys, filters_idx)

    out = out.cte(name="transform")

    if len(transform_keys) == 0:
        join_onclause_sql: Any = sa.literal(True)
    elif len(transform_keys) == 1:
        join_onclause_sql = agg_of_aggs.c[transform_keys[0]] == out.c[transform_keys[0]]
    else:  # len(transform_keys) > 1:
        join_onclause_sql = sa.and_(*[agg_of_aggs.c[key] == out.c[key] for key in transform_keys])

    sql = (
        sa.select(
            # Нам нужно выбирать хотя бы что-то, чтобы не было ошибки при
            # пустом transform_keys
            sa.literal(1).label("_datapipe_dummy"),
            *[sa.func.coalesce(agg_of_aggs.c[key], out.c[key]).label(key) for key in transform_keys],
        )
        .select_from(agg_of_aggs)
        .outerjoin(
            out,
            onclause=join_onclause_sql,
            full=True,
        )
        .where(
            sa.or_(
                sa.and_(
                    out.c.is_success == True,  # noqa
                    agg_of_aggs.c.update_ts > out.c.process_ts,
                ),
                out.c.is_success != True,  # noqa
                out.c.process_ts == None,  # noqa
            )
        )
    )
    if order_by is None:
        sql = sql.order_by(
            out.c.priority.desc().nullslast(),
            *[sa.column(k) for k in transform_keys],
        )
    else:
        if order == "desc":
            sql = sql.order_by(
                *[sa.desc(sa.column(k)) for k in order_by],
                out.c.priority.desc().nullslast(),
            )
        elif order == "asc":
            sql = sql.order_by(
                *[sa.asc(sa.column(k)) for k in order_by],
                out.c.priority.desc().nullslast(),
            )
    return (transform_keys, sql)

import itertools
import logging
import math
import time
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Literal,
    Optional,
    Tuple,
    cast,
)

import pandas as pd
import sqlalchemy as sa

from datapipe.run_config import RunConfig
from datapipe.sql_util import sql_apply_idx_filter_to_table, sql_apply_runconfig_filter
from datapipe.store.database import DBConn, MetaKey
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
    from datapipe.datatable import DataStore

logger = logging.getLogger("datapipe.meta.sql_meta")

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

        current_time = time.time()
        if now is None:
            now = current_time
        elif now < current_time - 1.0:  # Порог 1 секунда - игнорируем микросекундные различия
            # Предупреждение: использование timestamp из прошлого может привести к потере данных
            # при использовании offset optimization (Hypothesis 4: delayed records)
            logger.warning(
                f"store_chunk called with now={now:.2f} which is {current_time - now:.2f}s in the past. "
                f"This may cause data loss with offset optimization if offset > now."
            )

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
        Create a CTE that aggregates the table by transform keys and returns the
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
    sa.Column("is_success", sa.Boolean),  # Успешно ли обработана строка
    sa.Column("priority", sa.Integer),  # Приоритет обработки (чем больше, тем выше)
    sa.Column("error", sa.String),  # Текст ошибки
]


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
                    "is_success": False,
                    "priority": 0,
                    "error": None,
                    **idx_dict,  # type: ignore
                }
                for idx_dict in idx.to_dict(orient="records")
            ]
        )

        sql = insert_sql.on_conflict_do_nothing(index_elements=self.primary_keys)

        with self.dbconn.con.begin() as con:
            con.execute(sql)

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
                            "is_success": True,
                            "priority": 0,
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
                        "is_success": True,
                        "priority": 0,
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
                    "is_success": False,
                    "priority": 0,
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
                    "is_success": False,
                    "error": None,
                }
            )
            .where(self.sql_table.c.is_success == True)  # noqa: E712
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


def build_changed_idx_sql_v1(
    ds: "DataStore",
    meta_table: "TransformMetaTable",
    input_dts: List["ComputeInput"],
    transform_keys: List[str],
    filters_idx: Optional[IndexDF] = None,
    order_by: Optional[List[str]] = None,
    order: Literal["asc", "desc"] = "asc",
    run_config: Optional[RunConfig] = None,  # TODO remove
    additional_columns: Optional[List[str]] = None,
) -> Tuple[Iterable[str], Any]:
    """
    Args:
        additional_columns: Дополнительные колонки для включения в результат (для filtered join)
    """
    if additional_columns is None:
        additional_columns = []

    # Полный список колонок для SELECT (transform_keys + additional_columns)
    all_select_keys = list(transform_keys) + additional_columns

    all_input_keys_counts: Dict[str, int] = {}
    for col in itertools.chain(*[inp.dt.primary_schema for inp in input_dts]):
        all_input_keys_counts[col.name] = all_input_keys_counts.get(col.name, 0) + 1

    inp_ctes = []
    for inp in input_dts:
        # Используем all_select_keys для включения дополнительных колонок
        keys, cte = inp.dt.meta_table.get_agg_cte(
            transform_keys=all_select_keys,
            filters_idx=filters_idx,
            run_config=run_config,
        )
        inp_ctes.append(ComputeInputCTE(cte=cte, keys=keys, join_type=inp.join_type))

    agg_of_aggs = _make_agg_of_agg(
        ds=ds,
        transform_keys=all_select_keys,
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

    # Важно: Включаем все колонки (transform_keys + additional_columns)
    sql = (
        sa.select(
            # Нам нужно выбирать хотя бы что-то, чтобы не было ошибки при
            # пустом transform_keys
            sa.literal(1).label("_datapipe_dummy"),
            *[sa.func.coalesce(agg_of_aggs.c[key], out.c[key]).label(key) if key in transform_keys
              else agg_of_aggs.c[key].label(key) for key in all_select_keys if key in agg_of_aggs.c],
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
    return (all_select_keys, sql)


# Обратная совместимость: алиас для старой версии
def build_changed_idx_sql(
    ds: "DataStore",
    meta_table: "TransformMetaTable",
    input_dts: List["ComputeInput"],
    transform_keys: List[str],
    filters_idx: Optional[IndexDF] = None,
    order_by: Optional[List[str]] = None,
    order: Literal["asc", "desc"] = "asc",
    run_config: Optional[RunConfig] = None,
) -> Tuple[Iterable[str], Any]:
    """
    Обёртка для обратной совместимости. По умолчанию использует v1 (старую версию).
    """
    return build_changed_idx_sql_v1(
        ds=ds,
        meta_table=meta_table,
        input_dts=input_dts,
        transform_keys=transform_keys,
        filters_idx=filters_idx,
        order_by=order_by,
        order=order,
        run_config=run_config,
    )


def build_changed_idx_sql_v2(
    ds: "DataStore",
    meta_table: "TransformMetaTable",
    input_dts: List["ComputeInput"],
    transform_keys: List[str],
    offset_table: "TransformInputOffsetTable",
    transformation_id: str,
    filters_idx: Optional[IndexDF] = None,
    order_by: Optional[List[str]] = None,
    order: Literal["asc", "desc"] = "asc",
    run_config: Optional[RunConfig] = None,
    additional_columns: Optional[List[str]] = None,
) -> Tuple[Iterable[str], Any]:
    """
    Новая версия build_changed_idx_sql, использующая offset'ы для оптимизации.

    Вместо FULL OUTER JOIN всех входных таблиц, выбираем только записи с
    update_ts > offset для каждой входной таблицы, затем объединяем через UNION.

    Args:
        additional_columns: Дополнительные колонки для включения в результат (для filtered join)
    """
    if additional_columns is None:
        additional_columns = []

    # Полный список колонок для SELECT (transform_keys + additional_columns)
    all_select_keys = list(transform_keys) + additional_columns

    # Добавляем update_ts для ORDER BY если его еще нет
    # (нужно для правильной сортировки батчей по времени обновления)
    if 'update_ts' not in all_select_keys:
        all_select_keys.append('update_ts')

    # 1. Получить все offset'ы одним запросом для избежания N+1
    offsets = offset_table.get_offsets_for_transformation(transformation_id)
    # Для таблиц без offset используем 0.0 (обрабатываем все данные)
    for inp in input_dts:
        if inp.dt.name not in offsets:
            offsets[inp.dt.name] = 0.0

    # 2. Построить CTE для каждой входной таблицы с фильтром по offset
    # Для таблиц с join_keys нужен обратный JOIN к основной таблице
    changed_ctes = []

    # Сначала находим "основную" таблицу - первую без join_keys
    primary_inp = None
    for inp in input_dts:
        if not inp.join_keys:
            primary_inp = inp
            break

    for inp in input_dts:
        tbl = inp.dt.meta_table.sql_table

        # Разделяем ключи на те, что есть в meta table, и те, что нужны из data table
        meta_cols = [c.name for c in tbl.columns]
        keys_in_meta = [k for k in all_select_keys if k in meta_cols]
        keys_in_data_only = [k for k in all_select_keys if k not in meta_cols]

        if len(keys_in_meta) == 0:
            continue

        offset = offsets[inp.dt.name]

        # ОБРАТНЫЙ JOIN для справочных таблиц с join_keys
        # Когда изменяется справочная таблица, нужно найти все записи основной таблицы,
        # которые на нее ссылаются
        if inp.join_keys and primary_inp and hasattr(primary_inp.dt.table_store, 'data_table'):
            # Справочная таблица изменилась - нужен обратный JOIN к основной
            primary_data_tbl = primary_inp.dt.table_store.data_table

            # Строим SELECT для всех колонок из all_select_keys основной таблицы
            primary_data_cols = [c.name for c in primary_data_tbl.columns]
            select_cols = []
            group_by_cols = []
            for k in all_select_keys:
                if k in primary_data_cols:
                    select_cols.append(primary_data_tbl.c[k])
                    group_by_cols.append(primary_data_tbl.c[k])
                elif k == 'update_ts':
                    # КРИТИЧНО: Берем update_ts из мета-таблицы справочника (tbl.c.update_ts),
                    # а НЕ из primary_data_tbl. Это необходимо для корректной работы
                    # offset-оптимизации при reverse join (join_keys).
                    # Если использовать NULL, записи будут помечаться как error_records
                    # и переобрабатываться на каждом запуске.
                    select_cols.append(tbl.c.update_ts)
                    group_by_cols.append(tbl.c.update_ts)  # Добавляем в GROUP BY
                else:
                    select_cols.append(sa.literal(None).label(k))

            # Обратный JOIN: primary_table.join_key = reference_table.id
            # Например: posts.user_id = profiles.id
            # inp.join_keys = {'user_id': 'id'} означает:
            #   'user_id' - колонка в основной таблице (posts)
            #   'id' - колонка в справочной таблице (profiles)
            join_conditions = []
            for primary_col, ref_col in inp.join_keys.items():
                if primary_col in primary_data_cols and ref_col in meta_cols:
                    join_conditions.append(primary_data_tbl.c[primary_col] == tbl.c[ref_col])

            if len(join_conditions) == 0:
                # Не можем построить JOIN - пропускаем эту таблицу
                continue

            join_condition = sa.and_(*join_conditions) if len(join_conditions) > 1 else join_conditions[0]

            # SELECT primary_cols FROM reference_meta
            # JOIN primary_data ON primary.join_key = reference.id
            # WHERE reference.update_ts >= offset (используем >= вместо >)
            changed_sql = sa.select(*select_cols).select_from(
                tbl.join(primary_data_tbl, join_condition)
            ).where(
                sa.or_(
                    tbl.c.update_ts >= offset,
                    sa.and_(
                        tbl.c.delete_ts.isnot(None),
                        tbl.c.delete_ts >= offset
                    )
                )
            )

            # Применить filters и group by
            changed_sql = sql_apply_filters_idx_to_subquery(changed_sql, all_select_keys, filters_idx)
            # run_config фильтры применяются к справочной таблице
            changed_sql = sql_apply_runconfig_filter(changed_sql, tbl, inp.dt.primary_keys, run_config)

            if len(group_by_cols) > 0:
                changed_sql = changed_sql.group_by(*group_by_cols)

            changed_ctes.append(changed_sql.cte(name=f"{inp.dt.name}_changes"))
            continue

        # Если все ключи есть в meta table - используем простой запрос
        if len(keys_in_data_only) == 0:
            select_cols = [sa.column(k) for k in keys_in_meta]

            # SELECT keys FROM input_meta WHERE update_ts >= offset OR delete_ts >= offset
            changed_sql = sa.select(*select_cols).select_from(tbl).where(
                sa.or_(
                    tbl.c.update_ts >= offset,
                    sa.and_(
                        tbl.c.delete_ts.isnot(None),
                        tbl.c.delete_ts >= offset
                    )
                )
            )

            # Применить filters_idx и run_config
            changed_sql = sql_apply_filters_idx_to_subquery(changed_sql, keys_in_meta, filters_idx)
            changed_sql = sql_apply_runconfig_filter(changed_sql, tbl, inp.dt.primary_keys, run_config)

            if len(select_cols) > 0:
                changed_sql = changed_sql.group_by(*select_cols)
        else:
            # Есть колонки только в data table - нужен JOIN с data table
            # Проверяем что у table_store есть data_table (для TableStoreDB)
            if not hasattr(inp.dt.table_store, 'data_table'):
                # Fallback: если нет data_table, используем только meta keys
                select_cols = [sa.column(k) for k in keys_in_meta]
                changed_sql = sa.select(*select_cols).select_from(tbl).where(
                    sa.or_(
                        tbl.c.update_ts >= offset,
                        sa.and_(
                            tbl.c.delete_ts.isnot(None),
                            tbl.c.delete_ts >= offset
                        )
                    )
                )
                changed_sql = sql_apply_filters_idx_to_subquery(changed_sql, keys_in_meta, filters_idx)
                changed_sql = sql_apply_runconfig_filter(changed_sql, tbl, inp.dt.primary_keys, run_config)
                if len(select_cols) > 0:
                    changed_sql = changed_sql.group_by(*select_cols)
            else:
                # JOIN meta table с data table для получения дополнительных колонок
                data_tbl = inp.dt.table_store.data_table

                # Проверяем какие дополнительные колонки действительно есть в data table
                data_cols_available = [c.name for c in data_tbl.columns]
                keys_in_data_available = [k for k in keys_in_data_only if k in data_cols_available]

                if len(keys_in_data_available) == 0:
                    # Fallback: если нужных колонок нет в data table, используем только meta keys
                    select_cols = [sa.column(k) for k in keys_in_meta]
                    changed_sql = sa.select(*select_cols).select_from(tbl).where(
                        sa.or_(
                            tbl.c.update_ts >= offset,
                            sa.and_(
                                tbl.c.delete_ts.isnot(None),
                                tbl.c.delete_ts >= offset
                            )
                        )
                    )
                    changed_sql = sql_apply_filters_idx_to_subquery(changed_sql, keys_in_meta, filters_idx)
                    changed_sql = sql_apply_runconfig_filter(changed_sql, tbl, inp.dt.primary_keys, run_config)
                    if len(select_cols) > 0:
                        changed_sql = changed_sql.group_by(*select_cols)

                    changed_ctes.append(changed_sql.cte(name=f"{inp.dt.name}_changes"))
                    continue

                # SELECT meta_keys, data_keys FROM meta JOIN data ON primary_keys
                # WHERE update_ts > offset OR delete_ts > offset
                select_cols = [tbl.c[k] for k in keys_in_meta] + [data_tbl.c[k] for k in keys_in_data_available]

                # Строим JOIN condition по primary keys
                if len(inp.dt.primary_keys) == 1:
                    join_condition = tbl.c[inp.dt.primary_keys[0]] == data_tbl.c[inp.dt.primary_keys[0]]
                else:
                    join_condition = sa.and_(*[
                        tbl.c[pk] == data_tbl.c[pk] for pk in inp.dt.primary_keys
                    ])

                changed_sql = sa.select(*select_cols).select_from(
                    tbl.join(data_tbl, join_condition)
                ).where(
                    sa.or_(
                        tbl.c.update_ts >= offset,
                        sa.and_(
                            tbl.c.delete_ts.isnot(None),
                            tbl.c.delete_ts >= offset
                        )
                    )
                )

                # Применить filters_idx и run_config
                all_keys = keys_in_meta + keys_in_data_available
                changed_sql = sql_apply_filters_idx_to_subquery(changed_sql, all_keys, filters_idx)
                changed_sql = sql_apply_runconfig_filter(changed_sql, tbl, inp.dt.primary_keys, run_config)

                if len(select_cols) > 0:
                    changed_sql = changed_sql.group_by(*select_cols)

        changed_ctes.append(changed_sql.cte(name=f"{inp.dt.name}_changes"))

    # 3. Получить записи с ошибками из TransformMetaTable
    # Важно: error_records должен иметь все колонки из all_select_keys для UNION
    # Для additional_columns используем NULL, так как их нет в transform meta table
    tr_tbl = meta_table.sql_table
    # Для error_records нужно создать колонки из all_select_keys
    # Колонки из transform_keys берем из tr_tbl, остальные - NULL
    error_select_cols: List[Any] = []
    for k in all_select_keys:
        if k in transform_keys:
            error_select_cols.append(sa.column(k))
        else:
            # Для дополнительных колонок (включая update_ts) используем NULL с правильным типом
            # update_ts это Float, остальные - String
            if k == 'update_ts':
                error_select_cols.append(sa.cast(sa.literal(None), sa.Float).label(k))
            else:
                error_select_cols.append(sa.literal(None).label(k))

    error_records_sql: Any = sa.select(*error_select_cols).select_from(tr_tbl).where(
        sa.or_(
            tr_tbl.c.is_success != True,  # noqa
            tr_tbl.c.process_ts.is_(None)
        )
    )

    error_records_sql = sql_apply_filters_idx_to_subquery(
        error_records_sql, transform_keys, filters_idx
    )

    if len(transform_keys) > 0:
        error_records_sql = error_records_sql.group_by(*[sa.column(k) for k in transform_keys])

    error_records_cte = error_records_sql.cte(name="error_records")

    # 4. Объединить все изменения и ошибки через UNION
    if len(changed_ctes) == 0:
        # Если нет входных таблиц с изменениями, используем только ошибки
        union_sql: Any = sa.select(
            *[error_records_cte.c[k] for k in all_select_keys]
        ).select_from(error_records_cte)
    else:
        # UNION всех изменений и ошибок
        # Для отсутствующих колонок используем NULL
        union_parts = []
        for cte in changed_ctes:
            # Для каждой колонки из all_select_keys: берем из CTE если есть, иначе NULL
            select_cols = [
                cte.c[k] if k in cte.c else sa.literal(None).label(k)
                for k in all_select_keys
            ]
            union_parts.append(sa.select(*select_cols).select_from(cte))

        union_parts.append(
            sa.select(
                *[error_records_cte.c[k] for k in all_select_keys]
            ).select_from(error_records_cte)
        )

        union_sql = sa.union(*union_parts)

    # 5. Применить сортировку
    # Нам нужно join с transform meta для получения priority
    union_cte = union_sql.cte(name="changed_union")

    if len(transform_keys) == 0:
        join_onclause_sql: Any = sa.literal(True)
    elif len(transform_keys) == 1:
        join_onclause_sql = union_cte.c[transform_keys[0]] == tr_tbl.c[transform_keys[0]]
    else:
        join_onclause_sql = sa.and_(*[union_cte.c[key] == tr_tbl.c[key] for key in transform_keys])

    # Используем `out` для консистентности с v1
    # Важно: Включаем все колонки (transform_keys + additional_columns)

    # Error records имеют update_ts = NULL, используем это для их идентификации
    is_error_record = union_cte.c.update_ts.is_(None)

    out = (
        sa.select(
            sa.literal(1).label("_datapipe_dummy"),
            *[union_cte.c[k] for k in all_select_keys if k in union_cte.c],
        )
        .select_from(union_cte)
        .outerjoin(tr_tbl, onclause=join_onclause_sql)
        .where(
            # Фильтрация для предотвращения зацикливания при >= offset
            # Логика аналогична v1, но с учетом error_records
            sa.or_(
                # Error records (update_ts IS NULL) - всегда обрабатываем
                is_error_record,
                # Не обработано (первый раз)
                tr_tbl.c.process_ts.is_(None),
                # Успешно обработано, но данные обновились после обработки
                sa.and_(
                    tr_tbl.c.is_success == True,  # noqa
                    union_cte.c.update_ts > tr_tbl.c.process_ts
                )
                # Примечание: is_success != True НЕ проверяем, так как
                # ошибочные записи уже включены в error_records CTE
            )
        )
    )

    if order_by is None:
        # Сортировка: сначала по update_ts (для консистентности с offset),
        # затем по transform_keys (для детерминизма)
        # NULLS LAST - error_records (с update_ts = NULL) обрабатываются последними
        out = out.order_by(
            tr_tbl.c.priority.desc().nullslast(),
            union_cte.c.update_ts.asc().nullslast(),  # Сортировка по времени обновления, NULL в конце
            *[union_cte.c[k] for k in transform_keys],  # Детерминизм при одинаковых update_ts
        )
    else:
        # КРИТИЧНО: При кастомном order_by всё равно нужно сортировать по update_ts ПЕРВЫМ
        # для консистентности с offset (иначе данные могут быть пропущены)
        if order == "desc":
            out = out.order_by(
                tr_tbl.c.priority.desc().nullslast(),
                union_cte.c.update_ts.asc().nullslast(),  # update_ts ВСЕГДА первым
                *[sa.desc(union_cte.c[k]) for k in order_by],
            )
        elif order == "asc":
            out = out.order_by(
                tr_tbl.c.priority.desc().nullslast(),
                union_cte.c.update_ts.asc().nullslast(),  # update_ts ВСЕГДА первым
                *[sa.asc(union_cte.c[k]) for k in order_by],
            )

    return (all_select_keys, out)


TRANSFORM_INPUT_OFFSET_SCHEMA: DataSchema = [
    sa.Column("transformation_id", sa.String, primary_key=True),
    sa.Column("input_table_name", sa.String, primary_key=True),
    sa.Column("update_ts_offset", sa.Float),
]


class TransformInputOffsetTable:
    """
    Таблица для хранения offset'ов (последних обработанных update_ts) для каждой
    входной таблицы каждой трансформации.

    Используется для оптимизации поиска измененных данных: вместо FULL OUTER JOIN
    всех входных таблиц, выбираем только записи с update_ts > offset.
    """

    def __init__(self, dbconn: DBConn, create_table: bool = False):
        self.dbconn = dbconn
        self.sql_table = sa.Table(
            "transform_input_offsets",
            dbconn.sqla_metadata,
            *[col._copy() for col in TRANSFORM_INPUT_OFFSET_SCHEMA],
        )

        if create_table:
            # Создать таблицу если её нет (аналогично MetaTable)
            self.sql_table.create(dbconn.con, checkfirst=True)

            # Создать индекс на transformation_id для быстрого поиска
            idx = sa.Index(
                "ix_transform_input_offsets_transformation_id",
                self.sql_table.c.transformation_id,
            )
            idx.create(dbconn.con, checkfirst=True)

    def get_offset(self, transformation_id: str, input_table_name: str) -> Optional[float]:
        """Получить последний offset для трансформации и источника"""
        sql = sa.select(self.sql_table.c.update_ts_offset).where(
            sa.and_(
                self.sql_table.c.transformation_id == transformation_id,
                self.sql_table.c.input_table_name == input_table_name,
            )
        )
        with self.dbconn.con.begin() as con:
            result = con.execute(sql).scalar()
        return result

    def get_offsets_for_transformation(self, transformation_id: str) -> Dict[str, float]:
        """
        Получить все offset'ы для трансформации одним запросом.

        Returns: {input_table_name: update_ts_offset}
        """
        try:
            sql = sa.select(
                self.sql_table.c.input_table_name,
                self.sql_table.c.update_ts_offset,
            ).where(self.sql_table.c.transformation_id == transformation_id)

            with self.dbconn.con.begin() as con:
                results = con.execute(sql).fetchall()

            return {row[0]: row[1] for row in results}
        except Exception:
            # Таблица может не существовать если create_table=False
            # Возвращаем пустой словарь - все offset'ы будут 0.0 (обработаем все данные)
            return {}

    def _build_max_offset_expression(self, insert_sql: Any) -> Any:
        """
        Создать выражение для атомарного выбора максимального offset.

        Использует CASE WHEN для гарантии что offset только растет,
        работает и в SQLite и в PostgreSQL.
        """
        return sa.case(
            (self.sql_table.c.update_ts_offset > insert_sql.excluded.update_ts_offset,
             self.sql_table.c.update_ts_offset),
            else_=insert_sql.excluded.update_ts_offset
        )

    def update_offset(
        self, transformation_id: str, input_table_name: str, update_ts_offset: float
    ) -> None:
        """Обновить offset после успешной обработки"""
        insert_sql = self.dbconn.insert(self.sql_table).values(
            transformation_id=transformation_id,
            input_table_name=input_table_name,
            update_ts_offset=update_ts_offset,
        )

        max_offset = self._build_max_offset_expression(insert_sql)

        sql = insert_sql.on_conflict_do_update(
            index_elements=["transformation_id", "input_table_name"],
            set_={"update_ts_offset": max_offset},
        )
        with self.dbconn.con.begin() as con:
            con.execute(sql)

    def update_offsets_bulk(self, offsets: Dict[Tuple[str, str], float]) -> None:
        """
        Обновить несколько offset'ов за одну транзакцию
        offsets: {(transformation_id, input_table_name): update_ts_offset}
        """
        if not offsets:
            return

        values = [
            {
                "transformation_id": trans_id,
                "input_table_name": table_name,
                "update_ts_offset": offset,
            }
            for (trans_id, table_name), offset in offsets.items()
        ]

        insert_sql = self.dbconn.insert(self.sql_table).values(values)

        max_offset = self._build_max_offset_expression(insert_sql)

        sql = insert_sql.on_conflict_do_update(
            index_elements=["transformation_id", "input_table_name"],
            set_={"update_ts_offset": max_offset},
        )
        with self.dbconn.con.begin() as con:
            con.execute(sql)

    def reset_offset(self, transformation_id: str, input_table_name: Optional[str] = None) -> None:
        """Удалить offset (для полной переобработки)"""
        sql = self.sql_table.delete().where(self.sql_table.c.transformation_id == transformation_id)
        if input_table_name is not None:
            sql = sql.where(self.sql_table.c.input_table_name == input_table_name)

        with self.dbconn.con.begin() as con:
            con.execute(sql)

    def get_statistics(self, transformation_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Получить статистику по offset'ам для мониторинга

        Returns: [{
            'transformation_id': str,
            'input_table_name': str,
            'update_ts_offset': float,
            'offset_age_seconds': float  # time.time() - update_ts_offset
        }]
        """
        sql = sa.select(
            self.sql_table.c.transformation_id,
            self.sql_table.c.input_table_name,
            self.sql_table.c.update_ts_offset,
        )

        if transformation_id is not None:
            sql = sql.where(self.sql_table.c.transformation_id == transformation_id)

        with self.dbconn.con.begin() as con:
            results = con.execute(sql).fetchall()

        now = time.time()
        return [
            {
                "transformation_id": row[0],
                "input_table_name": row[1],
                "update_ts_offset": row[2],
                "offset_age_seconds": now - row[2] if row[2] else None,
            }
            for row in results
        ]

    def get_offset_count(self) -> int:
        """Получить общее количество offset'ов в таблице"""
        sql = sa.select(sa.func.count()).select_from(self.sql_table)

        with self.dbconn.con.begin() as con:
            result = con.execute(sql).scalar()
            return result if result is not None else 0


def initialize_offsets_from_transform_meta(
    ds: "DataStore",  # type: ignore  # noqa: F821
    transform_step: "BaseBatchTransformStep",  # type: ignore  # noqa: F821
) -> Dict[str, float]:
    """
    Инициализировать offset'ы для существующей трансформации на основе TransformMetaTable.

    Логика:
    1. Находим MIN(process_ts) из успешно обработанных записей в TransformMetaTable
    2. Для каждой входной таблицы находим MAX(update_ts) где update_ts <= min_process_ts
    3. Устанавливаем эти значения как начальные offset'ы
    4. Это гарантирует что мы не пропустим данные которые еще не были обработаны

    Args:
        ds: DataStore с мета-подключением
        transform_step: Шаг трансформации для которого инициализируются offset'ы

    Returns:
        Dict с установленными offset'ами {input_table_name: update_ts_offset}
    """
    meta_tbl = transform_step.meta_table.sql_table

    # Найти минимальный process_ts среди успешно обработанных записей
    sql = sa.select(sa.func.min(meta_tbl.c.process_ts)).where(
        meta_tbl.c.is_success == True  # noqa: E712
    )

    with ds.meta_dbconn.con.begin() as con:
        min_process_ts = con.execute(sql).scalar()

    if min_process_ts is None:
        # Нет успешно обработанных записей → offset не устанавливаем
        return {}

    # Для каждой входной таблицы найти максимальный update_ts <= min_process_ts
    offsets = {}
    for inp in transform_step.input_dts:
        input_tbl = inp.dt.meta_table.sql_table

        sql = sa.select(sa.func.max(input_tbl.c.update_ts)).where(
            sa.and_(
                input_tbl.c.update_ts <= min_process_ts,
                input_tbl.c.delete_ts.is_(None),
            )
        )

        with ds.meta_dbconn.con.begin() as con:
            max_update_ts = con.execute(sql).scalar()

        if max_update_ts is not None:
            offsets[(transform_step.get_name(), inp.dt.name)] = max_update_ts

    # Установить offset'ы
    if offsets:
        try:
            ds.offset_table.update_offsets_bulk(offsets)
        except Exception as e:
            # Таблица offset'ов может не существовать
            logger.warning(
                f"Failed to initialize offsets for {transform_step.get_name()}: {e}. "
                "Offset table may not exist (create_meta_table=False)"
            )
            return {}

    return {inp_name: offset for (_, inp_name), offset in offsets.items()}

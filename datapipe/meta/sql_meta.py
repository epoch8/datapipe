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
    from datapipe.store.database import TableStoreDB

logger = logging.getLogger("datapipe.meta.sql_meta")

# Эпсилон для offset optimization: смещаем offset на эту величину назад
# для захвата записей с одинаковыми timestamps при использовании строгого >
# Это предотвращает потерю данных (Hypothesis 1) при сохранении производительности
OFFSET_EPSILON_SECONDS = 0.01  # 10 миллисекунд

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


# ============================================================================
# OFFSET OPTIMIZATION V2 - IMPLEMENTATION
# ============================================================================
#
# Эта секция содержит функции для построения SQL запросов с offset optimization.
# Использует UNION вместо FULL OUTER JOIN для объединения changed records.
#
# Структура функций (от низкого к высокому уровню):
#
# 1. БАЗОВЫЕ УТИЛИТЫ
#    - Создание WHERE условий, фильтров, JOIN conditions
#
# 2. ФУНКЦИИ ПРИНЯТИЯ РЕШЕНИЯ
#    - Определяют какой путь использовать (meta vs data)
#
# 3. ФУНКЦИИ ПОСТРОЕНИЯ CTE
#    - Forward join: таблица изменилась сама
#    - Reverse join: справочная таблица изменилась, находим зависимые записи
#
# 4. КООРДИНАТОРЫ
#    - Объединяют CTE от всех таблиц, добавляют error records, сортировку
#
# 5. ГЛАВНАЯ ФУНКЦИЯ
#    - build_changed_idx_sql_v2: entry point для offset optimization
# ============================================================================


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


# ----------------------------------------------------------------------------
# 1. БАЗОВЫЕ УТИЛИТЫ
# ----------------------------------------------------------------------------

def _generate_unique_cte_name(table_name: str, suffix: str, usage_count: Dict[str, int]) -> str:
    """
    Генерирует уникальное имя CTE для таблицы.

    При первом использовании: "{table_name}_{suffix}"
    При повторном: "{table_name}_{suffix}_{N}"

    Args:
        table_name: Имя таблицы
        suffix: Суффикс для CTE (например "changes", "deleted")
        usage_count: Словарь счётчиков использования (мутируется!)

    Returns:
        Уникальное имя CTE
    """
    if table_name not in usage_count:
        usage_count[table_name] = 0
        return f"{table_name}_{suffix}"
    else:
        usage_count[table_name] += 1
        return f"{table_name}_{suffix}_{usage_count[table_name]}"


def _build_offset_where_clause(tbl: Any, offset: float) -> Any:
    """
    Строит WHERE условие для фильтрации по offset.

    Применяет epsilon-сдвиг для захвата записей с timestamp близкими к offset.
    Это предотвращает потерю данных при одинаковых update_ts (Hypothesis 1)
    при сохранении производительности (offset остаётся = MAX(update_ts)).

    WHERE update_ts > (offset - epsilon) эквивалентно >= для практических целей,
    но читает только новые записи из БД.
    """
    # Вычитаем epsilon при использовании offset для захвата записей
    # с update_ts близкими к offset (включая равные)
    adjusted_offset = offset - OFFSET_EPSILON_SECONDS

    return sa.or_(
        tbl.c.update_ts > adjusted_offset,
        sa.and_(
            tbl.c.delete_ts.isnot(None),
            tbl.c.delete_ts > adjusted_offset
        )
    )


def _build_join_condition(conditions: List[Any]) -> Any:
    """Объединяет условия JOIN через AND."""
    if len(conditions) == 0:
        return None
    if len(conditions) == 1:
        return conditions[0]
    return sa.and_(*conditions)


def _apply_sql_filters(
    sql: Any,
    keys: List[str],
    filters_idx: Optional[IndexDF],
    tbl: Any,
    primary_keys: List[str],
    run_config: Optional[RunConfig],
) -> Any:
    """Применяет filters_idx и run_config фильтры к SQL запросу."""
    sql = sql_apply_filters_idx_to_subquery(sql, keys, filters_idx)
    sql = sql_apply_runconfig_filter(sql, tbl, primary_keys, run_config)
    return sql


# ----------------------------------------------------------------------------
# 2. ФУНКЦИИ ПРИНЯТИЯ РЕШЕНИЯ
# ----------------------------------------------------------------------------

def _can_use_only_meta(
    required_keys: List[str],
    meta_cols: List[str],
    data_cols: List[str],
    join_keys: Optional[Dict[str, str]] = None,
) -> bool:
    """
    Базовая функция: проверяет можно ли обойтись только meta table без JOIN с data table.

    Args:
        required_keys: Колонки которые нужно выбрать
        meta_cols: Доступные колонки в meta table
        data_cols: Доступные колонки в data table
        join_keys: Опционально - FK (Dict[primary_col, ref_col]) которые должны быть в meta

    Returns:
        True - можно использовать только meta (не нужен JOIN с data)
        False - нужен JOIN с data (есть колонки или FK только в data)
    """
    # Если есть join_keys, проверяем что все FK в meta
    if join_keys:
        for primary_col in join_keys.keys():
            if primary_col not in meta_cols:
                return False

    # Проверяем все ли required_keys доступны без data table
    for k in required_keys:
        if k not in meta_cols:
            # Ключ не в meta - проверяем есть ли он в data
            if k in data_cols:
                # Ключ в data - нужен JOIN с data
                return False
            # Ключ ни в meta, ни в data - будет NULL (допустимо)

    return True


def _should_use_forward_meta(
    inp: "ComputeInput",
    all_select_keys: List[str],
) -> bool:
    """
    Определяет какой путь использовать для forward join.

    Returns:
        True - ПУТЬ А: только meta (все ключи в meta, 1 CTE)
        False - ПУТЬ Б: meta + data (нужен JOIN с data, 2 CTE)
    """
    tbl = inp.dt.meta_table.sql_table
    meta_cols = [c.name for c in tbl.columns]

    # Fallback: нет data_table
    if not hasattr(inp.dt.table_store, 'data_table'):
        return True

    data_tbl = inp.dt.table_store.data_table
    data_cols = [c.name for c in data_tbl.columns]

    return _can_use_only_meta(all_select_keys, meta_cols, data_cols)


def _should_use_reverse_meta(
    ref_join_keys: Dict[str, str],
    all_select_keys: List[str],
    primary_meta_tbl: Any,
    primary_store: "TableStoreDB",
) -> bool:
    """
    Определяет какой путь использовать для reverse join.

    Args:
        ref_join_keys: join_keys из справочной таблицы
        all_select_keys: Колонки которые нужно выбрать
        primary_meta_tbl: Meta table основной таблицы
        primary_store: TableStoreDB основной таблицы

    Returns:
        True - ПУТЬ А: JOIN meta с meta (FK и все ключи в primary_meta)
        False - ПУТЬ Б: JOIN meta с data (FK или ключи в primary_data)
    """
    primary_data_tbl = primary_store.data_table
    primary_meta_cols = [c.name for c in primary_meta_tbl.columns]
    primary_data_cols = [c.name for c in primary_data_tbl.columns]

    return _can_use_only_meta(all_select_keys, primary_meta_cols, primary_data_cols, ref_join_keys)


# ----------------------------------------------------------------------------
# 3. ФУНКЦИИ ПОСТРОЕНИЯ CTE
# ----------------------------------------------------------------------------

# 3.0 Внутренние helper'ы

def _meta_sql_helper(
    tbl: Any,
    keys_in_meta: List[str],
    offset: float,
    filters_idx: Optional[IndexDF],
    primary_keys: List[str],
    run_config: Optional[RunConfig],
) -> Any:
    """
    Строит SQL: SELECT из meta table с WHERE по offset.

    Использует UNION двух SELECT вместо OR для использования индексов:
    - SELECT WHERE update_ts > offset (использует индекс на update_ts)
    - UNION
    - SELECT WHERE delete_ts > offset (использует индекс на delete_ts)

    Это позволяет PostgreSQL использовать Index Scan вместо Sequential Scan.
    """
    select_cols = [sa.column(k) for k in keys_in_meta]
    adjusted_offset = offset - OFFSET_EPSILON_SECONDS

    # Часть 1: Измененные записи (update_ts > offset)
    updated_sql = sa.select(*select_cols).select_from(tbl).where(
        tbl.c.update_ts > adjusted_offset
    )
    updated_sql = _apply_sql_filters(updated_sql, keys_in_meta, filters_idx, tbl, primary_keys, run_config)
    if len(select_cols) > 0:
        updated_sql = updated_sql.group_by(*select_cols)

    # Часть 2: Удаленные записи (delete_ts > offset)
    # Примечание: IS NOT NULL не нужен - NULL > offset всегда FALSE
    deleted_sql = sa.select(*select_cols).select_from(tbl).where(
        tbl.c.delete_ts > adjusted_offset
    )
    deleted_sql = _apply_sql_filters(deleted_sql, keys_in_meta, filters_idx, tbl, primary_keys, run_config)
    if len(select_cols) > 0:
        deleted_sql = deleted_sql.group_by(*select_cols)

    # UNION двух частей для использования отдельных индексов
    sql = sa.union(updated_sql, deleted_sql)

    return sql


def _meta_data_sql_helper(
    tbl: Any,
    data_tbl: Any,
    keys_in_meta: List[str],
    keys_in_data_available: List[str],
    primary_keys: List[str],
    offset: float,
    filters_idx: Optional[IndexDF],
    run_config: Optional[RunConfig],
) -> Tuple[Any, Any]:
    """
    Строит SQL при JOIN meta с data table (2 CTE: changed + deleted).

    Returns:
        Tuple[changed_cte, deleted_cte]
    """
    join_conditions = [tbl.c[pk] == data_tbl.c[pk] for pk in primary_keys]
    join_condition = _build_join_condition(join_conditions)

    select_cols = [tbl.c[k] for k in keys_in_meta] + [data_tbl.c[k] for k in keys_in_data_available]
    all_keys = keys_in_meta + keys_in_data_available

    # CTE для измененных записей
    changed_sql = sa.select(*select_cols).select_from(
        tbl.join(data_tbl, join_condition)
    ).where(
        sa.and_(
            tbl.c.update_ts >= offset,
            tbl.c.delete_ts.is_(None)
        )
    )

    changed_sql = _apply_sql_filters(changed_sql, all_keys, filters_idx, tbl, primary_keys, run_config)

    if len(select_cols) > 0:
        changed_sql = changed_sql.group_by(*select_cols)

    # CTE для удаленных записей
    deleted_select_cols = [
        tbl.c[k] if k in keys_in_meta else sa.literal(None).label(k)
        for k in all_keys
    ]

    deleted_sql = sa.select(*deleted_select_cols).select_from(tbl).where(
        sa.and_(
            tbl.c.delete_ts.isnot(None),
            tbl.c.delete_ts >= offset
        )
    )

    deleted_sql = _apply_sql_filters(deleted_sql, keys_in_meta, filters_idx, tbl, primary_keys, run_config)

    if len(deleted_select_cols) > 0:
        deleted_sql = deleted_sql.group_by(*[tbl.c[k] for k in keys_in_meta])

    return changed_sql, deleted_sql


# 3.1 Forward join

def _build_forward_meta_cte(
    meta_tbl: Any,
    primary_keys: List[str],
    all_select_keys: List[str],
    offset: float,
    filters_idx: Optional[IndexDF],
    run_config: Optional[RunConfig],
) -> Any:
    """
    ПУТЬ А для forward join: Строит 1 CTE когда все ключи в meta table.

    Deleted records включены в WHERE через _build_offset_where_clause:
    WHERE (update_ts >= offset) OR (delete_ts >= offset)

    Returns:
        Один CTE с changed + deleted records
    """
    meta_cols = [c.name for c in meta_tbl.columns]
    keys_in_meta = [k for k in all_select_keys if k in meta_cols]

    return _meta_sql_helper(meta_tbl, keys_in_meta, offset, filters_idx, primary_keys, run_config)


def _build_forward_data_cte(
    meta_tbl: Any,
    store: "TableStoreDB",
    primary_keys: List[str],
    all_select_keys: List[str],
    offset: float,
    filters_idx: Optional[IndexDF],
    run_config: Optional[RunConfig],
) -> Tuple[Any, Any]:
    """
    ПУТЬ Б для forward join: Строит 2 CTE когда нужны FK из data table.

    Changed CTE: INNER JOIN с data_table WHERE delete_ts IS NULL
    Deleted CTE: SELECT FROM meta WHERE delete_ts >= offset (FK заменяем на NULL)

    Почему 2 CTE? Для deleted records НЕ нужен JOIN - они уже удалены из data table.

    Returns:
        Tuple[changed_cte, deleted_cte]
    """
    data_tbl = store.data_table

    meta_cols = [c.name for c in meta_tbl.columns]
    keys_in_meta = [k for k in all_select_keys if k in meta_cols]
    keys_in_data_only = [k for k in all_select_keys if k not in meta_cols]

    data_cols_available = [c.name for c in data_tbl.columns]
    keys_in_data_available = [k for k in keys_in_data_only if k in data_cols_available]

    return _meta_data_sql_helper(
        tbl=meta_tbl,
        data_tbl=data_tbl,
        keys_in_meta=keys_in_meta,
        keys_in_data_available=keys_in_data_available,
        primary_keys=primary_keys,
        offset=offset,
        filters_idx=filters_idx,
        run_config=run_config,
    )


# 3.2 Reverse join

def _build_reverse_meta_cte(
    ref_meta_tbl: Any,
    ref_join_keys: Dict[str, str],
    ref_primary_keys: List[str],
    primary_meta_tbl: Any,
    all_select_keys: List[str],
    offset: float,
    filters_idx: Optional[IndexDF],
    run_config: Optional[RunConfig],
) -> Optional[Any]:
    """
    ПУТЬ А для reverse join: JOIN meta справочника с meta основной таблицы.
    Используется когда FK в primary_meta.

    Returns:
        SQL query или None если не удалось построить JOIN
    """
    meta_cols = [c.name for c in ref_meta_tbl.columns]
    primary_meta_cols = [c.name for c in primary_meta_tbl.columns]

    select_cols = []
    group_by_cols = []
    for k in all_select_keys:
        if k == 'update_ts':
            # update_ts всегда из meta справочника (ref_meta_tbl)
            select_cols.append(ref_meta_tbl.c.update_ts)
            group_by_cols.append(ref_meta_tbl.c.update_ts)
        elif k in primary_meta_cols:
            select_cols.append(primary_meta_tbl.c[k])
            group_by_cols.append(primary_meta_tbl.c[k])
        elif k in meta_cols:
            # Берём колонки из meta справочника
            select_cols.append(ref_meta_tbl.c[k])
            group_by_cols.append(ref_meta_tbl.c[k])
        else:
            select_cols.append(sa.literal(None).label(k))

    join_conditions = [
        primary_meta_tbl.c[primary_col] == ref_meta_tbl.c[ref_col]
        for primary_col, ref_col in ref_join_keys.items()
        if primary_col in primary_meta_cols and ref_col in meta_cols
    ]

    join_condition = _build_join_condition(join_conditions)
    if join_condition is None:
        return None

    adjusted_offset = offset - OFFSET_EPSILON_SECONDS

    # Часть 1: update_ts > offset (использует индекс на update_ts)
    updated_sql = sa.select(*select_cols).select_from(
        ref_meta_tbl.join(primary_meta_tbl, join_condition)
    ).where(ref_meta_tbl.c.update_ts > adjusted_offset)
    updated_sql = _apply_sql_filters(updated_sql, all_select_keys, filters_idx, ref_meta_tbl, ref_primary_keys, run_config)
    if len(group_by_cols) > 0:
        updated_sql = updated_sql.group_by(*group_by_cols)

    # Часть 2: delete_ts > offset (использует индекс на delete_ts)
    deleted_sql = sa.select(*select_cols).select_from(
        ref_meta_tbl.join(primary_meta_tbl, join_condition)
    ).where(ref_meta_tbl.c.delete_ts > adjusted_offset)
    deleted_sql = _apply_sql_filters(deleted_sql, all_select_keys, filters_idx, ref_meta_tbl, ref_primary_keys, run_config)
    if len(group_by_cols) > 0:
        deleted_sql = deleted_sql.group_by(*group_by_cols)

    # UNION для использования отдельных индексов
    sql = sa.union(updated_sql, deleted_sql)

    return sql


def _build_reverse_data_cte(
    ref_meta_tbl: Any,
    ref_join_keys: Dict[str, str],
    ref_primary_keys: List[str],
    primary_meta_tbl: Any,
    primary_store: "TableStoreDB",
    all_select_keys: List[str],
    offset: float,
    filters_idx: Optional[IndexDF],
    run_config: Optional[RunConfig],
) -> Tuple[Optional[Any], Optional[Any]]:
    """
    ПУТЬ Б для reverse join: JOIN meta справочника с data основной таблицы.
    Используется когда FK в primary_data.

    Changed CTE: INNER JOIN с primary_data WHERE delete_ts IS NULL
    Deleted CTE: SELECT FROM primary_meta WHERE delete_ts >= offset (FK заменяем на NULL)

    Returns:
        Tuple[changed_cte, deleted_cte] или (None, None) если не удалось построить JOIN
    """
    primary_data_tbl = primary_store.data_table
    meta_cols = [c.name for c in ref_meta_tbl.columns]
    primary_data_cols = [c.name for c in primary_data_tbl.columns]
    primary_meta_cols = [c.name for c in primary_meta_tbl.columns]

    # CHANGED CTE: JOIN meta с data
    select_cols = []
    group_by_cols = []
    for k in all_select_keys:
        if k == 'update_ts':
            # update_ts всегда из meta справочника (ref_meta_tbl)
            select_cols.append(ref_meta_tbl.c.update_ts)
            group_by_cols.append(ref_meta_tbl.c.update_ts)
        elif k in primary_data_cols:
            select_cols.append(primary_data_tbl.c[k])
            group_by_cols.append(primary_data_tbl.c[k])
        elif k in meta_cols:
            # Берём колонки из meta справочника
            select_cols.append(ref_meta_tbl.c[k])
            group_by_cols.append(ref_meta_tbl.c[k])
        else:
            select_cols.append(sa.literal(None).label(k))

    join_conditions = [
        primary_data_tbl.c[primary_col] == ref_meta_tbl.c[ref_col]
        for primary_col, ref_col in ref_join_keys.items()
        if primary_col in primary_data_cols and ref_col in meta_cols
    ]

    join_condition = _build_join_condition(join_conditions)
    if join_condition is None:
        return None, None

    # Используем UNION вместо OR для использования индексов на update_ts и delete_ts
    adjusted_offset = offset - OFFSET_EPSILON_SECONDS

    # Часть 1: update_ts > offset (использует индекс на update_ts)
    updated_sql = sa.select(*select_cols).select_from(
        ref_meta_tbl.join(primary_data_tbl, join_condition)
    ).where(ref_meta_tbl.c.update_ts > adjusted_offset)

    updated_sql = _apply_sql_filters(
        updated_sql, all_select_keys, filters_idx, ref_meta_tbl, ref_primary_keys, run_config
    )

    if len(group_by_cols) > 0:
        updated_sql = updated_sql.group_by(*group_by_cols)

    # Часть 2: delete_ts > offset (использует индекс на delete_ts)
    deleted_part_sql = sa.select(*select_cols).select_from(
        ref_meta_tbl.join(primary_data_tbl, join_condition)
    ).where(ref_meta_tbl.c.delete_ts > adjusted_offset)

    deleted_part_sql = _apply_sql_filters(
        deleted_part_sql, all_select_keys, filters_idx, ref_meta_tbl, ref_primary_keys, run_config
    )

    if len(group_by_cols) > 0:
        deleted_part_sql = deleted_part_sql.group_by(*group_by_cols)

    # UNION для использования отдельных индексов
    changed_sql = sa.union(updated_sql, deleted_part_sql)

    # DELETED CTE: Только из primary_meta (FK заменяем на NULL)
    deleted_select_cols = []
    for k in all_select_keys:
        if k == 'update_ts':
            deleted_select_cols.append(ref_meta_tbl.c.update_ts)
        elif k in primary_meta_cols:
            deleted_select_cols.append(primary_meta_tbl.c[k])
        elif k in meta_cols:
            deleted_select_cols.append(ref_meta_tbl.c[k])
        else:
            # FK из primary_data недоступен для deleted records
            deleted_select_cols.append(sa.literal(None).label(k))

    # JOIN primary_meta с ref_meta_tbl для deleted records
    deleted_join_conditions = [
        primary_meta_tbl.c[primary_col] == ref_meta_tbl.c[ref_col]
        for primary_col, ref_col in ref_join_keys.items()
        if primary_col in primary_meta_cols and ref_col in meta_cols
    ]

    deleted_join_condition = _build_join_condition(deleted_join_conditions)
    if deleted_join_condition is None:
        return changed_sql, None

    deleted_sql = sa.select(*deleted_select_cols).select_from(
        ref_meta_tbl.join(primary_meta_tbl, deleted_join_condition)
    ).where(ref_meta_tbl.c.delete_ts > adjusted_offset)

    deleted_sql = _apply_sql_filters(
        deleted_sql, all_select_keys, filters_idx, ref_meta_tbl, ref_primary_keys, run_config
    )

    return changed_sql, deleted_sql


# ----------------------------------------------------------------------------
# 4. КООРДИНАТОРЫ
# ----------------------------------------------------------------------------

def _build_forward_input_cte(
    inp: "ComputeInput",
    all_select_keys: List[str],
    offset: float,
    filters_idx: Optional[IndexDF],
    run_config: Optional[RunConfig],
    cte_name: str,
) -> List[Any]:
    """
    Строит CTE для forward join (таблица без join_keys).

    Находит изменения в самой таблице inp (прямой путь).

    Returns:
        List of CTE objects (1 CTE для пути А, 2 CTE для пути Б)
    """
    meta_tbl = inp.dt.meta_table.sql_table
    primary_keys = inp.dt.primary_keys

    # Определяем какой путь использовать
    use_meta_path = _should_use_forward_meta(inp, all_select_keys)

    # Выполняем соответствующий путь
    if use_meta_path:
        # ПУТЬ А: строим 1 CTE из meta table (changed + deleted в одном WHERE)
        changed_sql = _build_forward_meta_cte(meta_tbl, primary_keys, all_select_keys, offset, filters_idx, run_config)
        deleted_sql = None
    else:
        # ПУТЬ Б: строим 2 CTE (changed с JOIN, deleted без JOIN)
        from datapipe.store.database import TableStoreDB

        store = inp.dt.table_store
        assert isinstance(store, TableStoreDB)

        changed_sql, deleted_sql = _build_forward_data_cte(
            meta_tbl, store, primary_keys, all_select_keys, offset, filters_idx, run_config
        )

    ctes = [changed_sql.cte(name=cte_name)]
    if deleted_sql is not None:
        deleted_cte_name = cte_name.replace('_changes', '_deleted')
        ctes.append(deleted_sql.cte(name=deleted_cte_name))

    return ctes


def _build_reverse_input_cte(
    ref_meta_tbl: Any,
    ref_join_keys: Dict[str, str],
    ref_primary_keys: List[str],
    primary_meta_tbl: Any,
    primary_store: "TableStoreDB",
    all_select_keys: List[str],
    offset: float,
    filters_idx: Optional[IndexDF],
    run_config: Optional[RunConfig],
    cte_name: str,
) -> List[Any]:
    """
    Строит CTE для reverse join (справочная таблица с join_keys).

    Когда изменяется справочная таблица (users), находит зависимые записи
    в основной таблице (subscriptions) через обратный JOIN.

    Args:
        ref_meta_tbl: Meta table справочной таблицы
        ref_join_keys: join_keys связывающие таблицы
        ref_primary_keys: Primary keys справочной таблицы
        primary_meta_tbl: Meta table основной таблицы
        primary_store: TableStoreDB основной таблицы
        all_select_keys: Колонки для выборки
        offset: Временная метка offset
        filters_idx: Дополнительные фильтры
        run_config: Конфигурация выполнения
        cte_name: Имя для CTE

    Returns:
        List of CTE objects (1 CTE для пути А, 2 CTE для пути Б)
    """
    # Определяем какой путь использовать для reverse_join
    use_meta_path = _should_use_reverse_meta(
        ref_join_keys, all_select_keys, primary_meta_tbl, primary_store
    )

    # Выполняем соответствующий путь
    if use_meta_path:
        # ПУТЬ А: JOIN meta с meta (FK в primary_meta)
        changed_sql = _build_reverse_meta_cte(
            ref_meta_tbl, ref_join_keys, ref_primary_keys,
            primary_meta_tbl, all_select_keys, offset, filters_idx, run_config
        )
        deleted_sql = None
    else:
        # ПУТЬ Б: JOIN meta с data (FK в primary_data) - возвращает 2 CTE
        changed_sql, deleted_sql = _build_reverse_data_cte(
            ref_meta_tbl, ref_join_keys, ref_primary_keys,
            primary_meta_tbl, primary_store,
            all_select_keys, offset, filters_idx, run_config
        )

    if changed_sql is None:
        return []

    ctes = [changed_sql.cte(name=cte_name)]
    if deleted_sql is not None:
        deleted_cte_name = cte_name.replace('_changes', '_deleted')
        ctes.append(deleted_sql.cte(name=deleted_cte_name))

    return ctes


def _build_input_ctes(
    input_dts: List["ComputeInput"],
    offsets: Dict[str, float],
    all_select_keys: List[str],
    filters_idx: Optional[IndexDF],
    run_config: Optional[RunConfig],
) -> List[Any]:
    """
    Строит CTE для каждой входной таблицы с фильтром по offset.

    Для таблиц с join_keys делает обратный JOIN к основной таблице.
    Для таблиц с join_keys также создаёт отдельный CTE для deleted records.

    Returns:
        List of CTE objects
    """
    changed_ctes = []

    # Сначала находим "основную" таблицу - первую без join_keys
    primary_inp = None
    for inp in input_dts:
        if not inp.join_keys:
            primary_inp = inp
            break

    # Отслеживаем использование таблиц для генерации уникальных имен CTE
    # Ключ: имя таблицы, значение: количество использований
    table_usage_count: Dict[str, int] = {}

    for inp in input_dts:
        tbl = inp.dt.meta_table.sql_table

        # Генерируем уникальное имя CTE для текущей таблицы
        cte_name = _generate_unique_cte_name(inp.dt.name, "changes", table_usage_count)

        # Проверяем есть ли ключи в meta table
        meta_cols = [c.name for c in tbl.columns]
        keys_in_meta = [k for k in all_select_keys if k in meta_cols]

        if len(keys_in_meta) == 0:
            continue

        offset = offsets[inp.dt.name]

        # Два взаимоисключающих пути:
        # 1. Reverse join: справочная таблица с join_keys
        # 2. Forward join: таблица без join_keys
        if inp.join_keys and primary_inp and hasattr(primary_inp.dt.table_store, 'data_table'):
            from datapipe.store.database import TableStoreDB

            # Reverse join: находим зависимые записи в primary таблице от изменений в referense таблице
            primary_store = primary_inp.dt.table_store
            assert isinstance(primary_store, TableStoreDB), "primary table must be TableStoreDB for reverse join"

            ctes = _build_reverse_input_cte(
                ref_meta_tbl=inp.dt.meta_table.sql_table,
                ref_join_keys=inp.join_keys,
                ref_primary_keys=inp.dt.primary_keys,
                primary_meta_tbl=primary_inp.dt.meta_table.sql_table,
                primary_store=primary_store,
                all_select_keys=all_select_keys,
                offset=offset,
                filters_idx=filters_idx,
                run_config=run_config,
                cte_name=cte_name,
            )
        else:
            # Forward join: находим изменения в самой primary таблице и зависимые referense записи
            ctes = _build_forward_input_cte(
                inp, all_select_keys, offset, filters_idx, run_config, cte_name
            )

        changed_ctes.extend(ctes)

    return changed_ctes


def _build_error_records_cte(
    meta_table: "TransformMetaTable",
    transform_keys: List[str],
    all_select_keys: List[str],
    filters_idx: Optional[IndexDF],
) -> Any:
    """
    Строит CTE для записей с ошибками из TransformMetaTable.

    Returns:
        CTE object для error_records
    """
    tr_tbl = meta_table.sql_table
    error_select_cols: List[Any] = []
    for k in all_select_keys:
        if k in transform_keys:
            error_select_cols.append(sa.column(k))
        else:
            # Для дополнительных колонок (включая update_ts) используем NULL
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

    return error_records_sql.cte(name="error_records")


def _union_or_cross_join_ctes(
    ds: "DataStore",
    changed_ctes: List[Any],
    error_records_cte: Any,
    transform_keys: List[str],
    additional_columns: List[str],
    all_select_keys: List[str],
) -> Tuple[Any, bool]:
    """
    Объединяет CTE через UNION или CROSS JOIN в зависимости от структуры ключей.

    Returns:
        Tuple[CTE, needs_cross_join_flag]
    """
    needs_cross_join = False

    if len(changed_ctes) == 0:
        # Если нет входных таблиц с изменениями, используем только ошибки
        union_sql: Any = sa.select(
            *[error_records_cte.c[k] for k in all_select_keys]
        ).select_from(error_records_cte)
    else:
        # Собираем информацию о том, какие transform_keys есть в каждом CTE
        cte_transform_keys_sets = []
        for cte in changed_ctes:
            cte_transform_keys = set(k for k in transform_keys if k in cte.c)
            cte_transform_keys_sets.append(cte_transform_keys)

        # Проверяем все ли CTE содержат одинаковый набор transform_keys
        unique_key_sets = set(map(frozenset, cte_transform_keys_sets))
        needs_cross_join = len(unique_key_sets) > 1

        if needs_cross_join:
            # Cross join сценарий: используем _make_agg_of_agg для FULL OUTER JOIN
            compute_input_ctes = []
            keys_for_join = transform_keys + additional_columns  # БЕЗ update_ts
            for i, cte in enumerate(changed_ctes):
                cte_keys = [k for k in keys_for_join if k in cte.c]
                compute_input_ctes.append(
                    ComputeInputCTE(cte=cte, keys=cte_keys, join_type='full')
                )

            cross_join_raw = _make_agg_of_agg(
                ds=ds,
                transform_keys=transform_keys + additional_columns,
                agg_col='update_ts',
                ctes=compute_input_ctes
            )

            cross_join_clean_select = sa.select(
                *[sa.column(k) for k in all_select_keys]
            ).select_from(cross_join_raw)

            cross_join_cte = cross_join_clean_select.cte(name="cross_join_clean")

            # Объединяем cross join результат с error_records через UNION
            union_sql = sa.union(
                sa.select(*[cross_join_cte.c[k] for k in all_select_keys]).select_from(cross_join_cte),
                sa.select(*[error_records_cte.c[k] for k in all_select_keys]).select_from(error_records_cte)
            )
        else:
            # Обычный UNION - все CTE содержат одинаковый набор transform_keys
            union_parts = []
            for cte in changed_ctes:
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

    return union_sql.cte(name="changed_union"), needs_cross_join


def _deduplicate_if_needed(
    union_cte: Any,
    input_dts: List["ComputeInput"],
    transform_keys: List[str],
    additional_columns: List[str],
    needs_cross_join: bool,
) -> Any:
    """
    Применяет дедупликацию при использовании join_keys (если нужно).

    Returns:
        Deduplicated CTE или исходный union_cte
    """
    has_join_keys = any(inp.join_keys for inp in input_dts)
    if has_join_keys and len(transform_keys) > 0 and not needs_cross_join:
        group_by_cols = transform_keys + additional_columns

        select_cols = []
        for k in group_by_cols:
            select_cols.append(union_cte.c[k])
        select_cols.append(sa.func.max(union_cte.c.update_ts).label('update_ts'))

        deduplicated_sql = (
            sa.select(*select_cols)
            .select_from(union_cte)
            .group_by(*[union_cte.c[k] for k in group_by_cols])
        )
        return deduplicated_sql.cte(name="changed_union_deduplicated")

    return union_cte


def _apply_final_filters_and_sort(
    union_cte: Any,
    meta_table: "TransformMetaTable",
    transform_keys: List[str],
    all_select_keys: List[str],
    order_by: Optional[List[str]],
    order: Literal["asc", "desc"],
) -> Any:
    """
    Применяет финальные фильтры и сортировку к результату.

    Returns:
        Final SQL query
    """
    tr_tbl = meta_table.sql_table

    if len(transform_keys) == 0:
        join_onclause_sql: Any = sa.literal(True)
    elif len(transform_keys) == 1:
        join_onclause_sql = union_cte.c[transform_keys[0]] == tr_tbl.c[transform_keys[0]]
    else:
        join_onclause_sql = sa.and_(*[union_cte.c[key] == tr_tbl.c[key] for key in transform_keys])

    is_error_record = union_cte.c.update_ts.is_(None)

    out = (
        sa.select(
            sa.literal(1).label("_datapipe_dummy"),
            *[union_cte.c[k] for k in all_select_keys if k in union_cte.c],
        )
        .select_from(union_cte)
        .outerjoin(tr_tbl, onclause=join_onclause_sql)
        .where(
            sa.or_(
                is_error_record,
                tr_tbl.c.process_ts.is_(None),
                sa.and_(
                    tr_tbl.c.is_success == True,  # noqa
                    union_cte.c.update_ts > tr_tbl.c.process_ts
                )
            )
        )
    )

    if order_by is None:
        out = out.order_by(
            tr_tbl.c.priority.desc().nullslast(),
            union_cte.c.update_ts.asc().nullslast(),
            *[union_cte.c[k] for k in transform_keys],
        )
    else:
        if order == "desc":
            out = out.order_by(
                tr_tbl.c.priority.desc().nullslast(),
                union_cte.c.update_ts.asc().nullslast(),
                *[sa.desc(union_cte.c[k]) for k in order_by],
            )
        elif order == "asc":
            out = out.order_by(
                tr_tbl.c.priority.desc().nullslast(),
                union_cte.c.update_ts.asc().nullslast(),
                *[sa.asc(union_cte.c[k]) for k in order_by],
            )

    return out


# ----------------------------------------------------------------------------
# 5. ГЛАВНАЯ ФУНКЦИЯ
# ----------------------------------------------------------------------------

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
    update_ts >= offset для каждой входной таблицы, затем объединяем через UNION.

    Args:
        additional_columns: Дополнительные колонки для включения в результат (для filtered join)
    """
    if additional_columns is None:
        additional_columns = []

    # Полный список колонок для SELECT (transform_keys + additional_columns)
    all_select_keys = list(transform_keys) + additional_columns

    # Добавляем update_ts для ORDER BY если его еще нет
    if 'update_ts' not in all_select_keys:
        all_select_keys.append('update_ts')

    # 1. Получить все offset'ы одним запросом для избежания N+1
    offsets = offset_table.get_offsets_for_transformation(transformation_id)
    for inp in input_dts:
        if inp.dt.name not in offsets:
            offsets[inp.dt.name] = 0.0

    # 2. Построить CTE для каждой входной таблицы с фильтром по offset
    changed_ctes = _build_input_ctes(
        input_dts=input_dts,
        offsets=offsets,
        all_select_keys=all_select_keys,
        filters_idx=filters_idx,
        run_config=run_config,
    )

    # 3. Построить CTE для error_records
    error_records_cte = _build_error_records_cte(
        meta_table=meta_table,
        transform_keys=transform_keys,
        all_select_keys=all_select_keys,
        filters_idx=filters_idx,
    )

    # 4. Объединить через UNION или CROSS JOIN
    union_cte, needs_cross_join = _union_or_cross_join_ctes(
        ds=ds,
        changed_ctes=changed_ctes,
        error_records_cte=error_records_cte,
        transform_keys=transform_keys,
        additional_columns=additional_columns,
        all_select_keys=all_select_keys,
    )

    # 5. Дедупликация если нужно
    union_cte = _deduplicate_if_needed(
        union_cte=union_cte,
        input_dts=input_dts,
        transform_keys=transform_keys,
        additional_columns=additional_columns,
        needs_cross_join=needs_cross_join,
    )

    # 6. Применить финальные фильтры и сортировку
    out = _apply_final_filters_and_sort(
        union_cte=union_cte,
        meta_table=meta_table,
        transform_keys=transform_keys,
        all_select_keys=all_select_keys,
        order_by=order_by,
        order=order,
    )

    return (all_select_keys, out)

# ----------------------------------------------------------------------------

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

import itertools
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

import cityhash
import pandas as pd
import sqlalchemy as sa

from datapipe.run_config import RunConfig
from datapipe.sql_util import sql_apply_idx_filter_to_table, sql_apply_runconfig_filter
from datapipe.store.database import DBConn, MetaKey
from datapipe.types import (
    DataDF,
    DataSchema,
    IndexDF,
    MetadataDF,
    MetaSchema,
    TAnyDF,
    data_to_index,
)

if TYPE_CHECKING:
    from datapipe.compute import ComputeInput
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
        return self.primary_keys + list(self.meta_keys.values()) + [column.name for column in TABLE_META_SCHEMA]

    def _get_hash_for_df(self, df) -> pd.Series:
        return df.apply(lambda x: str(list(x)), axis=1).apply(
            lambda x: int.from_bytes(cityhash.CityHash32(x).to_bytes(4, "little"), "little", signed=True)
        )

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
        existing_meta_df = self.get_metadata(data_to_index(data_df, self.primary_keys), include_deleted=True)
        data_cols = list(data_df.columns)
        meta_cols = self._get_meta_data_columns()

        # Дополняем данные методанными
        merged_df = pd.merge(
            data_df.assign(
                data_hash=self._get_hash_for_df(data_df),
            ),
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
        changed_meta_idx = (merged_df["hash"].notna()) & (merged_df["hash"] != merged_df["data_hash"]) | (
            merged_df["delete_ts"].notnull()
        )
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
) -> Tuple[Iterable[str], Any]:
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
) -> Tuple[Iterable[str], Any]:
    """
    Новая версия build_changed_idx_sql, использующая offset'ы для оптимизации.

    Вместо FULL OUTER JOIN всех входных таблиц, выбираем только записи с
    update_ts > offset для каждой входной таблицы, затем объединяем через UNION.
    """

    # 1. Получить все offset'ы одним запросом для избежания N+1
    offsets = offset_table.get_offsets_for_transformation(transformation_id)
    # Для таблиц без offset используем 0.0 (обрабатываем все данные)
    for inp in input_dts:
        if inp.dt.name not in offsets:
            offsets[inp.dt.name] = 0.0

    # 2. Построить CTE для каждой входной таблицы с фильтром по offset
    changed_ctes = []
    for inp in input_dts:
        tbl = inp.dt.meta_table.sql_table
        keys = [k for k in transform_keys if k in inp.dt.primary_keys]

        if len(keys) == 0:
            continue

        key_cols: List[Any] = [sa.column(k) for k in keys]
        offset = offsets[inp.dt.name]

        # SELECT transform_keys FROM input_meta WHERE update_ts > offset AND delete_ts IS NULL
        sql: Any = sa.select(*key_cols).select_from(tbl).where(
            sa.and_(
                tbl.c.update_ts > offset,
                tbl.c.delete_ts.is_(None)
            )
        )

        # Применить filters_idx и run_config
        sql = sql_apply_filters_idx_to_subquery(sql, keys, filters_idx)
        sql = sql_apply_runconfig_filter(sql, tbl, inp.dt.primary_keys, run_config)

        if len(key_cols) > 0:
            sql = sql.group_by(*key_cols)

        changed_ctes.append(sql.cte(name=f"{inp.dt.name}_changes"))

    # 3. Получить записи с ошибками из TransformMetaTable
    tr_tbl = meta_table.sql_table
    error_records_sql: Any = sa.select(
        *[sa.column(k) for k in transform_keys]
    ).select_from(tr_tbl).where(
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
        union_sql = sa.select(*[error_records_cte.c[k] for k in transform_keys]).select_from(error_records_cte)
    else:
        # UNION всех изменений и ошибок
        union_parts = []
        for cte in changed_ctes:
            union_parts.append(sa.select(*[cte.c[k] for k in transform_keys if k in cte.c]).select_from(cte))

        union_parts.append(
            sa.select(*[error_records_cte.c[k] for k in transform_keys]).select_from(error_records_cte)
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

    final_sql = (
        sa.select(
            sa.literal(1).label("_datapipe_dummy"),
            *[union_cte.c[k] for k in transform_keys]
        )
        .select_from(union_cte)
        .outerjoin(tr_tbl, onclause=join_onclause_sql)
    )

    if order_by is None:
        final_sql = final_sql.order_by(
            tr_tbl.c.priority.desc().nullslast(),
            *[sa.column(k) for k in transform_keys],
        )
    else:
        if order == "desc":
            final_sql = final_sql.order_by(
                *[sa.desc(sa.column(k)) for k in order_by],
                tr_tbl.c.priority.desc().nullslast(),
            )
        elif order == "asc":
            final_sql = final_sql.order_by(
                *[sa.asc(sa.column(k)) for k in order_by],
                tr_tbl.c.priority.desc().nullslast(),
            )

    return (transform_keys, final_sql)


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

    def update_offset(
        self, transformation_id: str, input_table_name: str, update_ts_offset: float
    ) -> None:
        """Обновить offset после успешной обработки"""
        insert_sql = self.dbconn.insert(self.sql_table).values(
            transformation_id=transformation_id,
            input_table_name=input_table_name,
            update_ts_offset=update_ts_offset,
        )
        sql = insert_sql.on_conflict_do_update(
            index_elements=["transformation_id", "input_table_name"],
            set_={"update_ts_offset": update_ts_offset},
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
        sql = insert_sql.on_conflict_do_update(
            index_elements=["transformation_id", "input_table_name"],
            set_={"update_ts_offset": insert_sql.excluded.update_ts_offset},
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
            return con.execute(sql).scalar()

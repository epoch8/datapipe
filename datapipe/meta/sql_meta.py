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
    Sequence,
    Tuple,
    cast,
)

import pandas as pd
import sqlalchemy as sa
from opentelemetry import trace

from datapipe.compute import ComputeInput
from datapipe.datatable import DataTable
from datapipe.meta.base import MetaPlane, TableDebugInfo, TableMeta, TransformMeta
from datapipe.run_config import LabelDict, RunConfig
from datapipe.sql_util import sql_apply_idx_filter_to_table, sql_apply_runconfig_filter
from datapipe.store.database import DBConn, MetaKey
from datapipe.types import (
    ChangeList,
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

tracer = trace.get_tracer("datapipe.step.batch_transform")

if TYPE_CHECKING:
    from datapipe.datatable import DataStore


TABLE_META_SCHEMA: List[sa.Column] = [
    sa.Column("hash", sa.Integer),
    sa.Column("create_ts", sa.Float),  # Время создания строки
    sa.Column("update_ts", sa.Float),  # Время последнего изменения
    sa.Column("process_ts", sa.Float),  # Время последней успешной обработки
    sa.Column("delete_ts", sa.Float),  # Время удаления
]


class SQLTableMeta(TableMeta):
    @classmethod
    def create(
        cls,
        dbconn: DBConn,
        name: str,
        primary_schema: DataSchema,
        meta_schema: MetaSchema = [],
    ) -> "SQLTableMeta":
        return cls(
            dbconn=dbconn,
            name=name,
            primary_schema=primary_schema,
            meta_schema=meta_schema,
            create_table=True,
        )

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


@dataclass
class SQLMetaComputeInput:
    table: "SQLTableMeta"
    join_type: Literal["inner", "full"] = "full"


class SQLTransformMeta(TransformMeta):
    def __init__(
        self,
        dbconn: DBConn,
        name: str,
        input_mts: Sequence[SQLMetaComputeInput],
        output_mts: Sequence[SQLTableMeta],
        transform_keys: Optional[List[str]],
        order_by: Optional[List[str]] = None,
        order: Literal["asc", "desc"] = "asc",
        create_table: bool = False,
    ) -> None:
        self.dbconn = dbconn
        self.name = name

        self.input_mts = input_mts
        self.output_mts = output_mts

        self.primary_keys, self.primary_schema = compute_transform_schema(
            input_mts=self.input_mts,
            output_mts=self.output_mts,
            transform_keys=transform_keys,
        )

        self.sql_schema = [i._copy() for i in self.primary_schema + TRANSFORM_META_SCHEMA]

        self.sql_table = sa.Table(
            name,
            dbconn.sqla_metadata,
            *self.sql_schema,
            # TODO remove in 0.15 release
            keep_existing=True,
        )

        self.order_by = order_by
        self.order = order

        if create_table:
            self.sql_table.create(self.dbconn.con, checkfirst=True)

    # TODO extract all complex logic into .create classmethod, make constructor simple
    def __reduce__(self) -> Tuple[Any, ...]:
        return self.__class__, (
            self.dbconn,
            self.name,
            self.input_mts,
            self.output_mts,
            self.primary_keys,
            self.order_by,
            self.order,
        )

    def get_changed_idx_count(
        self,
        ds: "DataStore",
        run_config: Optional[RunConfig] = None,
    ) -> int:
        _, sql = self._build_changed_idx_sql(ds=ds, run_config=run_config)

        with ds.meta_dbconn.con.begin() as con:
            idx_count = con.execute(
                sa.select(*[sa.func.count()]).select_from(sa.alias(sql.subquery(), name="union_select"))
            ).scalar()

        return cast(int, idx_count)

    def get_full_process_ids(
        self,
        ds: "DataStore",
        chunk_size: int,
        run_config: Optional[RunConfig] = None,
    ) -> Tuple[int, Iterable[IndexDF]]:
        with tracer.start_as_current_span("compute ids to process"):
            if len(self.input_mts) == 0:
                return (0, iter([]))

            idx_count = self.get_changed_idx_count(
                ds=ds,
                run_config=run_config,
            )

            join_keys, u1 = self._build_changed_idx_sql(
                ds=ds,
                run_config=run_config,
                order_by=self.order_by,
                order=self.order,  # type: ignore
            )

            # Список ключей из фильтров, которые нужно добавить в результат
            extra_filters: LabelDict
            if run_config is not None:
                extra_filters = {k: v for k, v in run_config.filters.items() if k not in join_keys}
            else:
                extra_filters = {}

            def alter_res_df():
                with ds.meta_dbconn.con.begin() as con:
                    for df in pd.read_sql_query(u1, con=con, chunksize=chunk_size):
                        assert isinstance(df, pd.DataFrame)
                        df = df[self.primary_keys]

                        for k, v in extra_filters.items():
                            df[k] = v

                        yield cast(IndexDF, df)

            return math.ceil(idx_count / chunk_size), alter_res_df()

    def get_change_list_process_ids(
        self,
        ds: "DataStore",
        change_list: ChangeList,
        chunk_size: int,
        run_config: Optional[RunConfig] = None,
    ) -> Tuple[int, Iterable[IndexDF]]:
        with tracer.start_as_current_span("compute ids to process"):
            changes = [pd.DataFrame(columns=self.primary_keys)]

            for inp in self.input_mts:
                if inp.table.name in change_list.changes:
                    idx = change_list.changes[inp.table.name]
                    if any([key not in idx.columns for key in self.primary_keys]):
                        # TODO пересмотреть эту логику, выглядит избыточной
                        # (возможно, достаточно посчитать один раз для всех
                        # input таблиц)
                        _, sql = self._build_changed_idx_sql(
                            ds=ds,
                            filters_idx=idx,
                            run_config=run_config,
                        )
                        with ds.meta_dbconn.con.begin() as con:
                            table_changes_df = pd.read_sql_query(
                                sql,
                                con=con,
                            )
                            table_changes_df = table_changes_df[self.primary_keys]

                        changes.append(table_changes_df)
                    else:
                        changes.append(data_to_index(idx, self.primary_keys))

            idx_df = pd.concat(changes).drop_duplicates(subset=self.primary_keys)
            idx = IndexDF(idx_df[self.primary_keys])

            chunk_count = math.ceil(len(idx) / chunk_size)

            def gen():
                for i in range(chunk_count):
                    yield cast(IndexDF, idx[i * chunk_size : (i + 1) * chunk_size])

            return chunk_count, gen()

    def insert_rows(
        self,
        idx: IndexDF,
    ) -> None:
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

    def _build_changed_idx_sql(
        self,
        ds: "DataStore",
        filters_idx: Optional[IndexDF] = None,
        order_by: Optional[List[str]] = None,
        order: Literal["asc", "desc"] = "asc",
        run_config: Optional[RunConfig] = None,  # TODO remove
    ) -> Tuple[Iterable[str], Any]:
        all_input_keys_counts: Dict[str, int] = {}
        for col in itertools.chain(*[inp.table.primary_schema for inp in self.input_mts]):
            all_input_keys_counts[col.name] = all_input_keys_counts.get(col.name, 0) + 1

        inp_ctes = []
        for inp in self.input_mts:
            keys, cte = inp.table.get_agg_cte(
                transform_keys=self.primary_keys,
                filters_idx=filters_idx,
                run_config=run_config,
            )
            inp_ctes.append(ComputeInputCTE(cte=cte, keys=keys, join_type=inp.join_type))

        agg_of_aggs = _make_agg_of_agg(
            ds=ds,
            transform_keys=self.primary_keys,
            ctes=inp_ctes,
            agg_col="update_ts",
        )

        tr_tbl = self.sql_table
        out: Any = (
            sa.select(
                *[sa.column(k) for k in self.primary_keys]
                + [tr_tbl.c.process_ts, tr_tbl.c.priority, tr_tbl.c.is_success]
            )
            .select_from(tr_tbl)
            .group_by(*[sa.column(k) for k in self.primary_keys])
        )

        out = sql_apply_filters_idx_to_subquery(out, self.primary_keys, filters_idx)

        out = out.cte(name="transform")

        if len(self.primary_keys) == 0:
            join_onclause_sql: Any = sa.literal(True)
        elif len(self.primary_keys) == 1:
            join_onclause_sql = agg_of_aggs.c[self.primary_keys[0]] == out.c[self.primary_keys[0]]
        else:  # len(transform_keys) > 1:
            join_onclause_sql = sa.and_(*[agg_of_aggs.c[key] == out.c[key] for key in self.primary_keys])

        sql = (
            sa.select(
                # Нам нужно выбирать хотя бы что-то, чтобы не было ошибки при
                # пустом transform_keys
                sa.literal(1).label("_datapipe_dummy"),
                *[sa.func.coalesce(agg_of_aggs.c[key], out.c[key]).label(key) for key in self.primary_keys],
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
                *[sa.column(k) for k in self.primary_keys],
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
        return (self.primary_keys, sql)


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


def compute_transform_schema(
    input_mts: Sequence[SQLMetaComputeInput],
    output_mts: Sequence[SQLTableMeta],
    transform_keys: Optional[List[str]],
) -> Tuple[List[str], MetaSchema]:
    # Hacky way to collect all the primary keys into a single set. Possible
    # problem that is not handled here is that theres a possibility that the
    # same key is defined differently in different input tables.
    all_keys = {
        col.name: col
        for col in itertools.chain(
            *([inp.table.primary_schema for inp in input_mts] + [dt.primary_schema for dt in output_mts])
        )
    }

    if transform_keys is not None:
        return (transform_keys, [all_keys[k] for k in transform_keys])

    assert len(input_mts) > 0

    inp_p_keys = set.intersection(*[set(inp.table.primary_keys) for inp in input_mts])
    assert len(inp_p_keys) > 0

    if len(output_mts) == 0:
        return (list(inp_p_keys), [all_keys[k] for k in inp_p_keys])

    out_p_keys = set.intersection(*[set(out.primary_keys) for out in output_mts])
    assert len(out_p_keys) > 0

    inp_out_p_keys = set.intersection(inp_p_keys, out_p_keys)
    assert len(inp_out_p_keys) > 0

    return (list(inp_out_p_keys), [all_keys[k] for k in inp_out_p_keys])


class SQLMetaPlane(MetaPlane):
    def __init__(self, dbconn: DBConn, create_meta_table: bool = False) -> None:
        self.dbconn = dbconn
        self.create_meta_table = create_meta_table

    def create_table_meta(
        self,
        name: str,
        primary_schema: DataSchema,
        meta_schema: MetaSchema = [],
    ) -> TableMeta:
        return SQLTableMeta(
            dbconn=self.dbconn,
            name=name,
            primary_schema=primary_schema,
            meta_schema=meta_schema,
            create_table=self.create_meta_table,
        )

    def create_transform_meta(
        self,
        name: str,
        input_dts: Sequence[ComputeInput],
        output_dts: Sequence[DataTable],
        transform_keys: Optional[List[str]] = None,
        order_by: Optional[List[str]] = None,
        order: Literal["asc", "desc"] = "asc",
    ) -> TransformMeta:
        input_mts = []
        for inp in input_dts:
            assert isinstance(inp.dt.meta, SQLTableMeta)
            input_mts.append(
                SQLMetaComputeInput(
                    table=inp.dt.meta,
                    join_type=inp.join_type,
                )
            )

        output_mts = []
        for out in output_dts:
            assert isinstance(out.meta, SQLTableMeta)
            output_mts.append(out.meta)

        return SQLTransformMeta(
            dbconn=self.dbconn,
            name=name,
            input_mts=input_mts,
            output_mts=output_mts,
            transform_keys=transform_keys,
            order_by=order_by,
            order=order,
            create_table=self.create_meta_table,
        )

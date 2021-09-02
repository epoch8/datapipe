from dataclasses import dataclass
from typing import List, Tuple, Optional, Dict, Union, Iterator

import logging
import time

from sqlalchemy.sql.expression import and_, bindparam, or_, select, update
from sqlalchemy import Table, Column, Numeric, Integer, Float, func, union, alias, delete

import pandas as pd

from datapipe.store.types import Index, ChunkMeta, DataSchema
from datapipe.store.database import DBConn, sql_schema_to_dtype
from datapipe.event_logger import EventLogger


logger = logging.getLogger('datapipe.metastore')

METADATA_SQL_SCHEMA = [
    Column('hash', Numeric),
    Column('create_ts', Float),   # Время создания строки
    Column('update_ts', Float),   # Время последнего изменения
    Column('process_ts', Float),  # Время последней успешной обработки
    Column('delete_ts', Float),   # Время удаления
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
        event_logger: EventLogger
    ):
        self.dbconn = dbconn
        self.name = name
        self.event_logger = event_logger
        self.primary_keys = [column.name for column in primary_schema]

        for item in primary_schema:
            item.primary_key = True

        sql_schema = primary_schema + METADATA_SQL_SCHEMA

        self.sql_schema = [i.copy() for i in sql_schema]

        self.sql_table = Table(
            f'{self.name}_meta',
            self.dbconn.sqla_metadata,
            *self.sql_schema,
        )

        self.sql_table.create(self.dbconn.con, checkfirst=True)

    def get_metadata(self, idx: Optional[Index] = None) -> pd.DataFrame:
        sql = select(self.sql_schema)

        if idx is not None:
            row_queries = []

            for _, row in idx.iterrows():
                and_params = [self.sql_table.c[key] == self._get_sql_param(row[key]) for key in self.primary_keys]
                and_query = and_(*and_params)
                row_queries.append(and_query)

            sql = sql.where(or_(*row_queries))

        return pd.read_sql_query(
            sql,
            con=self.dbconn.con
        )

    def _make_new_metadata_df(self, now, df) -> pd.DataFrame:
        res_df = df[self.primary_keys]

        res_df['hash'] = self._get_hash_for_df(df)
        res_df['create_ts'] = now
        res_df['update_ts'] = now
        res_df['process_ts'] = now
        res_df['delete_ts'] = None

        return res_df

    def _get_hash_for_df(self, df) -> pd.DataFrame:
        return pd.util.hash_pandas_object(df.apply(lambda x: str(list(x)), axis=1))

    # Fix numpy types in Index
    def _get_sql_param(self, param):
        return param.item() if hasattr(param, "item") else param

    def get_existing_idx(self, idx: Index = None) -> Index:
        sql = select(self.sql_schema)

        if idx is not None:
            idx_cols = list(set(idx.columns) & set(self.primary_keys))

            if not idx_cols:
                raise ValueError("Index does not contain any primary key ")

            row_queries = []

            for _, row in idx.iterrows():
                and_params = [self.sql_table.c[key] == self._get_sql_param(row[key]) for key in idx_cols]
                and_query = and_(*and_params)
                row_queries.append(and_query)

            sql = sql.where(or_(*row_queries))

        sql = sql.where(self.sql_table.c.delete_ts == None)
        res_df = pd.read_sql_query(
            sql,
            con=self.dbconn.con,
        )

        return res_df[self.primary_keys]

    def get_table_debug_info(self, name: str) -> TableDebugInfo:
        return TableDebugInfo(
            name=name,
            size=self.dbconn.con.execute(select([func.count()]).select_from(self.sql_table)).fetchone()[0]
        )

    # TODO Может быть переделать работу с метадатой на контекстный менеджер?
    def get_changes_for_store_chunk(
        self,
        data_df: pd.DataFrame,
        now: float = None
    ) -> Tuple[pd.Index, pd.Index, pd.DataFrame]:
        '''
        Returns:
            new_df, changed_df, new_meta_df, changed_meta_df
        '''

        if now is None:
            now = time.time()

        # получить meta по чанку
        existing_meta_df = self.get_metadata(data_df)

        # Дополняем данные методанными
        merged_df = pd.merge(data_df, existing_meta_df,  how='left', left_on=self.primary_keys, right_on=self.primary_keys)
        merged_df['data_hash'] = self._get_hash_for_df(data_df)

        # Ищем новые записи
        new_df = data_df[(merged_df['hash'].isna()) | (merged_df['delete_ts'].notnull())]

        # Создаем мета данные для новых записей
        new_meta_data_df = data_df[(merged_df['hash'].isna())]
        new_meta_df = self._make_new_metadata_df(now, new_meta_data_df)

        # Ищем изменившиеся записи
        changed_idx = (merged_df['hash'].notna()) & \
            (merged_df['delete_ts'].isnull()) & \
            (merged_df['hash'] != merged_df['data_hash'])
        changed_df = data_df[changed_idx]

        # Меняем мета данные для существующих записей
        changed_meta_idx = (merged_df['hash'].notna()) & (merged_df['hash'] != merged_df['data_hash']) | (merged_df['delete_ts'].notnull())
        changed_meta_df = merged_df[merged_df['hash'].notna()]

        changed_meta_df.loc[changed_meta_idx, 'update_ts'] = now
        changed_meta_df['process_ts'] = now
        changed_meta_df['delete_ts'] = None
        changed_meta_df['hash'] = changed_meta_df['data_hash']

        if len(new_df.index) > 0 or len(changed_idx) > 0:
            self.event_logger.log_state(
                self.name, added_count=len(new_df.index), updated_count=len(changed_idx), deleted_count=0
            )

        return new_df, changed_df, new_meta_df, changed_meta_df

    def _delete_rows(self, idx: pd.Index) -> None:
        if len(idx) > 0:
            logger.debug(f'Deleting {len(idx.index)} rows from {self.name} data')

            now = time.time()
            meta_df = self.get_metadata(idx)

            meta_df["delete_ts"] = now
            meta_df["process_ts"] = now

            self._update_existing_rows(meta_df)

    def _insert_rows(self, df: pd.DataFrame) -> None:
        if len(df) > 0:
            logger.debug(f'Inserting {len(df)} rows into {self.name} data')

            df.to_sql(
                name=self.sql_table.name,
                con=self.dbconn.con,
                schema=self.dbconn.schema,
                if_exists='append',
                index=False,
                chunksize=1000,
                method='multi',
                dtype=sql_schema_to_dtype(self.sql_schema),
            )

    def _update_existing_rows(self, df: pd.DataFrame) -> None:
        if len(df) > 0:
            stmt = (
                update(self.sql_table)
                .values({
                    'hash': bindparam('b_hash'),
                    'update_ts': bindparam('b_update_ts'),
                    'process_ts': bindparam('b_process_ts'),
                    'delete_ts': bindparam('b_delete_ts'),
                })
            )

            for key in self.primary_keys:
                stmt = stmt.where(self.sql_table.c[key] == bindparam(f'b_{key}'))

            columns = self.primary_keys + ['hash', 'update_ts', 'process_ts', 'delete_ts']

            self.dbconn.con.execute(
                stmt,
                [
                    {f'b_{k}': v for k, v in row.items()}
                    for row in
                    df.reset_index()[columns]
                    .to_dict(orient='records')
                ]
            )

    """
    def _update_rows(self, df: pd.DataFrame) -> None:
        existing_idx = self.get_existing_idx(df.index)

        missing_idx = df.index.difference(existing_idx)

        self._update_existing_rows(df.loc[existing_idx])
        self._insert_rows(df.loc[missing_idx])
    """
    def insert_meta_for_store_chunk(self, new_meta_df: pd.DataFrame) -> None:
        if len(new_meta_df) > 0:
            self._insert_rows(new_meta_df)

    def update_meta_for_store_chunk(self, changed_meta_df: pd.DataFrame) -> None:
        if len(changed_meta_df) > 0:
            self._update_existing_rows(changed_meta_df)

    def update_meta_for_sync_meta(self, deleted_idx: pd.Index) -> None:
        if len(deleted_idx) > 0:
            self._delete_rows(deleted_idx)

    def get_changes_for_sync_meta(self, chunks: List[ChunkMeta], processed_idx: pd.Index = None) -> pd.Index:
        idx = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame(columns=self.primary_keys)
        existing_idx = self.get_existing_idx(processed_idx)

        idx['exist'] = True

        merged_df = pd.merge(existing_idx, idx,  how='left', left_on=self.primary_keys, right_on=self.primary_keys)
        deleted_df = merged_df[merged_df['exist'].isna()]

        if len(deleted_df.index) > 0:
            # TODO вынести в compute
            self.event_logger.log_state(self.name, added_count=0, updated_count=0, deleted_count=len(deleted_df.index))

        return deleted_df[self.primary_keys]

    def get_stale_idx(self, process_ts: float) -> Iterator[pd.DataFrame]:
        idx_cols = [self.sql_table.c[key] for key in self.primary_keys]
        sql = select(idx_cols).where(
            and_(
                self.sql_table.c.process_ts < process_ts,
                self.sql_table.c.delete_ts.is_(None)
            )
        )

        return pd.read_sql_query(
            sql,
            con=self.dbconn.con,
            chunksize=1000
        )


class MetaStore:
    def __init__(self, dbconn: Union[str, DBConn]) -> None:
        if isinstance(dbconn, str):
            self.dbconn = DBConn(dbconn)
        else:
            self.dbconn = dbconn
        self.event_logger = EventLogger(self.dbconn)

        self.meta_tables: Dict[str, MetaTable] = {}

    def create_meta_table(self, name: str, primary_schema: DataSchema) -> MetaTable:
        assert(name not in self.meta_tables)

        res = MetaTable(
            dbconn=self.dbconn,
            name=name,
            primary_schema=primary_schema,
            event_logger=self.event_logger,
        )

        self.meta_tables[name] = res

        return res

    def get_process_ids(
        self,
        inputs: List[MetaTable],
        outputs: List[MetaTable],
        chunksize: int = 1000,
    ) -> Tuple[int, Iterator[pd.DataFrame]]:
        '''
        Метод для получения перечня индексов для обработки.

        Returns: (idx_size, iterator<idx_df>)
            idx_size - количество индексов требующих обработки
            idx_df - датафрейм без колонок с данными, только индексная колонка
        '''

        if len(inputs) == 0:
            return (0, iter([]))

        inp_p_keys = [set(inp.primary_keys) for inp in inputs]
        out_p_keys = [set(out.primary_keys) for out in outputs]
        join_keys = set.intersection(*inp_p_keys, *out_p_keys)

        if not join_keys:
            raise ValueError("Impossible to carry out transformation. datatables do not contain intersecting ids")

        def left_join(tbl_a, tbl_bbb):
            q = tbl_a.join(
                tbl_bbb[0],
                and_(tbl_a.c[key] == tbl_bbb[0].c[key] for key in join_keys),
                isouter=True
            )
            for tbl_b in tbl_bbb[1:]:
                q = q.join(
                    tbl_b,
                    and_(tbl_a.c[key] == tbl_b.c[key] for key in join_keys),
                    isouter=True
                )

            return q

        inp_tbls = [inp.sql_table.alias(f"inp_{inp.sql_table.name}") for inp in inputs]
        out_tbls = [out.sql_table.alias(f"out_{out.sql_table.name}") for out in outputs]
        sql_requests = []

        for inp in inp_tbls:
            fields = [inp.c[key] for key in join_keys]
            sql = select(fields).select_from(
                left_join(
                    inp,
                    out_tbls
                )
            ).where(
                or_(
                    and_(
                        or_(
                            (
                                out.c.process_ts
                                <
                                inp.c.update_ts
                            ),
                            out.c.process_ts.is_(None)
                        ),
                        inp.c.delete_ts.is_(None)
                    )
                    for out in out_tbls
                )
            )

            sql_requests.append(sql)

        for out in out_tbls:
            fields = [out.c[key] for key in join_keys]
            sql = select(fields).select_from(
                left_join(
                    out,
                    inp_tbls
                )
            ).where(
                and_(
                    or_(
                        (
                            out.c.process_ts
                            <
                            inp.c.delete_ts
                        ),
                        inp.c.create_ts.is_(None)
                    )
                    for inp in inp_tbls
                )
            )

            sql_requests.append(sql)

        u1 = union(*sql_requests)

        idx_count = self.dbconn.con.execute(
            select([func.count()])
            .select_from(
                alias(u1, name='union_select')
            )
        ).scalar()

        return idx_count, pd.read_sql_query(
            u1,
            con=self.dbconn.con,
            chunksize=chunksize
        )

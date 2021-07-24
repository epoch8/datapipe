from dataclasses import dataclass
from typing import List, Tuple, Optional, Dict, Union, Iterator

import logging
import time

from sqlalchemy.sql.expression import and_, or_, select
from sqlalchemy import Table, Column, Numeric, Float, String, func, union, alias, delete

import pandas as pd

from datapipe.store.types import Index, ChunkMeta
from datapipe.store.database import DBConn, sql_schema_to_dtype
from datapipe.event_logger import EventLogger


logger = logging.getLogger('datapipe.metastore')

METADATA_SQL_SCHEMA = [
    Column('id', String(100), primary_key=True),
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
    def __init__(self, dbconn: DBConn, name: str, event_logger: EventLogger):
        self.dbconn = dbconn
        self.name = name
        self.event_logger = event_logger

        self.sql_schema = [i.copy() for i in METADATA_SQL_SCHEMA]

        self.sql_table = Table(
            f'{self.name}_meta',
            self.dbconn.sqla_metadata,
            *self.sql_schema,
        )

        self.sql_table.create(self.dbconn.con, checkfirst=True)

    def get_metadata(self, idx: Optional[Index] = None) -> pd.DataFrame:
        sql = select(self.sql_schema)

        if idx is not None:
            sql = sql.where(self.sql_table.c.id.in_(list(idx)))

        return pd.read_sql_query(
            sql,
            con=self.dbconn.con,
            index_col='id',
        )

    def _make_new_metadata_df(self, now, df) -> pd.DataFrame:
        # data

        return pd.DataFrame(
            {
                'hash': pd.util.hash_pandas_object(df.apply(lambda x: str(list(x)), axis=1)),
                'create_ts': now,
                'update_ts': now,
                'process_ts': now,
                'delete_ts': None,
            },
            index=df.index
        )

    def get_existing_idx(self, idx: Index = None) -> Index:
        return self.get_metadata(idx).index

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
            new_idx, changed_idx, new_meta_df
        '''

        if now is None:
            now = time.time()

        # получить meta по чанку
        existing_meta_df = self.get_metadata(data_df.index)

        # найти что изменилось
        new_meta_df = self._make_new_metadata_df(now, data_df)

        common_idx = existing_meta_df.index.intersection(new_meta_df.index)
        changed_idx = common_idx[new_meta_df.loc[common_idx, 'hash'] != existing_meta_df.loc[common_idx, 'hash']]

        # найти что добавилось
        new_idx = new_meta_df.index.difference(existing_meta_df.index)

        # обновить метаданные (удалить и записать всю new_meta_df, потому что изменился processed_ts)
        if len(new_meta_df) > 0:
            not_changed_idx = existing_meta_df.index.difference(changed_idx)

            new_meta_df.loc[changed_idx, 'create_ts'] = existing_meta_df.loc[changed_idx, 'create_ts']
            new_meta_df.loc[not_changed_idx, 'update_ts'] = existing_meta_df.loc[not_changed_idx, 'update_ts']

        # TODO вынести в compute
        if len(new_idx) > 0 or len(changed_idx) > 0:
            self.event_logger.log_state(
                self.name, added_count=len(new_idx), updated_count=len(changed_idx), deleted_count=0
            )

        return new_idx, changed_idx, new_meta_df

    def _delete_rows(self, idx: pd.Index) -> None:
        if len(idx) > 0:
            logger.debug(f'Deleting {len(idx)} rows from {self.name} data')

            sql = delete(self.sql_table).where(
                self.sql_table.c.id.in_(list(idx))
            )

            self.dbconn.con.execute(sql)

    def _insert_rows(self, df: pd.DataFrame) -> None:
        if len(df) > 0:
            logger.debug(f'Inserting {len(df)} rows into {self.name} data')

            df.to_sql(
                name=self.sql_table.name,
                con=self.dbconn.con,
                schema=self.dbconn.schema,
                if_exists='append',
                index_label='id',
                chunksize=1000,
                method='multi',
                dtype=sql_schema_to_dtype(self.sql_schema),
            )

    def _update_rows(self, df: pd.DataFrame) -> None:
        # FIXME implement proper update
        self._delete_rows(df.index)
        self._insert_rows(df)

    def update_meta_for_store_chunk(self, new_meta_df: pd.DataFrame) -> None:
        if len(new_meta_df) > 0:
            self._update_rows(new_meta_df)

    def update_meta_for_sync_meta(self, deleted_idx: pd.Index) -> None:
        if len(deleted_idx) > 0:
            self._delete_rows(deleted_idx)

    def get_changes_for_sync_meta(self, chunks: List[ChunkMeta], processed_idx: pd.Index = None) -> pd.Index:
        idx = pd.Index([])
        for chunk in chunks:
            idx = idx.union(chunk)

        existing_meta_df = self.get_metadata(idx=processed_idx)

        deleted_idx = existing_meta_df.index.difference(idx)

        if len(deleted_idx) > 0:
            # TODO вынести в compute
            self.event_logger.log_state(self.name, added_count=0, updated_count=0, deleted_count=len(deleted_idx))

        return deleted_idx

    def get_stale_idx(self, process_ts: float) -> Iterator[pd.DataFrame]:
        return pd.read_sql_query(
            select([self.sql_table.c.id]).where(
                self.sql_table.c.process_ts < process_ts
            ),
            con=self.dbconn.con,
            index_col='id',
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

    def create_meta_table(self, name: str) -> MetaTable:
        assert(name not in self.meta_tables)

        res = MetaTable(
            dbconn=self.dbconn,
            name=name,
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

        def left_join(tbl_a, tbl_bbb):
            q = tbl_a.join(
                tbl_bbb[0],
                tbl_a.c.id == tbl_bbb[0].c.id,
                isouter=True
            )
            for tbl_b in tbl_bbb[1:]:
                q = q.join(
                    tbl_b,
                    tbl_a.c.id == tbl_b.c.id,
                    isouter=True
                )

            return q

        sql_requests = []

        for inp in inputs:
            sql = select([inp.sql_table.c.id]).select_from(
                left_join(
                    inp.sql_table,
                    [out.sql_table for out in outputs]
                )
            ).where(
                or_(
                    or_(
                        (
                            out.sql_table.c.process_ts
                            <
                            inp.sql_table.c.update_ts
                        ),
                        out.sql_table.c.process_ts.is_(None)
                    )
                    for out in outputs
                )
            )

            sql_requests.append(sql)

        for out in outputs:
            sql = select([out.sql_table.c.id]).select_from(
                left_join(
                    out.sql_table,
                    [inp.sql_table for inp in inputs]
                )
            ).where(
                and_(inp.sql_table.c.id.is_(None) for inp in inputs)
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
            index_col='id',
            chunksize=chunksize
        )

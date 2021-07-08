from dataclasses import dataclass
from typing import Iterator, List, Tuple, Optional, Dict, Union

import logging
import time
import typing

from sqlalchemy.sql.expression import and_, or_, select
from sqlalchemy import Column, Numeric, Float, func, String
import pandas as pd

from datapipe.store.types import Index, ChunkMeta
from datapipe.store.database import TableStoreDB, DBConn
from datapipe.event_logger import EventLogger

if typing.TYPE_CHECKING:
    from datapipe.datatable import DataTable


logger = logging.getLogger('datapipe.metastore')


COMMON_METADATA_SQL_SCHEMA = [
    Column('_hash', Numeric),
    Column('_create_ts', Float),   # Время создания строки
    Column('_update_ts', Float),   # Время последнего изменения
    Column('_process_ts', Float),  # Время последней успешной обработки
    Column('_delete_ts', Float),   # Время удаления
]


@dataclass
class TableDebugInfo:
    name: str
    size: int


@dataclass
class MetaTableChangeData:
    meta_df: pd.DataFrame  # Полный датафрейм для референса
    changed_meta_df: pd.DataFrame  # Измененные объекты
    deleted_meta_df: pd.DataFrame  # Удаленные объекты


class MetaTable:
    def __init__(self, dbconn: DBConn, name: str, index_columns: List[str], event_logger: EventLogger):
        self.dbconn = dbconn
        self.name = name
        self.event_logger = event_logger

        self.index_columns = index_columns

        index_columns_sqla = [
            Column(col_name, String(100), index=True)
            for col_name in index_columns
        ]

        self.store = TableStoreDB(
            dbconn=self.dbconn,
            name=f'{name}_meta',
            data_sql_schema=COMMON_METADATA_SQL_SCHEMA + index_columns_sqla,
            create_table=True,
            index_columns=index_columns,
        )

    def get_metadata(self, idx: Optional[Index] = None) -> pd.DataFrame:
        return self.store.read_rows(idx)

    def _make_new_metadata_df(self, now, df) -> pd.DataFrame:
        # data

        meta_df = df[self.index_columns]
        meta_df['_hash'] = pd.util.hash_pandas_object(df.apply(lambda x: str(list(x)), axis=1))
        meta_df['_create_ts'] = now
        meta_df['_update_ts'] = now
        meta_df['_process_ts'] = now
        meta_df['_delete_ts'] = None

        return meta_df

    def get_existing_idx(self, idx: Index = None) -> Index:
        return self.get_metadata(idx).index

    def get_table_debug_info(self, name: str) -> TableDebugInfo:
        return TableDebugInfo(
            name=name,
            size=self.dbconn.con.execute(select([func.count()]).select_from(self.store.data_table)).fetchone()[0]
        )

    # TODO Может быть переделать работу с метадатой на контекстный менеджер?
    def get_changes_for_store_chunk(
        self,
        data_df: pd.DataFrame,
        now: float = None
    ) -> Tuple[pd.Index, pd.Index, pd.DataFrame]:
        if now is None:
            now = time.time()

        # получить meta по чанку
        existing_meta_df = self.get_metadata(data_df.index)

        # найти что изменилось
        new_meta_df = self._make_new_metadata_df(now, data_df)

        common_idx = existing_meta_df.index.intersection(new_meta_df.index)
        changed_idx = common_idx[new_meta_df.loc[common_idx, '_hash'] != existing_meta_df.loc[common_idx, '_hash']]

        # найти что добавилось
        new_idx = new_meta_df.index.difference(existing_meta_df.index)

        # обновить метаданные (удалить и записать всю new_meta_df, потому что изменился processed_ts)
        if len(new_meta_df) > 0:
            not_changed_idx = existing_meta_df.index.difference(changed_idx)

            new_meta_df.loc[changed_idx, '_create_ts'] = existing_meta_df.loc[changed_idx, '_create_ts']
            new_meta_df.loc[not_changed_idx, '_update_ts'] = existing_meta_df.loc[not_changed_idx, '_update_ts']

        if len(new_idx) > 0 or len(changed_idx) > 0:
            self.event_logger.log_event(
                self.name, added_count=len(new_idx), updated_count=len(changed_idx), deleted_count=0
            )

        return new_idx, changed_idx, new_meta_df

    def update_meta_for_store_chunk(self, new_meta_df: pd.DataFrame) -> None:
        if len(new_meta_df) > 0:
            self.store.update_rows(new_meta_df)

    def get_changes_for_sync_meta(self, chunks: List[ChunkMeta], processed_idx: pd.Index = None) -> pd.Index:
        idx = pd.Index([])
        for chunk in chunks:
            idx = idx.union(chunk)

        existing_meta_df = self.get_metadata(idx=processed_idx)

        deleted_idx = existing_meta_df.index.difference(idx)

        if len(deleted_idx) > 0:
            self.event_logger.log_event(self.name, added_count=0, updated_count=0, deleted_count=len(deleted_idx))

        return deleted_idx

    def get_stale_idx(self, process_ts: float) -> pd.DataFrame:
        return pd.read_sql_query(
            select([self.store.data_table.c.id]).where(
                self.store.data_table.c.process_ts < process_ts
            ),
            con=self.store.dbconn.con,
            index_col='id',
            chunksize=1000
        )

    def update_meta_for_sync_meta(self, deleted_idx: pd.Index) -> None:
        if len(deleted_idx) > 0:
            self.store.delete_rows(deleted_idx)


class MetaStore:
    '''
    MetaStore - основной класс для работы с метаданными.

    Идея такая: мета-таблица, это таблица с мета-данными, на основании которых
    происходят все джоины и индексация данных.
    '''

    def __init__(self, dbconn: Union[str, DBConn]) -> None:
        if isinstance(dbconn, str):
            self.dbconn = DBConn(dbconn)
        else:
            self.dbconn = dbconn
        self.event_logger = EventLogger(self.dbconn)

        self.meta_tables: Dict[str, MetaTable] = {}

    def create_meta_table(self, name: str, index_columns: List[str]) -> MetaTable:
        assert(name not in self.meta_tables)

        res = MetaTable(
            dbconn=self.dbconn,
            name=name,
            index_columns=index_columns,
            event_logger=self.event_logger,
        )

        self.meta_tables[name] = res

        return res

    def get_process_ids(
        self,
        inputs: List[MetaTable],
        outputs: List[MetaTable],
    ) -> pd.Index:
        if len(inputs) == 0:
            return pd.Index([])

        idx = None

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

        for inp in inputs:
            sql = select([inp.store.data_table.c.id]).select_from(
                left_join(
                    inp.store.data_table,
                    [out.store.data_table for out in outputs]
                )
            ).where(
                or_(
                    or_(
                        (
                            out.store.data_table.c.process_ts
                            <
                            inp.store.data_table.c.update_ts
                        ),
                        out.store.data_table.c.process_ts.is_(None)
                    )
                    for out in outputs
                )
            )

            idx_df = pd.read_sql_query(
                sql,
                con=self.dbconn.con,
                index_col='id',
            )

            if idx is None:
                idx = idx_df.index
            else:
                idx = idx.union(idx_df.index)

        for out in outputs:
            sql = select([out.store.data_table.c.id]).select_from(
                left_join(
                    out.store.data_table,
                    [inp.store.data_table for inp in inputs]
                )
            ).where(
                and_(inp.store.data_table.c.id.is_(None) for inp in inputs)
            )

            idx_df = pd.read_sql_query(
                sql,
                con=self.dbconn.con,
                index_col='id',
            )

            if idx is None:
                idx = idx_df.index
            else:
                idx = idx.union(idx_df.index)

        return idx

    # TODO унести в DataTable
    def get_process_chunks(
        self,
        inputs: List['DataTable'],
        outputs: List['DataTable'],
        chunksize: int = 1000,
    ) -> Tuple[pd.Index, Iterator[List[pd.DataFrame]]]:
        idx = self.get_process_ids(
            inputs=[i.meta_table for i in inputs],
            outputs=[i.meta_table for i in outputs],
        )

        logger.info(f'Items to update {len(idx)}')

        def gen():
            if len(idx) > 0:
                for i in range(0, len(idx), chunksize):
                    yield [inp.get_data(idx[i:i+chunksize]) for inp in inputs]

        return idx, gen()

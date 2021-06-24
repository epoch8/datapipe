from dataclasses import dataclass
from typing import Iterator, List, Tuple, Optional, Dict, Union

import logging
import time

from sqlalchemy.sql.expression import and_, or_, select
from sqlalchemy import Column, Numeric, Float, func
import pandas as pd

from datapipe.store.types import Index, ChunkMeta
from datapipe.datatable import DataTable
from datapipe.store.database import TableStoreDB, DBConn
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


class MetaStore:
    def __init__(self, dbconn: Union[str, DBConn]) -> None:
        if isinstance(dbconn, str):
            self.dbconn = DBConn(dbconn)
        else:
            self.dbconn = dbconn
        self.event_logger = EventLogger(self.dbconn)

        self.meta_tables: Dict[str, TableStoreDB] = {}

    def get_meta_table(self, name: str) -> TableStoreDB:
        if name not in self.meta_tables:
            self.meta_tables[name] = TableStoreDB(
                self.dbconn,
                f'{name}_meta',
                METADATA_SQL_SCHEMA,
                create_table=True
            )

        return self.meta_tables[name]

    def get_metadata(self, name: str, idx: Optional[Index] = None) -> pd.DataFrame:
        return self.get_meta_table(name).read_rows(idx)

    def get_table_debug_info(self, name: str) -> TableDebugInfo:
        tbl = self.get_meta_table(name)

        return TableDebugInfo(
            name=name,
            size=self.dbconn.con.execute(select([func.count()]).select_from(tbl.data_table)).fetchone()[0]
        )

    def _make_new_metadata_df(self, now, df) -> pd.DataFrame:
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

    # TODO Может быть переделать работу с метадатой на контекстный менеджер?
    def get_changes_for_store_chunk(
        self,
        name: str,
        data_df: pd.DataFrame,
        now: float = None
    ) -> Tuple[pd.Index, pd.Index, pd.DataFrame]:
        if now is None:
            now = time.time()

        # получить meta по чанку
        existing_meta_df = self.get_metadata(name, data_df.index)

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

        if len(new_idx) > 0 or len(changed_idx) > 0:
            self.event_logger.log_event(
                name, added_count=len(new_idx), updated_count=len(changed_idx), deleted_count=0
            )

        return new_idx, changed_idx, new_meta_df

    def update_meta_for_store_chunk(self, name: str, new_meta_df: pd.DataFrame) -> None:
        if len(new_meta_df) > 0:
            self.get_meta_table(name).update_rows(new_meta_df)

    def get_changes_for_sync_meta(self, name: str, chunks: List[ChunkMeta], processed_idx: pd.Index = None) -> pd.Index:
        idx = pd.Index([])
        for chunk in chunks:
            idx = idx.union(chunk)

        existing_meta_df = self.get_metadata(name, idx=processed_idx)

        deleted_idx = existing_meta_df.index.difference(idx)

        if len(deleted_idx) > 0:
            self.event_logger.log_event(name, added_count=0, updated_count=0, deleted_count=len(deleted_idx))

        return deleted_idx

    def update_meta_for_sync_meta(self, name: str, deleted_idx: pd.Index) -> None:
        if len(deleted_idx) > 0:
            self.get_meta_table(name).delete_rows(deleted_idx)

    def get_process_ids(
        self,
        inputs: List[DataTable],
        outputs: List[DataTable],
        diff_chunksize: Union[int, None] = None
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
            sql = select([self.get_meta_table(inp.name).data_table.c.id]).select_from(
                left_join(
                    self.get_meta_table(inp.name).data_table,
                    [self.get_meta_table(out.name).data_table for out in outputs]
                )
            ).where(
                or_(
                    or_(
                        (
                            self.get_meta_table(out.name).data_table.c.process_ts
                            <
                            self.get_meta_table(inp.name).data_table.c.update_ts
                        ),
                        self.get_meta_table(out.name).data_table.c.process_ts.is_(None)
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
            sql = select([self.get_meta_table(out.name).data_table.c.id]).select_from(
                left_join(
                    self.get_meta_table(out.name).data_table,
                    [self.get_meta_table(inp.name).data_table for inp in inputs]
                )
            ).where(
                and_(self.get_meta_table(inp.name).data_table.c.id.is_(None) for inp in inputs)
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

    def get_process_chunks(
        self,
        inputs: List[DataTable],
        outputs: List[DataTable],
        chunksize: int = 1000,
        diff_chunksize: Union[int, None] = None
    ) -> Tuple[pd.Index, Iterator[List[pd.DataFrame]]]:
        idx = self.get_process_ids(
            inputs=inputs,
            outputs=outputs,
            diff_chunksize=diff_chunksize
        )

        if idx is not None and diff_chunksize is not None:
            logger.info(f'Total items need to be updated: {len(idx)}, chosen items to update: {diff_chunksize}')
            idx = idx[:diff_chunksize]
        else:
            logger.info(f'Items to update {len(idx)}')

        def gen():
            if len(idx) > 0:
                for i in range(0, len(idx), chunksize):
                    yield [inp.get_data(idx[i:i+chunksize]) for inp in inputs]

        return idx, gen()

import inspect
from typing import Callable, Generator, Iterator, List, Dict, Any, Optional, Tuple, Union
import logging
import time

from sqlalchemy.engine import Engine
from sqlalchemy.sql import text
from sqlalchemy import create_engine, MetaData, Table, Column, Float, String, Numeric
import pandas as pd
from sqlalchemy.sql.expression import delete, and_, or_, select

from c12n_pipe.event_logger import EventLogger

logger = logging.getLogger('c12n_pipe.datatable')


Index = Union[List, pd.Index]
ChunkMeta = Index

DataSchema = List[Column]


PRIMARY_KEY = [
    Column('id', String(100), primary_key=True),
]


METADATA_SQL_SCHEMA = [
    Column('hash', Numeric),
    Column('create_ts', Float),  # Время создания строки
    Column('update_ts', Float),  # Время последнего изменения
    Column('process_ts', Float), # Время последней успешной обработки
    Column('delete_ts', Float),  # Время удаления
]


def _sql_schema_to_dtype(schema: List[Column]) -> Dict[str, Any]:
    return {
        i.name: i.type for i in PRIMARY_KEY + schema
    }


class DataTable:
    def __init__(
        self,
        ds: 'DataStore',
        name: str,
        data_sql_schema: List[Column],
        create_tables: bool = True,
    ):
        self.ds = ds

        self.name = name
        self.data_sql_schema = data_sql_schema

        self.meta_table = Table(
            self.meta_table_name(), self.ds.sqla_metadata,
            *[i.copy() for i in PRIMARY_KEY + METADATA_SQL_SCHEMA]
        )

        self.data_table = Table(
            self.data_table_name(), self.ds.sqla_metadata,
            *[i.copy() for i in PRIMARY_KEY + self.data_sql_schema]
        )

        if create_tables:
            self.meta_table.create(self.ds.con, checkfirst=True)
            self.data_table.create(self.ds.con, checkfirst=True)

    def meta_table_name(self) -> str:
        return f'{self.name}_meta'

    def data_table_name(self) -> str:
        return f'{self.name}_data'

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

    def _make_deleted_meta_df(self, now, old_meta_df, deleted_idx) -> pd.DataFrame:
        res = old_meta_df.loc[deleted_idx]
        res.loc[:, 'delete_ts'] = now
        return res

    def load_all(self) -> pd.DataFrame:
        return pd.read_sql_table(
            table_name=self.data_table_name(),
            con=self.ds.con,
            schema=self.ds.schema,
            index_col='id',
        )

    def _delete_data(self, idx: Index) -> None:
        if len(idx) > 0:
            logger.info(f'Deleting {len(idx)} rows from {self.name} data')

            sql = delete(self.data_table).where(self.data_table.c.id.in_(list(idx)))
            self.ds.con.execute(sql)

    def _delete_metadata(self, idx: Index) -> None:
        if len(idx) > 0:
            logger.info(f'Deleting {len(idx)} rows from {self.name} metadata')

            sql = delete(self.meta_table).where(self.meta_table.c.id.in_(list(idx)))
            self.ds.con.execute(sql)

    def get_metadata(self, idx: Optional[Index] = None) -> pd.DataFrame:
        if idx is None:
            return pd.read_sql_query(
                select([self.meta_table]),
                con=self.ds.con,
                index_col='id',
            )

        else:
            return pd.read_sql_query(
                select([self.meta_table]).where(self.meta_table.c.id.in_(list(idx))),
                con=self.ds.con,
                index_col='id',
            )

    def get_data(self, idx: Optional[Index] = None) -> pd.DataFrame:
        if idx is None:
            return pd.read_sql_query(
                select([self.data_table]),
                con=self.ds.con,
                index_col='id',
            )
        else:
            return pd.read_sql_query(
                select([self.data_table]).where(self.data_table.c.id.in_(list(idx))),
                con=self.ds.con,
                index_col='id',
            )

    def store_chunk(self, data_df: pd.DataFrame, now: float = None) -> ChunkMeta:
        if now is None:
            now = time.time()

        logger.info(f'Inserting chunk {len(data_df)} rows into {self.name}')

        # получить meta по чанку
        existing_meta_df = self.get_metadata(data_df.index)

        # найти что изменилось
        new_meta_df = self._make_new_metadata_df(now, data_df)

        common_idx = existing_meta_df.index.intersection(new_meta_df.index)
        changed_idx = common_idx[new_meta_df.loc[common_idx, 'hash'] != existing_meta_df.loc[common_idx, 'hash']]

        # найти что добавилось
        new_idx = new_meta_df.index.difference(existing_meta_df.index)

        if len(new_idx) > 0 or len(changed_idx) > 0:
            self.ds.event_logger.log_event(
                self.name, added_count=len(new_idx), updated_count=len(changed_idx), deleted_count=0
            )

        # обновить данные (удалить только то, что изменилось, записать новое)
        to_write_idx = changed_idx.union(new_idx)
        self._delete_data(to_write_idx)

        if len(data_df.loc[to_write_idx]) > 0:
            data_df.loc[to_write_idx].to_sql(
                name=self.data_table_name(),
                con=self.ds.con,
                schema=self.ds.schema,
                if_exists='append',
                index_label='id',
                chunksize=1000,
                method='multi',
                dtype=_sql_schema_to_dtype(self.data_sql_schema),
            )

        # обновить метаданные (удалить и записать всю new_meta_df, потому что изменился processed_ts)

        if len(new_meta_df) > 0:
            self._delete_metadata(new_meta_df.index)

            not_changed_idx = existing_meta_df.index.difference(changed_idx)

            new_meta_df.loc[changed_idx, 'create_ts'] = existing_meta_df.loc[changed_idx, 'create_ts']
            new_meta_df.loc[not_changed_idx, 'update_ts'] = existing_meta_df.loc[not_changed_idx, 'update_ts']

            new_meta_df.to_sql(
                name=self.meta_table_name(),
                con=self.ds.con,
                schema=self.ds.schema,
                if_exists='append',
                index_label='id',
                chunksize=1000,
                method='multi',
                dtype=_sql_schema_to_dtype(METADATA_SQL_SCHEMA),
            )

        return data_df.index

    def sync_meta(self, chunks: List[ChunkMeta], processed_idx: pd.Index = None) -> None:
        ''' Пометить удаленными объекты, которых больше нет '''
        idx = pd.Index([])
        for chunk in chunks:
            idx = idx.union(chunk)

        existing_meta_df = self.get_metadata(idx=processed_idx)

        deleted_idx = existing_meta_df.index.difference(idx)

        if len(deleted_idx) > 0:
            self.ds.event_logger.log_event(self.name, added_count=0, updated_count=0, deleted_count=len(deleted_idx))

            self._delete_data(deleted_idx)
            self._delete_metadata(deleted_idx)

    def store(self, df: pd.DataFrame) -> None:
        now = time.time()

        chunk = self.store_chunk(
            data_df=df,
            now=now
        )
        self.sync_meta(
            chunks=[chunk],
        )

    def get_data_chunked(self, chunksize: int = 1000) -> Generator[pd.DataFrame, None, None]:
        meta_df = self.get_metadata(idx=None)

        for i in range(0, len(meta_df.index), chunksize):
            yield self.get_data(meta_df.index[i:i+chunksize])

    def get_indexes(self, idx: Optional[Index] = None) -> Index:
        return self.get_metadata(idx).index.tolist()


class DataStore:
    def __init__(self, connstr: str, schema: str):
        self._init(connstr, schema)

    def _init(self, connstr: str, schema: str) -> None:
        self.connstr = connstr
        self.schema = schema

        self.con = create_engine(
            connstr,
        )

        self.sqla_metadata = MetaData(schema=schema)

        self.event_logger = EventLogger(self)

    def __getstate__(self):
        return {
            'connstr': self.connstr,
            'schema': self.schema
        }

    def __setstate__(self, state):
        self._init(state['connstr'], state['schema'])

    def get_table(self, name: str, data_sql_schema: List[Column], create_tables: bool = True) -> DataTable:
        return DataTable(
            ds=self,
            name=name,
            data_sql_schema=data_sql_schema,
            create_tables=create_tables,
        )

    def get_process_ids(
        self,
        inputs: List[DataTable],
        output: DataTable,
    ) -> pd.Index:
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
            sql = select([inp.meta_table.c.id]).select_from(
                left_join(inp.meta_table, [output.meta_table])
            ).where(
                or_(
                    output.meta_table.c.process_ts < inp.meta_table.c.update_ts,
                    output.meta_table.c.process_ts.is_(None)
                )
            )

            idx_df = pd.read_sql_query(
                sql,
                con=self.con,
                index_col='id',
            )

            if idx is None:
                idx = idx_df.index
            else:
                idx = idx.union(idx_df.index)

        sql = select([output.meta_table.c.id]).select_from(
            left_join(output.meta_table, [inp.meta_table for inp in inputs])
        ).where(
            and_(inp.meta_table.c.id.is_(None) for inp in inputs)
        )

        idx_df = pd.read_sql_query(
            sql,
            con=self.con,
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
        output: DataTable,
        chunksize: int = 1000,
    ) -> Iterator[List[pd.DataFrame]]:
        idx = self.get_process_ids(
            inputs=inputs,
            output=output,
        )

        logger.info(f'Items to update {len(idx)}')

        if len(idx) > 0:
            for i in range(0, len(idx), chunksize):
                yield [inp.get_data(idx[i:i+chunksize]) for inp in inputs]


def gen_process_many(
    dts: List[DataTable],
    proc_func: Callable[..., Union[
        Tuple[pd.DataFrame, ...],
        Iterator[Tuple[pd.DataFrame, ...]]]
    ],
    **kwargs
) -> None:
    '''
    Создание новой таблицы из результатов запуска `proc_func`.
    Функция может быть как обычной, так и генерирующейся
    '''

    chunks_k: Dict[int, ChunkMeta] = {
        k: [] for k in range(len(dts))
    }

    if inspect.isgeneratorfunction(proc_func):
        iterable = proc_func(**kwargs)
    else:
        iterable = (proc_func(**kwargs),)

    for chunk_dfs in iterable:
        for k, dt_k in enumerate(dts):
            chunk_df_kth = chunk_dfs[k] if len(dts) > 1 else chunk_dfs
            chunks_k[k].append(dt_k.store_chunk(chunk_df_kth))

    for k, dt_k in enumerate(dts):
        dt_k.sync_meta(chunks=chunks_k[k])


def gen_process(
    dt: DataTable,
    proc_func: Callable[[], Union[
        pd.DataFrame,
        Iterator[pd.DataFrame]]
    ],
    **kwargs
) -> None:
    return gen_process_many(
        dts=[dt],
        proc_func=proc_func,
        **kwargs
    )


def inc_process_many(
    ds: DataStore,
    input_dts: List[DataTable],
    res_dts: List[DataTable],
    proc_func: Callable,
    chunksize: int = 1000,
    **kwargs
) -> None:
    '''
    Множественная инкрементальная обработка `input_dts' на основе изменяющихся индексов
    '''

    res_dt_k_to_idxs = {}
    res_dts_chunks: Dict[int, ChunkMeta] = {}
    for k, res_dt in enumerate(res_dts):
        # Создаем словарь изменяющихся индексов для K результирующих табличек
        res_dt_k_to_idxs[k] = ds.get_process_ids(
            inputs=input_dts,
            output=res_dt,
        )
        res_dts_chunks[k] = []

    # Ищем все индексы, которые нужно обработать
    idx = list(set([item for sublist in res_dt_k_to_idxs.values() for item in sublist]))
    logger.info(f'Items to update {len(idx)}')

    if len(idx) > 0:
        for i in range(0, len(idx), chunksize):
            input_dfs = [inp.get_data(idx[i:i+chunksize]) for inp in input_dts]

            if sum(len(j) for j in input_dfs) > 0:
                chunks_df = proc_func(*input_dfs, **kwargs)
                for k, res_dt in enumerate(res_dts):
                    # Берем k-ое значение функции для k-ой таблички
                    chunk_df_k = chunks_df[k] if len(res_dts) > 1 else chunks_df
                    # Для каждой k результирующей таблички ищем обработанные внутри неё индексы
                    res_dt_i_chunks_idxs = set(res_dt_k_to_idxs[k]) & set(chunk_df_k.index)
                    # Располагаем индексы в таком же порядке, в каком они были изначально
                    res_dt_i_chunks_idxs_arranged = [
                        idx for idx in res_dt_k_to_idxs[k] if idx in res_dt_i_chunks_idxs
                    ]
                    # Читаем табличку с этими индексами
                    chunk_df_k_idxs = chunk_df_k.loc[res_dt_i_chunks_idxs_arranged]
                    # Добавляем результат в результирующие чанки
                    res_dts_chunks[k].append(res_dt.store_chunk(chunk_df_k_idxs))

    # Синхронизируем мета-данные для всех K табличек
    for k, res_dt in enumerate(res_dts):
        res_dt.sync_meta(res_dts_chunks[k], processed_idx=res_dt_k_to_idxs[k])


def inc_process(
    ds: DataStore,
    input_dts: List[DataTable],
    res_dt: DataTable,
    proc_func: Callable,
    chunksize: int = 1000,
    **kwargs
) -> None:
    inc_process_many(
        ds=ds,
        input_dts=input_dts,
        res_dts=[res_dt],
        proc_func=proc_func,
        chunksize=chunksize,
        **kwargs
    )

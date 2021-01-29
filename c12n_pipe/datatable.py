from typing import Callable, Generator, Iterator, List, Dict, Any, Optional, Union
import logging
import time

from sqlalchemy.engine import Engine
from sqlalchemy.sql import text
from sqlalchemy import create_engine, MetaData, Table, Column, Float, String, Numeric
import pandas as pd

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
    Column('create_ts', Float),
    Column('update_ts', Float),
    Column('process_ts', Float),
    Column('delete_ts', Float),
]


def _sql_schema_to_dtype(schema: List[Column]) -> Dict[str, Any]:
    return {
        i.name: i.type for i in PRIMARY_KEY + schema
    }


class DataTable:
    def __init__(
        self,
        constr: str,
        schema: str,
        name: str,
        data_sql_schema: List[Column],
        event_logger: EventLogger,
        create_tables: bool = True,
    ):
        self.constr = constr
        self.schema = schema
        self.name = name
        self.data_sql_schema = data_sql_schema
        self.event_logger = event_logger

        con = create_engine(self.constr)
        if create_tables:
            self._create_table(con, self.meta_table_name(), METADATA_SQL_SCHEMA)
            self._create_table(con, self.data_table_name(), self.data_sql_schema)

    def meta_table_name(self) -> str:
        return f'{self.name}_meta'

    def data_table_name(self) -> str:
        return f'{self.name}_data'

    def _create_table(self, con: Engine, name: str, sql_schema: List[Column]) -> None:
        if not con.dialect.has_table(con, name, schema=self.schema):
            logger.info(f'Creating table {self.schema}.{name}')
            md = MetaData()
            table = Table(
                name, md,
                *[i.copy() for i in PRIMARY_KEY + sql_schema],
                schema=self.schema
            )
            table.create(con)

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
        con = create_engine(self.constr)
        return pd.read_sql_table(
            table_name=self.data_table_name(),
            con=con,
            schema=self.schema,
            index_col='id',
        )

    def _delete_data(self, idx: Index, con: Engine) -> None:
        if len(idx) > 0:
            logger.info(f'Deleting {len(idx)} rows from {self.name} data')

            sql = text(
                f'''
                delete from {self.schema}.{self.data_table_name()}
                where id = ANY(:ids)
                '''
            )
            con.execute(sql, ids=list(idx))

    def _delete_metadata(self, idx: Index, con: Engine) -> None:
        if len(idx) > 0:
            logger.info(f'Deleting {len(idx)} rows from {self.name} metadata')

            sql = text(
                f'''
                delete from {self.schema}.{self.meta_table_name()}
                where id = ANY(:ids)
                '''
            )
            con.execute(sql, ids=list(idx))

    def get_metadata(self, idx: Optional[Index] = None, con: Engine = None) -> pd.DataFrame:
        if con is None:
            con = create_engine(self.constr)

        if idx is None:
            return pd.read_sql_query(
                f'''
                select id, hash, create_ts, update_ts, delete_ts from {self.schema}.{self.meta_table_name()}
                ''',
                con=con,
                index_col='id',
            )

        else:
            return pd.read_sql_query(
                f'''
                select id, hash, create_ts, update_ts, delete_ts from {self.schema}.{self.meta_table_name()}
                where id = ANY(%(ids)s)
                ''',
                con=con,
                index_col='id',
                params={
                    'ids': list(idx)
                },
            )

    def get_data(self, idx: Optional[Index] = None, con: Engine = None) -> pd.DataFrame:
        if con is None:
            con = create_engine(self.constr)

        if idx is None:
            return pd.read_sql_query(
                f'''
                select * from {self.schema}.{self.data_table_name()}
                ''',
                con=con,
                index_col='id',
            )
        else:
            return pd.read_sql_query(
                f'''
                select * from {self.schema}.{self.data_table_name()}
                where id = ANY(%(ids)s)
                ''',
                con=con,
                index_col='id',
                params={
                    'ids': list(idx)
                },
            )

    def store_chunk(self, data_df: pd.DataFrame, con: Engine = None, now: float = None) -> ChunkMeta:
        if con is None:
            con = create_engine(self.constr)

        if now is None:
            now = time.time()

        logger.info(f'Inserting chunk {len(data_df)} rows into {self.name}')

        # получить meta по чанку
        existing_meta_df = self.get_metadata(data_df.index, con)

        # найти что изменилось
        new_meta_df = self._make_new_metadata_df(now, data_df)

        common_idx = existing_meta_df.index.intersection(new_meta_df.index)
        changed_idx = common_idx[new_meta_df.loc[common_idx, 'hash'] != existing_meta_df.loc[common_idx, 'hash']]

        # найти что добавилось
        new_idx = new_meta_df.index.difference(existing_meta_df.index)

        if len(new_idx) > 0 or len(changed_idx) > 0:
            self.event_logger.log_event(self.name, added_count=len(new_idx), updated_count=len(changed_idx), deleted_count=0)

        ### обновить данные (удалить только то, что изменилось, записать новое)
        to_write_idx = changed_idx.union(new_idx)
        self._delete_data(to_write_idx, con)

        if len(data_df.loc[to_write_idx]) > 0:
            data_df.loc[to_write_idx].to_sql(
                name=self.data_table_name(),
                con=con,
                schema=self.schema,
                if_exists='append',
                index_label='id',
                chunksize=1000,
                method='multi',
                dtype=_sql_schema_to_dtype(self.data_sql_schema),
            )

        ### обновить метаданные (удалить и записать всю new_meta_df, потому что изменился processed_ts)

        if len(new_meta_df) > 0:
            self._delete_metadata(new_meta_df.index, con)

            not_changed_idx = existing_meta_df.index.difference(changed_idx)

            new_meta_df.loc[changed_idx, 'create_ts'] = existing_meta_df.loc[changed_idx, 'create_ts']
            new_meta_df.loc[not_changed_idx, 'update_ts'] = existing_meta_df.loc[not_changed_idx, 'update_ts']

            new_meta_df.to_sql(
                name=self.meta_table_name(),
                con=con,
                schema=self.schema,
                if_exists='append',
                index_label='id',
                chunksize=1000,
                method='multi',
                dtype=_sql_schema_to_dtype(METADATA_SQL_SCHEMA),
            )

        return data_df.index

    def sync_meta(self, chunks: List[ChunkMeta], processed_idx: pd.Index = None, con: Engine = None) -> None:
        ''' Пометить удаленными объекты, которых больше нет '''
        if con is None:
            con = create_engine(self.constr)

        idx = pd.Index([])
        for chunk in chunks:
            idx = idx.union(chunk)

        existing_meta_df = self.get_metadata(idx=processed_idx, con=con)

        deleted_idx = existing_meta_df.index.difference(idx)

        if len(deleted_idx) > 0:
            self.event_logger.log_event(self.name, added_count=0, updated_count=0, deleted_count=len(deleted_idx))

            self._delete_data(deleted_idx, con=con)
            self._delete_metadata(deleted_idx, con=con)

    def store(self, df: pd.DataFrame, con: Engine = None) -> None:
        if con is None:
            con = create_engine(self.constr)

        now = time.time()

        chunk = self.store_chunk(
            data_df=df,
            con=con,
            now=now
        )
        self.sync_meta(
            chunks=[chunk],
            con=con,
        )

    def get_data_chunked(self, chunksize: int = 1000) -> Generator[pd.DataFrame, None, None]:
        con = create_engine(self.constr)

        meta_df = self.get_metadata(idx=None, con=con)

        for i in range(0, len(meta_df.index), chunksize):
            yield self.get_data(meta_df.index[i:i+chunksize], con=con)

    def get_indexes(self, idx: Optional[Index] = None, con: Engine = None) -> Index:
        if con is None:
            con = create_engine(self.constr)

        if idx is None:
            return pd.read_sql_query(
                f'''
                select id from {self.schema}.{self.meta_table_name()}
                ''',
                con=con,
                index_col='id',
            ).index.tolist()
        else:
            return pd.read_sql_query(
                f'''
                select id from {self.schema}.{self.meta_table_name()}
                where id = ANY(%(ids)s)
                ''',
                con=con,
                index_col='id',
                params={
                    'ids': list(idx)
                },
            ).index.tolist()


class DataStore:
    def __init__(self, connstr: str, schema: str):
        self.connstr = connstr
        self.schema = schema

        self.event_logger = EventLogger(self.connstr, self.schema)

    def get_table(self, name: str, data_sql_schema: List[Column], create_tables: bool = True) -> DataTable:
        return DataTable(
            constr=self.connstr,
            schema=self.schema,
            name=name,
            data_sql_schema=data_sql_schema,
            create_tables=create_tables,
            event_logger=self.event_logger,
        )

    def get_process_ids(
        self,
        inputs: List[DataTable],
        output: DataTable,
        con: Engine = None,
    ) -> pd.Index:
        if con is None:
            con = create_engine(self.connstr)

        sql = text(
            f'''
            select id
            from {self.schema}.{inputs[0].meta_table_name()} t0
            ''' +
            ''.join(f'''
            full join {self.schema}.{t.meta_table_name()} t{i+1} using (id)
            ''' for i, t in enumerate(inputs[1:])) +
            f'''
            full join {self.schema}.{output.meta_table_name()} out using (id)
            where
            out.process_ts is null
            ''' +
            ''.join(
            f'''
            or (t{i}.update_ts is not null and out.process_ts < t{i}.update_ts) or t{i}.update_ts is null
            '''
                for i, t in enumerate(inputs)
            )
        )

        idx_df = pd.read_sql_query(
            sql,
            con=con,
            index_col='id',
        )

        return idx_df.index

    def get_process_chunks(
        self,
        inputs: List[DataTable],
        output: DataTable,
        chunksize: int = 1000,
        con: Engine = None,
    ) -> Iterator[List[pd.DataFrame]]:
        if con is None:
            con = create_engine(self.connstr)


        idx = self.get_process_ids(
            inputs=inputs,
            output=output,
            con=con,
        )

        logger.info(f'Items to update {len(idx)}')

        if len(idx) > 0:
            for i in range(0, len(idx), chunksize):
                yield [inp.get_data(idx[i:i+chunksize], con=con) for inp in inputs]


def inc_process(
    ds: DataStore,
    input_dts: List[DataTable],
    res_dt: DataTable,
    proc_func: Callable,
    chunksize: int = 1000,
    **kwargs
) -> None:
    '''
    Инкрементальная обработка на основании `input_dts` и `DataTable(name)`
    '''

    chunks = []

    con = create_engine(ds.connstr)

    idx = ds.get_process_ids(
        inputs=input_dts,
        output=res_dt,
        con=con,
    )

    logger.info(f'Items to update {len(idx)}')

    if len(idx) > 0:
        for i in range(0, len(idx), chunksize):
            input_dfs = [inp.get_data(idx[i:i+chunksize], con=con) for inp in input_dts]

            if sum(len(j) for j in input_dfs) > 0:
                chunk_df = proc_func(*input_dfs, **kwargs)

                chunks.append(res_dt.store_chunk(chunk_df))

    res_dt.sync_meta(chunks, processed_idx=idx, con=con)


def gen_process(
    dt: DataTable,
    proc_func: Callable[[], Iterator[pd.DataFrame]],
) -> None:
    '''
    Создание новой таблицы из результатов запуска `proc_func`
    '''

    chunks = []

    for chunk_df in proc_func():
        chunks.append(dt.store_chunk(chunk_df))

    return dt.sync_meta(chunks=chunks)


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

    con = create_engine(ds.connstr)
    res_dt_k_to_idxs = {}
    res_dts_chunks = {}
    for k, res_dt in enumerate(res_dts):
        # Создаем словарь изменяющихся индексов для K результирующих табличек
        res_dt_k_to_idxs[k] = ds.get_process_ids(
            inputs=input_dts,
            output=res_dt,
            con=con,
        )
        res_dts_chunks[k] = []
    
    # Ищем все индексы, которые нужно обработать
    idx = list(set([item for sublist in res_dt_k_to_idxs.values() for item in sublist]))
    logger.info(f'Items to update {len(idx)}')

    if len(idx) > 0:
        for i in range(0, len(idx), chunksize):
            input_dfs = [inp.get_data(idx[i:i+chunksize], con=con) for inp in input_dts]

            if sum(len(j) for j in input_dfs) > 0:
                chunks_df = proc_func(*input_dfs, **kwargs)
                for k, res_dt in enumerate(res_dts):
                    # Берем k-ое значение функции для k-ой таблички
                    chunk_df_k = chunks_df[k] if len(res_dts) > 1 else chunks_df
                    # Для каждой k результирующей таблички ищем обработанные внутри неё индексы
                    res_dt_i_chunks_idxs = list(set(res_dt_k_to_idxs[i]) & set(chunk_df_k.index))
                    chunk_df_k_idxs = chunk_df_k.loc[res_dt_i_chunks_idxs]
                    # Добавляем результат в результирующие чанки
                    res_dts_chunks[k].append(res_dt.store_chunk(chunk_df_k_idxs))

    # Синхронизируем мета-данные для всех K табличек
    for k, res_dt in enumerate(res_dts):
        res_dt.sync_meta(res_dts_chunks[k], processed_idx=idx, con=con)

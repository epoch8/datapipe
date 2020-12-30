from typing import Callable, Generator, List, Dict, Any, Optional, Union
import logging
import time

from sqlalchemy.engine import Engine
from sqlalchemy.sql import text
from sqlalchemy import create_engine, MetaData, Table, Column, Float, String, Numeric
import pandas as pd

logger = logging.getLogger('c12n_pipe')


Index = Union[List, pd.Index]
ChunkMeta = Index

DataSchema = List[Column]

# Metadata
# - id: str
# - create_ts: ts
# - update_ts: ts
# - delete_ts: ts


PRIMARY_KEY = [
    Column('id', String(100), primary_key=True),
]


METADATA_SQL_SCHEMA = [
    Column('hash', Numeric),
    Column('create_ts', Float),
    Column('update_ts', Float),
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
        create_tables: bool = True
    ):
        self.constr = constr
        self.schema = schema
        self.name = name
        self.data_sql_schema = data_sql_schema

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
            logger.info(f'Deleting {len(idx)} rows from {self.name}')

            sql = text(
                f'''
                delete from {self.schema}.{self.data_table_name()}
                where id = ANY(:ids)
                '''
            )
            con.execute(sql, ids=list(idx))

            sql = text(
                f'''
                delete from {self.schema}.{self.meta_table_name()}
                where id = ANY(:ids)
                '''
            )
            con.execute(sql, ids=list(idx))

    def _insert_data(self, meta_df: pd.DataFrame, data_df: pd.DataFrame, con: Engine) -> None:
        if len(meta_df) > 0:
            logger.info(f'Inserting {len(meta_df)} rows into {self.name}')

            data_df.to_sql(
                name=self.data_table_name(),
                con=con,
                schema=self.schema,
                if_exists='append',
                index_label='id',
                chunksize=1000,
                method='multi',
                dtype=_sql_schema_to_dtype(self.data_sql_schema),
            )

            meta_df.to_sql(
                name=self.meta_table_name(),
                con=con,
                schema=self.schema,
                if_exists='append',
                index_label='id',
                chunksize=1000,
                method='multi',
                dtype=_sql_schema_to_dtype(METADATA_SQL_SCHEMA),
            )

    def get_metadata(self, idx: Optional[Index], con: Engine) -> pd.DataFrame:
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

        logger.info(f'Changed: {len(changed_idx)}; New: {len(new_idx)}')

        # удалить измененные строки
        to_write_idx = changed_idx.union(new_idx)
        self._delete_data(to_write_idx, con)

        # записать измененные строки обратно
        # записать добавленные строки
        new_meta_df.loc[changed_idx, 'create_ts'] = existing_meta_df.loc[changed_idx, 'create_ts']

        self._insert_data(
            meta_df=new_meta_df.loc[to_write_idx],
            data_df=data_df.loc[to_write_idx],
            con=con
        )

        return data_df.index

    def sync_meta(self, chunks: List[ChunkMeta], con: Engine = None) -> None:
        ''' Пометить удаленными объекты, которых больше нет '''
        if con is None:
            con = create_engine(self.constr)

        idx = pd.Index([])
        for chunk in chunks:
            idx = idx.union(chunk)

        existing_meta_df = self.get_metadata(idx=None, con=con)

        deleted_idx = existing_meta_df.index.difference(idx)

        self._delete_data(deleted_idx, con=con)

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
                select id from {self.schema}.{self.data_table_name()}
                ''',
                con=con,
                index_col='id',
            ).index.tolist()
        else:
            return pd.read_sql_query(
                f'''
                select id from {self.schema}.{self.data_table_name()}
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

    def get_table(self, name: str, data_sql_schema: List[Column], create_tables: bool = True) -> DataTable:
        return DataTable(
            constr=self.connstr,
            schema=self.schema,
            name=name,
            data_sql_schema=data_sql_schema,
            create_tables=create_tables
        )

    def get_process_chunks(
        self,
        inputs: List[DataTable],
        output: DataTable,
        chunksize: int = 1000
    ) -> Generator[List[pd.DataFrame], None, None]:
        con = create_engine(self.connstr)

        '''
        select t0.id
        from eva_feed_meta t0
        left eva_ozon_sync_stocks_request_meta out on t0.id = out.id
        where out.update_ts is null
        or out.update_ts < t0.update_ts
        '''

        sql = text(
            f'''
            select t0.id
            from {self.schema}.{inputs[0].meta_table_name()} t0
            ''' +
            ''.join(f'''
            join {self.schema}.{t.meta_table_name()} t{i+1} on t0.id = t{i+1}.id
            ''' for i, t in enumerate(inputs[1:])) +
            f'''
            left join {self.schema}.{output.meta_table_name()} out on t0.id = out.id
            where
            out.update_ts is null
            ''' +
            ''.join(
            f'''
            or out.update_ts < t{i}.update_ts
            '''
                for i, t in enumerate(inputs)
            )
        )

        idx_df = pd.read_sql_query(
            sql,
            con=con,
            index_col='id',
        )

        logger.info(f'Items to update {len(idx_df)}')
        if len(idx_df.index) > 0:
            for i in range(0, len(idx_df.index), chunksize):
                yield [inp.get_data(idx_df.index[i:i+chunksize], con=con) for inp in inputs]
        else:
            yield [inp.get_data() for inp in inputs]


def inc_process(
    ds: DataStore,
    input_dts: List[DataTable],
    name: str,
    data_sql_schema: DataSchema,
    proc_func: Callable,
    chunksize: int = 1000,
) -> DataTable:
    res_dt = ds.get_table(
        name=name,
        data_sql_schema=data_sql_schema,
    )

    chunks = []

    for input_dfs in ds.get_process_chunks(inputs=input_dts, output=res_dt, chunksize=chunksize):
        chunk_df = proc_func(*input_dfs)

        chunks.append(res_dt.store_chunk(chunk_df))

    # FIXME: здесь нельзя делать синк метадаты, потому что при запуске когда ничего не меняется - стирается все
    # res_dt.sync_meta(chunks)

    return res_dt

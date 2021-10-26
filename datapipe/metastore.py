from dataclasses import dataclass
from typing import Iterator, Tuple, Dict, cast

import copy
import logging
import time

from sqlalchemy.sql.expression import and_, bindparam, or_, select, tuple_, update
from sqlalchemy import Table, Column, Integer, Float, func

from cityhash import CityHash32
import pandas as pd

from datapipe.types import IndexDF, DataSchema, DataDF, MetadataDF, data_to_index
from datapipe.store.database import DBConn, sql_schema_to_sqltype
from datapipe.event_logger import EventLogger
from datapipe.step import RunConfig


logger = logging.getLogger('datapipe.metastore')

METADATA_SQL_SCHEMA = [
    Column('hash', Integer),
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
        event_logger: EventLogger = None
    ):
        self.dbconn = dbconn
        self.name = name

        if event_logger is None:
            self.event_logger = EventLogger(dbconn)
        else:
            self.event_logger = event_logger

        self.primary_keys = [column.name for column in primary_schema]

        for item in primary_schema:
            item.primary_key = True

        sql_schema = primary_schema + METADATA_SQL_SCHEMA

        self.sql_schema = [copy.copy(i) for i in sql_schema]

        self.sql_table = Table(
            f'{self.name}_meta',
            self.dbconn.sqla_metadata,
            *self.sql_schema,
        )

        self.sql_table.create(self.dbconn.con, checkfirst=True)

    def sql_apply_filters(self, sql: select, run_config: RunConfig = None) -> select:
        if run_config is not None:
            for k, v in run_config.filters.items():
                if k in self.primary_keys:
                    sql = sql.where(
                        self.sql_table.c[k] == v
                    )

        return sql

    def get_metadata(self, idx: IndexDF = None, include_deleted: bool = False) -> MetadataDF:
        '''
        Получить датафрейм с метаданными.

        idx - опциональный фильтр по целевым строкам
        include_deleted - флаг, возвращать ли удаленные строки, по умолчанию = False
        '''
        sql = select(self.sql_schema)

        if idx is not None:
            if len(self.primary_keys) == 1:
                # Подход с values не работает, когда индекс один
                key = self.primary_keys[0]
                sql = sql.where(
                    self.sql_table.c[key].in_(idx[key].to_list())
                )

            else:
                # Когда индексных полей много приходится мудрить
                #
                # Раньше здесь был where (id1=? AND id2=?) OR (id1=? AND id2=?)
                # но SQLite не дает делать "сложность" дерева запросов больше 1000
                # то есть больше 1000 OR подряд нельзя, а мы хотим уметь чанки большего размера
                #
                # Другой способ сделать то же самое - через VALUES:
                # https://sqlite.org/forum/info/6cb4fbeb3fc1e527

                keys = tuple_(*[
                    self.sql_table.c[key]
                    for key in self.primary_keys
                ])

                sql = sql.where(keys.in_([
                    tuple([r[key] for key in self.primary_keys])  # type: ignore
                    for r in idx.to_dict(orient='records')
                ]))

                # if self.dbconn.con.name == 'sqlite':
                #     sql = sql.where(keys.in_([
                #         tuple([r[key] for key in self.primary_keys])  # type: ignore
                #         for r in idx.to_dict(orient='records')
                #     ]))
                # else:
                #     sql = sql.where(keys.in_(
                #         values(
                #             *[column(key) for key in self.primary_keys],
                #             name='values',
                #         )
                #         .data([
                #             tuple_(*[r[key] for key in self.primary_keys])  # type: ignore
                #             for r in idx.to_dict(orient='records')
                #         ])
                #     ))

        if not include_deleted:
            sql = sql.where(self.sql_table.c.delete_ts.is_(None))

        return pd.read_sql_query(
            sql,
            con=self.dbconn.con,
        )

    def _make_new_metadata_df(self, now: float, df: DataDF) -> MetadataDF:
        res_df = df[self.primary_keys]

        res_df['hash'] = self._get_hash_for_df(df)
        res_df['create_ts'] = now
        res_df['update_ts'] = now
        res_df['process_ts'] = now
        res_df['delete_ts'] = None

        return cast(MetadataDF, res_df)

    def _get_meta_data_columns(self):
        return self.primary_keys + [column.name for column in METADATA_SQL_SCHEMA]

    def _get_hash_for_df(self, df) -> pd.DataFrame:
        return (
            df
            .apply(lambda x: str(list(x)), axis=1)
            .apply(lambda x: int.from_bytes(CityHash32(x).to_bytes(4, 'little'), 'little', signed=True))
        )

    # Fix numpy types in Index
    # FIXME разобраться, что это за грязный хак
    def _get_sql_param(self, param):
        return param.item() if hasattr(param, "item") else param

    def get_existing_idx(self, idx: IndexDF = None) -> IndexDF:
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

        sql = sql.where(self.sql_table.c.delete_ts.is_(None))
        res_df = pd.read_sql_query(
            sql,
            con=self.dbconn.con,
        )

        return res_df[self.primary_keys]

    def get_table_debug_info(self) -> TableDebugInfo:
        return TableDebugInfo(
            name=self.name,
            size=self.dbconn.con.execute(select([func.count()]).select_from(self.sql_table)).fetchone()[0]
        )

    # TODO Может быть переделать работу с метадатой на контекстный менеджер?
    # FIXME поправить возвращаемые структуры данных, _meta_df должны содержать только _meta колонки
    def get_changes_for_store_chunk(
        self,
        data_df: DataDF,
        now: float = None
    ) -> Tuple[DataDF, DataDF, MetadataDF, MetadataDF]:
        '''
        Анализирует блок данных data_df, выделяет строки new_ которые нужно добавить и строки changed_ которые нужно обновить

        Returns tuple:
            new_data_df     - строки данных, которые нужно добавить
            changed_data_df - строки данных, которые нужно изменить
            new_meta_df     - строки метаданных, которые нужно добавить
            changed_meta_df - строки метаданных, которые нужно изменить
        '''

        if now is None:
            now = time.time()

        # получить meta по чанку
        existing_meta_df = self.get_metadata(data_to_index(data_df, self.primary_keys), include_deleted=True)
        data_cols = list(data_df.columns)
        meta_cols = self._get_meta_data_columns()

        # Дополняем данные методанными
        merged_df = pd.merge(
            data_df.assign(data_hash=self._get_hash_for_df(data_df)),
            existing_meta_df,
            how='left',
            left_on=self.primary_keys,
            right_on=self.primary_keys
        )

        new_idx = (merged_df['hash'].isna() | merged_df['delete_ts'].notnull())

        # Ищем новые записи
        new_df = data_df.iloc[new_idx.values][data_cols]

        # Создаем мета данные для новых записей
        new_meta_data_df = merged_df.iloc[merged_df['hash'].isna().values][data_cols]
        new_meta_df = self._make_new_metadata_df(now, new_meta_data_df)

        # Ищем изменившиеся записи
        changed_idx = (
            (merged_df['hash'].notna()) &
            (merged_df['delete_ts'].isnull()) &
            (merged_df['hash'] != merged_df['data_hash'])
        )
        changed_df = merged_df.iloc[changed_idx.values][data_cols]

        # Меняем мета данные для существующих записей
        changed_meta_idx = (
            (merged_df['hash'].notna()) &
            (merged_df['hash'] != merged_df['data_hash']) |
            (merged_df['delete_ts'].notnull())
        )
        changed_meta_df = merged_df[merged_df['hash'].notna()]

        changed_meta_df.loc[changed_meta_idx, 'update_ts'] = now
        changed_meta_df['process_ts'] = now
        changed_meta_df['delete_ts'] = None
        changed_meta_df['hash'] = changed_meta_df['data_hash']

        return (
            cast(DataDF, new_df),
            cast(DataDF, changed_df),
            cast(MetadataDF, new_meta_df),
            cast(MetadataDF, changed_meta_df[meta_cols]),
        )

    def _insert_rows(self, df: MetadataDF) -> None:
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
                dtype=sql_schema_to_sqltype(self.sql_schema),
            )

    def _update_existing_metadata_rows(self, df: MetadataDF) -> None:
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
                    cast(
                        Dict,
                        df.reset_index()[columns].to_dict(orient='records')
                    )
                ]
            )

    # TODO объединить
    def insert_meta_for_store_chunk(self, new_meta_df: MetadataDF) -> None:
        if len(new_meta_df) > 0:
            self._insert_rows(new_meta_df)

    def update_meta_for_store_chunk(self, changed_meta_df: MetadataDF) -> None:
        if len(changed_meta_df) > 0:
            self._update_existing_metadata_rows(changed_meta_df)

    def mark_rows_deleted(self, deleted_idx: IndexDF, now: float = None) -> None:
        if len(deleted_idx) > 0:
            logger.debug(f'Deleting {len(deleted_idx.index)} rows from {self.name} data')
            self.event_logger.log_state(self.name, added_count=0, updated_count=0, deleted_count=len(deleted_idx))

            if now is None:
                now = time.time()

            meta_df = self.get_metadata(deleted_idx)

            meta_df.loc[:, "hash"] = 0
            meta_df.loc[:, "delete_ts"] = now
            meta_df.loc[:, "process_ts"] = now

            self._update_existing_metadata_rows(meta_df)

    def get_stale_idx(self, process_ts: float, run_config: RunConfig = None) -> Iterator[IndexDF]:
        idx_cols = [self.sql_table.c[key] for key in self.primary_keys]
        sql = select(idx_cols).where(
            and_(
                self.sql_table.c.process_ts < process_ts,
                self.sql_table.c.delete_ts.is_(None)
            )
        )

        sql = self.sql_apply_filters(sql, run_config=run_config)

        return pd.read_sql_query(
            sql,
            con=self.dbconn.con,
            chunksize=1000
        )

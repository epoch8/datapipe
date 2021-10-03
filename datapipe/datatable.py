from typing import Callable, Dict, Iterator, List, Optional, Tuple, Union

import inspect
import logging
import time
import math

import pandas as pd
from sqlalchemy import alias, func, select, union, and_, or_
import tqdm

from datapipe.types import DataDF, MetadataDF, IndexDF, data_to_index, index_difference
from datapipe.store.database import DBConn
from datapipe.metastore import MetaTable
from datapipe.store.table_store import TableStore
from datapipe.event_logger import EventLogger

from datapipe.step import ComputeStep


logger = logging.getLogger('datapipe.datatable')


class DataTable:
    def __init__(
        self,
        name: str,
        meta_dbconn: DBConn,
        meta_table: MetaTable,
        table_store: TableStore,
        event_logger: EventLogger,
    ):
        self.name = name
        self.meta_dbconn = meta_dbconn
        self.meta_table = meta_table
        self.table_store = table_store
        self.event_logger = event_logger

        self.primary_keys = meta_table.primary_keys

    def get_metadata(self, idx: Optional[IndexDF] = None) -> MetadataDF:
        return self.meta_table.get_metadata(idx)

    def get_data(self, idx: Optional[IndexDF] = None) -> DataDF:
        return self.table_store.read_rows(self.meta_table.get_existing_idx(idx))

    def store_chunk(self, data_df: DataDF, processed_idx: IndexDF = None, now: float = None) -> None:
        '''
        Записать новые данные в таблицу.

        При указанном `processed_idx` удалить те строки, которые находятся внутри `processed_idx`, но
        отсутствуют в `data_df`.
        '''

        logger.debug(f'Inserting chunk {len(data_df.index)} rows into {self.name}')

        new_df, changed_df, new_meta_df, changed_meta_df = self.meta_table.get_changes_for_store_chunk(data_df, now)
        # TODO implement transaction meckanism
        self.table_store.insert_rows(new_df)
        self.table_store.update_rows(changed_df)

        self.meta_table.insert_meta_for_store_chunk(new_meta_df)
        self.meta_table.update_meta_for_store_chunk(changed_meta_df)

        data_idx = data_to_index(data_df, self.primary_keys)

        if processed_idx is not None:
            existing_idx = self.meta_table.get_existing_idx(processed_idx)
            deleted_idx = index_difference(existing_idx, data_idx)

            self.table_store.delete_rows(deleted_idx)
            self.meta_table.mark_rows_deleted(deleted_idx)

    def delete_by_idx(self, idx: IndexDF, now: float = None) -> None:
        self.table_store.delete_rows(idx)
        self.meta_table.mark_rows_deleted(idx, now=now)

    def delete_stale_by_process_ts(self, process_ts: float, now: float = None) -> None:
        for deleted_df in self.meta_table.get_stale_idx(process_ts):
            deleted_idx = data_to_index(deleted_df, self.primary_keys)
            self.table_store.delete_rows(deleted_idx)
            self.meta_table.mark_rows_deleted(deleted_idx, now=now)


class DataStore:
    def __init__(
        self,
        meta_dbconn: DBConn
    ) -> None:
        self.meta_dbconn = meta_dbconn
        self.event_logger = EventLogger(self.meta_dbconn)
        self.tables: Dict[str, DataTable] = {}

    def create_table(self, name: str, table_store: TableStore) -> DataTable:
        assert(name not in self.tables)

        primary_schema = table_store.get_primary_schema()

        res = DataTable(
            name=name,
            meta_dbconn=self.meta_dbconn,
            meta_table=MetaTable(
                dbconn=self.meta_dbconn,
                name=name,
                primary_schema=primary_schema,
                event_logger=self.event_logger,
            ),
            table_store=table_store,
            event_logger=self.event_logger,
        )

        self.tables[name] = res

        return res

    def get_or_create_table(self, name: str, table_store: TableStore) -> DataTable:
        if name in self.tables:
            return self.tables[name]
        else:
            return self.create_table(name, table_store)

    def get_process_ids(
        self,
        inputs: List[DataTable],
        outputs: List[DataTable],
        chunksize: int = 1000,
    ) -> Tuple[int, Iterator[IndexDF]]:
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

        inp_tbls = [inp.meta_table.sql_table.alias(f"inp_{inp.meta_table.sql_table.name}") for inp in inputs]
        out_tbls = [out.meta_table.sql_table.alias(f"out_{out.meta_table.sql_table.name}") for out in outputs]
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
                or_(
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

        idx_count = self.meta_dbconn.con.execute(
            select([func.count()])
            .select_from(
                alias(u1, name='union_select')
            )
        ).scalar()

        return idx_count, pd.read_sql_query(
            u1,
            con=self.meta_dbconn.con,
            chunksize=chunksize
        )


# TODO перенести в compute.BatchGenerateStep
def gen_process_many(
    dts: List[DataTable],
    proc_func: Callable[
        ...,
        Iterator[Tuple[DataDF, ...]]
    ],
    **kwargs
) -> None:
    '''
    Создание новой таблицы из результатов запуска `proc_func`.
    Функция может быть как обычной, так и генерирующейся
    '''

    now = time.time()

    assert inspect.isgeneratorfunction(proc_func), "Starting v0.8.0 proc_func should be a generator"

    try:
        iterable = proc_func(**kwargs)
    except Exception as e:
        logger.exception(f"Generating failed ({proc_func.__name__}): {str(e)}")

    while True:
        try:
            chunk_dfs = next(iterable)

            if isinstance(chunk_dfs, pd.DataFrame):
                chunk_dfs = [chunk_dfs]
        except StopIteration:
            break
        except Exception as e:
            logger.exception(f"Generating failed ({proc_func.__name__}): {str(e)}")

            # TODO перенести get_process* в compute.BatchGenerateStep и пользоваться event_logger из metastore
            if dts:
                dts[0].event_logger.log_exception(e)
            return

        for k, dt_k in enumerate(dts):
            dt_k.store_chunk(chunk_dfs[k])

    for k, dt_k in enumerate(dts):
        dt_k.delete_stale_by_process_ts(now)


# TODO перенести в compute.BatchGenerateStep
def gen_process(
    dt: DataTable,
    proc_func: Callable[[], Union[
        DataDF,
        Iterator[DataDF]]
    ],
    **kwargs
) -> None:
    def proc_func_many():
        for i in proc_func():
            yield (i,)

    return gen_process_many(
        dts=[dt],
        proc_func=proc_func_many,
        **kwargs
    )


# TODO перенести в compute.BatchTransformStep
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


# TODO перенести в compute.BatchGenerateStep
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

    idx_count, idx_gen = ds.get_process_ids(
        inputs=input_dts,
        outputs=res_dts,
        chunksize=chunksize
    )

    logger.info(f'Items to update {idx_count}')

    if idx_count > 0:
        for idx in tqdm.tqdm(idx_gen, total=math.ceil(idx_count / chunksize)):
            logger.debug(f'Idx to process: {idx.to_records()}')

            input_dfs = [inp.get_data(idx) for inp in input_dts]

            if sum(len(j) for j in input_dfs) > 0:
                try:
                    chunks_df = proc_func(*input_dfs, **kwargs)
                except Exception as e:
                    logger.error(f"Transform failed ({proc_func.__name__}): {str(e)}")
                    ds.event_logger.log_exception(e)

                    continue

                for k, res_dt in enumerate(res_dts):
                    # Берем k-ое значение функции для k-ой таблички
                    chunk_df_k = chunks_df[k] if len(res_dts) > 1 else chunks_df

                    # Добавляем результат в результирующие чанки
                    res_dt.store_chunk(data_df=chunk_df_k, processed_idx=idx)

            else:
                for k, res_dt in enumerate(res_dts):
                    res_dt.delete_by_idx(idx)


class ExternalTableUpdater(ComputeStep):
    def __init__(self, name: str, table: DataTable):
        self.name = name
        self.table = table
        self.input_dts = []
        self.output_dts = [table]

    def run(self, ds: DataStore) -> None:
        ps_df = self.table.table_store.read_rows_meta_pseudo_df()

        _, _, new_meta_df, changed_meta_df = self.table.meta_table.get_changes_for_store_chunk(ps_df)

        # TODO switch to iterative store_chunk and self.table.sync_meta_by_process_ts

        self.table.meta_table.insert_meta_for_store_chunk(new_meta_df)
        self.table.meta_table.update_meta_for_store_chunk(changed_meta_df)

        deleted_idx = index_difference(
            self.table.meta_table.get_existing_idx(),
            data_to_index(ps_df, self.table.primary_keys)
        )

        self.table.meta_table.mark_rows_deleted(deleted_idx)

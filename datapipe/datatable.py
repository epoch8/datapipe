from datapipe.types import DataDF, MetadataDF
from typing import Callable, Iterator, List, Optional, Tuple, Union, cast

import inspect
import logging
import time
import math

import pandas as pd
import tqdm

from datapipe.metastore import MetaTable, MetaStore
from datapipe.types import IndexDF, ChunkMeta
from datapipe.store.table_store import TableStore

from datapipe.step import ComputeStep


logger = logging.getLogger('datapipe.datatable')


class DataTable:
    def __init__(
        self,
        name: str,
        meta_table: MetaTable,
        table_store: TableStore,  # Если None - создается по дефолту
    ):
        self.name = name
        self.meta_table = meta_table
        self.table_store = table_store
        self.primary_keys = meta_table.primary_keys

    def _make_deleted_meta_df(self, now, old_meta_df, deleted_idx) -> MetadataDF:
        res = old_meta_df.loc[deleted_idx]
        res.loc[:, 'delete_ts'] = now
        return res

    def get_metadata(self, idx: Optional[IndexDF] = None) -> MetadataDF:
        return self.meta_table.get_metadata(idx)

    def get_data(self, idx: Optional[IndexDF] = None) -> DataDF:
        return self.table_store.read_rows(self.meta_table.get_existing_idx(idx))

    def store_chunk(self, data_df: DataDF, now: float = None) -> MetadataDF:
        logger.debug(f'Inserting chunk {len(data_df.index)} rows into {self.name}')

        new_df, changed_df, new_meta_df, changed_meta_df = self.meta_table.get_changes_for_store_chunk(data_df, now)
        # TODO implement transaction meckanism
        self.table_store.insert_rows(new_df)
        self.table_store.update_rows(changed_df)

        self.meta_table.insert_meta_for_store_chunk(new_meta_df)
        self.meta_table.update_meta_for_store_chunk(changed_meta_df)

        return cast(MetadataDF, data_df[self.primary_keys])

    def sync_meta_by_idx_chunks(self, chunks: List[ChunkMeta], processed_idx: MetadataDF = None) -> None:
        ''' Пометить удаленными объекты, которых больше нет '''
        deleted_idx = self.meta_table.get_changes_for_sync_meta(chunks, processed_idx)

        if len(deleted_idx) > 0:
            self.table_store.delete_rows(deleted_idx)

        self.meta_table.update_meta_for_sync_meta(deleted_idx)

    def sync_meta_by_process_ts(self, process_ts: float) -> None:
        deleted_dfs = self.meta_table.get_stale_idx(process_ts)

        for deleted_df in deleted_dfs:
            deleted_idx = deleted_df[self.primary_keys]
            self.table_store.delete_rows(deleted_idx)
            self.meta_table.update_meta_for_sync_meta(cast(MetadataDF, deleted_idx))

    def store(self, df: DataDF) -> None:
        now = time.time()

        chunk = self.store_chunk(
            data_df=df,
            now=now
        )
        self.sync_meta_by_idx_chunks(
            chunks=[chunk],
        )

    def get_indexes(self, idx: Optional[IndexDF] = None) -> IndexDF:
        # FIXME неправильный тип
        return self.meta_table.get_metadata(idx).index.tolist()


def get_process_chunks(
    ms: MetaStore,
    inputs: List[DataTable],
    outputs: List[DataTable],
    chunksize: int = 1000,
) -> Tuple[int, Iterator[List[DataDF]]]:
    idx_count, idx_gen = ms.get_process_ids(
        inputs=[i.meta_table for i in inputs],
        outputs=[i.meta_table for i in outputs],
        chunksize=chunksize
    )

    logger.info(f'Items to update {idx_count}')

    def gen():
        if idx_count > 0:
            for idx in idx_gen:
                yield idx, [inp.get_data(idx) for inp in inputs]

    return idx_count, gen()


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
                dts[0].meta_table.event_logger.log_exception(e)
            return

        for k, dt_k in enumerate(dts):
            chunk_df_kth = cast(DataDF, chunk_dfs[k])
            dt_k.store_chunk(chunk_df_kth)

    for k, dt_k in enumerate(dts):
        dt_k.sync_meta_by_process_ts(now)


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


# TODO перенести в compute.BatchGenerateStep
def inc_process_many(
    ms: MetaStore,
    input_dts: List[DataTable],
    res_dts: List[DataTable],
    proc_func: Callable,
    chunksize: int = 1000,
    **kwargs
) -> None:
    '''
    Множественная инкрементальная обработка `input_dts' на основе изменяющихся индексов
    '''

    idx_count, input_dfs_gen = get_process_chunks(
        ms,
        inputs=input_dts,
        outputs=res_dts,
        chunksize=chunksize
    )

    if idx_count > 0:
        for idx, input_dfs in tqdm.tqdm(input_dfs_gen, total=math.ceil(idx_count / chunksize)):

            if sum(len(j) for j in input_dfs) > 0:
                try:
                    chunks_df = proc_func(*input_dfs, **kwargs)
                except Exception as e:
                    logger.error(f"Transform failed ({proc_func.__name__}): {str(e)}")
                    ms.event_logger.log_exception(e)

                    idx = pd.concat(input_dfs).index

                    continue

                for k, res_dt in enumerate(res_dts):
                    # Берем k-ое значение функции для k-ой таблички
                    chunk_df_k = chunks_df[k] if len(res_dts) > 1 else chunks_df

                    # Добавляем результат в результирующие чанки
                    res_index = res_dt.store_chunk(chunk_df_k)
                    res_dt.sync_meta_by_idx_chunks([res_index], processed_idx=idx)

            else:
                for k, res_dt in enumerate(res_dts):
                    res_dt.sync_meta_by_idx_chunks([], processed_idx=idx)


# TODO перенести в compute.BatchTransformStep
def inc_process(
    ds: MetaStore,
    input_dts: List[DataTable],
    res_dt: DataTable,
    proc_func: Callable,
    chunksize: int = 1000,
    **kwargs
) -> None:
    inc_process_many(
        ms=ds,
        input_dts=input_dts,
        res_dts=[res_dt],
        proc_func=proc_func,
        chunksize=chunksize,
        **kwargs
    )


class ExternalTableUpdater(ComputeStep):
    def __init__(self, name: str, table: DataTable):
        self.name = name
        self.table = table
        self.input_dts = []
        self.output_dts = [table]

    def run(self, ms: MetaStore) -> None:
        ps_df = cast(DataDF, self.table.table_store.read_rows_meta_pseudo_df())

        _, _, new_meta_df, changed_meta_df = self.table.meta_table.get_changes_for_store_chunk(ps_df)
        self.table.meta_table.insert_meta_for_store_chunk(new_meta_df)
        self.table.meta_table.update_meta_for_store_chunk(changed_meta_df)

        deleted_idx = self.table.meta_table.get_changes_for_sync_meta([ps_df.index])
        self.table.meta_table.update_meta_for_sync_meta(deleted_idx)

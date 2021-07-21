from typing import Callable, Generator, Iterator, List, Dict, Optional, Tuple, Union, TYPE_CHECKING

import inspect
import logging
import time
import math

import pandas as pd
import tqdm

from datapipe.store.types import Index, ChunkMeta
from datapipe.store.table_store import TableStore

from datapipe.step import ComputeStep


if TYPE_CHECKING:
    from datapipe.metastore import MetaTable, MetaStore


logger = logging.getLogger('datapipe.datatable')


class DataTable:
    def __init__(
        self,
        name: str,
        meta_table: 'MetaTable',
        table_store: TableStore,  # Если None - создается по дефолту
    ):
        self.name = name

        self.meta_table = meta_table
        self.table_store = table_store

    def _make_deleted_meta_df(self, now, old_meta_df, deleted_idx) -> pd.DataFrame:
        res = old_meta_df.loc[deleted_idx]
        res.loc[:, 'delete_ts'] = now
        return res

    def get_metadata(self, idx: Optional[Index] = None) -> pd.DataFrame:
        return self.meta_table.get_metadata(idx)

    def get_data(self, idx: Optional[Index] = None) -> pd.DataFrame:
        return self.table_store.read_rows(self.meta_table.get_existing_idx(idx))

    def store_chunk(self, data_df: pd.DataFrame, now: float = None) -> ChunkMeta:
        logger.debug(f'Inserting chunk {len(data_df)} rows into {self.name}')

        new_idx, changed_idx, new_meta_df = self.meta_table.get_changes_for_store_chunk(data_df, now)

        self.table_store.insert_rows(data_df.loc[new_idx])
        self.table_store.update_rows(data_df.loc[changed_idx])

        self.meta_table.update_meta_for_store_chunk(new_meta_df)

        return list(data_df.index)

    def sync_meta_by_idx_chunks(self, chunks: List[ChunkMeta], processed_idx: pd.Index = None) -> None:
        ''' Пометить удаленными объекты, которых больше нет '''
        deleted_idx = self.meta_table.get_changes_for_sync_meta(chunks, processed_idx)

        if len(deleted_idx) > 0:
            self.table_store.delete_rows(deleted_idx)

        self.meta_table.update_meta_for_sync_meta(deleted_idx)

    def sync_meta_by_process_ts(self, process_ts: float) -> None:
        deleted_dfs = self.meta_table.get_stale_idx(process_ts)

        for deleted_df in deleted_dfs:
            deleted_idx = deleted_df.index
            self.table_store.delete_rows(deleted_idx)
            self.meta_table.update_meta_for_sync_meta(deleted_idx)

    def store(self, df: pd.DataFrame) -> None:
        now = time.time()

        chunk = self.store_chunk(
            data_df=df,
            now=now
        )
        self.sync_meta_by_idx_chunks(
            chunks=[chunk],
        )

    def get_data_chunked(self, chunksize: int = 1000) -> Generator[pd.DataFrame, None, None]:
        meta_df = self.meta_table.get_metadata(idx=None)

        for i in range(0, len(meta_df.index), chunksize):
            yield self.get_data(meta_df.index[i:i+chunksize])

    def get_indexes(self, idx: Optional[Index] = None) -> Index:
        return self.meta_table.get_metadata(idx).index.tolist()


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

    now = time.time()

    if inspect.isgeneratorfunction(proc_func):
        iterable = proc_func(**kwargs)
    else:
        def gen_func(**kwargs):
            yield proc_func(**kwargs)

        iterable = gen_func(**kwargs)

    while True:
        try:
            chunk_dfs = next(iterable)
        except StopIteration:
            break
        except Exception as e:
            logger.error(f"Generating failed ({proc_func.__name__}): {str(e)}")

            if dts:
                dts[0].meta_table.event_logger.log_exception(e)
            return

        for k, dt_k in enumerate(dts):
            chunk_df_kth = chunk_dfs[k] if len(dts) > 1 else chunk_dfs
            dt_k.store_chunk(chunk_df_kth)

    for k, dt_k in enumerate(dts):
        dt_k.sync_meta_by_process_ts(now)


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
    ms: 'MetaStore',
    input_dts: List[DataTable],
    res_dts: List[DataTable],
    proc_func: Callable,
    chunksize: int = 1000,
    **kwargs
) -> None:
    '''
    Множественная инкрементальная обработка `input_dts' на основе изменяющихся индексов
    '''

    res_dts_chunks: Dict[int, ChunkMeta] = {k: [] for k, _ in enumerate(res_dts)}

    idx, input_dfs_gen = ms.get_process_chunks(
        inputs=input_dts,
        outputs=res_dts,
        chunksize=chunksize
    )

    if len(idx) > 0:
        for input_dfs in tqdm.tqdm(input_dfs_gen, total=math.ceil(len(idx) / chunksize)):
            if sum(len(j) for j in input_dfs) > 0:
                try:
                    chunks_df = proc_func(*input_dfs, **kwargs)
                except Exception as e:
                    logger.error(f"Transform failed ({proc_func.__name__}): {str(e)}")
                    ms.event_logger.log_exception(e)

                    idx = pd.concat(input_dfs).index

                    for k, res_dt in enumerate(res_dts):
                        res_dts_chunks[k].append(list(idx))

                    continue

                for k, res_dt in enumerate(res_dts):
                    # Берем k-ое значение функции для k-ой таблички
                    chunk_df_k = chunks_df[k] if len(res_dts) > 1 else chunks_df

                    # Добавляем результат в результирующие чанки
                    res_dts_chunks[k].append(res_dt.store_chunk(chunk_df_k))

        # Синхронизируем мета-данные для всех K табличек
        for k, res_dt in enumerate(res_dts):
            res_dt.sync_meta_by_idx_chunks(res_dts_chunks[k], processed_idx=idx)


def inc_process(
    ds: 'MetaStore',
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

    def run(self, ms: 'MetaStore') -> None:
        ps_df = self.table.table_store.read_rows_meta_pseudo_df()

        _, _, new_meta_df = self.table.meta_table.get_changes_for_store_chunk(ps_df)
        self.table.meta_table.update_meta_for_store_chunk(new_meta_df)

        deleted_idx = self.table.meta_table.get_changes_for_sync_meta([ps_df.index])
        self.table.meta_table.update_meta_for_sync_meta(deleted_idx)

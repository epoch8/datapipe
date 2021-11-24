from typing import List, Union, Optional, Iterator

import logging
import json
import pandas as pd
import numpy as np
import pickle
import plyvel

from pathlib import PosixPath
from collections import OrderedDict
from sqlalchemy.schema import Column

from datapipe.compute import RunConfig
from datapipe.types import DataDF, IndexDF, DataSchema, data_to_index
from datapipe.store.table_store import TableStore


logger = logging.getLogger('datapipe.store.levelDB')


class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)


class LevelDBStore(TableStore):
    def __init__(
        self,
        name: str,
        db_path: Union[str, PosixPath],
        data_sql_schema: List[Column],
    ) -> None:
        self.name = name
        self.db_path = str(db_path)
        self.data_sql_schema = data_sql_schema
        self.db = plyvel.DB(self.db_path, create_if_missing=True)

    def get_primary_schema(self) -> DataSchema:
        return [column for column in self.data_sql_schema if column.primary_key]

    def _get_sorted_dict(self, d):
        return OrderedDict([(k, d[k]) for k in sorted(d.keys())])

    def _dump_key(self, key):
        key_sorted = self._get_sorted_dict(key)
        return json.dumps(key_sorted, ensure_ascii=True, cls=NpEncoder).encode('ascii')

    def _load_key(self, key):
        return json.loads(key.decode('ascii'))

    def _dump_value(self, value):
        return pickle.dumps(value)

    def _load_value(self, value):
        return pickle.loads(value)

    def delete_rows(self, idx: IndexDF) -> None:
        if idx is not None and len(idx.index):
            logger.debug(f'Deleting {len(idx.index)} rows from {self.name} data')

            for row_i, row in idx.iterrows():
                k = self._dump_key(row)
                self.db.delete(k)

    def insert_rows(self, df: DataDF, chunk_size: int = 1000) -> None:
        if df.empty:
            return

        self.delete_rows(data_to_index(df, self.primary_keys))
        logger.debug(f'Inserting {len(df)} rows into {self.name} data')

        batch_count = int(np.ceil(len(df) / chunk_size))
        for batch_i in range(batch_count):
            with self.db.write_batch():
                for row_i, row in df.iloc[batch_i * chunk_size: (batch_i + 1) * chunk_size].iterrows():
                    row_idx = {col: row[col] for col in row.keys() if col in self.primary_keys}
                    # multi-index is not supported in levelDB
                    k = self._dump_key(row_idx)
                    v = self._dump_value({col: row[col] for col in row.keys() if col not in self.primary_keys})
                    self.db.put(k, v)

    def update_rows(self, df: DataDF) -> None:
        self.insert_rows(df)

    def read_rows(self, idx: Optional[IndexDF] = None) -> pd.DataFrame:
        rows = []

        if idx is not None:
            if not len(idx.index):
                return pd.DataFrame(columns=[column.name for column in self.data_sql_schema])

            for row_i, row in idx.iterrows():
                k = self._dump_key(row)
                encoded_row = self.db.get(k)

                if encoded_row is not None:
                    row_idx_sorted = self._get_sorted_dict(row)
                    row = self._load_value(encoded_row)
                    rows.append({**row_idx_sorted, **row})
        else:
            with self.db.iterator() as it:
                for k, v in it:
                    row_idx = self._load_key(k)
                    row_values = self._load_value(v)
                    rows.append({**row_idx, **row_values})

        return pd.DataFrame.from_records(rows, columns=[column.name for column in self.data_sql_schema])

    def read_rows_meta_pseudo_df(self, chunksize: int = 1000, run_config: RunConfig = None) -> Iterator[DataDF]:
        rows = []
        with self.db.iterator() as it:
            for k, v in it:
                row_idx = self._load_key(k)
                row_values = self._load_value(v)
                if run_config is None:
                    rows.append({**row_idx, **row_values})
                else:
                    for filter_k, filter_v in run_config.filters.items():
                        if filter_k in self.primary_keys and row_idx[filter_k] == filter_v:
                            rows.append({**row_idx, **row_values})
                if len(rows) % chunksize == 0 and len(rows) > 0:
                    yield pd.DataFrame.from_records(rows)
                    rows = []

        if len(rows) >= 0:
            yield pd.DataFrame.from_records(rows, columns=[column.name for column in self.data_sql_schema])

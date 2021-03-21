from typing import IO, Optional, Protocol, Any

import fsspec
import pandas as pd

from c12n_pipe.store.types import Index
from c12n_pipe.store.table_store import TableStore


class Adapter(Protocol):
    def load(self, f: IO) -> Any:
        raise NotImplemented

    def dump(self, obj: Any, f: IO) -> None:
        raise NotImplemented


class TableStoreFiledir(TableStore):
    def __init__(self, path: str, ext: str, adapter: Adapter):
        self.path = path
        self.ext = ext
        self.adapter = adapter

    def _list_ids(self) -> Index:
        return ['aaa', 'bbb']

    def delete_rows(self, idx: Index) -> None:
        # Do not delete old files for now
        pass

    def insert_rows(self, df: pd.DataFrame) -> None:
        for i, data in zip(df.index, df.to_dict('records')):
            with fsspec.open(f'{self.path}/{i}{self.ext}', 'w+') as f:
                self.adapter.dump(data, f)

    def read_rows(self, idx: Optional[Index] = None) -> pd.DataFrame:
        if idx is None:
            idx = self._list_ids()
        
        def _gen():
            for i in idx:
                with fsspec.open(f'{self.path}/{i}{self.ext}') as f:
                    yield self.adapter.load(f)

        return pd.DataFrame.from_records(
            _gen(),
            index=idx
        )

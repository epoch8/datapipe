from typing import IO, Optional, Protocol, Any, Dict

import json
import fsspec
import pandas as pd

from PIL import Image

from c12n_pipe.store.types import Index
from c12n_pipe.store.table_store import TableDataStore


class Adapter(Protocol):
    mode: str

    def load(self, f: IO) -> Dict[str, Any]:
        raise NotImplemented

    def dump(self, obj: Dict[str, Any], f: IO) -> None:
        raise NotImplemented


class JSONAdapter(Adapter):
    '''
    Converts each JSON file into Pandas record
    '''

    mode = 't'

    def load(self, f: IO) -> Dict[str, Any]:
        return json.load(f)
    
    def dump(self, obj: Dict[str, Any], f: IO) -> None:
        return json.dump(obj, f)


class PILAdapter(Adapter):
    '''
    Uses `image` column with PIL.Image for save/load
    '''

    mode = 'b'

    def __init__(self, format: str) -> None:
        self.format = format

    def load(self, f: IO) -> Dict[str, Any]:
        im = Image.open(f)
        im.load()
        return {'image': im}
    
    def dump(self, obj: Dict[str, Any], f: IO) -> None:
        im : Image.Image = obj['image']
        im.save(f, format=self.format)


class TableStoreFiledir(TableDataStore):
    def __init__(self, path: str, ext: str, adapter: Adapter):
        self.path = path
        self.ext = ext
        self.adapter = adapter

    def _list_ids(self) -> Index:
        # FIXME: Сделать более аккуратную работу с идентификаторами
        ff = fsspec.open_files(f'{self.path}/*{self.ext}')
        return [i.path.split('/')[-1][:-len(self.ext)] for i in ff]

    def delete_rows(self, idx: Index) -> None:
        # FIXME: Реализовать
        # Do not delete old files for now
        pass

    def insert_rows(self, df: pd.DataFrame) -> None:
        for i, data in zip(df.index, df.to_dict('records')):
            with fsspec.open(f'{self.path}/{i}{self.ext}', f'w{self.adapter.mode}+') as f:
                self.adapter.dump(data, f)

    def read_rows(self, idx: Optional[Index] = None) -> pd.DataFrame:
        if idx is None:
            idx = self._list_ids()
        
        def _gen():
            for i in idx:
                with fsspec.open(f'{self.path}/{i}{self.ext}', f'r{self.adapter.mode}') as f:
                    yield self.adapter.load(f)

        return pd.DataFrame.from_records(
            _gen(),
            index=idx
        )

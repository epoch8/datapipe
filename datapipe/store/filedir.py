from abc import ABC
from typing import IO, Optional, Any, Dict, List, Union
from pathlib import Path

import re
import json
import fsspec
import pandas as pd

from PIL import Image

from datapipe.store.types import Index
from datapipe.store.table_store import TableStore


class ItemStoreFileAdapter(ABC):
    mode: str

    def load(self, f: IO) -> Dict[str, Any]:
        raise NotImplementedError

    def dump(self, obj: Dict[str, Any], f: IO) -> None:
        raise NotImplementedError


class TextFile(ItemStoreFileAdapter):
    '''
    Reads file 
    '''

    mode = 't'

    def load(self, f: IO) -> Dict[str, Any]:
        return {'text': f.read()}
    
    def dump(self, obj: Dict[str, Any], f: IO) -> None:
        assert('text' in obj)
        f.write(obj['text'])


class JSONFile(ItemStoreFileAdapter):
    '''
    Converts each JSON file into Pandas record
    '''

    mode = 't'

    def load(self, f: IO) -> Dict[str, Any]:
        return json.load(f)

    def dump(self, obj: Dict[str, Any], f: IO) -> None:
        return json.dump(obj, f)


class PILFile(ItemStoreFileAdapter):
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
        im: Image.Image = obj['image']
        im.save(f, format=self.format)


def _pattern_to_attrnames(pat: str) -> List[str]:
    return re.findall(r'\{([^/]+?)\}', pat)


def _pattern_to_glob(pat: str) -> str:
    return re.sub(r'\{([^/]+?)\}', '*', pat)


def _pattern_to_match(pat: str) -> str:
    return re.sub(r'\{([^/]+?)\}', r'(?P<\1>[^/]+?)', pat)


class TableStoreFiledir(TableStore):
    def __init__(self, filename_pattern: Union[str, Path], adapter: ItemStoreFileAdapter):
        if isinstance(filename_pattern, Path):
            self.filename_pattern = str(filename_pattern.resolve())
        else:
            self.filename_pattern = str(filename_pattern)

        self.adapter = adapter

        # Другие схемы идентификации еще не реализованы
        assert(_pattern_to_attrnames(self.filename_pattern) == ['id'])

        self.filename_glob = _pattern_to_glob(self.filename_pattern)
        self.filename_match = _pattern_to_match(self.filename_pattern)

    def delete_rows(self, idx: Index) -> None:
        # FIXME: Реализовать
        # Do not delete old files for now
        pass

    def _filename(self, item_id: str) -> str:
        return re.sub(r'\{id\}', item_id, self.filename_pattern)

    def insert_rows(self, df: pd.DataFrame) -> None:
        for i, data in zip(df.index, df.to_dict('records')):
            filename = self._filename(i)

            with fsspec.open(filename, f'w{self.adapter.mode}+') as f:
                self.adapter.dump(data, f)

    def read_rows(self, idx: Optional[Index] = None) -> pd.DataFrame:
        if idx is None:
            idx = self.read_rows_meta_pseudo_df().index

        def _gen():
            for i in idx:
                with fsspec.open(self._filename(i), f'r{self.adapter.mode}') as f:
                    yield self.adapter.load(f)

        return pd.DataFrame.from_records(
            _gen(),
            index=idx
        )

    def read_rows_meta_pseudo_df(self, idx: Optional[Index] = None) -> pd.DataFrame:
        # Not implemented yet
        assert(idx is None)

        files = fsspec.open_files(self.filename_glob)

        ids = []
        rows = []

        for f in files:
            m = re.match(self.filename_match, f.path)
            assert(m is not None)

            ids.append(m.group('id'))

            rows.append(files.fs.info(f.path))

        if len(ids) > 0:
            pseudo_data_df = pd.DataFrame.from_records(
                rows,
                index=ids
            )

            return pseudo_data_df[['size', 'mtime']]
        else:
            return pd.DataFrame(
                {
                    'size': [],
                    'mtime': [],
                },
                index=pd.Series([], dtype=str)
            )

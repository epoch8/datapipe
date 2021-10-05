from abc import ABC
from typing import IO, Optional, Any, Dict, List, Union, cast
from pathlib import Path

import re
import json
import fsspec
import pandas as pd

from sqlalchemy import Column, String
from PIL import Image

from datapipe.types import DataDF, DataSchema, IndexDF, data_to_index
from datapipe.store.table_store import TableStore


class ItemStoreFileAdapter(ABC):
    mode: str

    def load(self, f: IO) -> Dict[str, Any]:
        raise NotImplementedError

    def dump(self, obj: Dict[str, Any], f: IO) -> None:
        raise NotImplementedError


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
    # TODO сделать трансформацию правильнее
    # * -> r'[^/]+'
    # ** -> r'([^/]+/)*?[^/]+'

    pat = re.sub(r'\*\*?', r'([^/]+/)*[^/]+', pat)
    pat = re.sub(r'\{([^/]+?)\}', r'(?P<\1>[^/]+?)', pat)
    return pat


class TableStoreFiledir(TableStore):
    def __init__(
        self,
        filename_pattern: Union[str, Path],
        adapter: ItemStoreFileAdapter,
        add_filepath_column: bool = False
    ):
        protocol, path = fsspec.core.split_protocol(filename_pattern)

        if protocol is None or protocol == 'file':
            self.filename_pattern = str(Path(path).resolve())
            filename_pattern_for_match = self.filename_pattern
            self.protocol_str = "" if protocol is None else "file://"
        else:
            self.filename_pattern = str(filename_pattern)
            filename_pattern_for_match = path
            self.protocol_str = f"{protocol}://"

        if '*' in path:
            self.readonly = True
        else:
            self.readonly = False

        self.adapter = adapter
        self.add_filepath_column = add_filepath_column

        # FIXME Другие схемы идентификации еще не реализованы
        assert(_pattern_to_attrnames(self.filename_pattern) == ['id'])

        self.filename_glob = _pattern_to_glob(self.filename_pattern)
        self.filename_match = _pattern_to_match(filename_pattern_for_match)

    def get_primary_schema(self) -> DataSchema:
        # FIXME реализовать поддержку других схем
        return [Column('id', String(100), primary_key=True)]

    def delete_rows(self, idx: IndexDF) -> None:
        # FIXME: Реализовать
        # Do not delete old files for now
        # Consider self.readonly as well
        assert(not self.readonly)

        pass

    def _filename(self, item_id: str) -> str:
        return re.sub(r'\{id\}', item_id, self.filename_pattern)

    def insert_rows(self, df: pd.DataFrame) -> None:
        assert(not self.readonly)

        for i, data in zip(df['id'], cast(List[Dict[str, Any]], df.to_dict('records'))):
            filename = self._filename(str(i))

            with fsspec.open(filename, f'w{self.adapter.mode}+') as f:
                self.adapter.dump(data, f)

    def read_rows(self, idx: IndexDF = None) -> DataDF:
        if idx is None:
            idx = data_to_index(self.read_rows_meta_pseudo_df(), self.primary_keys)

        def _gen():
            for i in idx['id']:
                with (file_open := fsspec.open(self._filename(i), f'r{self.adapter.mode}')) as f:
                    data = self.adapter.load(f)
                    data['id'] = i
                    if self.add_filepath_column:
                        data['filepath'] = f"{self.protocol_str}{file_open.path}"
                    yield data

        return pd.DataFrame.from_records(
            _gen()
        )

    def read_rows_meta_pseudo_df(self, idx: Optional[IndexDF] = None) -> DataDF:
        # Not implemented yet
        assert(idx is None)

        files = fsspec.open_files(self.filename_glob)

        ids = []
        ukeys = []

        for f in files:
            m = re.match(self.filename_match, f.path)
            assert(m is not None)

            ids.append(m.group('id'))

            ukeys.append(files.fs.ukey(f.path))

        if len(ids) > 0:
            pseudo_data_df = pd.DataFrame.from_records(
                {
                    'id': ids,
                    'ukey': ukeys,
                }
            )
            return pseudo_data_df
        else:
            return pd.DataFrame(
                {
                    'id': [],
                    'ukey': []
                }
            )

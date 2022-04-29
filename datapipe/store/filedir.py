from abc import ABC
from typing import IO, Any, Dict, List, Union, cast, Iterator
from pathlib import Path

import numpy as np
from iteration_utilities import duplicates

import re
import json
import fsspec
import pandas as pd

from sqlalchemy import Column, String
from PIL import Image

from datapipe.types import DataDF, DataSchema, MetaSchema, IndexDF
from datapipe.store.table_store import TableStore
from datapipe.run_config import RunConfig


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

    def __init__(self, **dump_params) -> None:
        self.dump_params = dump_params

    def load(self, f: IO) -> Dict[str, Any]:
        return json.load(f)

    def dump(self, obj: Dict[str, Any], f: IO) -> None:
        return json.dump(obj, f, **self.dump_params)


class PILFile(ItemStoreFileAdapter):
    '''
    Uses `image` column with PIL.Image for save/load
    '''

    mode = 'b'

    def __init__(self, format: str, **dump_params) -> None:
        self.format = format
        self.dump_params = dump_params

    def load(self, f: IO) -> Dict[str, Any]:
        im = Image.open(f)
        im.load()
        return {'image': im}

    def dump(self, obj: Dict[str, Any], f: IO) -> None:
        im: Image.Image = obj['image']
        im.save(f, format=self.format, **self.dump_params)


def _pattern_to_attrnames(pat: str) -> List[str]:
    attrnames = re.findall(r'\{([^/]+?)\}', pat)

    assert len(attrnames) > 0, "The scheme is not valid."
    if len(attrnames) >= 2:
        duplicates_attrnames = list(duplicates(attrnames))
        assert len(duplicates_attrnames) == 0, f"Some keys are repeated: {duplicates_attrnames}. Rename them."

    return attrnames


def _pattern_to_glob(pat: str) -> str:
    return re.sub(r'\{([^/]+?)\}', '*', pat)


def _pattern_to_match(pat: str) -> str:
    # TODO сделать трансформацию правильнее
    # * -> r'[^/]+'
    # ** -> r'([^/]+/)*?[^/]+'

    pat = re.sub(r'\*\*?', r'([^/]+/)*[^/]+', pat)
    pat = re.sub(r'\{([^/]+?)\}', r'(?P<\1>[^/]+?)', pat)
    return pat


class Replacer:
    def __init__(self, values: List[str]):
        self.counter = -1
        self.values = values

    def __call__(self, matchobj):
        self.counter += 1
        return str(self.values[self.counter])


class TableStoreFiledir(TableStore):
    def __init__(
        self,
        filename_pattern: Union[str, Path],
        adapter: ItemStoreFileAdapter,
        add_filepath_column: bool = False,
        primary_schema: DataSchema = None,
        read_data: bool = True
    ):
        """
        При построении `TableStoreFiledir` есть два способа указать схему
        индексов:

        1. Явный - в конструктор передается `primary_schema`, которая должна
           содержать все поля, упоминаемые в `filename_pattern`
        2. Неявный - `primary_schema` = `None`, тогда все поля получают
           дефолтный тип `String(100)`

        Args:

        filename_pattern -- Путь к файлам в формате fsspec (но без chaining),
        может содержать два типа шаблонов:
          - {id_field} - поле из индекса учитывается как при чтении так и при
            записи
          - * - не попадает в индекс, и стор работает режиме "только чтение"

        primary_schema -- дает возможность в явном виде задать типы полей,
        который упоминаются в filename_pattern

        adapter -- объект отвечающий за преобразование содержимого файла в
        структурированную запись и обратно

        add_filepath_column -- если True - в объект добавляется поле filepath с
        адресом файла

        read_data -- если False - при чтении не происходит парсинга содержимого
        файла
        """

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
        self.read_data = read_data

        self.attrnames = _pattern_to_attrnames(self.filename_pattern)
        self.filename_glob = _pattern_to_glob(self.filename_pattern)
        self.filename_match = _pattern_to_match(filename_pattern_for_match)

        if primary_schema is not None:
            assert sorted(self.attrnames) == sorted(i.name for i in primary_schema)

            self.primary_schema = primary_schema
        else:
            self.primary_schema = [
                Column(attrname, String(100), primary_key=True)
                for attrname in self.attrnames
            ]

    def get_primary_schema(self) -> DataSchema:
        return self.primary_schema

    def get_meta_schema(self) -> MetaSchema:
        return []

    def delete_rows(self, idx: IndexDF) -> None:
        # FIXME: Реализовать
        # Do not delete old files for now
        # Consider self.readonly as well
        assert(not self.readonly)

        pass

    def _filename_from_idxs_values(self, idxs_values: List[str]) -> str:
        return re.sub(r'\{([^/]+?)\}', Replacer(idxs_values), self.filename_pattern)

    def _assert_key_values(self, filepath: str, idxs_values: List[str]):
        _, filepath = fsspec.core.split_protocol(filepath)
        m = re.match(self.filename_match, filepath)

        idxs_values_np = np.array(idxs_values)
        idxs_values_parsed_from_filepath = np.array([
            m.group(attrname) for attrname in self.attrnames
            if m is not None
        ])

        assert (
            len(idxs_values_np) == len(idxs_values_parsed_from_filepath) and

            np.all(idxs_values_np == idxs_values_parsed_from_filepath)
        ), (
            "Multiply indexes have complex contradictory values, so that it couldn't unambiguously name the files. "
            "This is most likely due to imperfect separators between {id} keys in the scheme."
        )

    def insert_rows(self, df: pd.DataFrame) -> None:
        if df.empty:
            return

        assert(not self.readonly)

        # WARNING: Здесь я поставил .drop(columns=self.attrnames), тк ключи будут хранится снаружи, в имени
        for row_idx, data in zip(
            df.index, cast(List[Dict[str, Any]], df.drop(columns=self.attrnames).to_dict('records'))
        ):
            idxs_values = df.loc[row_idx, self.attrnames].tolist()
            filepath = self._filename_from_idxs_values(idxs_values)

            # Проверяем, что значения ключей не приведут к неоднозначному результату при парсинге регулярки
            self._assert_key_values(filepath, idxs_values)

            with fsspec.open(filepath, f'w{self.adapter.mode}') as f:
                self.adapter.dump(data, f)

    def read_rows(self, idx: IndexDF = None) -> DataDF:
        assert(idx is not None)

        def _gen():
            for row_idx in idx.index:
                with (file_open := fsspec.open(self._filename_from_idxs_values(idx.loc[row_idx, self.attrnames]),
                                               f'r{self.adapter.mode}')) as f:
                    data = {}

                    if self.read_data:
                        data = self.adapter.load(f)

                        attrnames_in_data = [attrname for attrname in self.attrnames if attrname in data]
                        assert len(attrnames_in_data) == 0, (
                            f"Found repeated keys inside data that are already used (from scheme): "
                            f"{attrnames_in_data}. "
                            f"Remove these keys from data."
                        )

                    for attrname in self.attrnames:
                        data[attrname] = idx.loc[row_idx, attrname]

                    if self.add_filepath_column:
                        assert 'filepath' not in data, (
                            "The key 'filepath' is already exists in data. "
                            "Switch argument add_filepath_column to False or rename this key in input data."
                        )
                        data['filepath'] = f"{self.protocol_str}{file_open.path}"

                    yield data

        df = pd.DataFrame.from_records(
            _gen()
        )
        if df.empty:
            df = pd.DataFrame(columns=self.primary_keys)
        return df

    def read_rows_meta_pseudo_df(self, chunksize: int = 1000, run_config: RunConfig = None) -> Iterator[DataDF]:
        # FIXME реализовать чанкирование

        files = fsspec.open_files(self.filename_glob)

        ids: Dict[str, List[str]] = {
            attrname: []
            for attrname in self.attrnames
        }
        ukeys = []

        for f in files:
            m = re.match(self.filename_match, f.path)

            assert(m is not None)
            for attrname in self.attrnames:
                ids[attrname].append(m.group(attrname))

            ukeys.append(files.fs.ukey(f.path))

        if len(ids) > 0:
            pseudo_data_df = pd.DataFrame.from_records(
                {
                    **ids,
                    'ukey': ukeys,
                }
            )
            yield pseudo_data_df
        else:
            yield pd.DataFrame(
                {
                    **ids,
                    'ukey': []
                }
            )

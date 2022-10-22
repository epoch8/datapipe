import itertools
import json
import re
from abc import ABC
from pathlib import Path
from typing import IO, Any, Dict, Iterator, List, Optional, Union, cast

import fsspec
import numpy as np
import pandas as pd
from iteration_utilities import duplicates
from PIL import Image
from sqlalchemy import Column, Integer, String

from datapipe.run_config import RunConfig
from datapipe.store.table_store import TableStore
from datapipe.types import DataDF, DataSchema, IndexDF, MetaSchema


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


def _pattern_to_patterns_or(pat) -> List[str]:
    pattern_or = re.compile(r'(?P<or>\(([a-zA-Z0-9]+\|)+[a-zA-Z0-9]+\))')
    # Ищем вхождения вида (aaa|bbb|ccc), в виду list of list [[aaa, bbb, ccc], [ddd, eee], ...]
    values = [
        list(dict.fromkeys(match.group('or')[1:-1].split('|')))
        for match in pattern_or.finditer(pat)
    ]
    # Всевозможные комбинации для замены [[aaa, ddd], [aaa, eee], [bbb, ddd], ...]
    possible_combinatios_values = [list(combination) for combination in itertools.product(*values)]
    # Получаем всевозможные списки шаблонов из комбинаций
    filename_patterns = [
        re.sub(pattern_or, Replacer(combination), pat)
        for combination in possible_combinatios_values
    ]
    return filename_patterns


def _pattern_to_glob(pat: str) -> str:
    return re.sub(r'\{([^/]+?)\}', '*', pat)  # Меняем все вхождения {id1}_{id2} в звездочки *_*


def _pattern_to_match(pat: str) -> str:
    # TODO сделать трансформацию правильнее
    # * -> r'[^/]+'
    # ** -> r'([^/]+/)*?[^/]+'

    pat = re.sub(r'\*\*?', r'([^/]+/)*[^/]+', pat)  # Меняем все вхождения * и ** в произвольные символы
    pat = re.sub(r'\{([^/]+?)\}', r'(?P<\1>[^/]+?)', pat)  # Меняем все вхождения вида {id} на непустые послед. символов
    pat = f"{pat}\\Z"  # Учитываем конец строки
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
        read_data: bool = True,
        readonly: Optional[bool] = None,
        enable_rm: bool = False
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

        readonly -- если True, отключить запись файлов; если None, то запись файлов включается в случае,
        если нету * и ** путей в шаблоне и нет множественных расширений файлов вида (jpg|png|mp4)

        enable_rm -- если True, включить удаление файлов
        """

        self.protocol, path = fsspec.core.split_protocol(filename_pattern)
        self.filesystem = fsspec.filesystem(self.protocol)

        if self.protocol is None or self.protocol == 'file':
            filename_pattern = str(Path(path).resolve())
            filename_pattern_for_match = filename_pattern
            self.protocol_str = "" if self.protocol is None else "file://"
        else:
            filename_pattern = str(filename_pattern)
            filename_pattern_for_match = path
            self.protocol_str = f"{self.protocol}://"

        self.filename_patterns = _pattern_to_patterns_or(filename_pattern)
        self.attrnames = _pattern_to_attrnames(filename_pattern)
        self.filename_glob = [_pattern_to_glob(pat) for pat in self.filename_patterns]
        self.filename_match = _pattern_to_match(filename_pattern_for_match)

        # Multiply extensions check
        if len(self.filename_glob) >= 2:
            if readonly is not None and not readonly:
                raise ValueError(
                    "When `readonly=False`, in filename_pattern shouldn't be several extensions."
                )
            elif readonly:
                readonly = True
        # Any * and ** pattern check
        if '*' in path:
            if readonly is not None and not readonly:
                raise ValueError(
                    "When `readonly=False`, in filename_pattern shouldn't be any `*` characters."
                )
            elif readonly is None:
                readonly = True
        elif readonly is None:
            readonly = False

        self.readonly = readonly
        self.enable_rm = enable_rm

        self.adapter = adapter
        self.add_filepath_column = add_filepath_column
        self.read_data = read_data

        type_to_cls = {
            String: str,
            Integer: int
        }

        if primary_schema is not None:
            assert sorted(self.attrnames) == sorted(i.name for i in primary_schema)
            assert all([isinstance(column.type, (String, Integer)) for column in primary_schema])
            self.primary_schema = primary_schema
        else:
            self.primary_schema = [
                Column(attrname, String(100), primary_key=True)
                for attrname in self.attrnames
            ]
        self.attrname_to_cls = {
            column.name: type_to_cls[type(column.type)]
            for column in self.primary_schema
        }

    def get_primary_schema(self) -> DataSchema:
        return self.primary_schema

    def get_meta_schema(self) -> MetaSchema:
        return []

    def delete_rows(self, idx: IndexDF) -> None:
        if not self.enable_rm:
            return

        assert not self.readonly

        for row_idx in idx.index:
            attrnames_series = idx.loc[row_idx, self.attrnames]
            assert isinstance(attrnames_series, pd.Series)

            _, path = fsspec.core.split_protocol(
                self._filenames_from_idxs_values(attrnames_series.tolist())[0]
            )
            self.filesystem.rm(path)

    def _filenames_from_idxs_values(self, idxs_values: List[str]) -> List[str]:
        return [
            re.sub(r'\{([^/]+?)\}', Replacer(idxs_values), pat) for pat in self.filename_patterns
        ]

    def _idxs_values_from_filepath(self, filepath: str) -> Dict[str, Any]:
        _, filepath = fsspec.core.split_protocol(filepath)
        m = re.match(self.filename_match, filepath)
        assert m is not None, f"Filepath {filepath} does not match the pattern {self.filename_match}"

        data = {}
        for attrname in self.attrnames:
            data[attrname] = self.attrname_to_cls[attrname](m.group(attrname))

        return data

    def _assert_key_values(self, filepath: str, idxs_values: List[str]):
        idx_data = self._idxs_values_from_filepath(filepath)
        idxs_values_np = np.array(idxs_values)
        idxs_values_parsed_from_filepath = np.array(
            [idx_data[attrname] for attrname in self.attrnames]
        )

        assert (
            len(idxs_values_np) == len(idxs_values_parsed_from_filepath) and

            np.all(idxs_values_np == idxs_values_parsed_from_filepath)
        ), (
            "Multiply indexes have complex contradictory values, so that it couldn't unambiguously name the files. "
            "This is most likely due to imperfect separators between {id} keys in the scheme or "
            " idxs types differences. ", f"{idxs_values_np=} not equals {idxs_values_parsed_from_filepath=}"
        )

    def insert_rows(self, df: pd.DataFrame, adapter: Optional[ItemStoreFileAdapter] = None) -> None:
        if df.empty:
            return
        assert not self.readonly
        if adapter is None:
            adapter = self.adapter

        # WARNING: Здесь я поставил .drop(columns=self.attrnames), тк ключи будут хранится снаружи, в имени
        for row_idx, data in zip(
            df.index, cast(List[Dict[str, Any]], df.drop(columns=self.attrnames).to_dict('records'))
        ):
            attrnames_series = df.loc[row_idx, self.attrnames]
            assert isinstance(attrnames_series, pd.Series)

            idxs_values = attrnames_series.tolist()
            filepath = self._filenames_from_idxs_values(idxs_values)[0]

            # Проверяем, что значения ключей не приведут к неоднозначному результату при парсинге регулярки
            self._assert_key_values(filepath, idxs_values)

            with fsspec.open(filepath, f'w{self.adapter.mode}') as f:
                self.adapter.dump(data, f)

    def read_rows(
        self,
        idx: IndexDF = None,
        read_data: Optional[bool] = None,
        adapter: Optional[ItemStoreFileAdapter] = None
    ) -> DataDF:

        if read_data is None:
            read_data = self.read_data
        if adapter is None:
            adapter = self.adapter

        def _iterate_files():
            if idx is None:
                for file_open in fsspec.open_files(self.filename_glob, f'r{adapter.mode}'):
                    yield file_open
            else:
                filepaths_extenstions = [
                    self._filenames_from_idxs_values(idx.loc[row_idx, self.attrnames])
                    for row_idx in idx.index
                ]
                for filepaths in filepaths_extenstions:
                    found_files = [
                        file_open
                        for file_open in fsspec.open_files(filepaths, f'r{adapter.mode}')
                        if self.filesystem.exists(file_open.path)
                    ]
                    if len(found_files) == 0:
                        raise FileNotFoundError(f"No such file: {' or '.join(filepaths)}")
                    elif len(found_files) > 1:
                        raise ValueError(
                            f"Some files are duplitcated as indexes in filepaths: {found_files}. "
                            "Change the pattern or delete them."
                        )
                    for file_open in found_files:
                        yield file_open

        df_records = []
        for file_open in _iterate_files():
            with file_open as f:
                data = {}

                if read_data:
                    data = adapter.load(f)

                    attrnames_in_data = [attrname for attrname in self.attrnames if attrname in data]
                    assert len(attrnames_in_data) == 0, (
                        f"Found repeated keys inside data that are already used (from scheme): "
                        f"{attrnames_in_data}. "
                        f"Remove these keys from data."
                    )

                idxs_values = self._idxs_values_from_filepath(file_open.path)
                data.update(idxs_values)

                if self.add_filepath_column:
                    assert 'filepath' not in data, (
                        "The key 'filepath' is already exists in data. "
                        "Switch argument add_filepath_column to False or rename this key in input data."
                    )
                    data['filepath'] = f"{self.protocol_str}{file_open.path}"

                df_records.append(data)

        df = pd.DataFrame(df_records)

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
        filepaths = []

        for f in files:
            m = re.match(self.filename_match, f.path)

            assert m is not None
            for attrname in self.attrnames:
                ids[attrname].append(m.group(attrname))

            ukeys.append(files.fs.ukey(f.path))
            filepaths.append(f"{self.protocol_str}{f.path}")

        keys_values = [
            (ids[attrname][i] for attrname in self.attrnames)
            for i in range(len(ukeys))
        ]
        duplicates_keys_values = list(duplicates(keys_values))
        assert len(duplicates_keys_values) == 0, (
            f"Some files are duplitcated as indexes in filepaths: {duplicates_keys_values}. "
            "Change the pattern or delete them."
        )

        if len(ids) > 0:
            pseudo_data_df = pd.DataFrame.from_records(
                {
                    **ids,
                    'ukey': ukeys,
                    **({'filepath': filepaths} if self.add_filepath_column else {})
                }
            )
            yield pseudo_data_df.astype(object)
        else:
            filepath_kw: Dict = {'filepath': []} if self.add_filepath_column else {}
            yield pd.DataFrame(
                {
                    'ukey': [],
                    **filepath_kw,
                }
            ).astype(object)

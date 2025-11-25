import base64
import io
import itertools
import json
import re
from abc import ABC, abstractmethod
from pathlib import Path
from typing import IO, Any, Dict, Iterator, List, Literal, Optional, Union, cast

import cityhash
import fsspec
import numpy as np
import pandas as pd
from PIL import Image
from sqlalchemy import Column, Integer, String

from datapipe.run_config import RunConfig
from datapipe.store.table_store import TableStore, TableStoreCaps
from datapipe.types import DataDF, DataSchema, HashDF, IndexDF, MetaSchema


class ItemStoreFileAdapter(ABC):
    mode: str

    @abstractmethod
    def load(self, f: IO) -> Dict[str, Any]: ...

    @abstractmethod
    def dump(self, obj: Dict[str, Any], f: IO) -> None: ...

    @abstractmethod
    def hash_rows(self, df: DataDF, keys: List[str]) -> HashDF: ...


class JSONFile(ItemStoreFileAdapter):
    """
    Converts each JSON file into Pandas record
    """

    mode = "t"

    def __init__(self, **dump_params) -> None:
        self.dump_params = dump_params

    def load(self, f: IO) -> Dict[str, Any]:
        return json.load(f)

    def dump(self, obj: Dict[str, Any], f: IO) -> None:
        return json.dump(obj, f, **self.dump_params)

    def hash_rows(self, df: DataDF, keys: List[str]) -> HashDF:
        hash_df = df[keys]
        hash_df["hash"] = df.apply(lambda x: str(list(x)), axis=1).apply(
            lambda x: int.from_bytes(cityhash.CityHash32(x).to_bytes(4, "little"), "little", signed=True)
        )

        return cast(HashDF, hash_df)


class BytesFile(ItemStoreFileAdapter):
    """
    Uses `bytes` column
    """

    mode = "b"

    def __init__(self, bytes_columns: str = "bytes"):
        self.bytes_columns = bytes_columns

    def hash_rows(self, df: DataDF, keys: List[str]) -> HashDF:
        data_df = df.copy()
        hash_df = df[keys]

        if self.bytes_columns in df.columns:
            data_df[self.bytes_columns] = data_df[self.bytes_columns].apply(
                lambda x: int.from_bytes(cityhash.CityHash32(x).to_bytes(4, "little"), "little", signed=True)
            )

        hash_df["hash"] = data_df.apply(lambda x: str(list(x)), axis=1).apply(
            lambda x: int.from_bytes(cityhash.CityHash32(x).to_bytes(4, "little"), "little", signed=True)
        )

        return cast(HashDF, hash_df)

    def load(self, f: IO) -> Dict[str, Any]:
        return {self.bytes_columns: f.read()}

    def dump(self, obj: Dict[str, Any], f: IO) -> None:
        f.write(obj[self.bytes_columns])


class PILFile(ItemStoreFileAdapter):
    """
    Uses `image` column with PIL.Image for save/load
    """

    mode = "b"

    def __init__(self, format: str, image_column: str = "image", **dump_params) -> None:
        self.format = format
        self.dump_params = dump_params
        self.image_column = image_column

    def hash_rows(self, df: DataDF, keys: List[str]) -> HashDF:
        data_df = df.copy()
        hash_df = df[keys]

        if self.image_column in df.columns:
            data_df[self.image_column] = data_df[self.image_column].apply(
                lambda x: int.from_bytes(cityhash.CityHash32(x.tobytes()).to_bytes(4, "little"), "little", signed=True)
            )

        hash_df["hash"] = data_df.apply(lambda x: str(list(x)), axis=1).apply(
            lambda x: int.from_bytes(cityhash.CityHash32(x).to_bytes(4, "little"), "little", signed=True)
        )

        return cast(HashDF, hash_df)

    def load(self, f: IO) -> Dict[str, Any]:
        im = Image.open(f)
        im.load()
        return {self.image_column: im}

    def dump(self, obj: Dict[str, Any], f: IO) -> None:
        image_data: Any = obj[self.image_column]

        if isinstance(image_data, Image.Image):
            image: Image.Image = image_data
        elif isinstance(image_data, np.ndarray):
            image = Image.fromarray(image_data)
        elif isinstance(image_data, str):
            image_binary = base64.b64decode(image_data.encode())
            image = Image.open(io.BytesIO(image_binary))
        else:
            raise Exception("Image must be a bytes string or np.array or Pillow Image object")

        image.save(f, format=self.format, **self.dump_params)


def duplicates(lst: List[Any]) -> Iterator[Any]:
    seen = set()
    for item in lst:
        if item in seen:
            yield item
        else:
            seen.add(item)


class PandasParquetFile(ItemStoreFileAdapter):
    """
    Uses `data` column to store Pandas DataFrame in parquet file
    """

    mode = "b"

    def __init__(
        self,
        pandas_column: str = "data",
        engine: Literal["auto", "pyarrow", "fastparquet"] = "auto",
        compression: Literal["snappy", "gzip", "brotli", "lz4", "zstd"] = "snappy",
    ):
        self.pandas_column = pandas_column
        self.engine = engine
        self.compression = compression

    def hash_rows(self, df: DataDF, keys: List[str]) -> HashDF:
        data_df = df.copy()
        hash_df = df[keys]

        if self.pandas_column in df.columns:
            data_df[self.pandas_column] = data_df[self.pandas_column].apply(
                lambda x: self.hash_df(cast(pd.DataFrame, x))
            )

        hash_df["hash"] = data_df.apply(lambda x: str(list(x)), axis=1).apply(
            lambda x: int.from_bytes(cityhash.CityHash32(x).to_bytes(4, "little"), "little", signed=True)
        )

        return cast(HashDF, hash_df)

    def hash_df(self, df: pd.DataFrame) -> str:
        return str(pd.util.hash_pandas_object(df).values)

    def load(self, f: IO) -> Dict[str, Any]:
        return {
            self.pandas_column: pd.read_parquet(
                f,
                engine=self.engine,  # type: ignore  ## pylance is wrong here
            ),
        }

    def dump(self, obj: Dict[str, Any], f: IO) -> None:
        df = cast(pd.DataFrame, obj[self.pandas_column])

        df.to_parquet(
            f,
            engine=self.engine,  # type: ignore  ## pylance is wrong here
            compression=self.compression,  # type: ignore  ## pylance is wrong here
        )


def _pattern_to_attrnames(pat: str) -> List[str]:
    attrnames = re.findall(r"\{([^/]+?)\}", pat)

    assert len(attrnames) > 0, "The scheme is not valid."
    if len(attrnames) >= 2:
        duplicates_attrnames = list(duplicates(attrnames))
        assert len(duplicates_attrnames) == 0, f"Some keys are repeated: {duplicates_attrnames}. Rename them."

    return attrnames


def _pattern_to_patterns_or(pat) -> List[str]:
    pattern_or = re.compile(r"(?P<or>\(([a-zA-Z0-9]+\|)+[a-zA-Z0-9]+\))")
    # Ищем вхождения вида (aaa|bbb|ccc), в виду list of list [[aaa, bbb, ccc], [ddd, eee], ...]
    values = [list(dict.fromkeys(match.group("or")[1:-1].split("|"))) for match in pattern_or.finditer(pat)]
    # Всевозможные комбинации для замены [[aaa, ddd], [aaa, eee], [bbb, ddd], ...]
    possible_combinatios_values = [list(combination) for combination in itertools.product(*values)]
    # Получаем всевозможные списки шаблонов из комбинаций
    filename_patterns = [re.sub(pattern_or, Replacer(combination), pat) for combination in possible_combinatios_values]
    return filename_patterns


def _pattern_to_glob(pat: str) -> str:
    return re.sub(r"\{([^/]+?)\}", "*", pat)  # Меняем все вхождения {id1}_{id2} в звездочки *_*


def _pattern_to_match(pat: str) -> str:
    # TODO сделать трансформацию правильнее
    # * -> r'[^/]+'
    # ** -> r'([^/]+/)*?[^/]+'

    pat = re.sub(r"\*\*?", r"([^/]+/)*[^/]+", pat)  # Меняем все вхождения * и ** в произвольные символы
    pat = re.sub(r"\{([^/]+?)\}", r"(?P<\1>[^/]+?)", pat)  # Меняем все вхождения вида {id} на непустые послед. символов
    pat = f"{pat}\\Z"  # Учитываем конец строки
    return pat


class Replacer:
    def __init__(self, values: List[str]):
        self.counter = -1
        self.values = list(values)

    def __call__(self, matchobj):
        self.counter += 1
        return str(self.values[self.counter])


class TableStoreFiledir(TableStore):
    caps = TableStoreCaps(
        supports_delete=True,
        supports_get_schema=False,
        supports_read_all_rows=True,
        supports_read_nonexistent_rows=False,
        supports_read_meta_pseudo_df=True,
    )

    def __init__(
        self,
        filename_pattern: Union[str, Path],
        adapter: ItemStoreFileAdapter,
        add_filepath_column: bool = False,
        primary_schema: Optional[DataSchema] = None,
        read_data: bool = True,
        readonly: Optional[bool] = None,
        enable_rm: bool = False,
        fsspec_kwargs: Optional[Dict[str, Any]] = None,
    ):
        """
        При построении `TableStoreFiledir` есть два способа указать схему
        индексов:

        1. Явный - в конструктор передается `primary_schema`, которая должна
           содержать все поля, упоминаемые в `filename_pattern`
        2. Неявный - `primary_schema` = `None`, тогда все поля получают
           дефолтный тип `String`

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

        readonly -- если True, отключить запись файлов; если None, то запись
        файлов включается в случае, если нету * и ** путей в шаблоне. Если есть
        множественные суффиксы файлов вида (jpg|png|mp4), то берется файл с
        первым попавшимся слева направо суффиксом

        enable_rm -- если True, включить удаление файлов. если есть
        множественные суффиксы файлов вида (jpg|png|mp4), то удаляется каждый из
        них

        fsspec_kwargs -- kwargs для fsspec
        """

        self.fsspec_kwargs = fsspec_kwargs or {}
        self.protocol, path = fsspec.core.split_protocol(filename_pattern)
        if "protocol" in self.fsspec_kwargs:
            self.protocol = self.fsspec_kwargs["protocol"]
        else:
            self.fsspec_kwargs["protocol"] = self.protocol

        if self.protocol == "file" or self.protocol is None:
            self.fsspec_kwargs["auto_mkdir"] = True

        self.filesystem = fsspec.filesystem(**self.fsspec_kwargs)

        if self.protocol is None or self.protocol == "file":
            filename_pattern = str(Path(path).resolve())
            filename_pattern_for_match = filename_pattern
            self.protocol_str = "" if self.protocol is None else "file://"
        else:
            filename_pattern = str(filename_pattern)
            filename_pattern_for_match = path
            if self.protocol in ["gdrivefs"]:
                self.protocol_str = ""
            else:
                self.protocol_str = f"{self.protocol}://"

        self.filename_patterns = _pattern_to_patterns_or(filename_pattern)
        self.attrnames = _pattern_to_attrnames(filename_pattern)
        self.filename_glob = [_pattern_to_glob(pat) for pat in self.filename_patterns]
        self.filename_match = _pattern_to_match(filename_pattern_for_match)
        self.filename_match_first_suffix = _pattern_to_match(self.filename_patterns[0])

        # Any * and ** pattern check
        if "*" in path:
            if readonly is not None and not readonly:
                raise ValueError("When `readonly=False`, in filename_pattern shouldn't be any `*` characters.")
            elif readonly is None:
                readonly = True
        elif readonly is None:
            readonly = False

        self.readonly = readonly
        self.enable_rm = enable_rm

        self.adapter = adapter
        self.add_filepath_column = add_filepath_column
        self.read_data = read_data

        type_to_cls = {String: str, Integer: int}

        if primary_schema is not None:
            assert sorted(self.attrnames) == sorted(i.name for i in primary_schema)
            assert all([isinstance(column.type, (String, Integer)) for column in primary_schema])
            self.primary_schema = primary_schema
        else:
            self.primary_schema = [Column(attrname, String, primary_key=True) for attrname in self.attrnames]
        self.attrname_to_cls = {
            column.name: type_to_cls[type(column.type)]
            for column in self.primary_schema  # type: ignore
        }

    def get_primary_schema(self) -> DataSchema:
        return self.primary_schema

    def get_meta_schema(self) -> MetaSchema:
        return []

    def hash_rows(self, df: DataDF) -> HashDF:
        return self.adapter.hash_rows(df, self.hash_keys)

    def delete_rows(self, idx: IndexDF) -> None:
        if not self.enable_rm:
            return

        assert not self.readonly

        for row_idx in idx.index:
            attrnames_series = idx.loc[row_idx, self.attrnames]
            assert isinstance(attrnames_series, pd.Series)

            attrnames = cast(List[str], attrnames_series.tolist())

            # Удаляем каждый файл с разными суффиксами, если они есть
            for filepath in self._filenames_from_idxs_values(attrnames):
                _, path = fsspec.core.split_protocol(filepath)
                if self.filesystem.exists(path):
                    self.filesystem.rm(path)

    def _filenames_from_idxs_values(self, idxs_values: List[str]) -> List[str]:
        """
        Возвращает по индексам список всевозможных путей согласно шаблону
        Например для шаблона filedir/{id}.(jpg|png) и индекса id=0
          ['filedir/0.jpg', 'filedir/0.png']
        """
        return [re.sub(r"\{([^/]+?)\}", Replacer(idxs_values), pat) for pat in self.filename_patterns]

    def _idxs_values_from_filepath(self, filepath: str) -> Dict[str, Any]:
        """
        По пути возвращает список индексов согласно шаблону
        """
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
        idxs_values_parsed_from_filepath = np.array([idx_data[attrname] for attrname in self.attrnames])

        assert len(idxs_values_np) == len(idxs_values_parsed_from_filepath) and np.all(
            idxs_values_np == idxs_values_parsed_from_filepath
        ), (
            "Multiply indexes have complex contradictory values, so that it couldn't unambiguously name the files. "
            "This is most likely due to imperfect separators between {id} keys in the scheme or "
            " idxs types differences. ",
            f"{idxs_values_np=} not equals {idxs_values_parsed_from_filepath=}",
        )

    def insert_rows(self, df: pd.DataFrame, adapter: Optional[ItemStoreFileAdapter] = None) -> None:
        if df.empty:
            return
        assert not self.readonly
        if adapter is None:
            adapter = self.adapter

        # WARNING: Здесь я поставил .drop(columns=self.attrnames), тк ключи будут хранится снаружи, в имени
        for row_idx, data in zip(
            df.index,
            cast(List[Dict[str, Any]], df.drop(columns=self.attrnames).to_dict("records")),
        ):
            attrnames_series = df.loc[row_idx, self.attrnames]
            assert isinstance(attrnames_series, pd.Series)

            idxs_values = attrnames_series.tolist()
            filepath = self._filenames_from_idxs_values(idxs_values)[0]  # берем первый суффикс

            # Проверяем, что значения ключей не приведут к неоднозначному результату при парсинге регулярки
            self._assert_key_values(filepath, idxs_values)

            with self.filesystem.open(filepath, f"w{self.adapter.mode}") as f:
                self.adapter.dump(data, f)

    def _read_rows_fast(
        self,
        idx: IndexDF,
    ) -> DataDF:
        res = idx.copy()
        res["filepath"] = None

        for row_idx in idx.index:
            attrnames_series = idx.loc[row_idx, self.attrnames]
            assert isinstance(attrnames_series, pd.Series)

            attrnames = cast(List[str], attrnames_series.tolist())

            _, path = fsspec.core.split_protocol(self._filenames_from_idxs_values(attrnames)[0])

            res.loc[row_idx, "filepath"] = f"{self.protocol_str}{path}"

        return res

    def read_rows(
        self,
        idx: Optional[IndexDF] = None,
        read_data: Optional[bool] = None,
        adapter: Optional[ItemStoreFileAdapter] = None,
    ) -> DataDF:
        if read_data is None:
            read_data = self.read_data
        if adapter is None:
            adapter = self.adapter

        if (not read_data) and (len(self.filename_patterns) == 1) and (idx is not None) and self.add_filepath_column:
            return self._read_rows_fast(idx)

        def _iterate_files():
            if idx is None:
                for file_open in fsspec.open_files(self.filename_glob, f"r{adapter.mode}", **self.fsspec_kwargs):
                    yield file_open
            else:
                filepaths_extenstions = [
                    # TODO fix typing
                    self._filenames_from_idxs_values(idx.loc[row_idx, self.attrnames])  # type: ignore
                    for row_idx in idx.index
                ]
                for filepaths in filepaths_extenstions:
                    found_files = [
                        file_open
                        for file_open in fsspec.open_files(filepaths, f"r{adapter.mode}", **self.fsspec_kwargs)
                        if self.filesystem.exists(file_open.path)
                    ]
                    if len(found_files) == 0:
                        raise FileNotFoundError(f"No such file: {' or '.join(filepaths)}")
                    # Открываем первый попавшися файл согласно суффиксу (aaa|bbb)
                    file_open = found_files[0]
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
                    assert "filepath" not in data, (
                        "The key 'filepath' is already exists in data. "
                        "Switch argument add_filepath_column to False or rename this key in input data."
                    )
                    data["filepath"] = f"{self.protocol_str}{file_open.path}"

                df_records.append(data)

        df = pd.DataFrame(df_records)

        if df.empty:
            df = pd.DataFrame(columns=self.primary_keys)

        return df

    def read_rows_meta_pseudo_df(
        self, chunksize: int = 1000, run_config: Optional[RunConfig] = None
    ) -> Iterator[DataDF]:
        # FIXME реализовать чанкирование

        files = fsspec.open_files(self.filename_glob, **self.fsspec_kwargs)

        ids: Dict[str, List[str]] = {attrname: [] for attrname in self.attrnames}
        ukeys = []
        filepaths = []

        for f in files:
            m = re.match(self.filename_match_first_suffix, f.path)

            if m is None:
                continue

            for attrname in self.attrnames:
                ids[attrname].append(m.group(attrname))

            ukeys.append(files.fs.ukey(f.path))  # type: ignore
            filepaths.append(f"{self.protocol_str}{f.path}")

        if len(ids) > 0:
            pseudo_data_df = pd.DataFrame.from_records(
                {
                    **ids,
                    "ukey": ukeys,
                    **({"filepath": filepaths} if self.add_filepath_column else {}),
                }
            )

            yield pseudo_data_df.astype(object)  # type: ignore # TODO fix typing issue
        else:
            filepath_kw: Dict = {"filepath": []} if self.add_filepath_column else {}
            yield pd.DataFrame({"ukey": [], **filepath_kw}).astype(object)  # type: ignore # TODO fix typing issue

from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterable, Iterator, List, Literal, Optional, Sequence, Tuple

from datapipe.run_config import RunConfig
from datapipe.types import ChangeList, DataSchema, HashDF, IndexDF, MetadataDF, MetaSchema

if TYPE_CHECKING:
    from datapipe.compute import ComputeInput
    from datapipe.datatable import DataStore, DataTable


class MetaPlane:
    """
    Control and factory for TableMeta and TransformMeta objects.
    """

    def create_table_meta(
        self,
        name: str,
        primary_schema: DataSchema,
        meta_schema: MetaSchema,
    ) -> "TableMeta":
        # TODO docstring

        raise NotImplementedError()

    def create_transform_meta(
        self,
        name: str,
        input_dts: Sequence["ComputeInput"],
        output_dts: Sequence["DataTable"],
        transform_keys: Optional[List[str]] = None,
        order_by: Optional[List[str]] = None,
        order: Literal["asc", "desc"] = "asc",
    ) -> "TransformMeta":
        # TODO docstring
        """
        We expect that TableMeta for input_dts and output_dts are from the same
        MetaPlane instance as this TransformMeta.
        """

        raise NotImplementedError()


@dataclass
class TableDebugInfo:
    name: str
    size: int


class TableMeta:
    primary_schema: DataSchema
    primary_keys: List[str]

    def get_metadata(self, idx: Optional[IndexDF] = None, include_deleted: bool = False) -> MetadataDF:
        """
        Получить датафрейм с метаданными.

        idx - опциональный фильтр по целевым строкам
        include_deleted - флаг, возвращать ли удаленные строки, по умолчанию = False
        """

        raise NotImplementedError()

    def get_metadata_size(self, idx: Optional[IndexDF] = None, include_deleted: bool = False) -> int:
        """
        Получить количество строк метаданных.

        idx - опциональный фильтр по целевым строкам
        include_deleted - флаг, возвращать ли удаленные строки, по умолчанию = False
        """

        raise NotImplementedError()

    def get_existing_idx(self, idx: Optional[IndexDF] = None) -> IndexDF:
        # TODO docstring

        raise NotImplementedError()

    def get_table_debug_info(self) -> TableDebugInfo:
        # TODO docstring

        raise NotImplementedError()

    def get_changes_for_store_chunk(
        self, hash_df: HashDF, now: Optional[float] = None
    ) -> Tuple[IndexDF, IndexDF, MetadataDF, MetadataDF]:
        """
        Анализирует блок hash_df, выделяет строки new_ которые нужно добавить и
        строки changed_ которые нужно обновить

        Returns tuple:
            new_index_df     - индекс данных, которые нужно добавить
            changed_index_df - индекс данных, которые нужно изменить new_meta_df
            - строки метаданных, которые нужно добавить changed_meta_df - строки
            метаданных, которые нужно изменить
        """

        raise NotImplementedError()

    def update_rows(self, df: MetadataDF) -> None:
        # TODO docstring

        raise NotImplementedError()

    def mark_rows_deleted(
        self,
        deleted_idx: IndexDF,
        now: Optional[float] = None,
    ) -> None:
        # TODO docstring

        raise NotImplementedError()

    def get_stale_idx(
        self,
        process_ts: float,
        run_config: Optional[RunConfig] = None,
    ) -> Iterator[IndexDF]:
        # TODO docstring

        raise NotImplementedError()

    def reset_metadata(
        self,
        run_config: Optional[RunConfig] = None,
    ) -> None:
        # TODO docstring

        raise NotImplementedError()
        # with self.meta_dbconn.con.begin() as con:
        #     con.execute(self.meta.sql_table.update().values(process_ts=0, update_ts=0))


class TransformMeta:
    primary_schema: DataSchema
    primary_keys: List[str]

    def get_changed_idx_count(
        self,
        ds: "DataStore",
        run_config: Optional[RunConfig] = None,
    ) -> int:
        # TODO docstring

        raise NotImplementedError()

    def get_full_process_ids(
        self,
        ds: "DataStore",
        chunk_size: int,
        run_config: Optional[RunConfig] = None,
    ) -> Tuple[int, Iterable[IndexDF]]:
        # TODO docstring

        raise NotImplementedError()

    def get_change_list_process_ids(
        self,
        ds: "DataStore",
        change_list: ChangeList,
        chunk_size: int,
        run_config: Optional[RunConfig] = None,
    ) -> Tuple[int, Iterable[IndexDF]]:
        # TODO docstring

        raise NotImplementedError()

    def insert_rows(
        self,
        idx: IndexDF,
    ) -> None:
        """
        Создает строки в таблице метаданных для указанных индексов. Если строки
        уже существуют - не делает ничего.
        """

        raise NotImplementedError()

    def mark_rows_processed_success(
        self,
        idx: IndexDF,
        process_ts: float,
        run_config: Optional[RunConfig] = None,
    ) -> None:
        # TODO docstring

        raise NotImplementedError()

    def mark_rows_processed_error(
        self,
        idx: IndexDF,
        process_ts: float,
        error: str,
        run_config: Optional[RunConfig] = None,
    ) -> None:
        # TODO docstring

        raise NotImplementedError()

    def get_metadata_size(self) -> int:
        """
        Получить количество строк метаданных трансформации.
        """

        raise NotImplementedError()

    def mark_all_rows_unprocessed(
        self,
        run_config: Optional[RunConfig] = None,
    ) -> None:
        # TODO docstring

        raise NotImplementedError()

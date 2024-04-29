from abc import abstractmethod
from dataclasses import dataclass
from typing import Any, Iterator, List, Optional, Tuple

from datapipe.run_config import RunConfig
from datapipe.types import IndexDF


@dataclass
class TableDebugInfo:
    name: str
    size: int


class ChangeTrackingMeta:
    # primary_schema
    # primary_keys

    # ??? data_schema

    @abstractmethod
    def get_existing_idx(self, idx: Optional[IndexDF] = None) -> IndexDF: ...

    @abstractmethod
    def get_table_debug_info(self) -> TableDebugInfo: ...

    @abstractmethod
    def get_stale_idx(
        self,
        process_ts: float,
        run_config: Optional[RunConfig] = None,
    ) -> Iterator[IndexDF]: ...

    @abstractmethod
    def get_metadata_size(
        self, idx: Optional[IndexDF] = None, include_deleted: bool = False
    ) -> int:
        """
        Получить количество строк метаданных.

        idx - опциональный фильтр по целевым строкам
        include_deleted - флаг, возвращать ли удаленные строки, по умолчанию = False
        """
        ...

    @abstractmethod
    def get_agg_cte(
        self,
        transform_keys: List[str],
        filters_idx: Optional[IndexDF] = None,
        run_config: Optional[RunConfig] = None,
    ) -> Tuple[List[str], Any]:
        """
        Create a CTE that aggregates the table by transform keys and returns the
        maximum update_ts for each group.

        CTE has the following columns:

        * transform keys which are present in primary keys
        * update_ts

        Returns a tuple of (keys, CTE).
        """
        ...

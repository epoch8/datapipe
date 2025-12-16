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
        """
        Create a backend-specific `TableMeta` for table `name` with the provided
        primary-key schema and extra metadata schema.

        Concrete implementations (e.g. `SQLMetaPlane`) are expected to return an
        object bound to the same storage as this MetaPlane instance.
        """

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
        """
        Create a `TransformMeta` describing metadata for a transform with the
        specified input and output tables.

        transform_keys/order_by/order control processing order; the concrete
        implementation should rely on TableMeta objects from the same MetaPlane.

        Note: We expect that TableMeta for input_dts and output_dts are from the
        same MetaPlane instance as this TransformMeta.
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
        Return a dataframe with metadata rows.

        idx - optional filter limiting target rows
        include_deleted - whether to include rows marked as deleted (default: False)
        """

        raise NotImplementedError()

    def get_metadata_size(self, idx: Optional[IndexDF] = None, include_deleted: bool = False) -> int:
        """
        Return the number of metadata rows.

        idx - optional filter limiting target rows
        include_deleted - whether to include rows marked as deleted (default: False)
        """

        raise NotImplementedError()

    def get_existing_idx(self, idx: Optional[IndexDF] = None) -> IndexDF:
        """
        Return indices already present in the metadata store (not marked
        deleted). If `idx` is provided, return its intersection; otherwise, all
        available indices.
        """

        raise NotImplementedError()

    def get_table_debug_info(self) -> TableDebugInfo:
        """
        Return debug info about the table: its name and current metadata row
        count (typically excluding deleted rows).
        """

        raise NotImplementedError()

    # TODO return a dataclass, pair it with `apply_changes_from_store_chunk`
    # TODO take processed_idx as input, compute deleted rows inside, see datatable.store_chunk
    def get_changes_for_store_chunk(
        self, hash_df: HashDF, now: Optional[float] = None
    ) -> Tuple[IndexDF, IndexDF, MetadataDF, MetadataDF]:
        """
        Analyze a hash_df chunk to detect rows that are new (to insert) and
        changed (to update).

        Returns a tuple:
            new_index_df     - index of data to insert
            changed_index_df - index of data to update
            new_meta_df      - metadata rows to insert
            changed_meta_df  - metadata rows to update
        """

        raise NotImplementedError()

    # TODO merge update_rows and mark_rows_deleted into apply_changes_from_store_chunk
    def update_rows(self, df: MetadataDF) -> None:
        """
        Upsert metadata rows for the provided dataframe. No-op if the dataframe
        is empty.
        """

        raise NotImplementedError()

    def mark_rows_deleted(
        self,
        deleted_idx: IndexDF,
        now: Optional[float] = None,
    ) -> None:
        """
        Mark the given indices as deleted by setting `delete_ts` (using `now` or
        current time). Empty indices are ignored.
        """

        raise NotImplementedError()

    def get_stale_idx(
        self,
        process_ts: float,
        run_config: Optional[RunConfig] = None,
    ) -> Iterator[IndexDF]:
        """
        Iterate over indices whose `process_ts` is below the given threshold
        (stale / requiring processing). May apply `run_config` filters and yield
        results in chunks.
        """

        raise NotImplementedError()

    def reset_metadata(
        self,
        run_config: Optional[RunConfig] = None,
    ) -> None:
        """
        Reset service metadata fields (e.g., process_ts / update_ts) for all
        rows to force a full reprocessing. May be limited by `run_config`
        filters.
        """

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
        """
        Count how many indices require processing given current input tables and
        transform records (honoring `run_config`).
        """

        raise NotImplementedError()

    def get_full_process_ids(
        self,
        ds: "DataStore",
        chunk_size: int,
        run_config: Optional[RunConfig] = None,
    ) -> Tuple[int, Iterable[IndexDF]]:
        """
        Compute indices for a full transform run: returns chunk count and an
        iterator of index dataframes ordered by priority/keys, respecting
        `run_config`.
        """

        raise NotImplementedError()

    def get_change_list_process_ids(
        self,
        ds: "DataStore",
        change_list: ChangeList,
        chunk_size: int,
        run_config: Optional[RunConfig] = None,
    ) -> Tuple[int, Iterable[IndexDF]]:
        """
        Like `get_full_process_ids`, but limited to changes from `change_list`
        (e.g., modified input tables). Returns chunk count and a generator of
        indices.
        """

        raise NotImplementedError()

    def insert_rows(
        self,
        idx: IndexDF,
    ) -> None:
        """
        Create metadata rows for the given indices. If rows already exist, do
        nothing.
        """

        raise NotImplementedError()

    def mark_rows_processed_success(
        self,
        idx: IndexDF,
        process_ts: float,
        run_config: Optional[RunConfig] = None,
    ) -> None:
        """
        Mark the given indices as successfully processed at `process_ts`,
        creating records when absent and updating existing ones (respecting
        `run_config` filters if the implementation supports them).
        """

        raise NotImplementedError()

    def mark_rows_processed_error(
        self,
        idx: IndexDF,
        process_ts: float,
        error: str,
        run_config: Optional[RunConfig] = None,
    ) -> None:
        """
        Record a processing error for the given indices: set `process_ts`, store
        the error text, and clear the success flag. Creates records if needed;
        may honor `run_config`.
        """

        raise NotImplementedError()

    def get_metadata_size(self) -> int:
        """
        Return the number of transform metadata rows.
        """

        raise NotImplementedError()

    def mark_all_rows_unprocessed(
        self,
        run_config: Optional[RunConfig] = None,
    ) -> None:
        """
        Clear success status and error for all transform rows (or those filtered
        by `run_config`) to re-queue them for processing.
        """

        raise NotImplementedError()

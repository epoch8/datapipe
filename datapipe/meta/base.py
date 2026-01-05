from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterable, Iterator, Literal, Sequence

import pandas as pd
from sqlalchemy import Column

from datapipe.run_config import RunConfig
from datapipe.types import ChangeList, DataSchema, FieldAccessor, HashDF, IndexDF, MetadataDF, MetaSchema

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
        transform_keys: list[str] | None = None,
        order_by: list[str] | None = None,
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
    primary_keys: list[str]

    def get_metadata(self, idx: IndexDF | None = None, include_deleted: bool = False) -> MetadataDF:
        """
        Return a dataframe with metadata rows.

        idx - optional filter limiting target rows
        include_deleted - whether to include rows marked as deleted (default: False)
        """

        raise NotImplementedError()

    def get_metadata_size(self, idx: IndexDF | None = None, include_deleted: bool = False) -> int:
        """
        Return the number of metadata rows.

        idx - optional filter limiting target rows
        include_deleted - whether to include rows marked as deleted (default: False)
        """

        raise NotImplementedError()

    def get_existing_idx(self, idx: IndexDF | None = None) -> IndexDF:
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
        self, hash_df: HashDF, now: float | None = None
    ) -> tuple[IndexDF, IndexDF, MetadataDF, MetadataDF]:
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
        now: float | None = None,
    ) -> None:
        """
        Mark the given indices as deleted by setting `delete_ts` (using `now` or
        current time). Empty indices are ignored.
        """

        raise NotImplementedError()

    def get_stale_idx(
        self,
        process_ts: float,
        run_config: RunConfig | None = None,
    ) -> Iterator[IndexDF]:
        """
        Iterate over indices whose `process_ts` is below the given threshold
        (stale / requiring processing). May apply `run_config` filters and yield
        results in chunks.
        """

        raise NotImplementedError()

    def reset_metadata(
        self,
        run_config: RunConfig | None = None,
    ) -> None:
        """
        Reset service metadata fields (e.g., process_ts / update_ts) for all
        rows to force a full reprocessing. May be limited by `run_config`
        filters.
        """

        raise NotImplementedError()
        # with self.meta_dbconn.con.begin() as con:
        #     con.execute(self.meta.sql_table.update().values(process_ts=0, update_ts=0))

    def transform_idx_to_table_idx(
        self,
        transform_idx: IndexDF,
        keys: dict[str, FieldAccessor] | None = None,
    ) -> IndexDF:
        """
        Given an index dataframe with transform keys, return an index dataframe
        with table keys, applying `keys` aliasing if provided.

        * `keys` is a mapping from table key to transform key
        """

        if keys is None:
            return transform_idx

        table_key_cols: dict[str, pd.Series] = {}
        for transform_col in transform_idx.columns:
            accessor = keys.get(transform_col) if keys is not None else transform_col
            if isinstance(accessor, str):
                table_key_cols[accessor] = transform_idx[transform_col]
            else:
                pass  # skip non-meta fields

        return IndexDF(pd.DataFrame(table_key_cols))


class TransformMeta:
    transform_keys_schema: DataSchema
    transform_keys: list[str]

    @classmethod
    def compute_transform_schema(
        cls,
        input_cis: Sequence["ComputeInput"],
        output_dts: Sequence["DataTable"],
        transform_keys: list[str] | None,
    ) -> tuple[list[str], MetaSchema]:
        # Hacky way to collect all the primary keys into a single set. Possible
        # problem that is not handled here is that theres a possibility that the
        # same key is defined differently in different input tables.
        all_keys: dict[str, Column] = {}

        for ci in input_cis:
            all_keys.update({col.name: col for col in ci.primary_schema})

        for dt in output_dts:
            all_keys.update({col.name: col for col in dt.primary_schema})

        if transform_keys is not None:
            return (transform_keys, [all_keys[k] for k in transform_keys])

        assert len(input_cis) > 0, "At least one input table is required to infer transform keys"

        inp_p_keys = set.intersection(*[set(inp.primary_keys) for inp in input_cis])
        assert len(inp_p_keys) > 0

        if len(output_dts) == 0:
            return (list(inp_p_keys), [all_keys[k] for k in inp_p_keys])

        out_p_keys = set.intersection(*[set(out.primary_keys) for out in output_dts])
        assert len(out_p_keys) > 0

        inp_out_p_keys = set.intersection(inp_p_keys, out_p_keys)
        assert len(inp_out_p_keys) > 0

        return (list(inp_out_p_keys), [all_keys[k] for k in inp_out_p_keys])

    def get_changed_idx_count(
        self,
        ds: "DataStore",
        run_config: RunConfig | None = None,
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
        run_config: RunConfig | None = None,
    ) -> tuple[int, Iterable[IndexDF]]:
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
        run_config: RunConfig | None = None,
    ) -> tuple[int, Iterable[IndexDF]]:
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
        run_config: RunConfig | None = None,
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
        run_config: RunConfig | None = None,
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
        run_config: RunConfig | None = None,
    ) -> None:
        """
        Clear success status and error for all transform rows (or those filtered
        by `run_config`) to re-queue them for processing.
        """

        raise NotImplementedError()

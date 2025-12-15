import logging
from typing import TYPE_CHECKING, Any, Dict, Optional, cast

import pandas as pd
from opentelemetry import trace

from datapipe.event_logger import EventLogger
from datapipe.meta.base import TableMeta
from datapipe.run_config import RunConfig
from datapipe.store.database import DBConn
from datapipe.store.table_store import TableStore
from datapipe.types import DataDF, IndexDF, MetadataDF, data_to_index, index_difference, index_to_data

if TYPE_CHECKING:
    try:
        # works only with sqlalchemy>=2
        from sqlalchemy.sql.expression import ColumnClause
    except ImportError:
        ColumnClause = Any  # type: ignore

logger = logging.getLogger("datapipe.datatable")
tracer = trace.get_tracer("datapipe.datatable")


class DataTable:
    def __init__(
        self,
        name: str,
        meta: TableMeta,
        table_store: TableStore,
        event_logger: EventLogger,
    ):
        self.name = name
        self.meta = meta
        self.table_store = table_store
        self.event_logger = event_logger

        self.primary_schema = meta.primary_schema
        self.primary_keys = meta.primary_keys

    def get_metadata(self, idx: Optional[IndexDF] = None) -> MetadataDF:
        return self.meta.get_metadata(idx)

    def get_data(self, idx: Optional[IndexDF] = None) -> DataDF:
        return self.table_store.read_rows(self.meta.get_existing_idx(idx))

    def reset_metadata(self):
        self.meta.reset_metadata()

    def get_size(self) -> int:
        """
        Get the number of non-deleted rows in the DataTable
        """
        return self.meta.get_metadata_size(idx=None, include_deleted=False)

    def store_chunk(
        self,
        data_df: DataDF,
        processed_idx: Optional[IndexDF] = None,
        now: Optional[float] = None,
        run_config: Optional[RunConfig] = None,
    ) -> IndexDF:
        """
        Записать новые данные в таблицу.

        При указанном `processed_idx` удалить те строки, которые находятся
        внутри `processed_idx`, но отсутствуют в `data_df`.
        """

        # In case `processed_idx` is not None, check that it contains any of
        # relevant columns
        if processed_idx is not None:
            relevant_keys = set(self.primary_keys) & set(processed_idx.columns)
            if not relevant_keys:
                processed_idx = None

        # Check that all index values in `data_df` is unique
        if data_df.duplicated(self.primary_keys).any():
            raise ValueError("`data_df` index values should be unique")

        changes = [IndexDF(pd.DataFrame(columns=self.primary_keys))]

        with tracer.start_as_current_span(f"{self.name} store_chunk"):
            if not data_df.empty:
                logger.debug(f"Inserting chunk {len(data_df.index)} rows into {self.name}")

                hash_df = self.table_store.hash_rows(data_df)

                with tracer.start_as_current_span("get_changes_for_store_chunk"):
                    (
                        new_index_df,
                        changed_index_df,
                        new_meta_df,
                        changed_meta_df,
                    ) = self.meta.get_changes_for_store_chunk(hash_df, now)

                new_df = index_to_data(data_df, new_index_df)
                changed_df = index_to_data(data_df, changed_index_df)

                # TODO implement transaction meckanism
                with tracer.start_as_current_span("store data"):
                    self.table_store.insert_rows(new_df)
                    self.table_store.update_rows(changed_df)

                with tracer.start_as_current_span("store metadata"):
                    self.meta.update_rows(
                        cast(
                            MetadataDF,
                            pd.concat([df for df in [new_meta_df, changed_meta_df] if not df.empty]),
                        )
                    )

                    if not new_df.empty:
                        changes.append(data_to_index(new_df, self.primary_keys))
                    if not changed_df.empty:
                        changes.append(data_to_index(changed_df, self.primary_keys))
            else:
                data_df = pd.DataFrame(columns=self.primary_keys)

            with tracer.start_as_current_span("cleanup deleted rows"):
                data_idx = data_to_index(data_df, self.primary_keys)

                if processed_idx is not None:
                    existing_idx = self.meta.get_existing_idx(processed_idx)
                    deleted_idx = index_difference(existing_idx, data_idx)

                    self.delete_by_idx(deleted_idx, now=now, run_config=run_config)

                    if not deleted_idx.empty:
                        changes.append(deleted_idx)

        return cast(IndexDF, pd.concat(changes))

    def delete_by_idx(
        self,
        idx: IndexDF,
        now: Optional[float] = None,
        run_config: Optional[RunConfig] = None,
    ) -> None:
        if len(idx) > 0:
            logger.debug(f"Deleting {len(idx.index)} rows from {self.name} data")

            self.table_store.delete_rows(idx)
            self.meta.mark_rows_deleted(idx, now=now)

    def delete_stale_by_process_ts(
        self,
        process_ts: float,
        now: Optional[float] = None,
        run_config: Optional[RunConfig] = None,
    ) -> None:
        for deleted_df in self.meta.get_stale_idx(process_ts, run_config=run_config):
            deleted_idx = data_to_index(deleted_df, self.primary_keys)

            self.delete_by_idx(deleted_idx, now=now, run_config=run_config)


class DataStore:
    def __init__(
        self,
        meta_dbconn: DBConn,
        create_meta_table: bool = False,
    ) -> None:
        from datapipe.meta.sql_meta import SQLMetaPlane

        self.meta_dbconn = meta_dbconn
        self.event_logger = EventLogger()
        self.tables: Dict[str, DataTable] = {}

        # TODO move initialization outside
        self.meta_plane = SQLMetaPlane(dbconn=meta_dbconn, create_meta_table=create_meta_table)

    def create_table(self, name: str, table_store: TableStore) -> DataTable:
        assert name not in self.tables

        primary_schema = table_store.get_primary_schema()
        meta_schema = table_store.get_meta_schema()

        res = DataTable(
            name=name,
            meta=self.meta_plane.create_table_meta(
                name=name,
                primary_schema=primary_schema,
                meta_schema=meta_schema,
            ),
            table_store=table_store,
            event_logger=self.event_logger,
        )

        self.tables[name] = res

        return res

    def get_or_create_table(self, name: str, table_store: TableStore) -> DataTable:
        if name in self.tables:
            return self.tables[name]
        else:
            return self.create_table(name, table_store)

    def get_table(self, name: str) -> DataTable:
        return self.tables[name]

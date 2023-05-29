import logging
import math
from typing import Dict, Iterable, List, Optional, Tuple, cast

import pandas as pd
from opentelemetry import trace
from sqlalchemy import MetaData

from datapipe.event_logger import EventLogger
from datapipe.metastore import MetaTable
from datapipe.run_config import RunConfig
from datapipe.store.database import DBConn
from datapipe.store.table_store import TableStore
from datapipe.types import (
    ChangeList,
    DataDF,
    IndexDF,
    MetadataDF,
    data_to_index,
    index_difference,
)

logger = logging.getLogger("datapipe.datatable")
tracer = trace.get_tracer("datapipe.datatable")


class DataTable:
    def __init__(
        self,
        name: str,
        meta_table: MetaTable,
        table_store: TableStore,
        event_logger: EventLogger,
    ):
        self.name = name
        self.meta_table = meta_table
        self.table_store = table_store
        self.event_logger = event_logger

        self.primary_schema = meta_table.primary_schema
        self.primary_keys = meta_table.primary_keys

    def get_metadata(self, idx: Optional[IndexDF] = None) -> MetadataDF:
        return self.meta_table.get_metadata(idx)

    def get_data(self, idx: Optional[IndexDF] = None) -> DataDF:
        return self.table_store.read_rows(self.meta_table.get_existing_idx(idx))

    def reset_metadata(self, ds: "DataStore"):
        ds.meta_dbconn.con.execute(
            self.meta_table.sql_table.update().values(process_ts=0, update_ts=0)
        )

    def get_size(self) -> int:
        """
        Get the number of non-deleted rows in the DataTable
        """
        return self.meta_table.get_metadata_size(idx=None, include_deleted=False)

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

        # Check that all index values in `data_df` is unique
        if data_df.duplicated(self.primary_keys).any():
            raise ValueError("`data_df` index values should be unique")

        changes = [IndexDF(pd.DataFrame(columns=self.primary_keys))]

        with tracer.start_as_current_span(f"{self.name} store_chunk"):
            if not data_df.empty:
                logger.debug(
                    f"Inserting chunk {len(data_df.index)} rows into {self.name}"
                )

                with tracer.start_as_current_span("get_changes_for_store_chunk"):
                    (
                        new_df,
                        changed_df,
                        new_meta_df,
                        changed_meta_df,
                    ) = self.meta_table.get_changes_for_store_chunk(data_df, now)

                self.event_logger.log_state(
                    self.name,
                    added_count=len(new_df),
                    updated_count=len(changed_df),
                    deleted_count=0,
                    processed_count=len(data_df),
                    run_config=run_config,
                )

                # TODO implement transaction meckanism
                with tracer.start_as_current_span("store data"):
                    self.table_store.insert_rows(new_df)
                    self.table_store.update_rows(changed_df)

                with tracer.start_as_current_span("store metadata"):
                    self.meta_table.insert_meta_for_store_chunk(new_meta_df)
                    self.meta_table.update_meta_for_store_chunk(changed_meta_df)

                    changes.append(data_to_index(new_df, self.primary_keys))
                    changes.append(data_to_index(changed_df, self.primary_keys))
            else:
                data_df = pd.DataFrame(columns=self.primary_keys)

            with tracer.start_as_current_span("cleanup deleted rows"):
                data_idx = data_to_index(data_df, self.primary_keys)

                if processed_idx is not None:
                    existing_idx = self.meta_table.get_existing_idx(processed_idx)
                    deleted_idx = index_difference(existing_idx, data_idx)

                    self.delete_by_idx(deleted_idx, now=now, run_config=run_config)

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
            self.event_logger.log_state(
                self.name,
                added_count=0,
                updated_count=0,
                deleted_count=len(idx),
                processed_count=len(idx),
                run_config=run_config,
            )

            self.table_store.delete_rows(idx)
            self.meta_table.mark_rows_deleted(idx, now=now)

    def delete_stale_by_process_ts(
        self,
        process_ts: float,
        now: Optional[float] = None,
        run_config: Optional[RunConfig] = None,
    ) -> None:
        for deleted_df in self.meta_table.get_stale_idx(
            process_ts, run_config=run_config
        ):
            deleted_idx = data_to_index(deleted_df, self.primary_keys)

            self.delete_by_idx(deleted_idx, now=now, run_config=run_config)


class DataStore:
    def __init__(
        self,
        meta_dbconn: DBConn,
        create_meta_table: bool = False,
    ) -> None:
        self.meta_dbconn = meta_dbconn
        self.sqla_metadata = MetaData()

        self.event_logger = EventLogger(
            self.meta_dbconn, create_table=create_meta_table
        )
        self.tables: Dict[str, DataTable] = {}

        self.create_meta_table = create_meta_table

    def create_table(self, name: str, table_store: TableStore) -> DataTable:
        assert name not in self.tables

        primary_schema = table_store.get_primary_schema()
        meta_schema = table_store.get_meta_schema()

        res = DataTable(
            name=name,
            meta_table=MetaTable(
                ds=self,
                name=name,
                primary_schema=primary_schema,
                meta_schema=meta_schema,
                create_table=self.create_meta_table,
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

    # TODO remove, because it's implemented in BatchTransformStep
    def get_join_keys(
        self, inputs: List[DataTable], outputs: List[DataTable]
    ) -> List[str]:
        inp_p_keys = [set(inp.primary_keys) for inp in inputs]
        out_p_keys = [set(out.primary_keys) for out in outputs]

        return list(set.intersection(*inp_p_keys, *out_p_keys))

    def get_table(self, name: str) -> DataTable:
        return self.tables[name]

    # TODO remove, because it's implemented in BatchTransformStep
    def get_change_list_process_ids(
        self,
        inputs: List[DataTable],
        outputs: List[DataTable],
        change_list: ChangeList,
        chunk_size: int = 1000,
        run_config: Optional[RunConfig] = None,
    ) -> Tuple[int, Iterable[IndexDF]]:
        join_keys = self.get_join_keys(inputs, outputs)
        changes = [pd.DataFrame(columns=join_keys)]

        if not join_keys:
            raise ValueError("Primary keys intersection for ChangeList are empty")

        for inp in inputs:
            if inp.name in change_list.changes:
                idx = change_list.changes[inp.name]

                changes.append(data_to_index(idx, join_keys))

        idx = IndexDF(pd.concat(changes).drop_duplicates(subset=join_keys))

        def gen():
            for i in range(math.ceil(len(idx) / chunk_size)):
                yield idx[i * chunk_size : (i + 1) * chunk_size]

        return len(idx), gen()

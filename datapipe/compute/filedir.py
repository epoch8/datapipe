from dataclasses import dataclass
from importlib.metadata import metadata
from typing import List, Union, Any, TYPE_CHECKING

import re

import fsspec

from c12n_pipe.datatable import DataTable
from c12n_pipe.store.table_store_filedir import TableStoreFiledir
from c12n_pipe.metastore import MetaStore

from ..dsl import TableStoreFiledir, FileStoreAdapter
from .steps import ComputeStep


class ExternalFiledirUpdater(ComputeStep):
    def __init__(self, name: str, table_name: str, filedir: TableStoreFiledir):
        self.name = name
        self.inputs = []
        self.outputs = [table_name]

        self.table_name = table_name
        self.table_data_store = filedir

    def run(self, ms: MetaStore) -> None:
        ps_df = self.table_data_store.read_rows_meta_pseudo_df()

        _, _, new_meta_df = ms.get_changes_for_store_chunk(self.table_name, ps_df)
        ms.update_meta_for_store_chunk(self.table_name, new_meta_df)

        deleted_idx = ms.get_changes_for_sync_meta(self.table_name, [ps_df.index])
        ms.update_meta_for_store_chunk(self.table_name, deleted_idx)

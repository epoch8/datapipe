from c12n_pipe.datatable import DataTable
from c12n_pipe.store.table_store_filedir import TableStoreFiledir
from c12n_pipe.metastore import MetaStore

from ..dsl import TableStoreFiledir
from .steps import ComputeStep


class ExternalFiledirUpdater(ComputeStep):
    def __init__(self, name: str, table: DataTable):
        self.name = name
        self.table = table
        self.input_dts = []
        self.output_dts = [table]

    def run(self, ms: MetaStore) -> None:
        ps_df = self.table.table_store.read_rows_meta_pseudo_df()

        _, _, new_meta_df = ms.get_changes_for_store_chunk(self.table.name, ps_df)
        ms.update_meta_for_store_chunk(self.table.name, new_meta_df)

        deleted_idx = ms.get_changes_for_sync_meta(self.table.name, [ps_df.index])
        ms.update_meta_for_store_chunk(self.table.name, deleted_idx)

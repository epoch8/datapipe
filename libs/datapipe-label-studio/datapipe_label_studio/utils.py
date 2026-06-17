from typing import List

from datapipe.datatable import DataStore
from datapipe.store.database import TableStoreDB
from datapipe.types import PipelineInput, get_pipeline_input_name


def check_columns_are_in_table(ds: DataStore, tbl_name: PipelineInput, columns: List[str]):
    table_name = get_pipeline_input_name(tbl_name)
    datatable = ds.get_table(table_name)
    assert isinstance(datatable.table_store, TableStoreDB)
    for column in columns:
        if column not in [x.name for x in datatable.table_store.data_sql_schema]:
            raise ValueError(f"Missing '{column}' column in table {table_name}")
    return True

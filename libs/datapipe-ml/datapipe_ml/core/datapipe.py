from collections.abc import Sequence
from datetime import timezone
from typing import Any, List, Protocol, Tuple, TypeAlias, cast

import numpy as np
import pandas as pd
from datapipe.datatable import DataStore, DataTable
from datapipe.store.database import TableStoreDB
from datapipe.store.filedir import TableStoreFiledir
from datapipe.types import (
    IndexDF,
    InputSpec,
    OutputSpec,
    PipelineInput,
    PipelineOutput,
    TableOrName,
    get_pipeline_input_table,
    get_pipeline_input_name,
    get_pipeline_output_table,
    get_pipeline_output_name,
)

PipelineInputOrList: TypeAlias = PipelineInput | Sequence[PipelineInput]
PipelineTableOrList: TypeAlias = PipelineInput | PipelineOutput | Sequence[PipelineInput | PipelineOutput]


class _DatatableMetaLike(Protocol):
    def get_existing_idx(self, idx: IndexDF) -> IndexDF: ...


def _get_datatable_meta(dt: DataTable) -> _DatatableMetaLike:
    meta = getattr(dt, "meta", None)
    if meta is None:
        meta = getattr(dt, "meta_table", None)
    if meta is None:
        raise AttributeError(f"DataTable {dt.name!r} has neither 'meta' nor 'meta_table'")
    return cast(_DatatableMetaLike, meta)


def get_pipeline_table_name(table: PipelineInput | PipelineOutput) -> str:
    if isinstance(table, OutputSpec):
        return get_pipeline_output_name(table)
    return get_pipeline_input_name(table)


def get_datatable(ds: DataStore, table: PipelineInput | PipelineOutput) -> DataTable:
    return ds.get_table(get_pipeline_table_name(table))


def check_columns_are_in_table(
    ds: DataStore,
    tbl_name: PipelineTableOrList,
    columns: List[str],
    raise_exc: bool = True,
):
    tbl_names: Sequence[PipelineInput | PipelineOutput]
    if isinstance(tbl_name, str) or not isinstance(tbl_name, Sequence):
        tbl_names = [tbl_name]
    else:
        tbl_names = tbl_name
    for tbl_name in tbl_names:
        table_name = get_pipeline_table_name(tbl_name)
        datatable = ds.get_table(table_name)
        assert isinstance(datatable.table_store, (TableStoreDB, TableStoreFiledir))
        if isinstance(datatable.table_store, TableStoreDB):
            data_sql_schema = [x.name for x in datatable.table_store.data_sql_schema]
        elif isinstance(datatable.table_store, TableStoreFiledir):
            data_sql_schema = datatable.table_store.attrnames + (
                ["filepath"] if datatable.table_store.add_filepath_column else []
            )
        else:
            raise ValueError(f"Unknown table_store: {datatable.table_store=}")
        for column in columns:
            if column not in data_sql_schema:
                if raise_exc:
                    raise ValueError(f"Missing '{column}' column in table {table_name}")
                else:
                    return False
    return True


def normalize_pipeline_inputs(tbl_name: PipelineInputOrList) -> List[PipelineInput]:
    if isinstance(tbl_name, str) or not isinstance(tbl_name, Sequence):
        return [tbl_name]
    return [item for item in tbl_name]


def pipeline_output_as_input(output: PipelineOutput) -> PipelineInput:
    if isinstance(output, OutputSpec):
        keys: dict[str, str] | None = {key: value for key, value in output.keys.items()} if output.keys is not None else None
        return InputSpec(table=output.table, keys=keys)
    return output


def pipeline_input_as_table(input: PipelineInput) -> TableOrName:
    return get_pipeline_input_table(input)


def pipeline_output_as_table(output: PipelineOutput) -> TableOrName:
    return get_pipeline_output_table(output)


def is_frozen_dataset_old(
    dt__frozen_dataset: DataTable,
    frozen_dataset_id__names: List[str],
    frozen_dataset__created_at__name: str,
    frozen_dataset_ids: Tuple[Any, ...],
    max_within_time: str,
    print_skipping: bool = True,
):
    df__frozen_dataset = dt__frozen_dataset.get_data()
    df__frozen_dataset.set_index(frozen_dataset_id__names, inplace=True)
    df__frozen_dataset.sort_values(by=frozen_dataset__created_at__name, inplace=True, ascending=False)
    last_timestamp = df__frozen_dataset.iloc[0][frozen_dataset__created_at__name]
    last_frozen_dataset_id = df__frozen_dataset.index[0]
    current_timestamp = df__frozen_dataset.loc[frozen_dataset_ids, frozen_dataset__created_at__name]
    if last_timestamp - current_timestamp >= pd.Timedelta(max_within_time):
        if print_skipping:
            print(
                f"Frozen Dataset {frozen_dataset_ids} is older more than "
                f"last available frozen dataset {last_frozen_dataset_id} for {max_within_time}. Skipping"
            )
        return True
    return False


def is_last_frozen_dataset_old_enough(
    dt__frozen_dataset: DataTable,
    frozen_dataset_id__name: str,
    frozen_dataset__created_at__name: str,
    min_within_time: str,
    print_skipping: bool = True,
):
    df__frozen_dataset = dt__frozen_dataset.get_data()
    if len(df__frozen_dataset) == 0:
        return True
    df__frozen_dataset.set_index(frozen_dataset_id__name, inplace=True)
    df__frozen_dataset.sort_values(by=frozen_dataset__created_at__name, inplace=True, ascending=False)
    last_timestamp = df__frozen_dataset.iloc[0][frozen_dataset__created_at__name].replace(tzinfo=timezone.utc)
    last_frozen_dataset_id = df__frozen_dataset.index[0]
    current_timestamp = pd.Timestamp.now(timezone.utc)
    if current_timestamp - last_timestamp <= pd.Timedelta(min_within_time):
        if print_skipping:
            print(
                f"Last Frozen Dataset {last_frozen_dataset_id} is not older more than {min_within_time}." f" Skipping."
            )
        return False
    return True


def get_data_split_by_batches(dt: DataTable, idx: IndexDF, batch_size: int = 1000) -> pd.DataFrame:
    return pd.concat(
        [dt.get_data(idx=cast(IndexDF, idx_batch)) for idx_batch in np.array_split(idx, batch_size)],
        ignore_index=True,
    )


def get_table_store_filedir_existing_idx_split_by_batches(
    dt: DataTable, idx: IndexDF, batch_size: int = 1000
) -> pd.DataFrame:
    assert isinstance(dt.table_store, TableStoreFiledir)
    return pd.concat(
        [
            cast(
                pd.DataFrame,
                dt.table_store.read_rows(
                    _get_datatable_meta(dt).get_existing_idx(idx=cast(IndexDF, idx_batch)), read_data=False
                ),
            )
            for idx_batch in np.array_split(idx, batch_size)
        ],
        ignore_index=True,
    )

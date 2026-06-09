from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pandas as pd
from datapipe.compute import (
    Catalog,
    ComputeStep,
    Pipeline,
    PipelineStep,
    Table,
    build_compute,
)
from datapipe.datatable import DataStore, DataTable
from datapipe.run_config import RunConfig
from datapipe.step.batch_transform import DatatableBatchTransform
from datapipe.store.database import TableStoreDB
from datapipe.types import PipelineInput, PipelineOutput, IndexDF, Labels
from sqlalchemy import Column
from sqlalchemy.sql import and_, functions, select
from sqlalchemy.sql.sqltypes import Integer, String

from datapipe_ml.core.datapipe import check_columns_are_in_table, get_datatable, get_pipeline_table_name


def count_labels_total_on_subset(
    ds: DataStore,
    idx: IndexDF,
    input_dts: List[DataTable],
    run_config: Optional[RunConfig] = None,
    kwargs: Optional[Dict[str, Any]] = None,
):
    kwargs = kwargs or {}
    primary_keys = kwargs["primary_keys"]
    dt__item__ground_truth, dt__subset__has__item = input_dts
    assert isinstance(dt__item__ground_truth.table_store, TableStoreDB)
    assert isinstance(dt__subset__has__item.table_store, TableStoreDB)
    tbl_gt = dt__item__ground_truth.table_store.data_table
    tbl_subset = dt__subset__has__item.table_store.data_table
    sql = (
        select(
            tbl_subset.c["subset_id"],
            tbl_gt.c["label"],
            functions.count().label("label__total"),
        )
        .select_from(tbl_gt)
        .join(tbl_subset, and_(*[tbl_gt.c[column] == tbl_subset.c[column] for column in primary_keys]))
        .group_by(tbl_subset.c["subset_id"], tbl_gt.c["label"])
    )
    df__subset__has__label = pd.read_sql_query(sql, con=ds.meta_dbconn.con)
    df__subset__has__label.dropna(inplace=True)
    return df__subset__has__label[["subset_id", "label", "label__total"]]


@dataclass
class CountTotalLabelOnSubset(PipelineStep):
    input__item__ground_truth: PipelineInput
    input__subset__has__item: PipelineInput
    output__subset__has__label__total: PipelineOutput
    primary_keys: List[str]
    labels: Optional[Labels] = None
    create_table: bool = False
    chunk_size: int = 1000000

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        dt__input__item__ground_truth = get_datatable(ds, self.input__item__ground_truth)
        assert isinstance(dt__input__item__ground_truth.table_store, TableStoreDB)
        assert isinstance(get_datatable(ds, self.input__subset__has__item).table_store, TableStoreDB)
        check_columns_are_in_table(ds, self.input__item__ground_truth, self.primary_keys + ["label"])
        check_columns_are_in_table(ds, self.input__subset__has__item, self.primary_keys + ["subset_id"])
        catalog.add_datatable(
            get_pipeline_table_name(self.output__subset__has__label__total),
            Table(
                ds.get_or_create_table(
                    get_pipeline_table_name(self.output__subset__has__label__total),
                    TableStoreDB(
                        dbconn=ds.meta_dbconn,
                        name=get_pipeline_table_name(self.output__subset__has__label__total),
                        data_sql_schema=[
                            Column("subset_id", String, primary_key=True),
                            Column("label", String, primary_key=True),
                            Column("label__total", Integer),
                        ],
                        create_table=self.create_table,
                    ),
                ).table_store
            ),
        )
        pipeline = Pipeline(
            [
                DatatableBatchTransform(
                    func=count_labels_total_on_subset,
                    inputs=[self.input__item__ground_truth, self.input__subset__has__item],
                    outputs=[self.output__subset__has__label__total],
                    transform_keys=self.primary_keys + ["subset_id"],
                    labels=self.labels,
                    kwargs=dict(primary_keys=self.primary_keys),
                    chunk_size=self.chunk_size,
                ),
            ]
        )
        return build_compute(ds, catalog, pipeline)


def count_labels_total(
    ds: DataStore,
    idx: IndexDF,
    input_dts: List[DataTable],
    run_config: Optional[RunConfig] = None,
    kwargs: Optional[Dict[str, Any]] = None,
):
    kwargs = kwargs or {}
    dt__item__ground_truth = input_dts[0]
    assert isinstance(dt__item__ground_truth.table_store, TableStoreDB)
    tbl_gt = dt__item__ground_truth.table_store.data_table
    sql = (
        select(
            tbl_gt.c["label"],
            functions.count().label("label__total"),
        )
        .select_from(tbl_gt)
        .group_by(tbl_gt.c["label"])
    )
    df__item__total_label = pd.read_sql_query(sql, con=ds.meta_dbconn.con)
    df__item__total_label.dropna(inplace=True)
    return df__item__total_label[["label", "label__total"]]


@dataclass
class CountTotalLabel(PipelineStep):
    input__item__ground_truth: PipelineInput
    output__item__has__total_label: PipelineOutput
    primary_keys: List[str]
    labels: Optional[Labels] = None
    create_table: bool = False
    chunk_size: int = 1000000

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        dt__input__item__ground_truth = get_datatable(ds, self.input__item__ground_truth)
        assert isinstance(dt__input__item__ground_truth.table_store, TableStoreDB)
        catalog.add_datatable(
            get_pipeline_table_name(self.output__item__has__total_label),
            Table(
                ds.get_or_create_table(
                    get_pipeline_table_name(self.output__item__has__total_label),
                    TableStoreDB(
                        dbconn=ds.meta_dbconn,
                        name=get_pipeline_table_name(self.output__item__has__total_label),
                        data_sql_schema=[Column("label", String, primary_key=True), Column("label__total", Integer)],
                        create_table=self.create_table,
                    ),
                ).table_store
            ),
        )
        pipeline = Pipeline(
            [
                DatatableBatchTransform(
                    func=count_labels_total,
                    inputs=[self.input__item__ground_truth],
                    outputs=[self.output__item__has__total_label],
                    transform_keys=self.primary_keys,
                    labels=self.labels,
                    kwargs=dict(primary_keys=self.primary_keys),
                    chunk_size=self.chunk_size,
                ),
            ]
        )
        return build_compute(ds, catalog, pipeline)

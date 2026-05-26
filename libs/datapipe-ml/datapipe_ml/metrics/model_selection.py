from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional, Tuple, Union, cast

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
from datapipe.step.batch_transform import BatchTransform, DatatableBatchTransform
from datapipe.store.database import TableStoreDB
from datapipe.types import PipelineInput, PipelineOutput, IndexDF, Labels
from sqlalchemy import Column
from sqlalchemy.sql.sqltypes import Boolean

from datapipe_ml.core.datapipe import check_columns_are_in_table, pipeline_output_as_input


def get_best_model_flag(
    ds: DataStore,
    idx: IndexDF,
    input_dts: List[DataTable],
    run_config: Optional[RunConfig] = None,
    kwargs: Optional[Dict[str, Any]] = None,
):
    kwargs = kwargs or {}
    primary_keys: List[str] = kwargs["primary_keys"]
    group_by: Optional[List[str]] = kwargs["group_by"]
    is_best__name: bool = kwargs["is_best__name"]
    subset_id: str = kwargs["subset_id"]
    metric__name: str = kwargs["metric__name"]
    func: Literal["min", "max"] = kwargs["func"]
    threshold: Optional[float] = kwargs["threshold"]
    dt__model, dt__model__metrics_on__subset = input_dts

    df__model = dt__model.get_data()
    df__model__metrics_on__subset = dt__model__metrics_on__subset.get_data(
        idx=cast(IndexDF, pd.DataFrame([{"subset_id": subset_id}]))
    )
    df__model__metrics_on__subset.dropna(subset=[metric__name], inplace=True)
    if len(df__model__metrics_on__subset) == 0:
        df__model[is_best__name] = False
        return df__model[primary_keys + [is_best__name]]
    df__model__metrics_on__subset = pd.merge(df__model, df__model__metrics_on__subset, on=primary_keys, how="outer")
    df__model[is_best__name] = False
    df__model.set_index(primary_keys, inplace=True)
    # make both branches produce the same type:
    if group_by:
        # if group_by has one element, pandas wants a string not a list
        key: str | List[str] = group_by if len(group_by) > 1 else group_by[0]
        groups: List[Tuple[Any, pd.DataFrame]] = list(df__model__metrics_on__subset.groupby(key))
    else:
        groups = [(None, df__model__metrics_on__subset)]  # same List[Tuple[...]]

    for _, subgroup in groups:
        subgroup.set_index(primary_keys, inplace=True)
        if func == "min":
            best_model_idx = subgroup[metric__name].idxmin()
        else:  # func == "max"
            best_model_idx = subgroup[metric__name].idxmax()
        value = float(cast(Any, subgroup.at[best_model_idx, metric__name]))
        if not (
            threshold is not None and ((func == "min" and value > threshold) or (func == "max" and value < threshold))
        ):
            df__model.at[best_model_idx, is_best__name] = True
    df__model.reset_index(inplace=True)
    return df__model[primary_keys + [is_best__name]]


def get_best_model(df__attr__model__is_best: pd.DataFrame, primary_keys: List[str], is_best__name: str):
    df__best_model = df__attr__model__is_best[df__attr__model__is_best[is_best__name]]
    return df__best_model[primary_keys]


@dataclass
class FindBestModel(PipelineStep):
    input__model: PipelineInput
    input__model__metrics_on__subset: PipelineInput
    output__attr__model__is_best: PipelineOutput
    output__best_model: PipelineOutput
    subset_id: str
    is_best__name: str
    primary_keys: List[str]
    metric__name: str
    func: Literal["min", "max"]
    group_by: Optional[List[str]] = None
    create_table: bool = False
    labels: Optional[Labels] = None
    threshold: Optional[float] = None

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        if self.func not in ["min", "max"]:
            raise ValueError("func must be 'min' or 'max'")
        check_columns_are_in_table(ds, self.input__model, self.primary_keys)
        dt__input__model = ds.get_table(self.input__model)
        check_columns_are_in_table(
            ds,
            self.input__model__metrics_on__subset,
            self.primary_keys + ["subset_id", self.metric__name],
        )
        catalog.add_datatable(
            self.output__attr__model__is_best,
            Table(
                ds.get_or_create_table(
                    self.output__attr__model__is_best,
                    TableStoreDB(
                        dbconn=ds.meta_dbconn,
                        name=self.output__attr__model__is_best,
                        data_sql_schema=[
                            column for column in dt__input__model.primary_schema if column.name in self.primary_keys
                        ]
                        + [Column(self.is_best__name, Boolean)],
                        create_table=self.create_table,
                    ),
                ).table_store
            ),
        )
        catalog.add_datatable(
            self.output__best_model,
            Table(
                ds.get_or_create_table(
                    self.output__best_model,
                    TableStoreDB(
                        dbconn=ds.meta_dbconn,
                        name=self.output__best_model,
                        data_sql_schema=[
                            column for column in dt__input__model.primary_schema if column.name in self.primary_keys
                        ],
                        create_table=self.create_table,
                    ),
                ).table_store
            ),
        )
        pipeline = Pipeline(
            [
                DatatableBatchTransform(
                    func=get_best_model_flag,
                    inputs=[self.input__model, self.input__model__metrics_on__subset],
                    outputs=[self.output__attr__model__is_best],
                    transform_keys=self.primary_keys,
                    labels=self.labels,
                    kwargs=dict(
                        primary_keys=self.primary_keys,
                        group_by=self.group_by,
                        is_best__name=self.is_best__name,
                        subset_id=self.subset_id,
                        metric__name=self.metric__name,
                        func=self.func,
                        threshold=self.threshold,
                    ),
                    chunk_size=10000000,
                ),
                BatchTransform(
                    func=get_best_model,
                    inputs=[pipeline_output_as_input(self.output__attr__model__is_best)],
                    outputs=[self.output__best_model],
                    transform_keys=self.primary_keys,
                    labels=self.labels,
                    kwargs=dict(primary_keys=self.primary_keys, is_best__name=self.is_best__name),
                ),
            ]
        )
        return build_compute(ds, catalog, pipeline)

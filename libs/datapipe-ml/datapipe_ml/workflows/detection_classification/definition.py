from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pandas as pd
from cv_pipeliner.inferencers.pipeline import PipelineInferencer
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
from datapipe.step.batch_transform import (
    DatatableBatchTransform,
    DatatableBatchTransformFunc,
)
from datapipe.store.database import TableStoreDB
from datapipe.types import PipelineInput, PipelineOutput, IndexDF, Labels

from datapipe_ml.core.datapipe import check_columns_are_in_table, pipeline_output_as_input


def define_pipeline_model(
    ds: DataStore,
    idx: IndexDF,
    input_dts: List[DataTable],
    run_config: Optional[RunConfig] = None,
    kwargs: Optional[Dict[str, Any]] = None,
) -> PipelineInferencer:
    kwargs = kwargs or {}
    max_within_time: str = kwargs["max_within_time"]
    detection_model_primary_keys: List[str] = kwargs["detection_model_primary_keys"]
    classification_model_primary_keys: List[str] = kwargs["classification_model_primary_keys"]

    dt__detection_model, dt__classification_model, dt__pipeline_model = input_dts
    df__detection_model = pd.merge(
        dt__detection_model.get_data(idx=idx),
        dt__detection_model.get_metadata(idx=idx)[detection_model_primary_keys + ["create_ts"]].rename(
            columns={"create_ts": "detection_model__create_ts"}
        ),
    )
    df__classification_model = pd.merge(
        dt__classification_model.get_data(idx=idx),
        dt__classification_model.get_metadata(idx=idx)[classification_model_primary_keys + ["create_ts"]].rename(
            columns={"create_ts": "classification_model__create_ts"}
        ),
    )
    df__pipeline_model = pd.merge(df__detection_model, df__classification_model, how="cross")
    df__pipeline_model["within_time"] = (
        (df__pipeline_model["detection_model__create_ts"] - df__pipeline_model["classification_model__create_ts"])
        .abs()
        .apply(lambda x: pd.Timedelta(x, unit="s"))
    )
    df__pipeline_model = df__pipeline_model[df__pipeline_model["within_time"] <= pd.Timedelta(max_within_time)]
    return df__pipeline_model[detection_model_primary_keys + classification_model_primary_keys]


@dataclass
class Define_PipelineModel(PipelineStep):
    input__detection_model: PipelineInput
    input__classification_model: PipelineInput
    output__pipeline_model: PipelineOutput
    create_table: bool = False
    labels: Optional[Labels] = None
    max_within_time: str = "1w"
    define_pipeline_model_func: DatatableBatchTransformFunc = define_pipeline_model

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        dt__input__detection_model = ds.get_table(self.input__detection_model)
        dt__input__classification_model = ds.get_table(self.input__classification_model)
        assert (
            len(set(dt__input__detection_model.primary_keys).intersection(dt__input__classification_model.primary_keys))
            == 0
        )

        check_columns_are_in_table(
            ds,
            self.input__detection_model,
            [
                "detection_model__input_size",
                "detection_model__score_threshold",
                "detection_model__model_path",
                "detection_model__type",
                "detection_model__class_names",
            ],
        )
        check_columns_are_in_table(
            ds,
            self.input__classification_model,
            [
                "classification_model__input_size",
                "classification_model__model_path",
                "classification_model__type",
                "classification_model__class_names",
                "classification_model__preprocess_input_script_path",
            ],
        )
        # ---
        catalog.add_datatable(
            self.output__pipeline_model,
            Table(
                ds.get_or_create_table(
                    self.output__pipeline_model,
                    TableStoreDB(
                        dbconn=ds.meta_dbconn,
                        name=self.output__pipeline_model,
                        data_sql_schema=dt__input__detection_model.primary_schema
                        + dt__input__classification_model.primary_schema,
                        create_table=self.create_table,
                    ),
                ).table_store
            ),
        )
        # ---
        pipeline = Pipeline(
            [
                DatatableBatchTransform(
                    func=self.define_pipeline_model_func,
                    inputs=[
                        self.input__detection_model,
                        self.input__classification_model,
                        pipeline_output_as_input(self.output__pipeline_model),
                    ],
                    outputs=[self.output__pipeline_model],
                    transform_keys=dt__input__detection_model.primary_keys
                    + dt__input__classification_model.primary_keys,
                    labels=self.labels,
                    kwargs=dict(
                        max_within_time=self.max_within_time,
                        detection_model_primary_keys=dt__input__detection_model.primary_keys,
                        classification_model_primary_keys=dt__input__classification_model.primary_keys,
                    ),
                ),
            ]
        )
        return build_compute(ds, catalog, pipeline)

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
from cv_pipeliner import ImageData, TensorFlow_ClassificationModelSpec
from cv_pipeliner.inferencers.classification.core import ClassificationModelSpec
from datapipe.compute import (
    Catalog,
    ComputeStep,
    Pipeline,
    PipelineStep,
    Table,
    build_compute,
)
from datapipe.datatable import DataStore
from datapipe.executor import ExecutorConfig
from datapipe.run_config import LabelDict
from datapipe.step.batch_transform import BatchTransform
from datapipe.store.database import TableStoreDB
from datapipe.types import required_pipeline_input, PipelineInput, PipelineOutput, Labels
from sqlalchemy import Column, Float
from sqlalchemy.sql.sqltypes import Integer, String

from datapipe_ml.core.datapipe import check_columns_are_in_table, normalize_pipeline_inputs
from datapipe_ml.core.image_data import check_if_images_opens


def get_classification_model_spec(
    classification_model__input_size: Tuple[int, int],
    classification_model__model_path: str,
    classification_model__class_names: Tuple[str, ...],
    classification_model__type: str,
    classification_model__preprocess_input_script_path: str,
) -> ClassificationModelSpec:
    if classification_model__type == "tf.keras":
        cls_model_spec = TensorFlow_ClassificationModelSpec(
            input_size=classification_model__input_size,
            class_names=classification_model__class_names,
            model_path=classification_model__model_path,
            saved_model_type=classification_model__type,
            preprocess_input=classification_model__preprocess_input_script_path,
        )
    else:
        raise ValueError(f"Unknown {classification_model__type=}")
    return cls_model_spec


def classification_inference(
    *dfs,
    image__image_path__name: str,
    primary_keys: List[str],
    top_n: int,
    min_idx: int,
    max_idx: int,
    batch_size_default: int,
    classification_model_primary_keys: List[str],
):
    df__image = dfs[0]
    if len(dfs) >= 3:
        for df in dfs[1:-1]:
            df__image = pd.merge(df, df__image, on=primary_keys)
    df__classification_model: pd.DataFrame = dfs[-1]
    classification_model_other_primary_keys = [
        primary_key for primary_key in classification_model_primary_keys if primary_key not in primary_keys
    ]
    if df__image.empty:
        return pd.DataFrame(
            columns=primary_keys
            + classification_model_other_primary_keys
            + [
                "label",
                "prediction__top_n",
                "prediction__classification_score",
            ]
        )
    image_paths_to_check = np.array(df__image[image__image_path__name])
    idxs = check_if_images_opens([str(path) for path in image_paths_to_check])
    df__image = df__image.loc[idxs].reset_index(drop=True)

    if len(set(df__image.columns).intersection(set(df__classification_model.columns))) != 0:
        df__cross = pd.merge(df__image, df__classification_model)
    else:
        df__cross = pd.merge(df__image, df__classification_model, how="cross")
    df__prediction_classification_list = []
    for _, df_grouped in df__cross.groupby(classification_model_primary_keys):
        cls_model_spec = get_classification_model_spec(
            classification_model__input_size=(
                df_grouped.iloc[0]["classification_model__input_size"][0],
                df_grouped.iloc[0]["classification_model__input_size"][1],
            ),
            classification_model__model_path=df_grouped.iloc[0]["classification_model__model_path"],
            classification_model__class_names=tuple(
                [str(class_name) for class_name in df_grouped.iloc[0]["classification_model__class_names"]]
            ),
            classification_model__type=df_grouped.iloc[0]["classification_model__type"],
            classification_model__preprocess_input_script_path=df_grouped.iloc[0][
                "classification_model__preprocess_input_script_path"
            ],
        )
        cls_model = cls_model_spec.load_classification_inferencer()
        images_data = [ImageData(image_path=image_path) for image_path in df_grouped[image__image_path__name]]
        pred_images_data: List[ImageData] = cls_model.predict(
            images_data,
            open_images_in_data=False,
            top_n=top_n,
            disable_tqdm=True,
            batch_size_default=batch_size_default,
        )
        for index, pred_image_data in zip(df_grouped.index, pred_images_data):
            label_to_cls_scores: Dict[str, List[float]] = {}
            for label, classification_score in zip(
                pred_image_data.labels_top_n, pred_image_data.classification_scores_top_n
            ):
                if label not in label_to_cls_scores:
                    label_to_cls_scores[label] = []
                label_to_cls_scores[label].append(classification_score)
            label_to_cls_score = {label: sum(label_to_cls_scores[label]) for label in label_to_cls_scores}
            pred_labels = list(label_to_cls_score)
            df__prediction_classification_list.extend(
                [
                    dict(
                        **{primary_key: df_grouped.loc[index, primary_key] for primary_key in primary_keys},
                        **{
                            primary_key: df_grouped.loc[index, primary_key]
                            for primary_key in classification_model_other_primary_keys
                        },
                        label=pred_labels[top_n],
                        prediction__top_n=top_n + 1,
                        prediction__classification_score=label_to_cls_score[pred_labels[top_n]],
                    )
                    for top_n in range(min_idx, min(len(pred_labels) + 1, max_idx + 1))
                ]
            )

    df__prediction_classification = pd.DataFrame(
        df__prediction_classification_list,
        columns=primary_keys
        + classification_model_other_primary_keys
        + [
            "label",
            "prediction__top_n",
            "prediction__classification_score",
        ],
    )
    return df__prediction_classification


@dataclass
class Inference_ClassificationModel(PipelineStep):
    input__image: PipelineInput | Sequence[PipelineInput]
    input__classification_model: PipelineInput
    output__classification_prediction: PipelineOutput
    primary_keys: List[str]
    chunk_size: int = 1000
    create_table: bool = False
    labels: Optional[Labels] = None
    image__image_path__name: str = "image__image_path"
    top_n: int = 1
    min_idx: int = 0
    max_idx: int = 0
    batch_size_default: int = 16
    executor_config: Optional[ExecutorConfig] = None
    filters: Union[LabelDict, Callable[[], LabelDict], None] = None
    classification_model_primary_keys: Optional[List[str]] = None

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        if self.classification_model_primary_keys is None:
            self.classification_model_primary_keys = ["classification_model_id"]
        check_columns_are_in_table(ds, self.input__image, self.primary_keys)
        input__images = normalize_pipeline_inputs(self.input__image)
        assert any(
            [
                check_columns_are_in_table(ds, input__image, [self.image__image_path__name], raise_exc=False)
                for input__image in input__images
            ]
        )
        dt__input__images = [ds.get_table(input__image) for input__image in input__images]
        dt__input__classification_model = ds.get_table(self.input__classification_model)
        check_columns_are_in_table(
            ds,
            self.input__classification_model,
            self.classification_model_primary_keys
            + [
                "classification_model__input_size",
                "classification_model__model_path",
                "classification_model__type",
                "classification_model__class_names",
                "classification_model__preprocess_input_script_path",
            ],
        )
        # ---
        catalog.add_datatable(
            self.output__classification_prediction,
            Table(
                ds.get_or_create_table(
                    self.output__classification_prediction,
                    TableStoreDB(
                        dbconn=ds.meta_dbconn,
                        name=self.output__classification_prediction,
                        data_sql_schema=[
                            column for column in dt__input__images[0].primary_schema if column.name in self.primary_keys
                        ]
                        + [
                            column
                            for column in dt__input__classification_model.primary_schema
                            if column.name not in self.primary_keys
                        ]
                        + [
                            Column("prediction__top_n", Integer, primary_key=True),
                            Column("label", String),
                            Column("prediction__classification_score", Float),
                        ],
                        create_table=self.create_table,
                    ),
                ).table_store
            ),
        )
        # ---
        pipeline = Pipeline(
            [
                BatchTransform(
                    func=classification_inference,
                    inputs=[
                        *[required_pipeline_input(input__image) for input__image in input__images],
                        self.input__classification_model,
                    ],
                    outputs=[self.output__classification_prediction],
                    transform_keys=list(set(self.primary_keys + self.classification_model_primary_keys)),
                    chunk_size=self.chunk_size,
                    labels=self.labels,
                    order_by=self.classification_model_primary_keys,
                    order="desc",
                    executor_config=self.executor_config,
                    kwargs=dict(
                        primary_keys=self.primary_keys,
                        image__image_path__name=self.image__image_path__name,
                        top_n=self.top_n,
                        min_idx=self.min_idx,
                        max_idx=self.max_idx,
                        batch_size_default=self.batch_size_default,
                        classification_model_primary_keys=self.classification_model_primary_keys,
                    ),
                    filters=self.filters,
                ),
            ]
        )
        return build_compute(ds, catalog, pipeline)

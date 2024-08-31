from typing import cast

import pandas as pd
from sqlalchemy import Column
from sqlalchemy.sql.sqltypes import Integer, String

from datapipe.compute import Catalog, Pipeline, Table, build_compute, run_steps
from datapipe.datatable import DataStore
from datapipe.step.batch_generate import BatchGenerate
from datapipe.step.batch_transform import BatchTransform
from datapipe.store.database import TableStoreDB
from datapipe.types import IndexDF

from .util import assert_datatable_equal, assert_df_equal

TEST__ITEM = pd.DataFrame(
    {
        "item_id": [f"item_id{i}" for i in range(10)],
        "item__attribute": [f"item__attribute{i}" for i in range(10)],
    }
)

TEST__KEYPOINT = pd.DataFrame(
    {
        "keypoint_id": list(range(5)),
        "keypoint_name": [f"keypoint_name{i}" for i in range(5)],
    }
)

TEST__PIPELINE = pd.DataFrame(
    {
        "pipeline_id": [f"pipeline_id{i}" for i in range(3)],
        "pipeline__attribute": [f"pipeline_attribute{i}" for i in range(3)],
    }
)

TEST__PREDICTION_LEFT = pd.DataFrame(
    {
        "item_id": [f"item_id{i}" for i in range(10)],
    }
)
TEST__PREDICTION_CENTER = pd.DataFrame(
    {
        "pipeline_id": [f"pipeline_id{i}" for i in range(3)],
    }
)
TEST__PREDICTION_RIGHT = pd.DataFrame(
    {
        "keypoint_name": [f"keypoint_name{i}" for i in range(5)],
        "prediction__attribute": [f"prediction__attribute{i}" for i in range(5)],
    }
)
TEST__PREDICTION = pd.merge(TEST__PREDICTION_LEFT, TEST__PREDICTION_CENTER, how="cross")
TEST__PREDICTION = pd.merge(TEST__PREDICTION, TEST__PREDICTION_RIGHT, how="cross")
TEST__PREDICTION = TEST__PREDICTION[
    ["item_id", "pipeline_id", "keypoint_name", "prediction__attribute"]
]


def test_complex_pipeline(dbconn):
    ds = DataStore(dbconn, create_meta_table=True)
    catalog = Catalog(
        {
            "item": Table(
                store=TableStoreDB(
                    dbconn,
                    "item",
                    [
                        Column("item_id", String, primary_key=True),
                        Column("item__attribute", String),
                    ],
                    True,
                )
            ),
            "pipeline": Table(
                store=TableStoreDB(
                    dbconn,
                    "pipeline",
                    [
                        Column("pipeline_id", String, primary_key=True),
                        Column("pipeline__attribute", String),
                    ],
                    True,
                )
            ),
            "prediction": Table(
                store=TableStoreDB(
                    dbconn,
                    "prediction",
                    [
                        Column("item_id", String, primary_key=True),
                        Column("pipeline_id", String, primary_key=True),
                        Column("keypoint_name", String, primary_key=True),
                        Column("prediction__attribute", String),
                    ],
                    True,
                )
            ),
            "keypoint": Table(
                store=TableStoreDB(
                    dbconn,
                    "keypoint",
                    [
                        Column("keypoint_id", Integer, primary_key=True),
                        Column("keypoint_name", String, primary_key=True),
                    ],
                    True,
                )
            ),
            "output": Table(
                store=TableStoreDB(
                    dbconn,
                    "output",
                    [
                        Column("item_id", String, primary_key=True),
                        Column("pipeline_id", String, primary_key=True),
                        Column("attirbute", String),
                    ],
                    True,
                )
            ),
        }
    )

    def complex_function(
        df__item, df__pipeline, df__prediction, df__keypoint, idx: IndexDF
    ):
        assert idx[idx[["item_id", "pipeline_id"]].duplicated()].empty
        assert len(df__keypoint) == len(TEST__KEYPOINT)
        df__output = pd.merge(df__item, df__prediction, on=["item_id"])
        df__output = pd.merge(df__output, df__pipeline, on=["pipeline_id"])
        df__output = pd.merge(df__output, df__keypoint, on=["keypoint_name"])
        df__output = df__output[["item_id", "pipeline_id"]].drop_duplicates()
        df__output["attirbute"] = "attribute"
        return df__output

    pipeline = Pipeline(
        [
            BatchTransform(
                func=complex_function,
                inputs=["item", "pipeline", "prediction", "keypoint"],
                outputs=["output"],
                transform_keys=["item_id", "pipeline_id"],
                chunk_size=50,
            ),
        ]
    )
    steps = build_compute(ds, catalog, pipeline)
    ds.get_table("item").store_chunk(TEST__ITEM)
    ds.get_table("pipeline").store_chunk(TEST__PIPELINE)
    ds.get_table("prediction").store_chunk(TEST__PREDICTION)
    ds.get_table("keypoint").store_chunk(TEST__KEYPOINT)
    TEST_RESULT = complex_function(
        TEST__ITEM,
        TEST__PIPELINE,
        TEST__PREDICTION,
        TEST__KEYPOINT,
        idx=cast(IndexDF, pd.DataFrame(columns=["item_id", "pipeline_id"])),
    )
    run_steps(ds, steps)
    assert_datatable_equal(ds.get_table("output"), TEST_RESULT)


TEST__FROZEN_DATASET = pd.DataFrame(
    {
        "frozen_dataset_id": [f"frozen_dataset_id{i}" for i in range(2)],
    }
)

TEST__TRAIN_CONFIG = pd.DataFrame(
    {
        "train_config_id": [f"train_config_id{i}" for i in range(2)],
        "train_config__params": [f"train_config__params{i}" for i in range(2)],
    }
)


def test_complex_train_pipeline(dbconn):
    ds = DataStore(dbconn, create_meta_table=True)
    catalog = Catalog(
        {
            "frozen_dataset": Table(
                store=TableStoreDB(
                    dbconn,
                    "frozen_dataset",
                    [
                        Column("frozen_dataset_id", String, primary_key=True),
                    ],
                    True,
                )
            ),
            "train_config": Table(
                store=TableStoreDB(
                    dbconn,
                    "train_config",
                    [
                        Column("train_config_id", String, primary_key=True),
                        Column("train_config__params", String),
                    ],
                    True,
                )
            ),
            "pipeline": Table(
                store=TableStoreDB(
                    dbconn,
                    "pipeline",
                    [
                        Column("pipeline_id", String, primary_key=True),
                        Column("pipeline__attribute", String),
                    ],
                    True,
                )
            ),
            "pipeline__is_trained_on__frozen_dataset": Table(
                store=TableStoreDB(
                    dbconn,
                    "pipeline__is_trained_on__frozen_dataset",
                    [
                        Column("pipeline_id", String, primary_key=True),
                        Column("frozen_dataset_id", String, primary_key=True),
                        Column("train_config_id", String, primary_key=True),
                    ],
                    True,
                )
            ),
        }
    )

    def train(
        df__frozen_dataset,
        df__train_config,
        df__pipeline__total,
        df__pipeline__is_trained_on__frozen_dataset__total,
    ):
        assert len(df__frozen_dataset) == 1 and len(df__train_config) == 1
        frozen_dataset_id = df__frozen_dataset.iloc[0]["frozen_dataset_id"]
        train_config_id = df__train_config.iloc[0]["train_config_id"]
        df__pipeline = pd.DataFrame(
            [
                {
                    "pipeline_id": f"new_pipeline__{frozen_dataset_id}x{train_config_id}",
                    "pipeline__attribute": "new_pipeline__attr",
                }
            ]
        )
        df__pipeline__is_trained_on__frozen_dataset = pd.DataFrame(
            [
                {
                    "pipeline_id": f"new_pipeline__{frozen_dataset_id}x{train_config_id}",
                    "frozen_dataset_id": frozen_dataset_id,
                    "train_config_id": train_config_id,
                }
            ]
        )
        df__pipeline__total = pd.concat(
            [df__pipeline__total, df__pipeline], ignore_index=True
        )
        df__pipeline__is_trained_on__frozen_dataset__total = pd.concat(
            [
                df__pipeline__is_trained_on__frozen_dataset__total,
                df__pipeline__is_trained_on__frozen_dataset,
            ],
            ignore_index=True,
        )
        return df__pipeline__total, df__pipeline__is_trained_on__frozen_dataset__total

    pipeline = Pipeline(
        [
            BatchTransform(
                func=train,
                inputs=[
                    "frozen_dataset",
                    "train_config",
                    "pipeline",
                    "pipeline__is_trained_on__frozen_dataset",
                ],
                outputs=["pipeline", "pipeline__is_trained_on__frozen_dataset"],
                transform_keys=["frozen_dataset_id", "train_config_id"],
                chunk_size=1,
            ),
        ]
    )
    steps = build_compute(ds, catalog, pipeline)
    ds.get_table("frozen_dataset").store_chunk(TEST__FROZEN_DATASET)
    ds.get_table("train_config").store_chunk(TEST__TRAIN_CONFIG)
    run_steps(ds, steps)
    assert len(ds.get_table("pipeline").get_data()) == len(TEST__FROZEN_DATASET) * len(
        TEST__TRAIN_CONFIG
    )
    assert len(
        ds.get_table("pipeline__is_trained_on__frozen_dataset").get_data()
    ) == len(TEST__FROZEN_DATASET) * len(TEST__TRAIN_CONFIG)


def complex_transform_with_many_recordings_but_take_only_val_items(dbconn, train_N: int, val_N: int):
    ds = DataStore(dbconn, create_meta_table=True)
    catalog = Catalog(
        {
            "tbl_image": Table(
                store=TableStoreDB(
                    dbconn,
                    "tbl_image",
                    [
                        Column("image_id", Integer, primary_key=True),
                        Column("image__attribute", Integer),
                    ],
                    True,
                )
            ),
            "tbl_subset__has__image": Table(
                store=TableStoreDB(
                    dbconn,
                    "tbl_subset__has__image",
                    [
                        Column("image_id", Integer, primary_key=True),
                        Column("subset_id", String, primary_key=True),
                    ],
                    True,
                )
            ),
            "tbl_model": Table(
                store=TableStoreDB(
                    dbconn,
                    "tbl_best_model",
                    [
                        Column("model_id", Integer, primary_key=True),
                        Column("model__attribute", Integer),
                    ],
                    True,
                )
            ),
            "tbl_prediction": Table(
                store=TableStoreDB(
                    dbconn,
                    "tbl_prediction",
                    [
                        Column("image_id", Integer, primary_key=True),
                        Column("model_id", Integer, primary_key=True),
                        Column("prediction__attribite", Integer),
                    ],
                    True,
                )
            ),
        }
    )

    def gen_tbls(df1, df2, df3):
        yield df1, df2, df3

    test_df__image = pd.DataFrame({
        "image_id": range(train_N+val_N),
        "image__attribute": range(train_N+val_N)
    })
    test_df__subset__has__image = pd.DataFrame(
        {
            "image_id": list(range(train_N)) + list(range(train_N, train_N+val_N)),
            "subset_id": ["train"] * train_N + ["val"] * val_N,
        }
    )
    test_df__model = pd.DataFrame(
        {
            "model_id": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            "model__attribute": [100 * model_id for model_id in range(10)]
        }
    )

    def inference_model(
        df__image: pd.DataFrame,
        df__image__has__subset: pd.DataFrame,
        df__model: pd.DataFrame,
    ):
        df__image__has__subset = df__image__has__subset[
            df__image__has__subset["subset_id"] == "val"  # FIXME: хочется, чтобы это было снаружи функции
        ]
        df__image = pd.merge(df__image, df__image__has__subset)
        df__prediction = pd.merge(df__image, df__model, how="cross")
        df__prediction["prediction__attribite"] = df__prediction["image__attribute"] + df__prediction["model__attribute"]
        return df__prediction[["image_id", "model_id", "prediction__attribite"]]

    pipeline = Pipeline(
        [
            BatchGenerate(
                func=gen_tbls,
                outputs=[
                    "tbl_image",
                    "tbl_subset__has__image",
                    "tbl_model",
                ],
                kwargs=dict(
                    df1=test_df__image,
                    df2=test_df__subset__has__image,
                    df3=test_df__model,
                ),
            ),
            BatchTransform(
                func=inference_model,
                inputs=[
                    "tbl_image",  # ["image_id"]
                    "tbl_subset__has__image",  # ["image_id", "subset_id"]
                    "tbl_model"  # ["model_id"]
                ],
                outputs=["tbl_prediction"],  # ["image_id", "model_id", "prediction__attribite"]
                transform_keys=["image_id", "model_id"],
                chunk_size=100
            ),
        ]
    )
    steps = build_compute(ds, catalog, pipeline)
    run_steps(ds, steps)
    test__df_prediction = pd.DataFrame(
        {
            "image_id": list(range(train_N, train_N+val_N)) * 10,
            "model_id": [model_id for model_id in range(10) for _ in range(val_N)],
            "prediction__attribite": [
                (model_id * 100) + x
                for model_id in range(10)
                for x in range(train_N, train_N+val_N)
            ]
        }
    )
    assert_df_equal(
        ds.get_table("tbl_prediction").get_data(),
        test__df_prediction,
        index_cols=["image_id", "model_id"],
    )


def test_complex_transform_with_many_recordings_but_take_only_val_items_trainN100_valN100(dbconn):
    complex_transform_with_many_recordings_but_take_only_val_items(dbconn, train_N=100, val_N=100)


def test_complex_transform_with_many_recordings_but_take_only_val_items_trainN10000_valN1000(dbconn):
    complex_transform_with_many_recordings_but_take_only_val_items(dbconn, train_N=10000, val_N=1000)


def test_complex_transform_with_many_recordings_but_take_only_val_items_trainN100000_valN1000(dbconn):
    complex_transform_with_many_recordings_but_take_only_val_items(dbconn, train_N=100000, val_N=1000)

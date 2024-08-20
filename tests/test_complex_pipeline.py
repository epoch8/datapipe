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
        idx=pd.DataFrame(columns=["item_id", "pipeline_id"]),
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


def test_complex_transform_with_filters(dbconn):
    ds = DataStore(dbconn, create_meta_table=True)
    catalog = Catalog({
        "tbl_image": Table(
            store=TableStoreDB(
                dbconn,
                "tbl_image",
                [
                    Column("image_id", Integer, primary_key=True),
                ],
                True
            )
        ),
        "tbl_subset__has__image": Table(
            store=TableStoreDB(
                dbconn,
                "tbl_subset__has__image",
                [
                    Column("image_id", Integer, primary_key=True),
                    Column("subset_id", Integer, primary_key=True),
                ],
                True
            )
        ),
        "tbl_prediction": Table(
            store=TableStoreDB(
                dbconn,
                "tbl_prediction",
                [
                    Column("image_id", Integer, primary_key=True),
                    Column("model_id", Integer, primary_key=True),
                ],
                True
            )
        ),
        "tbl_output": Table(
            store=TableStoreDB(
                dbconn,
                "tbl_output",
                [
                    Column("subset_id", Integer, primary_key=True),
                    Column("model_id", Integer, primary_key=True),
                    Column("count", Integer),
                ],
                True
            )
        )
    })

    def gen_tbl(df):
        yield df

    test_df__image = pd.DataFrame({
        "image_id": range(1000)
    })
    test_df__subset__has__image = pd.DataFrame({
        "image_id": range(1000),
        "subset_id": [0 for _ in range(200)] + [1 for _ in range(200)] + [2 for _ in range(600)]
    })
    test_df__prediction = pd.DataFrame({
        "image_id": list(range(1000)) + list(range(1000)),
        "model_id": [0] * 1000 + [1] * 1000
    })

    def count_func(
        df__image: pd.DataFrame,
        df__subset__has__image: pd.DataFrame,
        df__prediction: pd.DataFrame,
    ):
        df__image = pd.merge(df__image, df__subset__has__image, on=["image_id"])
        df__image = pd.merge(df__image, df__prediction, on=["image_id"])
        df__output = df__image.groupby(["subset_id", "model_id"]).agg(len).reset_index().rename(columns={"image_id": "count"})
        return df__output

    pipeline = Pipeline(
        [
            BatchGenerate(
                func=gen_tbl,
                outputs=["tbl_image"],
                kwargs=dict(df=test_df__image),
            ),
            BatchGenerate(
                func=gen_tbl,
                outputs=["tbl_subset__has__image"],
                kwargs=dict(df=test_df__subset__has__image),
            ),
            BatchGenerate(
                func=gen_tbl,
                outputs=["tbl_prediction"],
                kwargs=dict(df=test_df__prediction),
            ),
            BatchTransform(
                func=count_func,
                inputs=["tbl_image", "tbl_subset__has__image", "tbl_prediction"],
                outputs=["tbl_output"],
                transform_keys=["subset_id", "model_id"],
                chunk_size=100,
                filters=[
                    {"subset_id": 0},
                    {"subset_id": 1},
                ]
            ),
        ]
    )
    steps = build_compute(ds, catalog, pipeline)
    run_steps(ds, steps)
    test__df_output = count_func(
        df__image=test_df__image,
        df__subset__has__image=test_df__subset__has__image[
            test_df__subset__has__image["subset_id"].isin([0, 1])
        ],
        df__prediction=test_df__prediction
    )
    assert_df_equal(
        ds.get_table("tbl_output").get_data(),
        test__df_output,
        index_cols=["model_id"]
    )


def test_complex_transform_with_filters2(dbconn):
    ds = DataStore(dbconn, create_meta_table=True)
    catalog = Catalog({
        "tbl_image": Table(
            store=TableStoreDB(
                dbconn,
                "tbl_image",
                [
                    Column("image_id", Integer, primary_key=True),
                ],
                True
            )
        ),
        "tbl_model": Table(
            store=TableStoreDB(
                dbconn,
                "tbl_model",
                [
                    Column("model_id", Integer, primary_key=True),
                ],
                True
            )
        ),
        "tbl_prediction": Table(
            store=TableStoreDB(
                dbconn,
                "tbl_prediction",
                [
                    Column("image_id", Integer, primary_key=True),
                    Column("model_id", Integer, primary_key=True),
                ],
                True
            )
        )
    })

    def gen_tbl(df):
        yield df

    test_df__image = pd.DataFrame({
        "image_id": range(1000)
    })
    test_df__model = pd.DataFrame({
        "model_id": [0, 1, 2, 3, 4]
    })
    
    def filters_images():
        return [{"image_id": i} for i in range(500)]


    def make_prediction(
        df__image: pd.DataFrame,
        df__model: pd.DataFrame,
    ):
        df__prediction = pd.merge(df__image, df__model, how="cross")
        return df__prediction

    pipeline = Pipeline(
        [
            BatchGenerate(
                func=gen_tbl,
                outputs=["tbl_image"],
                kwargs=dict(df=test_df__image),
            ),
            BatchGenerate(
                func=gen_tbl,
                outputs=["tbl_model"],
                kwargs=dict(df=test_df__model),
            ),
            BatchTransform(
                func=make_prediction,
                inputs=["tbl_image", "tbl_model"],
                outputs=["tbl_prediction"],
                transform_keys=["image_id", "model_id"],
                chunk_size=100,
                filters=filters_images
            ),
        ]
    )
    steps = build_compute(ds, catalog, pipeline)
    run_steps(ds, steps)
    test__df_output = make_prediction(
        df__image=test_df__image[
            test_df__image["image_id"].isin([r["image_id"] for r in filters_images()])
        ],
        # df__image=test_df__image,
        df__model=test_df__model
    )
    assert_df_equal(
        ds.get_table("tbl_prediction").get_data(),
        test__df_output,
        index_cols=["model_id"]
    )

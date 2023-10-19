import pandas as pd
from sqlalchemy import Column
from sqlalchemy.sql.sqltypes import Integer, String

from datapipe.compute import Catalog, Pipeline, Table, build_compute, run_steps
from datapipe.datatable import DataStore
from datapipe.step.batch_transform import BatchTransform
from datapipe.store.database import TableStoreDB
from datapipe.types import IndexDF

from .util import assert_datatable_equal

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


TEST__FROZEN_DATASET1 = pd.DataFrame(
    {
        "frozen_dataset_id": ["frozen_dataset_id1"],
    }
)
TEST__FROZEN_DATASET2 = pd.DataFrame(
    {
        "frozen_dataset_id": ["frozen_dataset_id2"],
    }
)

TEST__FROZEN_DATASET__HAS__ITEM1 = pd.DataFrame(
    {
        "frozen_dataset_id": ["frozen_dataset_id1" for i in range(10)],
        "item_id": [f"item_id{i}" for i in range(10)],
    }
)
TEST__FROZEN_DATASET__HAS__ITEM2 = pd.DataFrame(
    {
        "frozen_dataset_id": ["frozen_dataset_id2" for i in range(20)],
        "item_id": [f"item_id{i}" for i in range(20)],
    }
)


TEST__TRAIN_CONFIG = pd.DataFrame(
    {
        "train_config_id": ["train_config_id1", "train_config_id2"],
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
            "frozen_dataset__has__item": Table(
                store=TableStoreDB(
                    dbconn,
                    "frozen_dataset__has__item",
                    [
                        Column("frozen_dataset_id", String, primary_key=True),
                        Column("item_id", String, primary_key=True),
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
                    ],
                    True,
                )
            ),
            "model": Table(
                store=TableStoreDB(
                    dbconn,
                    "model",
                    [
                        Column("frozen_dataset_id", String, primary_key=True),
                        Column("train_config_id", String, primary_key=True),
                    ],
                    True,
                )
            ),
        }
    )

    def train_model(
        df__frozen_datset,
        df__frozen_dataset__has__item,
        df__train_config,
    ):
        df__model = pd.merge(df__frozen_datset, df__train_config, how="cross")
        # assert idx[idx[["item_id", "pipeline_id"]].duplicated()].empty
        # assert len(df__keypoint) == len(TEST__KEYPOINT)
        return df__model

    pipeline = Pipeline(
        [
            BatchTransform(
                func=train_model,
                inputs=["frozen_dataset", "frozen_dataset__has__item", "train_config"],
                outputs=["model"],
                transform_keys=["frozen_dataset_id", "train_config_id"],
                chunk_size=1000,
            ),
        ]
    )
    steps = build_compute(ds, catalog, pipeline)
    step = steps[0]
    ds.get_table("frozen_dataset").store_chunk(TEST__FROZEN_DATASET1)
    ds.get_table("frozen_dataset__has__item").store_chunk(TEST__FROZEN_DATASET__HAS__ITEM1)
    ds.get_table("train_config").store_chunk(TEST__TRAIN_CONFIG)
    idx_count, _ = step.get_full_process_ids(ds)
    assert idx_count == 1
    TEST_RESULT1 = train_model(
        TEST__FROZEN_DATASET1,
        TEST__FROZEN_DATASET__HAS__ITEM1,
        TEST__TRAIN_CONFIG,
    )
    run_steps(ds, steps)
    assert_datatable_equal(ds.get_table("model"), TEST_RESULT1)

    ds.get_table("frozen_dataset").store_chunk(TEST__FROZEN_DATASET2)
    ds.get_table("frozen_dataset__has__item").store_chunk(TEST__FROZEN_DATASET__HAS__ITEM2)
    idx_count, _ = step.get_full_process_ids(ds)
    assert idx_count == 1
    run_steps(ds, steps)
    TEST_RESULT2 = train_model(
        TEST__FROZEN_DATASET2,
        TEST__FROZEN_DATASET__HAS__ITEM2,
        TEST__TRAIN_CONFIG,
    )
    assert_datatable_equal(ds.get_table("model"), pd.merge(TEST_RESULT1, TEST_RESULT2, how='outer'))


TEST__ITEM_LARGE = pd.DataFrame(
    {
        "item_id": [f"item_id{i}" for i in range(10000)],
        "item__attribute": [f"item__attribute{i}" for i in range(10000)],
    }
)
TEST__ITEM_HAS_GROUND_TRUTH = pd.DataFrame(
    {
        "item_id": [f"item_id{i}" for i in range(5000)],
    }
)

TEST__MODEL_LARGE = pd.DataFrame(
    {
        "model_id": [f"model_id{i}" for i in range(10)],
        "model__attribute": [f"item__attribute{i}" for i in range(10)],
    }
)


def test_complex_cross_merge_scenary_with_inner_filter(dbconn):
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
            "item__has__ground_truth": Table(
                store=TableStoreDB(
                    dbconn,
                    "item__has__ground_truth",
                    [
                        Column("item_id", String, primary_key=True),
                    ],
                    True,
                )
            ),
            "model": Table(
                store=TableStoreDB(
                    dbconn,
                    "model",
                    [
                        Column("model_id", String, primary_key=True),
                        Column("model__attribute", String),
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
                        Column("model_id", String, primary_key=True),
                    ],
                    True,
                )
            ),
        }
    )

    def inference_model(df__item, df__item__has__ground_truth, df__model):
        df__item = pd.merge(df__item, df__item__has__ground_truth)
        df__prediction = pd.merge(df__item[["item_id"]], df__model[["model_id"]], how="cross")
        return df__prediction

    pipeline = Pipeline(
        [
            BatchTransform(
                func=inference_model,
                inputs=["item", "item__has__ground_truth", "model"],
                outputs=["prediction"],
                transform_keys=["item_id", "model_id"],
                chunk_size=1000,
            ),
        ]
    )
    steps = build_compute(ds, catalog, pipeline)
    ds.get_table("item").store_chunk(TEST__ITEM_LARGE)
    ds.get_table("item__has__ground_truth").store_chunk(TEST__ITEM_HAS_GROUND_TRUTH)
    ds.get_table("model").store_chunk(TEST__MODEL_LARGE)
    TEST_RESULT = inference_model(
        df__item=TEST__ITEM_LARGE,
        df__item__has__ground_truth=TEST__ITEM_HAS_GROUND_TRUTH,
        df__model=TEST__MODEL_LARGE,
    )
    run_steps(ds, steps)
    assert_datatable_equal(ds.get_table("prediction"), TEST_RESULT)


TEST__MODEL = pd.DataFrame(
    {
        "model_id": [f"model_id{i}" for i in range(2)],
        "model__attribute": [f"item__attribute{i}" for i in range(2)],
    }
)


def test_cyclic_cross_merge(dbconn):
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
            "model": Table(
                store=TableStoreDB(
                    dbconn,
                    "model",
                    [
                        Column("model_id", String, primary_key=True),
                        Column("model__attribute", String),
                    ],
                    True,
                )
            ),
            "best_model": Table(
                store=TableStoreDB(
                    dbconn,
                    "best_model",
                    [
                        Column("model_id", String, primary_key=True),
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
                        Column("model_id", String, primary_key=True),
                    ],
                    True,
                )
            ),
        }
    )

    def inference_best_model(
        df__item,
        df__model,
        df__best_model,
        df__prediction,
    ):
        df__model = pd.merge(df__model, df__best_model)
        df__prediction_res = pd.merge(df__item[["item_id"]], df__model[["model_id"]], how="cross")
        df__prediction = pd.merge(df__prediction_res, df__prediction, how="outer")
        return df__model

    pipeline = Pipeline(
        [
            BatchTransform(
                func=inference_best_model,
                inputs=["item", "model", "best_model", "prediction"],
                outputs=["prediction"],
                transform_keys=["item_id", "model_id"],
                chunk_size=1000,
            ),
        ]
    )
    steps = build_compute(ds, catalog, pipeline)
    step = steps[0]
    test_df__item1 = TEST__ITEM.iloc[0:5]
    test_df__model1 = TEST__MODEL.iloc[0:1]
    test_df__best_model1 = TEST__MODEL.iloc[0:1][["model_id"]]
    ds.get_table("item").store_chunk(test_df__item1)
    ds.get_table("model").store_chunk(test_df__model1)
    ds.get_table("best_model").store_chunk(test_df__best_model1)
    idx_count, _ = step.get_full_process_ids(ds)
    assert idx_count > 0
    TEST_RESULT1 = inference_best_model(
        df__item=test_df__item1,
        df__model=test_df__model1,
        df__best_model=test_df__best_model1,
        df__prediction=pd.DataFrame({}, columns=["item_id", "model_id"]),
    )
    run_steps(ds, steps)
    assert_datatable_equal(ds.get_table("prediction"), TEST_RESULT1)

    test_df__item2 = TEST__ITEM.iloc[5:10]
    test_df__model2 = TEST__MODEL.iloc[1:2]
    test_df__best_model2 = TEST__MODEL.iloc[1:2][["model_id"]]
    ds.get_table("item").store_chunk(test_df__item2)
    ds.get_table("model").store_chunk(test_df__model2)
    ds.get_table("best_model").delete_by_idx(ds.get_table("best_model").get_data())
    ds.get_table("best_model").store_chunk(test_df__best_model2)
    idx_count, _ = step.get_full_process_ids(ds)
    assert idx_count > 0
    run_steps(ds, steps)
    TEST_RESULT2 = inference_best_model(
        df__item=pd.merge(test_df__item1, test_df__item2, how="outer"),
        df__model=pd.merge(test_df__model1, test_df__model2, how="outer"),
        df__best_model=test_df__best_model2,
        df__prediction=ds.get_table("prediction").get_data(
            idx=pd.merge(test_df__item1, test_df__item2, how="outer")
        ),
    )
    TEST_RESULT = pd.merge(TEST_RESULT1, TEST_RESULT2, how="outer")
    assert_datatable_equal(ds.get_table("prediction"), TEST_RESULT)

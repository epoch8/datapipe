from pathlib import Path
from typing import Optional

import pytest
from sqlalchemy import Column
from sqlalchemy.sql.sqltypes import JSON, String

from tests.fixtures.smoke_data import (
    SmokeDataset,
    define_ground_truth_for_classification,
    generate_smoke_data,
    make_smoke_dataset,
)


def _require_datapipe_runtime():
    pytest.importorskip("tqdm")
    pytest.importorskip("datapipe")


def build_base_catalog(dbconn):
    from datapipe.compute import Catalog, Table
    from datapipe.store.database import TableStoreDB

    return Catalog(
        {
            "image": Table(
                store=TableStoreDB(
                    dbconn=dbconn,
                    name="image",
                    data_sql_schema=[
                        Column("image_id", String, primary_key=True),
                        Column("image__image_path", String),
                    ],
                    create_table=True,
                )
            ),
            "image__ground_truth": Table(
                store=TableStoreDB(
                    dbconn=dbconn,
                    name="image__ground_truth",
                    data_sql_schema=[
                        Column("image_id", String, primary_key=True),
                        Column("bboxes", JSON),
                        Column("labels", JSON),
                        Column("masks", JSON),
                    ],
                    create_table=True,
                )
            ),
            "image__ground_truth_for_classification": Table(
                store=TableStoreDB(
                    dbconn=dbconn,
                    name="image__ground_truth_for_classification",
                    data_sql_schema=[
                        Column("image_id", String, primary_key=True),
                        Column("label", String),
                    ],
                    create_table=True,
                )
            ),
            "subset__has__image": Table(
                store=TableStoreDB(
                    dbconn=dbconn,
                    name="subset__has__image",
                    data_sql_schema=[
                        Column("image_id", String, primary_key=True),
                        Column("subset_id", String, primary_key=True),
                    ],
                    create_table=True,
                )
            ),
        }
    )


def build_smoke_pipeline(dataset: SmokeDataset):
    from datapipe.compute import Pipeline
    from datapipe.step.batch_generate import BatchGenerate
    from datapipe.step.batch_transform import BatchTransform

    return Pipeline(
        [
            BatchGenerate(
                lambda: generate_smoke_data(dataset),
                outputs=[
                    "image",
                    "image__ground_truth",
                    "subset__has__image",
                ],
            ),
            BatchTransform(
                func=define_ground_truth_for_classification,
                inputs=["image__ground_truth"],
                outputs=["image__ground_truth_for_classification"],
            ),
        ]
    )


def build_smoke_app(dbconn, working_dir: Path, dataset: Optional[SmokeDataset] = None):
    from datapipe.compute import DatapipeApp
    from datapipe.datatable import DataStore

    dataset = dataset or make_smoke_dataset(working_dir / "images")
    ds = DataStore(dbconn, create_meta_table=True)
    catalog = build_base_catalog(dbconn)
    pipeline = build_smoke_pipeline(dataset)
    return DatapipeApp(ds, catalog, pipeline)


def test_smoke_app_builders_are_local_to_test_module():
    assert callable(build_smoke_app)
    assert callable(build_smoke_pipeline)


def test_smoke_app_is_constructed(dbconn, datapipe_dir, smoke_dataset):
    _require_datapipe_runtime()

    app = build_smoke_app(dbconn, datapipe_dir, smoke_dataset)

    assert app is not None
    assert app.ds is not None
    assert app.catalog is not None
    assert app.pipeline is not None

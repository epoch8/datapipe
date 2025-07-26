from pathlib import Path
from typing import Generator

import pandas as pd
from qdrant_client.models import Distance, VectorParams
from sqlalchemy import ARRAY, Float, Integer, String
from sqlalchemy.sql.schema import Column

from datapipe.compute import Catalog, Pipeline, Table, build_compute, run_steps
from datapipe.datatable import DataStore
from datapipe.step.batch_generate import BatchGenerate
from datapipe.step.batch_transform import BatchTransform
from datapipe.store.database import DBConn
from datapipe.store.pandas import TableStoreJsonLine
from datapipe.store.qdrant import CollectionParams, QdrantStore


def extract_id(df: pd.DataFrame) -> pd.DataFrame:
    return df[["id"]]


def generate_data() -> Generator[pd.DataFrame, None, None]:
    yield pd.DataFrame({"id": [1], "embedding": [[0.1]], "str_payload": ["foo"], "int_payload": [42]})


def test_qdrant_table_to_json(dbconn: DBConn, tmp_dir: Path) -> None:
    ds = DataStore(dbconn, create_meta_table=True)
    catalog = Catalog(
        {
            "input": Table(
                store=QdrantStore(
                    name="test_collection",
                    url="http://localhost:6333",
                    schema=[
                        Column("id", Integer, primary_key=True),
                        Column("embedding", ARRAY(Float, dimensions=1)),
                        Column("str_payload", String),
                        Column("int_payload", Integer),
                    ],
                    collection_params=CollectionParams(
                        vectors=VectorParams(
                            size=1,
                            distance=Distance.COSINE,
                        )
                    ),
                    pk_field="id",
                    embedding_field="embedding",
                    index_schema={
                        "str_payload": "keyword",
                        "int_payload": {
                            "type": "integer",
                            "lookup": False,
                            "range": True,
                        },
                    },
                )
            ),
            "output": Table(
                store=TableStoreJsonLine(
                    filename=tmp_dir / "output.jsonline",
                    primary_schema=[Column("id", Integer)],
                )
            ),
        }
    )

    pipeline = Pipeline(
        [
            BatchGenerate(generate_data, outputs=["input"]),
            BatchTransform(extract_id, inputs=["input"], outputs=["output"]),
        ]
    )

    steps = build_compute(ds, catalog, pipeline)
    run_steps(ds, steps)

    assert len(catalog.get_datatable(ds, "input").get_data()) == 1

    assert len(catalog.get_datatable(ds, "output").get_data()) == 1

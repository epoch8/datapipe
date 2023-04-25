import pandas as pd

from sqlalchemy.sql.schema import Column
from sqlalchemy.sql.sqltypes import Integer

from datapipe.store.database import TableStoreDB
from datapipe.datatable import DataStore
from datapipe.compute import Catalog, Pipeline, Table
from datapipe.core_steps import BatchGenerate, BatchTransform
from datapipe.store.database import DBConn, MetaKey
from datapipe.cli import main
from datapipe.run_config import RunConfig


PRODUCTS_SCHEMA = [
    Column("product_id", Integer, primary_key=True),
    Column("pipeline_id", Integer, primary_key=True),
    Column("b", Integer),
]

ITEMS_SCHEMA = [
    Column("item_id", Integer, primary_key=True),
    Column("pipeline_id", Integer, primary_key=True),
    Column("product_id", Integer, MetaKey()),
    Column("a", Integer),
]

dbconn = DBConn("sqlite+pysqlite3:///db.sqlite")
# dbconn = DBConn('postgresql://postgres:password@localhost/postgres', schema='test')
ds = DataStore(dbconn, create_meta_table=True)

run_config = RunConfig(
    filters={}, labels={"pipeline_name": "test_name", "pipeline_id": 1}
)

catalog = Catalog(
    {
        "test_products": Table(
            store=TableStoreDB(dbconn, "test_products_data", PRODUCTS_SCHEMA)
        ),
        "test_attr_products": Table(
            store=TableStoreDB(dbconn, "test_attr_products_data", ITEMS_SCHEMA)
        ),
        "test_new_attr_products": Table(
            store=TableStoreDB(dbconn, "test_new_attr_products_data", ITEMS_SCHEMA)
        ),
    }
)


def generate_products():
    products_df = pd.DataFrame(
        {
            "product_id": list(range(2)),
            "pipeline_id": list(range(2)),
            "b": range(10, 12),
        }
    )

    items_df = pd.DataFrame(
        {
            "item_id": list(range(5)) * 2,
            "pipeline_id": list(range(2)) * 5,
            "product_id": list(range(2)) * 5,
            "a": range(10),
        }
    )

    yield products_df, items_df


def transform_attrs(products: pd.DataFrame, items: pd.DataFrame):
    merged_df = pd.merge(items, products, on=["product_id", "pipeline_id"])
    merged_df["a"] = merged_df.apply(lambda x: x["a"] + x["b"], axis=1)

    return merged_df[["item_id", "pipeline_id", "product_id", "a"]]


pipeline = Pipeline(
    [
        BatchGenerate(
            generate_products, outputs=["test_products", "test_attr_products"]
        ),
        BatchTransform(
            transform_attrs,
            inputs=["test_products", "test_attr_products"],
            outputs=["test_new_attr_products"],
        ),
    ]
)

main(ds, catalog, pipeline, run_config)

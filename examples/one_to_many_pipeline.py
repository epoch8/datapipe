import pandas as pd

from sqlalchemy.sql.schema import Column
from sqlalchemy.sql.sqltypes import Integer, String, Boolean
from sqlalchemy.dialects.postgresql import JSONB

from datapipe.store.database import TableStoreDB
from datapipe.datatable import DataStore
from datapipe.dsl import Catalog, Pipeline, Table, BatchGenerate, BatchTransform
from datapipe.store.database import DBConn
from datapipe.cli import main


dbconn = DBConn("", '')
ds = DataStore(dbconn)


PRODUCT_SCHEMA = [
    Column('pipeline_id', Integer(), primary_key=True),
    Column('offer_id', Integer(), primary_key=True),
    Column('attributes', JSONB)
]

PRODUCT_ATTR_SCHEMA = [
    Column('pipeline_id', Integer(), primary_key=True),
    Column('offer_id', Integer(), primary_key=True),
    Column('name', String(), primary_key=True),
    Column('value', Integer()),
]

PRODUCT_OZON_SCHEMA = [
    Column('pipeline_id', Integer(), primary_key=True),
    Column('offer_id', Integer(), primary_key=True),
    Column('attributes', JSONB)
]

PRODUCT_OFFERS_SCHEMA = [
    Column('pipeline_id', Integer(), primary_key=True),
    Column('name', String(), primary_key=True),
    Column('offers', JSONB),
]

PRODUCT_ALL_SCHEMA = [
    Column('pipeline_id', Integer(), primary_key=True),
    Column('offer_id', Integer(), primary_key=True),
    Column('attributes_base', JSONB),
    Column('attributes_new', JSONB)
]

PRODUCT_STORE_SCHEMA = [
    Column('pipeline_id', Integer(), primary_key=True),
    Column('offer_id', Integer(), primary_key=True),
    Column('attributes', JSONB),
    Column('is_deleted', Boolean())
]


def generate_products():
    for pipeline_id in range(1, 3):
        df_data = []

        for offer_id in range(1, 6):
            df_data.append({
                "pipeline_id": pipeline_id,
                "offer_id": offer_id,
                "attributes": {f'attr_{id}': id for id in range(1, offer_id + 1)}
            })

        yield pd.DataFrame(data=df_data)


def unpack_attr(df: pd.DataFrame) -> pd.DataFrame:
    dfs = []

    for _, row in df.iterrows():
        data = [{
            "pipeline_id": row["pipeline_id"],
            "offer_id": row["offer_id"],
            "name": key,
            "value": value
        } for key, value in row["attributes"].items()]

        dfs.append(pd.DataFrame(data=data))

    res_df = pd.concat(dfs, ignore_index=True)

    if not len(res_df.index):
        return pd.DataFrame(columns=["pipeline_id", "offer_id", "name"])

    return res_df


def pack_attr(df: pd.DataFrame) -> pd.DataFrame:
    data = {}

    for _, row in df.iterrows():
        key = f'{row["pipeline_id"]}_{row["offer_id"]}'

        if key not in data:
            data[key] = {
                "pipeline_id": row["pipeline_id"],
                "offer_id": row["offer_id"],
                "attributes": {}
            }

        data[key]["attributes"][row["value"]] = row["name"]

    if not len(data.keys()):
        return pd.DataFrame(columns=["pipeline_id", "offer_id"])

    return pd.DataFrame(data=data.values())


def pack_offers(df: pd.DataFrame) -> pd.DataFrame:
    data = {}

    for _, row in df.iterrows():
        key = f'{row["pipeline_id"]}_{row["value"]}'

        if key not in data:
            data[key] = {
                "pipeline_id": row["pipeline_id"],
                "name": row["name"],
                "offers": {}
            }

        data[key]["offers"][row["offer_id"]] = row["value"]

    if not len(data.keys()):
        return pd.DataFrame(columns=["pipeline_id", "name"])

    return pd.DataFrame(data=data.values())


def gen_product_all(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    keys = ["pipeline_id", "offer_id"]
    merged_df = pd.merge(df1, df2,  how='outer', left_on=keys, right_on=keys, suffixes=("_base", "_new"))

    return merged_df[['pipeline_id', "offer_id", "attributes_base", "attributes_new"]]


def gen_product_store(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:

    keys = ["pipeline_id", "offer_id"]
    merged_df = pd.merge(df1, df2,  how='outer', left_on=keys, right_on=keys)

    merged_df['attributes'] = merged_df.apply(
        lambda x: x["attributes_x"] if pd.notna(x["attributes_x"]) else x["attributes_y"], axis=1)
    merged_df['is_deleted'] = merged_df.apply(
        lambda x: pd.isna(x["attributes_x"]), axis=1)

    return merged_df[['pipeline_id', 'offer_id', 'attributes', 'is_deleted']]


def filter(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["pipeline_id"] == 1]


catalog = Catalog({
    'test_products': Table(
        store=TableStoreDB(
            dbconn,
            'test_products_data',
            PRODUCT_SCHEMA
        )
    ),
    'test_attr_products': Table(
        store=TableStoreDB(
            dbconn,
            'test_attr_products_data',
            PRODUCT_ATTR_SCHEMA
        )
    ),
    'test_ozon_products': Table(
        store=TableStoreDB(
            dbconn,
            'test_ozon_products_data',
            PRODUCT_OZON_SCHEMA
        )
    ),
    'test_offers_products': Table(
        store=TableStoreDB(
            dbconn,
            'test_offers_products_data',
            PRODUCT_OFFERS_SCHEMA
        )
    ),
    'test_all_products': Table(
        store=TableStoreDB(
            dbconn,
            'test_all_products_data',
            PRODUCT_ALL_SCHEMA
        )
    ),
    'test_store_products': Table(
        store=TableStoreDB(
            dbconn,
            'test_store_products_data',
            PRODUCT_STORE_SCHEMA
        )
    ),
    'test_filter_products': Table(
        store=TableStoreDB(
            dbconn,
            'test_filter_products_data',
            PRODUCT_SCHEMA
        )
    )
})

pipeline = Pipeline([
    BatchGenerate(
        generate_products,
        outputs=["test_products"]
    ),
    BatchTransform(
        unpack_attr,
        inputs=["test_products"],
        outputs=["test_attr_products"],
        chunk_size=2
    ),
    BatchTransform(
        pack_attr,
        inputs=["test_attr_products"],
        outputs=["test_ozon_products"],
        chunk_size=2
    ),
    BatchTransform(
        pack_offers,
        inputs=["test_attr_products"],
        outputs=["test_offers_products"],
        chunk_size=2
    ),
    BatchTransform(
        gen_product_all,
        inputs=["test_products", "test_ozon_products"],
        outputs=["test_all_products"],
        chunk_size=2
    ),
    BatchTransform(
        gen_product_store,
        inputs=["test_products", "test_store_products"],
        outputs=["test_store_products"],
        chunk_size=2
    ),
    BatchTransform(
        filter,
        inputs=["test_products"],
        outputs=["test_filter_products"],
        chunk_size=2
    ),
])


main(ds, catalog, pipeline)

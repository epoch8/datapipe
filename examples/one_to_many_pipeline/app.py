import pandas as pd
from sqlalchemy import JSON
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from datapipe.compute import Catalog, DatapipeApp, Pipeline
from datapipe.datatable import DataStore
from datapipe.step.batch_generate import BatchGenerate
from datapipe.step.batch_transform import BatchTransform
from datapipe.store.database import DBConn


class Base(DeclarativeBase):
    pass


class TestProducts(Base):
    __tablename__ = "test_products"

    pipeline_id: Mapped[int] = mapped_column(primary_key=True)
    offer_id: Mapped[int] = mapped_column(primary_key=True)
    attributes: Mapped[dict] = mapped_column(type_=JSON)


class TestAttrProducts(Base):
    __tablename__ = "test_attr_products"

    pipeline_id: Mapped[int] = mapped_column(primary_key=True)
    offer_id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(primary_key=True)
    value: Mapped[int]


class TestOzonProducts(Base):
    __tablename__ = "test_ozon_products"

    pipeline_id: Mapped[int] = mapped_column(primary_key=True)
    offer_id: Mapped[int] = mapped_column(primary_key=True)
    attributes: Mapped[dict] = mapped_column(type_=JSON)


class TestOffersProducts(Base):
    __tablename__ = "test_offers_products"

    pipeline_id: Mapped[int] = mapped_column(primary_key=True)
    offer_id: Mapped[int] = mapped_column(primary_key=True)
    offers: Mapped[dict] = mapped_column(type_=JSON)


class TestAllProducts(Base):
    __tablename__ = "test_all_products"

    pipeline_id: Mapped[int] = mapped_column(primary_key=True)
    offer_id: Mapped[int] = mapped_column(primary_key=True)
    attributes_base: Mapped[dict] = mapped_column(type_=JSON)
    attributes_new: Mapped[dict] = mapped_column(type_=JSON)


class TestStoreProducts(Base):
    __tablename__ = "test_store_products"

    pipeline_id: Mapped[int] = mapped_column(primary_key=True)
    offer_id: Mapped[int] = mapped_column(primary_key=True)
    attributes: Mapped[dict] = mapped_column(type_=JSON)
    is_deleted: Mapped[bool]


class TestFilterProducts(Base):
    __tablename__ = "test_filter_products"

    pipeline_id: Mapped[int] = mapped_column(primary_key=True)
    offer_id: Mapped[int] = mapped_column(primary_key=True)
    attributes: Mapped[dict] = mapped_column(type_=JSON)


def generate_products():
    for pipeline_id in range(1, 4):
        df_data = []

        for offer_id in range(1, 6):
            df_data.append(
                {
                    "pipeline_id": pipeline_id,
                    "offer_id": offer_id,
                    "attributes": {f"attr_{id}": id for id in range(1, offer_id + 1)},
                }
            )

        yield pd.DataFrame(data=df_data)


def unpack_attr(df: pd.DataFrame) -> pd.DataFrame:
    dfs = []

    for _, row in df.iterrows():
        data = [
            {
                "pipeline_id": row["pipeline_id"],
                "offer_id": row["offer_id"],
                "name": key,
                "value": value,
            }
            for key, value in row["attributes"].items()
        ]

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
                "attributes": {},
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
                "offers": {},
            }

        data[key]["offers"][row["offer_id"]] = row["value"]

    if not len(data.keys()):
        return pd.DataFrame(columns=["pipeline_id", "name"])

    return pd.DataFrame(data=data.values())


def gen_product_all(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    keys = ["pipeline_id", "offer_id"]
    merged_df = pd.merge(
        df1, df2, how="outer", left_on=keys, right_on=keys, suffixes=("_base", "_new")
    )

    return merged_df[["pipeline_id", "offer_id", "attributes_base", "attributes_new"]]


def gen_product_store(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    keys = ["pipeline_id", "offer_id"]
    merged_df = pd.merge(df1, df2, how="outer", left_on=keys, right_on=keys)

    merged_df["attributes"] = merged_df.apply(
        lambda x: (
            x["attributes_x"] if pd.notna(x["attributes_x"]) else x["attributes_y"]
        ),
        axis=1,
    )
    merged_df["is_deleted"] = merged_df.apply(
        lambda x: pd.isna(x["attributes_x"]), axis=1
    )

    return merged_df[["pipeline_id", "offer_id", "attributes", "is_deleted"]]


def filter(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["pipeline_id"] == 1]


pipeline = Pipeline(
    [
        BatchGenerate(generate_products, outputs=[TestProducts]),
        BatchTransform(
            unpack_attr,
            inputs=[TestProducts],
            outputs=[TestAttrProducts],
            chunk_size=2,
        ),
        BatchTransform(
            pack_attr,
            inputs=[TestAttrProducts],
            outputs=[TestOzonProducts],
            chunk_size=2,
        ),
        BatchTransform(
            pack_offers,
            inputs=[TestAttrProducts],
            outputs=[TestOffersProducts],
            chunk_size=2,
        ),
        BatchTransform(
            gen_product_all,
            inputs=[TestProducts, TestOzonProducts],
            outputs=[TestAllProducts],
            chunk_size=2,
        ),
        BatchTransform(
            gen_product_store,
            inputs=[TestProducts, TestStoreProducts],
            outputs=[TestStoreProducts],
            chunk_size=2,
        ),
        BatchTransform(
            filter,
            inputs=[TestProducts],
            outputs=[TestFilterProducts],
            chunk_size=2,
        ),
    ]
)


dbconn = DBConn("sqlite+pysqlite3:///db.sqlite", sqla_metadata=Base.metadata)
# dbconn = DBConn('postgresql://postgres:password@localhost/postgres', schema='test')
ds = DataStore(dbconn)
app = DatapipeApp(ds, Catalog({}), pipeline)


if __name__ == "__main__":
    from datapipe.compute import run_steps

    ds.meta_dbconn.sqla_metadata.create_all(ds.meta_dbconn.con)
    run_steps(app.ds, app.steps)

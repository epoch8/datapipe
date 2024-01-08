import pandas as pd
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from datapipe.compute import Catalog, DatapipeApp, Pipeline, Table
from datapipe.datatable import DataStore, MetaMixin
from datapipe.step.batch_transform import BatchTransform
from datapipe.step.update_external_table import UpdateExternalTable
from datapipe.store.database import DBConn
from datapipe.store.filedir import PILFile, TableStoreFiledir


class Base(DeclarativeBase):
    pass


class InputImage(Base, MetaMixin):
    __tablename__ = "input_images"
    id: Mapped[int] = mapped_column(primary_key=True)


catalog = Catalog(
    {
        "input_images": Table(
            store=TableStoreFiledir("input/{id}.jpeg", PILFile("jpg")),
        ),
        # 'input_img_metadata': ExternalTable(
        #     store=Filedir(CATALOG_DIR / 'input/{id}.csv', CSVFile()),
        # ),
        "preprocessed_images": Table(
            store=TableStoreFiledir("output/{id}.png", PILFile("png")),
        ),
    }
)


def batch_preprocess_images(df: pd.DataFrame) -> pd.DataFrame:
    df["image"] = df["image"].apply(lambda im: im.resize((50, 50)))
    return df


pipeline = Pipeline(
    [
        UpdateExternalTable(output="input_images"),
        BatchTransform(
            batch_preprocess_images,
            inputs=["input_images"],
            outputs=["preprocessed_images"],
            chunk_size=100,
        ),
    ]
)


ds = DataStore(DBConn("sqlite+pysqlite3:///db.sqlite"))

app = DatapipeApp(ds, catalog, pipeline)

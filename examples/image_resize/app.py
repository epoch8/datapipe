import pandas as pd

from datapipe.compute import Catalog, DatapipeApp, Pipeline, Table
from datapipe.datatable import DataStore
from datapipe.step.batch_transform import BatchTransform
from datapipe.step.update_external_table import UpdateExternalTable
from datapipe.store.database import DBConn
from datapipe.store.filedir import PILFile, TableStoreFiledir

input_images_tbl = Table(
    name="input_images",
    store=TableStoreFiledir(
        "datapipe-examples/image_resize/input/{id}.jpeg",
        PILFile("jpg"),
        fsspec_kwargs={"protocol": "gs"},
    ),
)

preprocessed_images_tbl = Table(
    name="preprocessed_images",
    store=TableStoreFiledir("output/{id}.png", PILFile("png")),
)


def batch_preprocess_images(df: pd.DataFrame) -> pd.DataFrame:
    df["image"] = df["image"].apply(lambda im: im.resize((50, 50)))
    return df


pipeline = Pipeline(
    [
        UpdateExternalTable(output=input_images_tbl),
        BatchTransform(
            batch_preprocess_images,
            inputs=[input_images_tbl],
            outputs=[preprocessed_images_tbl],
            chunk_size=100,
        ),
    ]
)


ds = DataStore(DBConn("sqlite+pysqlite3:///db.sqlite"))

app = DatapipeApp(ds, Catalog({}), pipeline)

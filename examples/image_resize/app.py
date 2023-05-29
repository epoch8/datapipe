import pandas as pd

from datapipe import (
    BatchTransform,
    Catalog,
    DatapipeApp,
    DataStore,
    DBConn,
    Pipeline,
    Table,
    UpdateExternalTable,
)
from datapipe.store.filedir import PILFile, TableStoreFiledir

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

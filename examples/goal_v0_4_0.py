from pathlib import Path

import pandas as pd

from datapipe.compute import Catalog, Pipeline, Table
from datapipe.core_steps import BatchTransform, UpdateExternalTable
from datapipe.datatable import DataStore
from datapipe.store.database import DBConn
from datapipe.store.filedir import PILFile, TableStoreFiledir

CATALOG_DIR = Path("test_data")


catalog = Catalog(
    {
        "input_images": Table(
            store=TableStoreFiledir(CATALOG_DIR / "A/{id}_test.jpg", PILFile("jpg")),
        ),
        # 'input_img_metadata': ExternalTable(
        #     store=Filedir(CATALOG_DIR / 'input/{id}.csv', CSVFile()),
        # ),
        "preprocessed_images": Table(
            store=TableStoreFiledir(CATALOG_DIR / "ppcs/{id}.png", PILFile("png")),
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


ds = DataStore(DBConn("sqlite+pysqlite3:///./test_data/metadata.sqlite"), create_meta_table=True)

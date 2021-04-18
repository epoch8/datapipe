from pathlib import Path

import pandas as pd

from datapipe.metastore import MetaStore
from datapipe.store.filedir import TableStoreFiledir, PILFile
from datapipe.dsl import Catalog, ExternalTable, Table, Pipeline, BatchTransform
from datapipe.cli import main


CATALOG_DIR = Path('test_data')


catalog = Catalog({
    'input_images': ExternalTable(
        store=TableStoreFiledir(CATALOG_DIR / 'A/{id}_test.jpg', PILFile('jpg')),
    ),
    # 'input_img_metadata': ExternalTable(
    #     store=Filedir(CATALOG_DIR / 'input/{id}.csv', CSVFile()),
    # ),
    'preprocessed_images': Table(
        store=TableStoreFiledir(CATALOG_DIR / 'ppcs/{id}.png', PILFile('png')),
    )
})


def batch_preprocess_images(df: pd.DataFrame) -> pd.DataFrame:
    df['image'] = df['image'].apply(lambda im: im.resize((50, 50)))
    return df


pipeline = Pipeline([
    BatchTransform(
        batch_preprocess_images,
        inputs=['input_images'],
        outputs=['preprocessed_images'],
        chunk_size=100
    )
])


ms = MetaStore('sqlite:///./test_data/metadata.sqlite')


if __name__ == '__main__':
    main(ms, catalog, pipeline)

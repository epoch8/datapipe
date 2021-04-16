from pathlib import Path

import pandas as pd

from datapipe.metastore import MetaStore
from datapipe.store.filedir import TableStoreFiledir, PILFile
from datapipe.dsl import Catalog, ExternalTable, Table, Pipeline, BatchTransform
from datapipe.compute import run_pipeline


CATALOG_DIR = Path('test_data/mnist')


catalog = Catalog({
    'input_images': ExternalTable(
        store=TableStoreFiledir(CATALOG_DIR / 'testSet/testSet/img_{id}.jpg', PILFile('jpg')),
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


def main() -> None:
    ms = MetaStore('sqlite:///./test_data/metadata.sqlite')

    run_pipeline(ms, catalog, pipeline)


if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)

    main()

from pathlib import Path

import pandas as pd

from datapipe.dsl import Catalog, ExternalTable, Table, Pipeline, Transform, TableStoreFiledir
from datapipe.dsl_io import PILAdapter, CSVFile
from datapipe.compute import MetaStore, DBConn, build_compute, run_steps


CATALOG_DIR = Path('test_data/mnist')


cat = Catalog({
    'input_images': ExternalTable(
        store=TableStoreFiledir(CATALOG_DIR / 'testSet/testSet/img_{id}.jpg', PILAdapter('jpg')),
    ),
    # 'input_img_metadata': ExternalTable(
    #     store=Filedir(CATALOG_DIR / 'input/{id}.csv', CSVFile()),
    # ),
    'preprocessed_images': Table(
        store=TableStoreFiledir(CATALOG_DIR / 'ppcs/{id}.png', PILAdapter('png')),
    )
})


def preprocess_images(input_images_df: pd.DataFrame, input_img_metadata_df: pd.DataFrame) -> pd.DataFrame:
    pass


pipeline = Pipeline([
    Transform(
        preprocess_images,
        inputs=['input_images'],
        outputs=['preprocessed_images'],
    )
])


def main() -> None:
    ms = MetaStore(DBConn('sqlite:///./test_data/test.sqlite'))
    steps = build_compute(cat, pipeline)

    run_steps(ms, steps)


if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.DEBUG)

    logging.debug('Starting')

    main()

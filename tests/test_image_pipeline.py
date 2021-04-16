import pytest

import glob
import tempfile
import pandas as pd
import numpy as np
from PIL import Image

from datapipe.dsl import Catalog, Pipeline, Table, BatchGenerate, BatchTransform
from datapipe.metastore import MetaStore
from datapipe.datatable import DataTable, gen_process, inc_process
from datapipe.store.table_store_filedir import TableStoreFiledir, PILFile
from datapipe.compute import run_pipeline

from .util import dbconn


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield d


def make_df():
    idx = [f'im_{i}' for i in range(10)]
    return pd.DataFrame(
        {
            'image': [Image.fromarray(np.random.randint(0, 256, (100, 100, 3)), 'RGB') for i in idx]
        },
        index=idx
    )

def gen_images():
    yield make_df()

def resize_images(df):
    df['image'] = df['image'].apply(lambda im: im.resize((50,50)))
    return df


def test_image_datatables(dbconn, tmp_dir):
    ms = MetaStore(dbconn)

    tbl1 = DataTable(
        ms,
        'tbl1',
        table_store=TableStoreFiledir(
            f'{tmp_dir}/tbl1/{{id}}.png',
            adapter=PILFile('png')
        )
    )

    tbl2 = DataTable(
        ms,
        'tbl2',
        table_store=TableStoreFiledir(
            f'{tmp_dir}/tbl2/{{id}}.png',
            adapter=PILFile('png')
        )
    )

    assert(len(glob.glob(f'{tmp_dir}/tbl1/*.png')) == 0)
    assert(len(glob.glob(f'{tmp_dir}/tbl2/*.png')) == 0)

    gen_process(
        tbl1,
        gen_images
    )

    inc_process(
        ms,
        [tbl1],
        tbl2,
        resize_images
    )

    assert(len(glob.glob(f'{tmp_dir}/tbl1/*.png')) == 10)
    assert(len(glob.glob(f'{tmp_dir}/tbl2/*.png')) == 10)


def test_image_pipeline(dbconn, tmp_dir):
    catalog = Catalog({
        'tbl1': Table(
            store=TableStoreFiledir(
                f'{tmp_dir}/tbl1/{{id}}.png',
                adapter=PILFile('png')
            )
        ),
        'tbl2': Table(
            store=TableStoreFiledir(
                f'{tmp_dir}/tbl2/{{id}}.png',
                adapter=PILFile('png')
            )
        ),
    })

    pipeline = Pipeline([
        BatchGenerate(
            gen_images,
            outputs=['tbl1'],
        ),
        BatchTransform(
            resize_images,
            inputs=['tbl1'],
            outputs=['tbl2'],
        )
    ])

    assert(len(glob.glob(f'{tmp_dir}/tbl1/*.png')) == 0)
    assert(len(glob.glob(f'{tmp_dir}/tbl2/*.png')) == 0)

    ms = MetaStore(dbconn)
    run_pipeline(ms, catalog, pipeline)

    assert(len(glob.glob(f'{tmp_dir}/tbl1/*.png')) == 10)
    assert(len(glob.glob(f'{tmp_dir}/tbl2/*.png')) == 10)

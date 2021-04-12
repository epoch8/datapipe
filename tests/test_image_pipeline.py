import pytest

import tempfile
import pandas as pd
import numpy as np
from PIL import Image

from c12n_pipe.metastore import MetaStore, DBConn
from c12n_pipe.datatable import DataTable, gen_process, inc_process
from c12n_pipe.store.table_store_filedir import TableStoreFiledir, PILAdapter


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield d


def test_image_datatables(tmp_dir):
    ds = MetaStore(DBConn(f'sqlite:///{tmp_dir}/db.sqlite'))

    tbl1 = DataTable(
        ds,
        'tbl1',
        data_store=TableStoreFiledir(
            f'{tmp_dir}/tbl1',
            '.png',
            adapter=PILAdapter('png')
        )
    )

    tbl2 = DataTable(
        ds,
        'tbl2',
        data_store=TableStoreFiledir(
            f'{tmp_dir}/tbl2',
            '.png',
            adapter=PILAdapter('png')
        )
    )

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

    gen_process(
        tbl1,
        gen_images
    )

    def resize_images(df):
        df['image'] = df['image'].apply(lambda im: im.resize((50,50)))
        return df
    
    inc_process(
        ds,
        [tbl1],
        tbl2,
        resize_images
    )

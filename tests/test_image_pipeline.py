# flake8: noqa
import pytest

import tempfile

import pandas as pd
import numpy as np
from PIL import Image

from datapipe.dsl import Catalog, ExternalTable, Pipeline, Table, BatchGenerate, BatchTransform
from datapipe.metastore import MetaStore
from datapipe.datatable import DataTable, gen_process, inc_process
from datapipe.store.filedir import TableStoreFiledir, PILFile
from datapipe.compute import build_compute, run_pipeline, run_steps


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
    df['image'] = df['image'].apply(lambda im: im.resize((50, 50)))
    return df


def test_image_datatables(dbconn, tmp_dir):
    ms = MetaStore(dbconn)

    tbl1 = DataTable(
        ms,
        'tbl1',
        table_store=TableStoreFiledir(
            tmp_dir / 'tbl1' / '{id}.png',
            adapter=PILFile('png')
        )
    )

    tbl2 = DataTable(
        ms,
        'tbl2',
        table_store=TableStoreFiledir(
            tmp_dir / 'tbl2' / '{id}.png',
            adapter=PILFile('png')
        )
    )

    assert len(list(tmp_dir.glob('tbl1/*.png'))) == 0
    assert len(list(tmp_dir.glob('tbl2/*.png'))) == 0

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

    assert len(list(tmp_dir.glob('tbl1/*.png'))) == 10
    assert len(list(tmp_dir.glob('tbl2/*.png'))) == 10


def test_image_pipeline(dbconn, tmp_dir):
    catalog = Catalog({
        'tbl1': Table(
            metadata=['id'],
            store=TableStoreFiledir(
                tmp_dir / 'tbl1' / '{id}.png',
                adapter=PILFile('png')
            )
        ),
        'tbl2': Table(
            metadata=['id'],
            store=TableStoreFiledir(
                tmp_dir / 'tbl2' / '{id}.png',
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

    assert len(list(tmp_dir.glob('tbl1/*.png'))) == 0
    assert len(list(tmp_dir.glob('tbl2/*.png'))) == 0

    ms = MetaStore(dbconn)
    run_pipeline(ms, catalog, pipeline)

    assert len(list(tmp_dir.glob('tbl1/*.png'))) == 10
    assert len(list(tmp_dir.glob('tbl2/*.png'))) == 10


def test_image_batch_generate_with_later_deleting(dbconn, tmp_dir):

    # Add images to tmp_dir
    df_images = make_df()
    (tmp_dir / 'tbl1').mkdir()
    for id in df_images.index:
        df_images.loc[id, 'image'].save(tmp_dir / 'tbl1' / f'{id}.png')

    catalog = Catalog({
        'tbl1': ExternalTable(
            store=TableStoreFiledir(
                tmp_dir / 'tbl1' / '{id}.png',
                adapter=PILFile('png')
            )
        ),
        'tbl2': Table(
            store=TableStoreFiledir(
                tmp_dir / 'tbl2' / '{id}.png',
                adapter=PILFile('png')
            )
        ),
    })

    pipeline = Pipeline([
        BatchTransform(
            lambda df: df,
            inputs=["tbl1"],
            outputs=["tbl2"]
        )
    ])

    print(f"{list(tmp_dir.glob('tbl1/*.png'))=}")
    assert len(list(tmp_dir.glob('tbl1/*.png'))) == 10
    assert len(list(tmp_dir.glob('tbl2/*.png'))) == 0

    ms = MetaStore(dbconn)
    steps = build_compute(ms, catalog, pipeline)
    run_steps(ms, steps)

    assert len(list(tmp_dir.glob('tbl1/*.png'))) == 10
    assert len(list(tmp_dir.glob('tbl2/*.png'))) == 10
    assert len(catalog.get_datatable(ms, 'tbl1').get_data()) == 10
    assert len(catalog.get_datatable(ms, 'tbl2').get_data()) == 10

    # Delete some files from the folder
    for id in [0, 5, 7, 8, 9]:
        (tmp_dir / 'tbl1' / f'im_{id}.png').unlink()

    run_steps(ms, steps)

    assert len(list(tmp_dir.glob('tbl1/*.png'))) == 5
    assert len(catalog.get_datatable(ms, 'tbl1').get_data()) == 5
    assert len(catalog.get_datatable(ms, 'tbl1').get_metadata()) == 5

    # TODO: uncomment follow when we make files deletion
    # assert len(list(tmp_dir.glob('tbl2/*.png'))) == 5
    assert len(catalog.get_datatable(ms, 'tbl2').get_data()) == 5
    assert len(catalog.get_datatable(ms, 'tbl2').get_metadata()) == 5

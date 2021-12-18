# flake8: noqa

import pandas as pd
import numpy as np
from PIL import Image

from datapipe.compute import Catalog, Pipeline, Table
from datapipe.core_steps import (
    BatchGenerate, do_batch_generate,
    BatchTransform, do_full_batch_transform,
    UpdateExternalTable,
)
from datapipe.datatable import DataStore
from datapipe.store.filedir import TableStoreFiledir, PILFile
from datapipe.compute import build_compute, run_pipeline, run_steps


def make_df():
    idx = [f'im_{i}' for i in range(10)]
    return pd.DataFrame(
        {
            'id': idx,
            'image': [Image.fromarray(np.random.randint(0, 256, (100, 100, 3)), 'RGB') for i in idx]
        }
    )


def gen_images():
    yield make_df()


def resize_images(df):
    df['image'] = df['image'].apply(lambda im: im.resize((50, 50)))
    return df


def test_image_datatables(dbconn, tmp_dir):
    ds = DataStore(dbconn)

    tbl1 = ds.create_table(
        'tbl1',
        table_store=TableStoreFiledir(
            tmp_dir / 'tbl1' / '{id}.png',
            adapter=PILFile('png')
        )
    )

    tbl2 = ds.create_table(
        'tbl2',
        table_store=TableStoreFiledir(
            tmp_dir / 'tbl2' / '{id}.png',
            adapter=PILFile('png')
        )
    )

    assert len(list(tmp_dir.glob('tbl1/*.png'))) == 0
    assert len(list(tmp_dir.glob('tbl2/*.png'))) == 0

    do_batch_generate(
        func=gen_images,
        ds=ds,
        output_dts=[tbl1],
    )

    do_full_batch_transform(
        func=resize_images,
        ds=ds,
        input_dts=[tbl1],
        output_dts=[tbl2],
    )

    assert len(list(tmp_dir.glob('tbl1/*.png'))) == 10
    assert len(list(tmp_dir.glob('tbl2/*.png'))) == 10


def test_image_pipeline(dbconn, tmp_dir):
    catalog = Catalog({
        'tbl1': Table(
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

    ds = DataStore(dbconn)
    run_pipeline(ds, catalog, pipeline)

    assert len(list(tmp_dir.glob('tbl1/*.png'))) == 10
    assert len(list(tmp_dir.glob('tbl2/*.png'))) == 10


def test_image_batch_generate_with_later_deleting(dbconn, tmp_dir):

    # Add images to tmp_dir
    df_images = make_df()
    (tmp_dir / 'tbl1').mkdir()
    for _, row in df_images[['id', 'image']].iterrows():
        row['image'].save(tmp_dir / 'tbl1' / f'{row["id"]}.png')

    catalog = Catalog({
        'tbl1': Table(
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
        UpdateExternalTable("tbl1"),
        BatchTransform(
            lambda df: df,
            inputs=["tbl1"],
            outputs=["tbl2"]
        )
    ])

    print(f"{list(tmp_dir.glob('tbl1/*.png'))=}")
    assert len(list(tmp_dir.glob('tbl1/*.png'))) == 10
    assert len(list(tmp_dir.glob('tbl2/*.png'))) == 0

    ds = DataStore(dbconn)
    steps = build_compute(ds, catalog, pipeline)
    run_steps(ds, steps)

    assert len(list(tmp_dir.glob('tbl1/*.png'))) == 10
    assert len(list(tmp_dir.glob('tbl2/*.png'))) == 10
    assert len(catalog.get_datatable(ds, 'tbl1').get_data()) == 10
    assert len(catalog.get_datatable(ds, 'tbl2').get_data()) == 10

    # Delete some files from the folder
    for id in [0, 5, 7, 8, 9]:
        (tmp_dir / 'tbl1' / f'im_{id}.png').unlink()

    run_steps(ds, steps)

    assert len(list(tmp_dir.glob('tbl1/*.png'))) == 5
    assert len(catalog.get_datatable(ds, 'tbl1').get_data()) == 5
    assert len(catalog.get_datatable(ds, 'tbl1').get_metadata()) == 5

    # TODO: uncomment follow when we make files deletion
    # assert len(list(tmp_dir.glob('tbl2/*.png'))) == 5
    assert len(catalog.get_datatable(ds, 'tbl2').get_data()) == 5
    assert len(catalog.get_datatable(ds, 'tbl2').get_metadata()) == 5

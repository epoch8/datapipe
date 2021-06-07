import json
import pandas as pd
import numpy as np

import tempfile

import pytest
import fsspec
from PIL import Image

from datapipe.store.filedir import PILFile, JSONFile, TableStoreFiledir

from tests.util import assert_df_equal, assert_idx_equal

TEST_DF = pd.DataFrame(
    {
        "a": [1, 2],
        "b": [10, 20],
    },
    index=['aaa', 'bbb'],
)

TEST_JSONS = {
    'aaa': {'a': 1, 'b': 10},
    'bbb': {'a': 2, 'b': 20},
}


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield d


@pytest.fixture
def tmp_dir_with_json_data(tmp_dir):
    for fn, data in TEST_JSONS.items():
        with fsspec.open(f'{tmp_dir}/{fn}.json', 'w+') as f:
            json.dump(data, f)
    yield tmp_dir


def test_read_json_rows(tmp_dir_with_json_data):
    ts = TableStoreFiledir(
        f'{tmp_dir_with_json_data}/{{id}}.json',
        adapter=JSONFile()
    )

    assert_df_equal(ts.read_rows(), TEST_DF)


def test_read_json_rows_file_fs(tmp_dir_with_json_data):
    ts = TableStoreFiledir(
        f'file://{tmp_dir_with_json_data}/{{id}}.json',
        adapter=JSONFile()
    )

    assert_df_equal(ts.read_rows(), TEST_DF)


def test_read_json_rows_gcs_fs():
    ts = TableStoreFiledir(
        'gs://datapipe-test/{id}.json',
        adapter=JSONFile()
    )

    assert_df_equal(ts.read_rows(), TEST_DF)


def test_insert_json_rows(tmp_dir_with_json_data):
    ts = TableStoreFiledir(
        f'{tmp_dir_with_json_data}/{{id}}.json',
        adapter=JSONFile()
    )

    ts.insert_rows(pd.DataFrame(
        {'a': [3], 'b': [30]},
        index=['ccc']
    ))

    with open(f'{tmp_dir_with_json_data}/ccc.json') as f:
        assert(json.load(f) == {'a': 3, 'b': 30})


@pytest.fixture
def tmp_dir_with_img_data(tmp_dir):
    with fsspec.open(f'{tmp_dir}/aaa.png', 'wb+') as f:
        im = Image.fromarray(np.zeros((100, 100, 3), dtype='u8'), 'RGB')
        im.save(f, format='png')

    yield tmp_dir


def test_read_png_rows(tmp_dir_with_img_data):
    ts = TableStoreFiledir(
        f'{tmp_dir_with_img_data}/{{id}}.png',
        adapter=PILFile('png')
    )

    rows = ts.read_rows()

    assert_idx_equal(rows.index, ['aaa'])
    assert('image' in rows.columns)


def test_insert_png_rows(tmp_dir_with_img_data):
    ts = TableStoreFiledir(
        f'{tmp_dir_with_img_data}/{{id}}.png',
        adapter=PILFile('png')
    )

    ts.insert_rows(pd.DataFrame(
        {'image': [Image.fromarray(np.zeros((100, 100, 3), 'u8'), 'RGB')]},
        index=['bbb']
    ))

    assert(fsspec.open(f'{tmp_dir_with_img_data}/bbb.png'))

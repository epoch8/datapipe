import json
from datapipe.metastore import MetaStore
import pandas as pd
import numpy as np

import pytest
import fsspec
from PIL import Image

from datapipe.store.filedir import PILFile, JSONFile, TableStoreFiledir
from datapipe.dsl import Catalog, Table

from .util import tmp_dir, assert_df_equal, assert_idx_equal

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
def tmp_dir_with_json_data(tmp_dir):
    for fn, data in TEST_JSONS.items():
        with fsspec.open(f'{tmp_dir}/{fn}.json', 'w+') as f:
            json.dump(data, f)
    yield tmp_dir


def get_test_df_filepath(test_df, tmp_dir_):
    test_df_filepath = test_df.copy()
    test_df_filepath['filepath'] = test_df_filepath.index.map(
        lambda idx: f'{tmp_dir_}/{idx}.json'
    )
    return test_df_filepath


def test_read_json_rows(tmp_dir_with_json_data):
    ts = TableStoreFiledir(
        f'{tmp_dir_with_json_data}/{{id}}.json',
        adapter=JSONFile()
    )

    assert_df_equal(ts.read_rows(), TEST_DF)

    ts_with_filepath = TableStoreFiledir(
        f'{tmp_dir_with_json_data}/{{id}}.json',
        adapter=JSONFile(), add_filepath_column=True
    )
    assert_df_equal(ts_with_filepath.read_rows(), get_test_df_filepath(TEST_DF, tmp_dir_with_json_data))


def test_read_json_rows_file_fs(tmp_dir_with_json_data):
    ts = TableStoreFiledir(
        f'file://{tmp_dir_with_json_data}/{{id}}.json',
        adapter=JSONFile()
    )

    assert_df_equal(ts.read_rows(), TEST_DF)

    ts_with_filepath = TableStoreFiledir(
        f'file://{tmp_dir_with_json_data}/{{id}}.json',
        adapter=JSONFile(), add_filepath_column=True
    )
    assert_df_equal(ts_with_filepath.read_rows(), get_test_df_filepath(TEST_DF, f'file://{tmp_dir_with_json_data}'))


def test_read_json_rows_gcs_fs():
    ts = TableStoreFiledir(
        'gs://datapipe-test/{id}.json',
        adapter=JSONFile()
    )

    assert_df_equal(ts.read_rows(), TEST_DF)

    ts_with_filepath = TableStoreFiledir(
        'gs://datapipe-test/{id}.json',
        adapter=JSONFile(), add_filepath_column=True
    )
    assert_df_equal(ts_with_filepath.read_rows(), get_test_df_filepath(TEST_DF, 'gs://datapipe-test'))


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


@pytest.fixture
def tmp_several_dirs_with_json_data(tmp_dir):
    for i in range(3):
        for j in range(3):
            with fsspec.open(f'{tmp_dir}/folder{i}/folder{j}/{i}{j}.json', 'w') as out:
                out.write(f'{{"a": {i}, "b": {j}}}')
        with fsspec.open(f'{tmp_dir}/folder{i}/{i}.json', 'w') as out:
            out.write(f'{{"a": {i}, "b": -1}}')
    yield tmp_dir


TEST_DF_FOLDER0 = pd.DataFrame(
    {
        "a": [0, 0, 0],
        "b": [0, 1, 2],
    },
    index=['00', '01', '02'],
)


def test_read_json_rows_folders(tmp_several_dirs_with_json_data):
    ts = TableStoreFiledir(
        f'{tmp_several_dirs_with_json_data}/folder0/*/{{id}}.json',
        adapter=JSONFile()
    )
    assert_df_equal(ts.read_rows(), TEST_DF_FOLDER0)

    ts_with_filepath = TableStoreFiledir(
        f'{tmp_several_dirs_with_json_data}/folder0/*/{{id}}.json',
        adapter=JSONFile(), add_filepath_column=True
    )
    TEST_DF_FOLDER0_WITH_FILEPATH = TEST_DF_FOLDER0.copy()
    TEST_DF_FOLDER0_WITH_FILEPATH['filepath'] = TEST_DF_FOLDER0_WITH_FILEPATH.index.map(
        lambda idx: (
            f"{tmp_several_dirs_with_json_data}/folder0/folder{idx[1]}/{idx}.json"
        )
    )
    assert_df_equal(ts_with_filepath.read_rows(), TEST_DF_FOLDER0_WITH_FILEPATH)


TEST_DF_FOLDER_RECURSIVELY = pd.DataFrame(
    {
        "a": [0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2],
        "b": [-1, 0, 1, 2, -1, 0, 1, 2, -1, 0, 1, 2],
    },
    index=['0', '00', '01', '02', '1', '10', '11', '12', '2', '20', '21', '22'],
)


def test_read_json_rows_recursively(tmp_several_dirs_with_json_data):
    ts = TableStoreFiledir(
        f'{tmp_several_dirs_with_json_data}/**/{{id}}.json',
        adapter=JSONFile()
    )
    assert_df_equal(ts.read_rows(), TEST_DF_FOLDER_RECURSIVELY)

    ts_with_filepath = TableStoreFiledir(
        f'{tmp_several_dirs_with_json_data}/**/{{id}}.json',
        adapter=JSONFile(), add_filepath_column=True
    )
    TEST_DF_FOLDER_RECURSIVELY_WITH_FILEPATH = TEST_DF_FOLDER_RECURSIVELY.copy()
    TEST_DF_FOLDER_RECURSIVELY_WITH_FILEPATH['filepath'] = TEST_DF_FOLDER_RECURSIVELY_WITH_FILEPATH.index.map(
        lambda idx: (
            f"{tmp_several_dirs_with_json_data}/folder{idx}/{idx}.json"
            if idx in ['0', '1', '2']
            else f"{tmp_several_dirs_with_json_data}/folder{idx[0]}/folder{idx[1]}/{idx}.json"
        )
    )
    assert_df_equal(ts_with_filepath.read_rows(), TEST_DF_FOLDER_RECURSIVELY_WITH_FILEPATH)

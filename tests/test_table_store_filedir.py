import json
import pandas as pd

import tempfile

import pytest
import fsspec

from c12n_pipe.store.table_store_filedir import TableStoreFiledir

from tests.util import assert_df_equal

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
def tmp_dir_with_data(tmp_dir):
    for fn, data in TEST_JSONS.items():
        with fsspec.open(f'{tmp_dir}/{fn}.json', 'w+') as f:
            json.dump(data, f)
    yield tmp_dir


def test_read_rows(tmp_dir_with_data):
    ts = TableStoreFiledir(
        path=tmp_dir_with_data,
        ext='.json',
        adapter=json
    )

    assert_df_equal(ts.read_rows(), TEST_DF)

def test_insert_rows(tmp_dir_with_data):
    ts = TableStoreFiledir(
        path=tmp_dir_with_data,
        ext='.json',
        adapter=json
    )

    ts.insert_rows(pd.DataFrame(
        {'a': [3], 'b': [30]},
        index=['ccc']
    ))

    with open(f'{tmp_dir_with_data}/ccc.json') as f:
        assert(json.load(f) == {'a': 3, 'b': 30})

import pytest

import pandas as pd

from datapipe.metastore import MetaStore

@pytest.fixture
def ms(dbconn) -> MetaStore:
    return MetaStore(dbconn=dbconn)


def test_create_table(ms: MetaStore):
    ms.create_meta_table('test_a', ['a'])


def test_store_chunk(ms: MetaStore):
    a_meta = ms.create_meta_table('test_a', ['id'])

    data_df = pd.DataFrame({
        'id': ['1', '2'],
        'text': ['line 1', 'line 2'],
    })

    _ = a_meta.update_data(data_df)

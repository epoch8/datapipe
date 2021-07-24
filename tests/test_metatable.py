import pandas as pd

from datapipe.metastore import MetaStore
from datapipe.store.database import DBConn

from .util import assert_idx_equal

TEST_DF = pd.DataFrame(
    {
        'a': range(10)
    },
    index=pd.Index([f'id_{i}' for i in range(10)], name='id'),
)


def test_insert_rows(dbconn: DBConn):
    ms = MetaStore(dbconn=dbconn)

    mt = ms.create_meta_table('test')

    new_idx, changed_idx, new_meta_df = mt.get_changes_for_store_chunk(TEST_DF)
    assert_idx_equal(new_idx, TEST_DF.index)

    mt.update_meta_for_store_chunk(new_meta_df=new_meta_df)

    assert_idx_equal(mt.get_metadata().index, TEST_DF.index)
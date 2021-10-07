import pytest
from pytest_cases import parametrize_with_cases, case, parametrize

import pandas as pd
from sqlalchemy import Column, Integer, String

from datapipe.store.table_store import TableStore
from datapipe.store.database import TableStoreDB
from datapipe.store.pandas import TableStoreJsonLine, TableStoreExcel
from datapipe.store.filedir import JSONFile, TableStoreFiledir

from .util import assert_df_equal


DATA_PARAMS = [
    pytest.param(
        pd.DataFrame({
            'id': range(100),
            'name': [f'Product {i}' for i in range(100)],
            'price': [1000 + i for i in range(100)],
        }),
        [Column('id', Integer, primary_key=True)],
        id='int_id'
    ),
    pytest.param(
        pd.DataFrame({
            'id': [f'id_{i}' for i in range(100)],
            'name': [f'Product {i}' for i in range(100)],
            'price': [1000 + i for i in range(100)],
        }),
        [Column('id', String(100), primary_key=True)],
        id='str_id'
    ),
    pytest.param(
        pd.DataFrame({
            'id_int': range(100),
            'id_str': [f'id_{i}' for i in range(100)],
            'name': [f'Product {i}' for i in range(100)],
            'price': [1000 + i for i in range(100)],
        }),
        [
            Column('id_int', Integer, primary_key=True),
            Column('id_str', String(100), primary_key=True),
        ],
        id='multi_id'
    )
]

FILEDIR_DATA_PARAMS = [
    pytest.param(
        pd.DataFrame({
            'id': [f'id_{i}' for i in range(100)],
            'name': [f'Product {i}' for i in range(100)],
            'price': [1000 + i for i in range(100)],
        }),
        '{id}.json',
        id='str_id'
    ),
    pytest.param(
        pd.DataFrame({
            'id1': [f'id_{i}' for i in range(100)],
            'id2': [f'id_{i}' for i in range(100)],
            'name': [f'Product {i}' for i in range(100)],
            'price': [1000 + i for i in range(100)],
        }),
        '{id1}__{id2}.json',
        id='multi_id'
    ),
    pytest.param(
        pd.DataFrame({
            'id1': [f'id_{i}' for i in range(100)],
            'id2': [f'id_{i}' for i in range(100)],
            'id3': [f'id__{i}' for i in range(100)],
            'id4': [f'id___{i}' for i in range(100)],
            'id5': [f'id_{i}_' for i in range(100)],
            'name': [f'Product {i}' for i in range(100)],
            'price': [1000 + i for i in range(100)],
        }),
        '{id1}______{id2}______{id3}______{id4}______{id5}.json',
        id='multi_id'
    ),
    pytest.param(
        pd.DataFrame({
            'id1': [f'id_{i}' for i in range(100)],
            'id2': [f'id_{i}' for i in range(100, 200)],
            'id3': [f'id_{i}' for i in range(150, 250)],
            'name': [f'Product {i}' for i in range(100)],
            'price': [1000 + i for i in range(100)],
        }),
        '{id2}__{id1}__{id3}.json',
        id='multi_id'
    )
]


class CasesTableStore:
    @case(tags='supports_delete')
    @parametrize('df,schema', DATA_PARAMS)
    def case_db(self, dbconn, df, schema):
        return (
            TableStoreDB(
                dbconn,
                'tbl1',
                schema + [
                    Column('name', String(100)),
                    Column('price', Integer),
                ]
            ),
            df
        )

    @case(tags='supports_delete')
    @parametrize('df,schema', DATA_PARAMS)
    def case_jsonline(self, tmp_dir, df, schema):
        return (
            TableStoreJsonLine(
                tmp_dir / "data.json",
                primary_schema=schema
            ),
            df
        )

    @case(tags='supports_delete')
    @parametrize('df,schema', DATA_PARAMS)
    def case_excel(self, tmp_dir, df, schema):
        return (
            TableStoreExcel(
                tmp_dir / "data.xlsx",
                primary_schema=schema
            ),
            df
        )

    @parametrize('df,fn_template', FILEDIR_DATA_PARAMS)
    def case_filedir_json(self, tmp_dir, df, fn_template):
        return (
            TableStoreFiledir(
                tmp_dir / fn_template,
                adapter=JSONFile(),
            ),
            df
        )


@parametrize_with_cases('store,test_df', cases=CasesTableStore)
def test_write_read_rows(store: TableStore, test_df: pd.DataFrame) -> None:
    assert(store.read_rows().empty)

    store.insert_rows(test_df)

    assert_df_equal(store.read_rows(), test_df, index_cols=store.primary_keys)


@parametrize_with_cases('store,test_df', cases=CasesTableStore)
def test_partial_update_rows(store: TableStore, test_df: pd.DataFrame) -> None:
    assert(store.read_rows().empty)

    store.insert_rows(test_df)

    assert_df_equal(store.read_rows(), test_df, index_cols=store.primary_keys)

    test_df_mod = test_df.copy()
    test_df_mod.loc[50:, 'price'] = test_df_mod.loc[50:, 'price'] + 1

    store.update_rows(test_df_mod.loc[50:])

    assert_df_equal(store.read_rows(), test_df_mod, index_cols=store.primary_keys)


@parametrize_with_cases('store,test_df', cases=CasesTableStore)
def test_full_update_rows(store: TableStore, test_df: pd.DataFrame) -> None:
    assert(store.read_rows().empty)

    store.insert_rows(test_df)

    assert_df_equal(store.read_rows(), test_df, index_cols=store.primary_keys)

    test_df_mod = test_df.copy()
    test_df_mod.loc[:, 'price'] = test_df_mod.loc[:, 'price'] + 1

    store.update_rows(test_df_mod)

    assert_df_equal(store.read_rows(), test_df_mod, index_cols=store.primary_keys)


@parametrize_with_cases('store,test_df', cases=CasesTableStore, has_tag='supports_delete')
def test_delete_rows(store: TableStore, test_df: pd.DataFrame) -> None:
    assert(store.read_rows().empty)

    store.insert_rows(test_df)

    assert_df_equal(store.read_rows(), test_df, index_cols=store.primary_keys)

    store.delete_rows(test_df.loc[20:50, store.primary_keys])

    assert_df_equal(
        store.read_rows(),
        pd.concat([test_df.loc[0:19], test_df.loc[51:]]),
        index_cols=store.primary_keys
    )

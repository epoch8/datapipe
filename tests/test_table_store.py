from typing import Iterable, cast
import pytest
from pytest_cases import parametrize_with_cases, case, parametrize

import pandas as pd
from sqlalchemy import Column, Integer, String
from datapipe.step import RunConfig

from datapipe.types import DataDF, IndexDF
from datapipe.store.table_store import TableStore
from datapipe.store.database import TableStoreDB
from datapipe.store.pandas import TableStoreJsonLine, TableStoreExcel
from datapipe.store.filedir import JSONFile, TableStoreFiledir
from datapipe.store.leveldb import LevelDBStore

from .util import assert_df_equal, assert_ts_contains


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
    ),
    pytest.param(
        pd.DataFrame({
            'id1': [f'id_{i}' for i in range(1000)],
            'id2': [f'id_{i}' for i in range(1000)],
            'name': [f'Product {i}' for i in range(1000)],
            'price': [1000 + i for i in range(1000)],
        }),
        [
            Column('id1', String(100), primary_key=True),
            Column('id2', String(100), primary_key=True),
        ],
        id='double_id_1000_records'
    ),
    pytest.param(
        pd.DataFrame({
            'id1': [f'id_{i}' for i in range(1000)],
            'id2': [f'id_{i}' for i in range(1000)],
            'id3': [f'id_{i}' for i in range(1000)],
            'name': [f'Product {i}' for i in range(1000)],
            'price': [1000 + i for i in range(1000)],
        }),
        [
            Column('id1', String(100), primary_key=True),
            Column('id2', String(100), primary_key=True),
            Column('id3', String(100), primary_key=True),
        ],
        id='triple_id_1000_records'
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
        id='multi_ids2'
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
        id='multi_ids_check_commutativity'
    )
]


class CasesTableStore:
    @case(tags=['supports_delete', 'supports_all_read_rows'])
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

    @case(tags=['supports_delete', 'supports_all_read_rows'])
    @parametrize('df,schema', DATA_PARAMS)
    def case_jsonline(self, tmp_dir, df, schema):
        return (
            TableStoreJsonLine(
                tmp_dir / "data.json",
                primary_schema=schema
            ),
            df
        )

    @case(tags=['supports_delete', 'supports_all_read_rows'])
    @parametrize('df,schema', DATA_PARAMS)
    def case_excel(self, tmp_dir, df, schema):
        return (
            TableStoreExcel(
                tmp_dir / "data.xlsx",
                primary_schema=schema
            ),
            df
        )
    
    @case(tags=['supports_delete', 'supports_all_read_rows'])
    @parametrize('df,schema', DATA_PARAMS)
    def case_leveldb(self, tmp_dir, df, schema):
        return (
            LevelDBStore(
                'tbl1',
                tmp_dir / 'levelDB-tbl1',
                schema + [
                    Column('name', String(100)),
                    Column('price', Integer),
                ]
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
    store.insert_rows(test_df)

    assert_ts_contains(store, test_df)


@parametrize_with_cases('store,test_df', cases=CasesTableStore)
def test_insert_identical_rows_twice_and_read_rows(store: TableStore, test_df: pd.DataFrame) -> None:
    store.insert_rows(test_df)

    test_df_mod = test_df.copy()
    test_df_mod.loc[50:, 'price'] = test_df_mod.loc[50:, 'price'] + 1

    store.insert_rows(test_df_mod.loc[50:])

    assert_ts_contains(store, test_df_mod)


@parametrize_with_cases('store,test_df', cases=CasesTableStore)
def test_read_empty_df(store: TableStore, test_df: pd.DataFrame) -> None:
    store.insert_rows(test_df)

    df_empty = pd.DataFrame()

    assert store.read_rows(cast(IndexDF, df_empty)).empty


@parametrize_with_cases('store,test_df', cases=CasesTableStore, has_tag='supports_all_read_rows')
def test_insert_empty_df(store: TableStore, test_df: pd.DataFrame) -> None:
    df_empty = pd.DataFrame()
    store.insert_rows(df_empty)

    assert store.read_rows().empty


@parametrize_with_cases('store,test_df', cases=CasesTableStore, has_tag='supports_all_read_rows')
def test_update_empty_df(store: TableStore, test_df: pd.DataFrame) -> None:
    df_empty = pd.DataFrame()
    store.update_rows(df_empty)

    assert store.read_rows().empty


@parametrize_with_cases('store,test_df', cases=CasesTableStore)
def test_partial_update_rows(store: TableStore, test_df: pd.DataFrame) -> None:
    store.insert_rows(test_df)

    assert_ts_contains(store, test_df)

    test_df_mod = test_df.copy()
    test_df_mod.loc[50:, 'price'] = test_df_mod.loc[50:, 'price'] + 1

    store.update_rows(test_df_mod.loc[50:])

    assert_ts_contains(store, test_df_mod)


@parametrize_with_cases('store,test_df', cases=CasesTableStore)
def test_full_update_rows(store: TableStore, test_df: pd.DataFrame) -> None:
    store.insert_rows(test_df)

    assert_ts_contains(store, test_df)

    test_df_mod = test_df.copy()
    test_df_mod.loc[:, 'price'] = test_df_mod.loc[:, 'price'] + 1

    store.update_rows(test_df_mod)

    assert_ts_contains(store, test_df_mod)


@parametrize_with_cases('store,test_df', cases=CasesTableStore, has_tag='supports_delete')
def test_delete_rows(store: TableStore, test_df: pd.DataFrame) -> None:
    store.insert_rows(test_df)

    assert_df_equal(store.read_rows(), test_df, index_cols=store.primary_keys)

    store.delete_rows(test_df.loc[20:50, store.primary_keys])

    assert_df_equal(
        store.read_rows(),
        pd.concat([test_df.loc[0:19], test_df.loc[51:]]),
        index_cols=store.primary_keys
    )


@parametrize_with_cases('store,test_df', cases=CasesTableStore)
def test_read_rows_meta_pseudo_df(store: TableStore, test_df: pd.DataFrame) -> None:
    store.insert_rows(test_df)

    assert_ts_contains(store, test_df)

    pseudo_df_iter = store.read_rows_meta_pseudo_df()

    assert(isinstance(pseudo_df_iter, Iterable))
    assert(isinstance(next(pseudo_df_iter), DataDF))


@parametrize_with_cases('store,test_df', cases=CasesTableStore)
def test_read_rows_meta_pseudo_df_with_runconfig(store: TableStore, test_df: pd.DataFrame) -> None:
    store.insert_rows(test_df)

    assert_ts_contains(store, test_df)

    # TODO проверять, что runconfig реально влияет на результирующие данные
    pseudo_df_iter = store.read_rows_meta_pseudo_df(run_config=RunConfig(filters={'a': 1}))

    assert(isinstance(pseudo_df_iter, Iterable))
    assert(isinstance(next(pseudo_df_iter), DataDF))

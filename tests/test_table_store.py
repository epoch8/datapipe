# import pytest
from datapipe.store.filedir import JSONFile, TableStoreFiledir
from pytest_cases import parametrize_with_cases, case

import pandas as pd
from sqlalchemy import Column, Integer, String

from datapipe.store.table_store import TableStore
from datapipe.store.database import TableStoreDB
from datapipe.store.pandas import TableStoreJsonLine, TableStoreExcel

from .util import assert_df_equal

# TODO реализовать стандартный набор тестов для всех table_store.

TEST_DF_INTID = pd.DataFrame(
    {
        'id': range(100),
        'name': [f'Product {i}' for i in range(100)],
        'price': [1000 + i for i in range(100)],
    }
)

TEST_DF_STRID = pd.DataFrame(
    {
        'id': [f'id_{i}' for i in range(100)],
        'name': [f'Product {i}' for i in range(100)],
        'price': [1000 + i for i in range(100)],
    }
)


class CasesTableStore:
    @case(tags='supports_delete')
    def case_db_intid(self, dbconn):
        return (
            TableStoreDB(
                dbconn,
                'tbl1',
                [
                    Column('id', Integer, primary_key=True),
                    Column('name', String(100)),
                    Column('price', Integer),
                ]
            ),
            TEST_DF_INTID
        )

    @case(tags='supports_delete')
    def case_db_strid(self, dbconn):
        return (
            TableStoreDB(
                dbconn,
                'tbl1',
                [
                    Column('id', String(100), primary_key=True),
                    Column('name', String(100)),
                    Column('price', Integer),
                ]
            ),
            TEST_DF_STRID
        )

    @case(tags='supports_delete')
    def case_jsonline_intid(self, tmp_dir):
        return (
            TableStoreJsonLine(
                tmp_dir / "data.json",
                primary_schema=[Column('id', Integer, primary_key=True)]
            ),
            TEST_DF_INTID
        )

    @case(tags='supports_delete')
    def case_jsonline_strid(self, tmp_dir):
        return (
            TableStoreJsonLine(
                tmp_dir / "data.json",
                primary_schema=[Column('id', String(100), primary_key=True)]
            ),
            TEST_DF_STRID
        )

    @case(tags='supports_delete')
    def case_excel_intid(self, tmp_dir):
        return (
            TableStoreExcel(
                tmp_dir / "data.xlsx",
                primary_schema=[Column('id', Integer, primary_key=True)]
            ),
            TEST_DF_INTID
        )

    @case(tags='supports_delete')
    def case_excel_strid(self, tmp_dir):
        return (
            TableStoreExcel(
                tmp_dir / "data.xlsx",
                primary_schema=[Column('id', String(100), primary_key=True)]
            ),
            TEST_DF_STRID
        )

    def case_filedir_json_strid(self, tmp_dir):
        return (
            TableStoreFiledir(
                f'{tmp_dir}/{{id}}.json',
                adapter=JSONFile()
            ),
            TEST_DF_STRID
        )


@parametrize_with_cases('store,test_df', cases=CasesTableStore)
def test_write_read_rows(store: TableStore, test_df: pd.DataFrame) -> None:
    assert(store.read_rows().empty)

    store.insert_rows(test_df)

    assert_df_equal(store.read_rows(), test_df)


@parametrize_with_cases('store,test_df', cases=CasesTableStore)
def test_partial_update_rows(store: TableStore, test_df: pd.DataFrame) -> None:
    assert(store.read_rows().empty)

    store.insert_rows(test_df)

    assert_df_equal(store.read_rows(), test_df)

    test_df_mod = test_df.copy()
    test_df_mod.loc[50:, 'price'] = test_df_mod.loc[50:, 'price'] + 1

    store.update_rows(test_df_mod.loc[50:])

    assert_df_equal(store.read_rows(), test_df_mod)


@parametrize_with_cases('store,test_df', cases=CasesTableStore)
def test_full_update_rows(store: TableStore, test_df: pd.DataFrame) -> None:
    assert(store.read_rows().empty)

    store.insert_rows(test_df)

    assert_df_equal(store.read_rows(), test_df)

    test_df_mod = test_df.copy()
    test_df_mod.loc[:, 'price'] = test_df_mod.loc[:, 'price'] + 1

    store.update_rows(test_df_mod)

    assert_df_equal(store.read_rows(), test_df_mod)


@parametrize_with_cases('store,test_df', cases=CasesTableStore, has_tag='supports_delete')
def test_delete_rows(store: TableStore, test_df: pd.DataFrame) -> None:
    assert(store.read_rows().empty)

    store.insert_rows(test_df)

    assert_df_equal(store.read_rows(), test_df)

    store.delete_rows(test_df.loc[20:50, ['id']])

    assert_df_equal(
        store.read_rows(),
        pd.concat([test_df.loc[0:19], test_df.loc[51:]])
    )

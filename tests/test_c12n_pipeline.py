import pytest

from pathlib import Path
import pandas as pd
import tempfile
import os
import time

from sqlalchemy.sql.sqltypes import JSON
from sqlalchemy import create_engine, Column, Numeric

from datapipe.store.table_store_sql import DBConn
from datapipe.datatable import DataTable, gen_process, gen_process_many, inc_process, inc_process_many
from datapipe.metastore import MetaStore, PRIMARY_KEY

from c12n_pipe.io.data_catalog import DBTable, DataCatalog
from c12n_pipe.io.node import Pipeline, StoreNode, PythonNode, LabelStudioNode

from tests.util import assert_df_equal, assert_idx_equal


DBCONNSTR = 'sqlite:///:memory:'
DB_TEST_SCHEMA = None


@pytest.fixture
def dbconn():
    yield DBConn(DBCONNSTR, DB_TEST_SCHEMA)


TEST_DF = pd.DataFrame(
    {
        'a': range(10)
    },
    index=[f'id_{i}' for i in range(10)]
)

TEST_DF_INC1 = TEST_DF.assign(a = lambda df: df['a'] + 1)
TEST_DF_INC2 = TEST_DF.assign(a = lambda df: df['a'] + 2)
TEST_DF_INC3 = TEST_DF.assign(a = lambda df: df['a'] + 3)


def test_data_catalog(dbconn) -> None:
    ds = MetaStore(dbconn)
    data_catalog = DataCatalog(
        ds=ds,
        catalog={
            'test_df': DBTable([Column('a', Numeric)])
        },
    )

    tbl = data_catalog.get_data_table('test_df')
    tbl.store(TEST_DF)
    assert(assert_df_equal(tbl.get_data(), TEST_DF))


def test_store_node(dbconn) -> None:
    ds = MetaStore(dbconn)
    data_catalog = DataCatalog(
        ds=ds,
        catalog={
            'test_df': DBTable([Column('a', Numeric)])
        },
    )

    def proc_func():
        return TEST_DF

    store_node = StoreNode(
        proc_func=proc_func,
        outputs=['test_df']
    )
    store_node.run(data_catalog=data_catalog)

    tbl = data_catalog.get_data_table('test_df')
    assert(assert_df_equal(tbl.get_data(), TEST_DF))


def test_python_node(dbconn) -> None:
    ds = MetaStore(dbconn)
    data_catalog = DataCatalog(
        ds=ds,
        catalog={
            'test_df': DBTable([Column('a', Numeric)]),
            'test_df_inc1': DBTable([Column('a', Numeric)]),
            'test_df_inc2': DBTable([Column('a', Numeric)]),
            'test_df_inc3': DBTable([Column('a', Numeric)])
        },
    )
    data_catalog.get_data_table('test_df').store(TEST_DF)

    def proc_func(df):
        df1 = df.copy()
        df2 = df.copy()
        df3 = df.copy()
        df1['a'] += 1
        df2['a'] += 2
        df3['a'] += 3
        return df1, df2, df3

    python_node = PythonNode(
        proc_func=proc_func,
        inputs=['test_df'],
        outputs=['test_df_inc1', 'test_df_inc2', 'test_df_inc3']
    )
    python_node.run(data_catalog=data_catalog)

    tbl = data_catalog.get_data_table('test_df')
    tbl1 = data_catalog.get_data_table('test_df_inc1')
    tbl2 = data_catalog.get_data_table('test_df_inc2')
    tbl3 = data_catalog.get_data_table('test_df_inc3')

    assert(assert_df_equal(tbl.get_data(), TEST_DF))
    assert(assert_df_equal(tbl1.get_data(), TEST_DF_INC1))
    assert(assert_df_equal(tbl2.get_data(), TEST_DF_INC2))
    assert(assert_df_equal(tbl3.get_data(), TEST_DF_INC3))


@pytest.fixture
def temp_dir():
    old_cwd = Path.cwd()
    with tempfile.TemporaryDirectory() as tmp_dir_path:
        os.chdir(tmp_dir_path)
        yield 'ok'
        os.chdir(old_cwd)


@pytest.mark.usefixtures('temp_dir')
def test_label_studio_node(dbconn) -> None:
    ds = MetaStore(dbconn)
    data_catalog = DataCatalog(
        ds=ds,
        catalog={
            'data_input': DBTable([Column('data', JSON)]),
            'data_annotation': DBTable([Column('data', JSON)])
        },
    )

    label_studio_node = LabelStudioNode(
        project_path='ls_project_test/',
        input='data_input',
        output='data_annotation',
        port=8080
    )
    label_studio_node.run(data_catalog=data_catalog)
    time.sleep(2)
    label_studio_node.terminate_services()


def test_pipeline(dbconn) -> None:
    ds = MetaStore(dbconn)
    data_catalog = DataCatalog(
        ds=ds,
        catalog={
            'test_df': DBTable([Column('a', Numeric)]),
            'test_df_inc1': DBTable([Column('a', Numeric)]),
            'test_df_inc2': DBTable([Column('a', Numeric)]),
            'test_df_inc3': DBTable([Column('a', Numeric)])
        },
    )

    def store_func():
        return TEST_DF

    def inc_func(df):
        df['a'] += 1
        return df

    def inc2_func(df1, df2):
        df1['a'] += 2
        df2['a'] += 2
        return df1, df2

    pipeline = Pipeline(
        data_catalog=data_catalog,
        pipeline=[
            StoreNode(
                proc_func=store_func,
                outputs=['test_df']
            ),
            PythonNode(
                proc_func=inc_func,
                inputs=['test_df'],
                outputs=['test_df_inc1']
            ),
            PythonNode(
                proc_func=inc2_func,
                inputs=['test_df', 'test_df_inc1'],
                outputs=['test_df_inc2', 'test_df_inc3']
            )
        ]
    )

    pipeline.run()

    tbl = data_catalog.get_data_table('test_df')
    tbl1 = data_catalog.get_data_table('test_df_inc1')
    tbl2 = data_catalog.get_data_table('test_df_inc2')
    tbl3 = data_catalog.get_data_table('test_df_inc3')

    assert(assert_df_equal(tbl.get_data(), TEST_DF))
    assert(assert_df_equal(tbl1.get_data(), TEST_DF_INC1))
    assert(assert_df_equal(tbl2.get_data(), TEST_DF_INC2))
    assert(assert_df_equal(tbl3.get_data(), TEST_DF_INC3))

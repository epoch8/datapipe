import pandas as pd
from sqlalchemy import Column
from sqlalchemy.sql.sqltypes import Integer, Boolean

from datapipe.store.database import TableStoreDB
from datapipe.datatable import DataStore

from datapipe.compute import Catalog, Pipeline, Table, run_pipeline
from datapipe.core_steps import BatchGenerate, BatchTransform
from .util import assert_datatable_equal


TEST_SCHEMA = [
    Column('id1', Integer, primary_key=True),
    Column('id2', Integer, primary_key=True),
    Column('a', Integer),
]

TEST_FINAL_SCHEMA = [
    Column('id1', Integer, primary_key=True),
    Column('a', Integer),
]

TEST_FILTER_SCHEMA = [
    Column('id1', Integer, primary_key=True),
    Column('id2', Integer, primary_key=True),
    Column('filter', Boolean),
]

TEST_DF = pd.DataFrame(
    {
        'id1': range(10),
        'id2': [-i for i in range(10)],
        'a': range(10),
    },
)


def get_tbl_filter(df, value):
    df['filter'] = df['a'] <= value
    return df[['id1', 'id2', 'filter']]


def get_tbl_final(df, df_filter):
    df = pd.merge(df, df_filter, on=['id1', 'id2'])
    df = df[df['filter']]
    return df[['id1', 'id2', 'a']]


def test_delete_table_after_filter(dbconn):
    catalog = Catalog({
        'tbl': Table(
            store=TableStoreDB(dbconn, 'tbl', TEST_SCHEMA, True)
        ),
        'tbl_filter': Table(
            store=TableStoreDB(dbconn, 'tbl_filter', TEST_FILTER_SCHEMA, True)
        ),
        'tbl_final_id1_id2': Table(
            store=TableStoreDB(dbconn, 'tbl_final_id1_id2', TEST_SCHEMA, True)
        ),
        'tbl_final_id1': Table(
            store=TableStoreDB(dbconn, 'tbl_final_id1', TEST_FINAL_SCHEMA, True)
        ),
    })

    def gen_tbl():
        yield TEST_DF

    old_pipeline = Pipeline([
        BatchGenerate(
            func=gen_tbl,
            outputs=['tbl'],
        ),
        BatchTransform(
            func=get_tbl_filter,
            inputs=['tbl'],
            outputs=['tbl_filter'],
            kwargs=dict(
                value=10
            )
        ),
        BatchTransform(
            func=get_tbl_final,
            inputs=['tbl', 'tbl_filter'],
            outputs=['tbl_final_id1_id2'],
        ),
        BatchTransform(
            func=lambda df: df[['id1', 'a']],
            inputs=['tbl_final_id1_id2'],
            outputs=['tbl_final_id1'],
        ),
    ])

    # Отличается от old_pipeline другой фильтрацией
    new_pipeline = Pipeline([
        BatchGenerate(
            func=gen_tbl,
            outputs=['tbl'],
        ),
        BatchTransform(
            func=get_tbl_filter,
            inputs=['tbl'],
            outputs=['tbl_filter'],
            kwargs=dict(
                value=4
            )
        ),
        BatchTransform(
            func=get_tbl_final,
            inputs=['tbl', 'tbl_filter'],
            outputs=['tbl_final_id1_id2'],
        ),
        BatchTransform(
            func=lambda df: df[['id1', 'a']],
            inputs=['tbl_final_id1_id2'],
            outputs=['tbl_final_id1'],
        ),
    ])

    ds = DataStore(dbconn, create_meta_table=True)

    tbl_filter = catalog.get_datatable(ds, 'tbl_filter')
    tbl_final_id1_id2 = catalog.get_datatable(ds, 'tbl_final_id1_id2')
    tbl_final_id1 = catalog.get_datatable(ds, 'tbl_final_id1')

    # Чистый Фильтр (10 значений)
    run_pipeline(ds, catalog, old_pipeline)
    assert len(tbl_final_id1_id2.get_data()) == 10
    assert len(tbl_final_id1.get_data()) == 10
    assert_datatable_equal(tbl_final_id1_id2, TEST_DF)
    assert_datatable_equal(tbl_final_id1, TEST_DF.drop(columns=['id2']))

    # Меняем пайплайн в одной трансформации -> делаем сброс метаданных
    tbl_final_id1_id2.reset_metadata()

    # Фильтр отсеивает 5 значений, поэтому они должны быть удалены из дальнейших таблиц
    run_pipeline(ds, catalog, new_pipeline)
    assert len(tbl_final_id1_id2.get_data()) == 5
    assert len(tbl_final_id1.get_data()) == 5
    assert_datatable_equal(tbl_final_id1_id2, TEST_DF[:5])
    assert_datatable_equal(tbl_final_id1, TEST_DF[:5].drop(columns=['id2']))

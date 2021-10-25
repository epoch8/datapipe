import datetime
import os
import time
from typing import Iterable
import pytest
from pytest_cases import parametrize_with_cases, case, parametrize

import pandas as pd
from sqlalchemy import Column, Integer, String

from datapipe.types import DataDF
from datapipe.store.table_store import TableStore
from datapipe.store.database import TableStoreDB
from datapipe.store.pandas import TableStoreJsonLine, TableStoreExcel
from datapipe.store.filedir import JSONFile, TableStoreFiledir
from datapipe.yandex_toloka.store import TableStoreYandexToloka

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
    @case(tags='supports_delete,supports_all_read_rows')
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

    @case(tags='supports_delete,supports_all_read_rows')
    @parametrize('df,schema', DATA_PARAMS)
    def case_jsonline(self, tmp_dir, df, schema):
        return (
            TableStoreJsonLine(
                tmp_dir / "data.json",
                primary_schema=schema
            ),
            df
        )

    @case(tags='supports_delete,supports_all_read_rows')
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

    @pytest.mark.skipif('YANDEX_TOLOKA_TOKEN' not in os.environ, reason="env variable 'YANDEX_TOLOKA_TOKEN' is not set")
    @case(tags='supports_delete,supports_all_read_rows')
    @parametrize('df,schema', DATA_PARAMS)
    def case_yandex_toloka(self, dbconn, df, schema, request):
        yandex_toloka_token = os.environ['YANDEX_TOLOKA_TOKEN']
        dbconn_backend = "postgresql" if "postgresql" in dbconn.connstr else "sqlite"
        project_identifier = f'test_{request.node.callspec.id} [{dbconn_backend}]'
        table_store_yandex_toloka = TableStoreYandexToloka(
            dbconn=dbconn,
            token=yandex_toloka_token,
            environment='SANDBOX',
            input_data_sql_schema=schema + [
                Column('name', String(100)),
                Column('price', Integer),
            ],
            output_data_sql_schema=[
                Column('annotations', String())
            ],
            project_identifier=project_identifier,
            user_id_column=None,
            assignment_id_column=None,
            kwargs_at_create_project={
                'public_name': project_identifier
            },
            kwargs_at_create_pool={
                'may_contain_adult_content': False,
                'reward_per_assignment': 0.01,
                'assignment_max_duration_seconds': 60*5,
                'will_expire': datetime.datetime.utcnow() + datetime.timedelta(days=365)
            }
        )
        # Wait until project is empty
        while True:
            if len(list(table_store_yandex_toloka._get_all_tasks())) == 0:
                break
            time.sleep(10)

        df = df.copy()
        df['annotations'] = None
        yield table_store_yandex_toloka, df

        try:
            table_store_yandex_toloka.toloka_client._request(
                'post', f'/new/requester/pools/{table_store_yandex_toloka.pool.id}/tasks/delete-all'
            )
        except Exception:
            pass


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
    df_empty = pd.DataFrame()
    assert store.read_rows(df_empty).empty


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
    idxs_dfs = []
    for pseudo_df_iter in pseudo_df_iter:
        assert(isinstance(pseudo_df_iter, DataDF))
        idxs_dfs.append(pseudo_df_iter[store.primary_keys])
    idxs_df = pd.concat(idxs_dfs)
    assert len(idxs_df) == len(test_df)
    assert(sorted(list(idxs_df.columns)) == sorted(store.primary_keys))

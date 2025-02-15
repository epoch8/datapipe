from typing import Iterable, cast

import pandas as pd
import pytest
from pytest_cases import case, parametrize, parametrize_with_cases
from sqlalchemy import Column, Integer, String

from datapipe.run_config import RunConfig
from datapipe.store.database import TableStoreDB
from datapipe.store.filedir import JSONFile, TableStoreFiledir
from datapipe.store.pandas import TableStoreExcel, TableStoreJsonLine
from datapipe.store.redis import RedisStore
from datapipe.store.table_store import TableStore
from datapipe.tests.util import assert_df_equal, assert_ts_contains
from datapipe.types import DataDF, IndexDF, data_to_index

DATA_PARAMS = [
    pytest.param(
        pd.DataFrame(
            {
                "id": range(100),
                "name": [f"Product {i}" for i in range(100)],
                "price": [1000 + i for i in range(100)],
            }
        ),
        [Column("id", Integer, primary_key=True)],
        id="int_id",
    ),
    pytest.param(
        pd.DataFrame(
            {
                "id": [f"id_{i}" for i in range(100)],
                "name": [f"Product {i}" for i in range(100)],
                "price": [1000 + i for i in range(100)],
            }
        ),
        [Column("id", String(100), primary_key=True)],
        id="str_id",
    ),
    pytest.param(
        pd.DataFrame(
            {
                "id_int": range(100),
                "id_str": [f"id_{i}" for i in range(100)],
                "name": [f"Product {i}" for i in range(100)],
                "price": [1000 + i for i in range(100)],
            }
        ),
        [
            Column("id_int", Integer, primary_key=True),
            Column("id_str", String(100), primary_key=True),
        ],
        id="multi_id",
    ),
    pytest.param(
        pd.DataFrame(
            {
                "id1": [f"id_{i}" for i in range(1000)],
                "id2": [f"id_{i}" for i in range(1000)],
                "name": [f"Product {i}" for i in range(1000)],
                "price": [1000 + i for i in range(1000)],
            }
        ),
        [
            Column("id1", String(100), primary_key=True),
            Column("id2", String(100), primary_key=True),
        ],
        id="double_id_1000_records",
    ),
    pytest.param(
        pd.DataFrame(
            {
                "id1": [f"id_{i}" for i in range(1000)],
                "id2": [f"id_{i}" for i in range(1000)],
                "id3": [f"id_{i}" for i in range(1000)],
                "name": [f"Product {i}" for i in range(1000)],
                "price": [1000 + i for i in range(1000)],
            }
        ),
        [
            Column("id1", String(100), primary_key=True),
            Column("id2", String(100), primary_key=True),
            Column("id3", String(100), primary_key=True),
        ],
        id="triple_id_1000_records",
    ),
]

FILEDIR_DATA_PARAMS = [
    pytest.param(
        pd.DataFrame(
            {
                "id": [f"id_{i}" for i in range(100)],
                "name": [f"Product {i}" for i in range(100)],
                "price": [1000 + i for i in range(100)],
            }
        ),
        "{id}.json",
        [
            Column("id", String(100)),
        ],
        id="str_id",
    ),
    pytest.param(
        pd.DataFrame(
            {
                "id": [i for i in range(100)],
                "name": [f"Product {i}" for i in range(100)],
                "price": [1000 + i for i in range(100)],
            }
        ),
        "{id}.json",
        [Column("id", Integer)],
        id="integer_id",
    ),
    pytest.param(
        pd.DataFrame(
            {
                "id1": [f"id_{i}" for i in range(100)],
                "id2": [f"id_{i}" for i in range(100)],
                "name": [f"Product {i}" for i in range(100)],
                "price": [1000 + i for i in range(100)],
            }
        ),
        "{id1}__{id2}.json",
        [
            Column("id1", String(100)),
            Column("id2", String(100)),
        ],
        id="multi_id",
    ),
    pytest.param(
        pd.DataFrame(
            {
                "id1": [f"id_{i}" for i in range(100)],
                "id2": [f"id_{i}" for i in range(100)],
                "id3": [f"id__{i}" for i in range(100)],
                "id4": [f"id___{i}" for i in range(100)],
                "id5": [f"id_{i}_" for i in range(100)],
                "name": [f"Product {i}" for i in range(100)],
                "price": [1000 + i for i in range(100)],
            }
        ),
        "{id1}______{id2}______{id3}______{id4}______{id5}.json",
        [
            Column("id1", String(100)),
            Column("id2", String(100)),
            Column("id3", String(100)),
            Column("id4", String(100)),
            Column("id5", String(100)),
        ],
        id="multi_ids2",
    ),
    pytest.param(
        pd.DataFrame(
            {
                "id1": [f"id_{i}" for i in range(100)],
                "id2": [f"id_{i}" for i in range(100, 200)],
                "id3": [f"id_{i}" for i in range(150, 250)],
                "name": [f"Product {i}" for i in range(100)],
                "price": [1000 + i for i in range(100)],
            }
        ),
        "{id2}__{id1}__{id3}.json",
        [
            Column("id1", String(100)),
            Column("id2", String(100)),
            Column("id3", String(100)),
        ],
        id="multi_ids_check_commutativity",
    ),
    pytest.param(
        pd.DataFrame(
            {
                "id1": [i for i in range(100)],
                "id2": [i for i in range(100, 200)],
                "id3": [str(i) for i in range(150, 250)],
                "name": [f"Product {i}" for i in range(100)],
                "price": [1000 + i for i in range(100)],
            }
        ),
        "{id2}__{id1}__{id3}.json",
        [
            Column("id1", Integer),
            Column("id2", Integer),
            Column("id3", String(100)),
        ],
        id="columns_types",
    ),
    pytest.param(
        pd.DataFrame(
            {
                "id1": [i for i in range(100)],
                "id2": [str(i) for i in range(100, 200)],
                "id3": [i for i in range(100, 200)],
                "id4": [str(i) for i in range(100, 200)],
                "name": [f"Product {i}" for i in range(100)],
                "price": [1000 + i for i in range(100)],
            }
        ),
        "{id1}/{id2}/{id3}/{id4}.json",
        [
            Column("id1", Integer),
            Column("id2", String(100)),
            Column("id3", Integer),
            Column("id4", String(100)),
        ],
        id="multi_ids_slash2",
    ),
    pytest.param(
        pd.DataFrame(
            {
                "id": [
                    "json",
                    "json.",
                    "AAjson",
                    "AAAjsonBB.j",
                    "assdjson.json",
                    "jsonjson.jsonasdad.fdsds.json",
                ],
                "name": [f"Product {i}" for i in range(6)],
                "price": [1000 + i for i in range(6)],
            }
        ),
        "{id}.json",
        [
            Column("id", String(100)),
        ],
        id="complex_values",
    ),
]


class CasesTableStore:
    @case(tags=["supports_delete", "supports_read_nonexistent_rows"])
    @parametrize("df,schema", DATA_PARAMS)
    def case_redis(self, redis_conn, df, schema):
        return (
            RedisStore(
                redis_conn,
                "redis_table",
                schema
                + [
                    Column("name", String(100)),
                    Column("price", Integer),
                ],
            ),
            df,
        )

    @case(
        tags=[
            "supports_delete",
            "supports_read_all_rows",
            "supports_get_schema",
            "supports_read_nonexistent_rows",
            "supports_read_meta_pseudo_df",
        ]
    )
    @parametrize("df,schema", DATA_PARAMS)
    def case_db(self, dbconn, df, schema):
        return (
            TableStoreDB(
                dbconn,
                "tbl1",
                schema
                + [
                    Column("name", String(100)),
                    Column("price", Integer),
                ],
                create_table=True,
            ),
            df,
        )

    @case(
        tags=[
            "supports_delete",
            "supports_read_all_rows",
            "supports_read_nonexistent_rows",
            "supports_read_meta_pseudo_df",
        ]
    )
    @parametrize("df,schema", DATA_PARAMS)
    def case_jsonline(self, tmp_dir, df, schema):
        return (TableStoreJsonLine(tmp_dir / "data.json", primary_schema=schema), df)

    @case(
        tags=[
            "supports_delete",
            "supports_read_all_rows",
            "supports_read_nonexistent_rows",
            "supports_read_meta_pseudo_df",
        ]
    )
    @parametrize("df,schema", DATA_PARAMS)
    def case_excel(self, tmp_dir, df, schema):
        return (TableStoreExcel(tmp_dir / "data.xlsx", primary_schema=schema), df)

    @case(
        tags=[
            "supports_delete",
            "supports_read_all_rows",
            "supports_read_meta_pseudo_df",
        ]
    )
    @parametrize("df,fn_template,primary_schema", FILEDIR_DATA_PARAMS)
    def case_filedir_json(self, tmp_dir, df, fn_template, primary_schema):
        return (
            TableStoreFiledir(
                tmp_dir / fn_template,
                adapter=JSONFile(),
                primary_schema=primary_schema,
                enable_rm=True,
            ),
            df,
        )


@parametrize_with_cases("store,test_df", cases=CasesTableStore)
def test_get_schema(store: TableStore, test_df: pd.DataFrame) -> None:
    if not store.caps.supports_get_schema:
        raise pytest.skip("Store does not support get_schema")

    store.get_schema()


@parametrize_with_cases("store,test_df", cases=CasesTableStore)
def test_write_read_rows(store: TableStore, test_df: pd.DataFrame) -> None:
    store.insert_rows(test_df)

    assert_ts_contains(store, test_df)


@parametrize_with_cases("store,test_df", cases=CasesTableStore)
def test_write_read_full_rows(store: TableStore, test_df: pd.DataFrame) -> None:
    if not store.caps.supports_read_all_rows:
        raise pytest.skip("Store does not support read_all_rows")

    store.insert_rows(test_df)

    assert_df_equal(store.read_rows(), test_df, index_cols=store.primary_keys)


@parametrize_with_cases("store,test_df", cases=CasesTableStore)
def test_insert_identical_rows_twice_and_read_rows(
    store: TableStore, test_df: pd.DataFrame
) -> None:
    store.insert_rows(test_df)

    test_df_mod = test_df.copy()
    test_df_mod.loc[50:, "price"] = test_df_mod.loc[50:, "price"] + 1

    store.insert_rows(test_df_mod.loc[50:])

    assert_ts_contains(store, test_df_mod)


@parametrize_with_cases("store,test_df", cases=CasesTableStore)
def test_read_non_existent_rows(store: TableStore, test_df: pd.DataFrame) -> None:
    if not store.caps.supports_read_nonexistent_rows:
        raise pytest.skip("Store does not support read_nonexistent_rows")

    test_df_to_store = test_df.drop(range(1, 5))

    store.insert_rows(test_df_to_store)

    assert_df_equal(
        store.read_rows(data_to_index(test_df, store.primary_keys)),
        test_df_to_store,
        index_cols=store.primary_keys,
    )


@parametrize_with_cases("store,test_df", cases=CasesTableStore)
def test_read_empty_df(store: TableStore, test_df: pd.DataFrame) -> None:
    store.insert_rows(test_df)

    df_empty = pd.DataFrame()

    df_result = store.read_rows(cast(IndexDF, df_empty))
    assert df_result.empty
    df_result[store.primary_keys]  # Empty df must have primary keys columns


@parametrize_with_cases("store,test_df", cases=CasesTableStore)
def test_insert_empty_df(store: TableStore, test_df: pd.DataFrame) -> None:
    if not store.caps.supports_read_all_rows:
        raise pytest.skip("Store does not support read_all_rows")

    df_empty = pd.DataFrame()
    store.insert_rows(df_empty)

    df_result = store.read_rows()
    assert df_result.empty
    df_result[store.primary_keys]  # Empty df must have primary keys columns


@parametrize_with_cases("store,test_df", cases=CasesTableStore)
def test_update_empty_df(store: TableStore, test_df: pd.DataFrame) -> None:
    if not store.caps.supports_read_all_rows:
        raise pytest.skip("Store does not support read_all_rows")

    df_empty = pd.DataFrame()
    store.update_rows(df_empty)

    df_result = store.read_rows()
    assert df_result.empty
    df_result[store.primary_keys]  # Empty df must have primary keys columns


@parametrize_with_cases("store,test_df", cases=CasesTableStore)
def test_partial_update_rows(store: TableStore, test_df: pd.DataFrame) -> None:
    store.insert_rows(test_df)

    assert_ts_contains(store, test_df)

    test_df_mod = test_df.copy()
    test_df_mod.loc[50:, "price"] = test_df_mod.loc[50:, "price"] + 1

    store.update_rows(test_df_mod.loc[50:])

    assert_ts_contains(store, test_df_mod)


@parametrize_with_cases("store,test_df", cases=CasesTableStore)
def test_full_update_rows(store: TableStore, test_df: pd.DataFrame) -> None:
    store.insert_rows(test_df)

    assert_ts_contains(store, test_df)

    test_df_mod = test_df.copy()
    test_df_mod.loc[:, "price"] = test_df_mod.loc[:, "price"] + 1

    store.update_rows(test_df_mod)

    assert_ts_contains(store, test_df_mod)


# TODO add test which does not require read_all_rows support
@parametrize_with_cases("store,test_df", cases=CasesTableStore)
def test_delete_rows(store: TableStore, test_df: pd.DataFrame) -> None:
    if not store.caps.supports_delete:
        raise pytest.skip("Store does not support delete")
    if not store.caps.supports_read_all_rows:
        raise pytest.skip("Store does not support read_all_rows")

    store.insert_rows(test_df)

    assert_df_equal(
        store.read_rows(data_to_index(test_df, store.primary_keys)),
        test_df,
        index_cols=store.primary_keys,
    )

    store.delete_rows(cast(IndexDF, test_df.loc[20:50, store.primary_keys]))

    assert_df_equal(
        store.read_rows(),
        pd.concat([test_df.loc[0:19], test_df.loc[51:]]),
        index_cols=store.primary_keys,
    )


@parametrize_with_cases("store,test_df", cases=CasesTableStore)
def test_read_rows_meta_pseudo_df(store: TableStore, test_df: pd.DataFrame) -> None:
    if not store.caps.supports_read_meta_pseudo_df:
        raise pytest.skip("Store does not support read_meta_pseudo_df")

    store.insert_rows(test_df)

    assert_ts_contains(store, test_df)

    pseudo_df_iter = store.read_rows_meta_pseudo_df()

    assert isinstance(pseudo_df_iter, Iterable)
    assert isinstance(next(pseudo_df_iter), DataDF)


@parametrize_with_cases("store,test_df", cases=CasesTableStore)
def test_read_empty_rows_meta_pseudo_df(
    store: TableStore, test_df: pd.DataFrame
) -> None:
    if not store.caps.supports_read_meta_pseudo_df:
        raise pytest.skip("Store does not support read_meta_pseudo_df")

    pseudo_df_iter = store.read_rows_meta_pseudo_df()
    assert isinstance(pseudo_df_iter, Iterable)
    for pseudo_df in pseudo_df_iter:
        assert isinstance(pseudo_df, DataDF)
        pseudo_df[store.primary_keys]  # Empty df must have primary keys columns


@parametrize_with_cases("store,test_df", cases=CasesTableStore)
def test_read_rows_meta_pseudo_df_with_runconfig(
    store: TableStore, test_df: pd.DataFrame
) -> None:
    if not store.caps.supports_read_meta_pseudo_df:
        raise pytest.skip("Store does not support read_meta_pseudo_df")

    store.insert_rows(test_df)

    assert_ts_contains(store, test_df)

    # TODO проверять, что runconfig реально влияет на результирующие данные
    pseudo_df_iter = store.read_rows_meta_pseudo_df(
        run_config=RunConfig(filters={"a": 1})
    )
    assert isinstance(pseudo_df_iter, Iterable)
    for pseudo_df in pseudo_df_iter:
        assert isinstance(pseudo_df, DataDF)

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
from datapipe.store.tests.stubs import DATA_PARAMS
from datapipe.tests.util import assert_df_equal, assert_ts_contains
from datapipe.types import DataDF, IndexDF, data_to_index

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

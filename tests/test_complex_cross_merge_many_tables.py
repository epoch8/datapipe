import itertools
from typing import List

import pandas as pd
import pytest
from pytest_cases import parametrize
from sqlalchemy import Column
from sqlalchemy.sql.sqltypes import Integer

from datapipe.compute import Catalog, Pipeline, Table, run_pipeline
from datapipe.datatable import DataStore
from datapipe.step.batch_generate import BatchGenerate
from datapipe.step.batch_transform import BatchTransform
from datapipe.store.database import TableStoreDB
from datapipe.tests.util import assert_datatable_equal

TEST_SCHEMA_LEFT = [
    pytest.param(
        [
            Column("id_left", Integer, primary_key=True),
            Column("a_left", Integer),
        ],
        id="left",
    ),
    pytest.param(
        [
            Column("id_left", Integer, primary_key=True),
            Column("id1", Integer, primary_key=True),
            Column("id2", Integer, primary_key=True),
            Column("a_left", Integer),
        ],
        id="left_with_id_x2",
    ),
]

TEST_SCHEMA_CENTER = [
    pytest.param(
        [
            Column("id_center", Integer, primary_key=True),
            Column("c_center", Integer),
        ],
        id="center",
    ),
    pytest.param(
        [
            Column("id_center", Integer, primary_key=True),
            Column("id1", Integer, primary_key=True),
            Column("id2", Integer, primary_key=True),
            Column("c_center", Integer),
        ],
        id="center_with_id_x2",
    ),
]

TEST_SCHEMA_RIGHT = [
    pytest.param(
        [
            Column("id_right", Integer, primary_key=True),
            Column("b_right", Integer),
        ],
        id="right",
    ),
    pytest.param(
        [
            Column("id_right", Integer, primary_key=True),
            Column("id1", Integer, primary_key=True),
            Column("id2", Integer, primary_key=True),
            Column("b_right", Integer),
        ],
        id="right_with_id_x2",
    ),
]

TEST_DF_VALUES = [x for x in range(5)] + [6, 7, -5, -9]


def get_all_cases_schemes():
    for len_input in range(2, 4):
        for len_output in range(2, 4):
            for input_schema_tables in itertools.combinations(
                TEST_SCHEMA_LEFT + TEST_SCHEMA_CENTER + TEST_SCHEMA_RIGHT, len_input
            ):
                for output_schema_tables in itertools.combinations(
                    TEST_SCHEMA_LEFT + TEST_SCHEMA_CENTER + TEST_SCHEMA_RIGHT,
                    len_output,
                ):
                    yield input_schema_tables, output_schema_tables


def get_primary_key_to_their_tables(
    schemas: List[List[Column]], table_names: List[str]
):
    primary_keys = [
        set([x.name for x in schema if x.primary_key]) for schema in schemas
    ]
    idxs = range(len(schemas))
    pairs = itertools.combinations(idxs, 2)
    nt = lambda a, b: primary_keys[a].intersection(primary_keys[b])
    table_name1_table_name2_to_intersection_idxs = {
        (table_names[t[0]], table_names[t[1]]): nt(*t) for t in pairs
    }
    return table_name1_table_name2_to_intersection_idxs


def cross_merge_func(
    *dfs, input_intersection_idxs: List[str], output_schema_tables: List[List[Column]]
):
    df_res = dfs[0]
    for df in dfs[1:]:
        if len(input_intersection_idxs) > 0:
            df_res = pd.merge(df_res, df)
        else:
            # TODO: добавить случай с разными попарного пересечения индексов таблиц
            df_res = pd.merge(df_res, df, how="cross")
    return [
        df_res[[x.name for x in columns]].drop_duplicates([x.name for x in columns])
        for columns in output_schema_tables
    ]


def get_all_cases():
    looked_total_id = set()
    for (
        input_schema_tables_params,
        output_schema_tables_params,
    ) in get_all_cases_schemes():
        input_schema_tables = [
            input_schema_tables_param.values[0]
            for input_schema_tables_param in input_schema_tables_params
        ]
        output_schema_tables = [
            output_schema_tables_param.values[0]
            for output_schema_tables_param in output_schema_tables_params
        ]
        input_tables_names = [
            f"table_input{i}" for i in range(len(input_schema_tables))
        ]
        output_tables_names = [
            f"table_output{i}" for i in range(len(output_schema_tables))
        ]
        input_table_name1_table_name2_to_intersection_idxs = (
            get_primary_key_to_their_tables(input_schema_tables, input_tables_names)
        )
        input_all_columns = list(
            set([x.name for schema in input_schema_tables for x in schema])
        )
        output_all_columns = list(
            set([x.name for schema in output_schema_tables for x in schema])
        )
        if any(
            [
                output_column not in input_all_columns
                for output_column in output_all_columns
            ]
        ):
            continue
        outputs_primary_keys = [
            [x.name for x in schema if x.primary_key] for schema in output_schema_tables
        ]
        total_output_primary_keys = list(
            set(
                [
                    x.name
                    for schema in output_schema_tables
                    for x in schema
                    if x.primary_key
                ]
            )
        )
        input_intersection_idxs = max(
            input_table_name1_table_name2_to_intersection_idxs.values(),
            key=lambda x: len(x),
        )
        for sets in input_table_name1_table_name2_to_intersection_idxs.values():
            input_intersection_idxs = input_intersection_idxs.intersection(sets)
        if len(input_schema_tables) > 2 and len(input_intersection_idxs) == 0:
            # TODO: добавить случай с разными попарными пересечения индексов таблиц
            continue
        test_input_dfs = [
            pd.DataFrame({column.name: TEST_DF_VALUES for column in input_schema})
            for input_schema in input_schema_tables
        ]
        test_output_dfs = cross_merge_func(
            *test_input_dfs,
            input_intersection_idxs=input_intersection_idxs,
            output_schema_tables=output_schema_tables,
        )
        primary_keys = (
            list(input_intersection_idxs)
            if len(input_intersection_idxs) > 0
            else total_output_primary_keys
        )
        input_id = "__".join(sorted([param.id for param in input_schema_tables_params]))
        output_id = "__".join(
            sorted([param.id for param in output_schema_tables_params])
        )
        for len_transform_keys in range(1, len(primary_keys) + 1):
            for transform_keys in itertools.combinations(
                primary_keys, len_transform_keys
            ):
                if not all(
                    all(
                        [
                            transform_key in output_primary_keys
                            for transform_key in transform_keys
                        ]
                    )
                    for output_primary_keys in outputs_primary_keys
                ):
                    # Все ключи трансформаций должны полностью входить в ключи выходных таблиц
                    continue
                id_transform_keys = "__".join(sorted(transform_keys))
                total_id = f"inputs-[{input_id}]-outputs-[{output_id}]-trasnforms-keys-[{id_transform_keys}]"
                if total_id in looked_total_id:
                    continue
                looked_total_id.add(total_id)
                yield pytest.param(
                    [
                        input_tables_names,
                        input_schema_tables,
                        output_tables_names,
                        output_schema_tables,
                        test_input_dfs,
                        test_output_dfs,
                        input_intersection_idxs,
                        transform_keys,
                    ],
                    id=total_id,
                )


@parametrize("test_case", list(get_all_cases()))
def test_complex_cross_merge_on_many_tables(dbconn, test_case):
    (
        input_tables_names,
        input_schema_tables,
        output_tables_names,
        output_schema_tables,
        test_input_dfs,
        test_output_dfs,
        input_intersection_idxs,
        transform_keys,
    ) = test_case
    ds = DataStore(dbconn, create_meta_table=True)
    catalog = Catalog(
        {
            **{
                input_table_name: Table(
                    store=TableStoreDB(dbconn, input_table_name, input_schema, True)
                )
                for input_table_name, input_schema in zip(
                    input_tables_names, input_schema_tables
                )
            },
            **{
                output_table_name: Table(
                    store=TableStoreDB(dbconn, output_table_name, output_schema, True)
                )
                for output_table_name, output_schema in zip(
                    output_tables_names, output_schema_tables
                )
            },
        }
    )

    def gen_tbl(df):
        yield df

    pipeline_case = Pipeline(
        [
            BatchGenerate(
                func=gen_tbl, outputs=[input_table_name], kwargs=dict(df=test_input_df)
            )
            for input_table_name, test_input_df in zip(
                input_tables_names, test_input_dfs
            )
        ]
        + [
            BatchTransform(
                func=cross_merge_func,
                inputs=input_tables_names,
                outputs=output_tables_names,
                transform_keys=transform_keys,
                chunk_size=6,
                kwargs=dict(
                    input_intersection_idxs=input_intersection_idxs,
                    output_schema_tables=output_schema_tables,
                ),
            )
        ]
    )
    run_pipeline(ds, catalog, pipeline_case)
    print(f"{input_intersection_idxs=}")
    for input_table_name, test_input_df in zip(input_tables_names, test_input_dfs):
        print(f"{input_table_name}\n{test_input_df}")
    for output_table_name, test_output_df in zip(output_tables_names, test_output_dfs):
        print(f"{output_table_name} should be \n{test_output_df}")
        tbl_output = catalog.get_datatable(ds, output_table_name)
        print(f"but {output_table_name} in pipeline =\n{tbl_output.get_data()}")

    for input_table_name, test_input_df in zip(input_tables_names, test_input_dfs):
        tbl_input = catalog.get_datatable(ds, input_table_name)
        assert_datatable_equal(tbl_input, test_input_df)
    for output_table_name, test_output_df in zip(output_tables_names, test_output_dfs):
        tbl_output = catalog.get_datatable(ds, output_table_name)
        assert_datatable_equal(tbl_output, test_output_df)

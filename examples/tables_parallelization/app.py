from typing import Iterator

from pathlib import Path

import pandas as pd
import random

from sqlalchemy import Column, String

from datapipe.compute import Catalog, Pipeline, Table
from typing import Any, Dict, List, Optional, Protocol
from datapipe.types import ChangeList
from datapipe.run_config import RunConfig
from datapipe.core_steps import BatchTransform
from datapipe.compute import (
    Catalog,
    Pipeline,
    Table,
    run_changelist,
)
from datapipe.datatable import DataStore
from datapipe.store.database import DBConn, TableStoreDB

from datapipe_app import DatapipeApp

from datapipe.core_steps import BatchGenerate
import time
from examples.tables_parallelization.test_steps import stepAB, stepBD, stepBCE, stepFC


def gen_some_data_A() -> Iterator[pd.DataFrame]:
    yield pd.DataFrame(
        {
            "id": [str(i) for i in range(10)],
            "col": [f"dataA_{i}" for i in range(10)],
        }
    )


def gen_some_data_F() -> Iterator[pd.DataFrame]:
    yield pd.DataFrame(
        {
            "id": [str(i) for i in range(10)],
            "col": [f"dataF_{i}" for i in range(10)],
        }
    )


dbconn = DBConn("postgresql://postgres:password@localhost/postgres")
catalog = Catalog(
    {
        "A": Table(
            store=TableStoreDB(
                name="A",
                dbconn=dbconn,
                data_sql_schema=[
                    Column("id", String, primary_key=True),
                    Column("col", String),
                ],
            ),
        ),
        "B": Table(
            store=TableStoreDB(
                name="B",
                dbconn=dbconn,
                data_sql_schema=[
                    Column("id", String, primary_key=True),
                    Column("col", String),
                ],
            )
        ),
        "C": Table(
            store=TableStoreDB(
                name="C",
                dbconn=dbconn,
                data_sql_schema=[
                    Column("id", String, primary_key=True),
                    Column("col", String),
                ],
            )
        ),
        "D": Table(
            store=TableStoreDB(
                name="D",
                dbconn=dbconn,
                data_sql_schema=[
                    Column("id", String, primary_key=True),
                    Column("col", String),
                ],
            )
        ),
        "E": Table(
            store=TableStoreDB(
                name="E",
                dbconn=dbconn,
                data_sql_schema=[
                    Column("id", String, primary_key=True),
                    Column("col", String),
                ],
            )
        ),
        "F": Table(
            store=TableStoreDB(
                name="F",
                dbconn=dbconn,
                data_sql_schema=[
                    Column("id", String, primary_key=True),
                    Column("col", String),
                ],
            )
        ),
    }
)

pipeline = Pipeline(
    [
        BatchGenerate(
            gen_some_data_A,
            outputs=["A"],
        ),
        BatchGenerate(
            gen_some_data_F,
            outputs=["F"],
        ),
        BatchTransform(
            stepAB,
            inputs=["A"],
            outputs=["B"],
            chunk_size=10,
        ),
        BatchTransform(
            stepBD,
            inputs=["B"],
            outputs=["D"],
            chunk_size=10,
        ),
        BatchTransform(
            stepBCE,
            inputs=["B", "C"],
            outputs=["E"],
            chunk_size=10,
        ),
        BatchTransform(
            stepFC,
            inputs=["F"],
            outputs=["C"],
            chunk_size=10,
        ),
    ]
)
ds = DataStore(dbconn)
app = DatapipeApp(ds, catalog, pipeline)

if __name__ == "__main__":
    start_time = time.time()
    new_id = time.time()
    # Change A
    print("Running A changes...")
    change_list = ChangeList()
    data_table = catalog.get_datatable(ds, "A")
    change_list_data = data_table.store_chunk(
        pd.DataFrame(
            {
                "id": [str(new_id)],
                "col": ["newdataA_" + str(random.randrange(100, 200))],
            }
        )
    )
    change_list.append(data_table.name, change_list_data)
    run_changelist(
        ds=ds,
        catalog=catalog,
        pipeline=pipeline,
        changelist=change_list,
    )

    # Change E
    print("Running F changes...")
    change_list = ChangeList()
    data_table = catalog.get_datatable(ds, "F")
    change_list_data = data_table.store_chunk(
        pd.DataFrame(
            {
                "id": [str(new_id) + "1"],
                "col": ["newdataF_" + str(random.randrange(100, 200))],
            }
        )
    )
    change_list.append(data_table.name, change_list_data)
    run_changelist(
        ds=ds,
        catalog=catalog,
        pipeline=pipeline,
        changelist=change_list,
    )

    print("Done!")
    print("--- %s seconds ---" % (time.time() - start_time))

    time.sleep(1000)

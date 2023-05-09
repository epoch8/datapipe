from typing import Iterator

from pathlib import Path

import pandas as pd
from random import random

from sqlalchemy import Column, String

from datapipe.compute import Catalog, Pipeline, Table
from typing import Any, Dict, List, Optional, Protocol
from datapipe.types import ChangeList
from datapipe.run_config import RunConfig
from datapipe.core_steps import BatchTransform, UpdateExternalTable
from datapipe.compute import (
    Catalog,
    Pipeline,
    Table,
    ComputeStep,
    build_compute,
    run_changelist,
    run_steps,
)
from datapipe.datatable import DataStore
from datapipe.store.database import DBConn, TableStoreDB

from datapipe_app import DatapipeApp

from datapipe.core_steps import do_full_batch_transform, BatchGenerate
import ray
import time
import uuid


def gen_some_data_A() -> Iterator[pd.DataFrame]:
    yield pd.DataFrame(
        {
            "id": [str(i) for i in range(100)],
            "col": [f"dataA_{i}" for i in range(100)],
        }
    )


def gen_some_data_C() -> Iterator[pd.DataFrame]:
    yield pd.DataFrame(
        {
            "id": [str(i) for i in range(100)],
            "col": [f"dataC_{i}" for i in range(100)],
        }
    )


def stepAB(dfA: pd.DataFrame) -> pd.DataFrame:
    df = dfA.copy()
    df["col"] = df["col"] + "_AB"
    time.sleep(3)
    return df


def stepBD(dfB: pd.DataFrame) -> pd.DataFrame:
    df = dfB.copy()
    df["col"] = df["col"] + "_BD"
    time.sleep(3)
    return df


def stepBCE(dfB: pd.DataFrame, dfC: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame(columns=["id", "col"])
    df["col"] = dfB["col"] + dfC["col"] + "_BCE"
    time.sleep(3)
    return df


def step_worker(step, changelist):
    # Выполнить шаг и получить changelist
    # Запускае shedule_step(changelist)
    pass


def shedule_step(changelist):
    # Пройтись по списку измененных таблиц
    #   Найти зависящие трансофрмации
    #   зашедулить исполнение step_worker(step, changelist)
    pass
    # return queue.enqueue()


# def run_steps_changelist(
#     ds: DataStore,
#     steps: List[ComputeStep],
#     changelist: ChangeList,
#     run_config: Optional[RunConfig] = None,
# ) -> None:
#     shedule_step(changelist)

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
    }
)

pipeline = Pipeline(
    [
        BatchGenerate(
            gen_some_data_A,
            outputs=["A"],
        ),
        BatchGenerate(
            gen_some_data_C,
            outputs=["C"],
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
    ]
)
ds = DataStore(dbconn)
app = DatapipeApp(ds, catalog, pipeline)

if __name__ == "__main__":
    print("1")
    steps = build_compute(ds, catalog, pipeline)
    print("2")
    run_steps(ds, steps)
    print("3")

    # change_list = ChangeList()
    # data_table = catalog.get_datatable(ds, "output")
    # change_list_data = data_table.store_chunk(
    #     pd.DataFrame({"id": [str(101)], "a": [str(random())]})
    # )
    # change_list.append(data_table.name, change_list_data)
    # run_changelist(
    #     ds=ds,
    #     catalog=catalog,
    #     pipeline=pipeline,
    #     changelist=change_list,
    # )

    # print(change_list)

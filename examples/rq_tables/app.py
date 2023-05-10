from typing import Iterator

from pathlib import Path

import pandas as pd
import random
from rq import Queue
from redis import Redis

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
    # run_changelist,
    # run_steps,
)
from datapipe.datatable import DataStore
from datapipe.store.database import DBConn, TableStoreDB

from datapipe_app import DatapipeApp

from datapipe.core_steps import do_full_batch_transform, BatchGenerate
import ray
import time
import uuid
from step_worker import step_worker


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


def stepFC(dfF: pd.DataFrame) -> pd.DataFrame:
    df = dfF.copy()
    df["col"] = df["col"] + "_FC"
    time.sleep(3)
    return df


def stepBCE(dfB: pd.DataFrame, dfC: pd.DataFrame) -> pd.DataFrame:
    df = dfB.copy()
    df["col"] = dfB["col"] + "___" + dfC["col"] + "_BCE"
    time.sleep(3)
    return df


redis_conn = Redis()
queue = Queue(connection=redis_conn)


def schedule_runtime(
    ds: DataStore,
    steps: List[ComputeStep],
    changelist: ChangeList,
    run_config: Optional[RunConfig] = None,
):
    if changelist:
        changed_tables = set(changelist.changes.keys())
        for step in steps:
            for input_dt in step.get_input_dts():
                if input_dt.name in changed_tables:
                    job = queue.enqueue(
                        step_worker, step, ds, steps, changelist, run_config
                    )
                    schedule_runtime(ds, steps, job.result, run_config)
                    # step_worker(step, ds, steps, changelist, run_config)


def run_changelist(
    ds: DataStore,
    catalog: Catalog,
    pipeline: Pipeline,
    changelist: ChangeList,
    run_config: Optional[RunConfig] = None,
) -> None:
    steps = build_compute(ds, catalog, pipeline)

    return run_steps_changelist(ds, steps, changelist, run_config)


def run_steps_changelist(
    ds: DataStore,
    steps: List[ComputeStep],
    changelist: ChangeList,
    run_config: Optional[RunConfig] = None,
    parallel_runtime: Optional[bool] = True,
) -> None:
    if parallel_runtime:
        schedule_runtime(ds, steps, changelist, run_config)

    else:
        current_changes = changelist
        next_changes = ChangeList()
        iteration = 0

        while not current_changes.empty() and iteration < 100:
            for step in steps:
                step_changes = step.run_changelist(ds, current_changes, run_config)
                next_changes.extend(step_changes)
            current_changes = next_changes
            next_changes = ChangeList()
            iteration += 1


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
    # print("Running build_compute...")
    # steps = build_compute(ds, catalog, pipeline)
    # print("Running run_steps...")
    # run_steps(ds, steps)

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
                "id": [str(new_id)],
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

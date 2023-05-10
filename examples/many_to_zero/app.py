from typing import Dict, List

import pandas as pd

from sqlalchemy import Integer
from sqlalchemy.sql.schema import Column

from datapipe_app import DatapipeApp

from datapipe.datatable import DataStore, DataTable
from datapipe.compute import Pipeline, Catalog, Table
from datapipe.core_steps import BatchGenerate, DatatableTransform
from datapipe.run_config import RunConfig
from datapipe.store.database import DBConn
from datapipe.store.pandas import TableStoreJsonLine


catalog = Catalog(
    {
        "input": Table(
            store=TableStoreJsonLine(
                filename="input.json",
                primary_schema=[
                    Column("id", Integer, primary_key=True),
                ],
            )
        ),
        "result": Table(
            store=TableStoreJsonLine(
                filename="result.json",
                primary_schema=[Column("result_id", Integer, primary_key=True)],
            )
        ),
    }
)


def generate_data():
    yield pd.DataFrame(
        {
            "id": range(10),
            "a": [f"a{i}" for i in range(10)],
        }
    )


def count(
    ds: DataStore,
    input_dts: List[DataTable],
    output_dts: List[DataTable],
    kwargs: Dict,
    run_config: RunConfig = None,
) -> None:
    assert len(input_dts) == 1
    assert len(output_dts) == 1

    input_dt = input_dts[0]
    output_dt = output_dts[0]

    output_dt.store_chunk(
        pd.DataFrame(
            {"result_id": [0], "count": [len(input_dt.meta_table.get_existing_idx())]}
        )
    )


pipeline = Pipeline(
    [
        BatchGenerate(
            generate_data,
            outputs=["input"],
        ),
        DatatableTransform(
            count,
            inputs=["input"],
            outputs=["result"],
            check_for_changes=False
        ),
    ]
)


ds = DataStore(DBConn("sqlite+pysqlite3:///metadata.sqlite"))

app = DatapipeApp(ds, catalog, pipeline)

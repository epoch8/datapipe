from typing import List

import pandas as pd

from sqlalchemy import Integer
from sqlalchemy.sql.schema import Column

from datapipe.datatable import DataStore, DataTable
from datapipe.compute import Pipeline, Catalog, Table
from datapipe.core_steps import BatchGenerate, DatatableTransform
from datapipe.store.database import DBConn
from datapipe.store.pandas import TableStoreJsonLine
from datapipe.cli import main


catalog = Catalog({
    'input': Table(
        store=TableStoreJsonLine(
            filename='input.json',
            primary_schema=[
                Column('id', Integer, primary_key=True),
            ],
        )
    ),
    'result': Table(
        store=TableStoreJsonLine(
            filename='result.json',
            primary_schema=[
                Column('result_id', Integer, primary_key=True)
            ],
        )
    )
})


def generate_data():
    yield pd.DataFrame({
        'id': range(10),
        'a': [f'a{i}' for i in range(10)],
    })


def count(ds: DataStore, input_dts: List[DataTable], output_dts: List[DataTable]) -> None:
    assert len(input_dts) == 1
    assert len(output_dts) == 1

    input_dt = input_dts[0]
    output_dt = output_dts[0]

    output_dt.store_chunk(
        pd.DataFrame({
            'result_id': [0],
            'count': [len(input_dt.meta_table.get_existing_idx())]
        })
    )


pipeline = Pipeline([
    BatchGenerate(
        generate_data,
        outputs=['input'],
    ),
    DatatableTransform(
        count,
        inputs=['input'],
        outputs=['result'],
    )
])


ds = DataStore(DBConn('sqlite:///metadata.sqlite'))

main(ds, catalog, pipeline)

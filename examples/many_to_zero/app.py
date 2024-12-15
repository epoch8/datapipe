import pandas as pd
from sqlalchemy import Integer
from sqlalchemy.sql.schema import Column

from datapipe.compute import Catalog, DatapipeApp, Pipeline, Table
from datapipe.datatable import DataStore
from datapipe.step.batch_generate import BatchGenerate
from datapipe.step.batch_transform import BatchTransform
from datapipe.store.database import DBConn
from datapipe.store.pandas import TableStoreJsonLine


def generate_data():
    yield pd.DataFrame(
        {
            "id": range(10),
            "a": [f"a{i}" for i in range(10)],
        }
    )


def count(
    input_df: pd.DataFrame,
) -> pd.DataFrame:
    return pd.DataFrame({"result_id": [0], "count": [len(input_df)]})


input_tbl = Table(
    name="input",
    store=TableStoreJsonLine(
        filename="input.json",
        primary_schema=[
            Column("id", Integer, primary_key=True),
        ],
    ),
)

result_tbl = Table(
    name="result",
    store=TableStoreJsonLine(
        filename="result.json",
        primary_schema=[Column("result_id", Integer, primary_key=True)],
    ),
)


pipeline = Pipeline(
    [
        BatchGenerate(
            generate_data,
            outputs=[input_tbl],
        ),
        BatchTransform(
            count,
            inputs=[input_tbl],
            outputs=[result_tbl],
            transform_keys=[],
        ),
    ]
)


ds = DataStore(DBConn("sqlite+pysqlite3:///db.sqlite"))

app = DatapipeApp(ds, Catalog({}), pipeline)


if __name__ == "__main__":
    from datapipe.compute import run_steps

    run_steps(ds, app.steps, None, None)

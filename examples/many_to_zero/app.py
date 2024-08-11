import pandas as pd
from sqlalchemy import Integer
from sqlalchemy.sql.schema import Column

from datapipe.compute import Catalog, DatapipeApp, Pipeline, Table
from datapipe.datatable import DataStore
from datapipe.step.batch_generate import BatchGenerate
from datapipe.step.batch_transform import BatchTransform
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
    input_df: pd.DataFrame,
) -> pd.DataFrame:
    return pd.DataFrame({"result_id": [0], "count": [len(input_df)]})


pipeline = Pipeline(
    [
        BatchGenerate(
            generate_data,
            outputs=["input"],
        ),
        BatchTransform(
            count,  # type: ignore
            inputs=["input"],
            outputs=["result"],
            transform_keys=[],
        ),
    ]
)


ds = DataStore(DBConn("sqlite+pysqlite3:///db.sqlite"))

app = DatapipeApp(ds, catalog, pipeline)


if __name__ == "__main__":
    from datapipe.compute import run_steps

    run_steps(ds, app.steps, None, None)

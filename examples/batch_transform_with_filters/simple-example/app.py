import pandas as pd
import sqlalchemy as sa

from datapipe.compute import Catalog, DatapipeApp, Pipeline, Table
from datapipe.datatable import DataStore
from datapipe.step.batch_transform import BatchTransform
from datapipe.step.update_external_table import UpdateExternalTable
from datapipe.store.database import DBConn
from datapipe.store.pandas import TableStoreJsonLine

dbconn = DBConn("sqlite+pysqlite3:///db.sqlite")
ds = DataStore(dbconn)


def apply_transformation(input_df: pd.DataFrame) -> pd.DataFrame:
    input_df["text"] = "Yay! I have been transformed."
    return input_df


input_tbl = Table(
    name="input",
    store=TableStoreJsonLine(
        filename="input.jsonline",
        primary_schema=[
            sa.Column("input_id", sa.Integer, primary_key=True),
        ],
    ),
)

output_tbl = Table(
    name="output",
    store=TableStoreJsonLine(
        filename="output.jsonline",
        primary_schema=[
            sa.Column("input_id", sa.Integer, primary_key=True),
        ],
    ),
)


pipeline = Pipeline(
    [
        UpdateExternalTable(
            output=input_tbl,
        ),
        BatchTransform(
            apply_transformation,
            inputs=[input_tbl],
            outputs=[output_tbl],
            filters=[
                {"input_id": 1},
                {"input_id": 3},
                {"input_id": 4},
                {"input_id": 6},
                {"input_id": 9}
            ]
        ),
    ]
)

app = DatapipeApp(ds, Catalog({}), pipeline)

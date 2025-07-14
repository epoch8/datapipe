import pandas as pd
import sqlalchemy as sa

from datapipe.compute import Catalog, DatapipeApp, Pipeline, Table
from datapipe.datatable import DataStore
from datapipe.run_config import RunConfig
from datapipe.step.batch_transform import BatchTransform
from datapipe.step.update_external_table import UpdateExternalTable
from datapipe.store.database import DBConn
from datapipe.store.pandas import TableStoreJsonLine

dbconn = DBConn("sqlite+pysqlite3:///db.sqlite")
ds = DataStore(dbconn)


def filter_cases(ds: DataStore, run_config: RunConfig):
    label = run_config.labels.get("stage", None)
    if label == "label1":
        return pd.DataFrame({"input_id": [1, 3, 4, 6, 9]})
    elif label == "label2":
        return pd.DataFrame({"input_id": [2, 6, 9]})
    else:
        return pd.DataFrame({"input_id": [6, 9]})


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
            labels=[("stage", "label1"), ("stage", "label2")],
            filters=filter_cases
        ),
    ]
)

app = DatapipeApp(ds, Catalog({}), pipeline)

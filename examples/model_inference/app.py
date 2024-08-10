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


def apply_model(input_df: pd.DataFrame, model_df: pd.DataFrame) -> pd.DataFrame:
    merge_df = input_df.merge(model_df, on="pipeline_id")

    res = []

    for func, group in merge_df.groupby("func"):
        assert isinstance(func, str)
        res.append(
            group.assign(
                text=group["text"].apply(getattr(str, func)),
            )
        )

    return pd.concat(res, ignore_index=True)[
        ["pipeline_id", "input_id", "model_id", "text"]
    ]


catalog = Catalog(
    {
        "input": Table(
            store=TableStoreJsonLine(
                filename="input.jsonline",
                primary_schema=[
                    sa.Column("pipeline_id", sa.String, primary_key=True),
                    sa.Column("input_id", sa.Integer, primary_key=True),
                ],
            )
        ),
        "models": Table(
            store=TableStoreJsonLine(
                filename="models.jsonline",
                primary_schema=[
                    sa.Column("pipeline_id", sa.String, primary_key=True),
                    sa.Column("model_id", sa.String, primary_key=True),
                ],
            )
        ),
        "output": Table(
            store=TableStoreJsonLine(
                filename="output.jsonline",
                primary_schema=[
                    sa.Column("pipeline_id", sa.String, primary_key=True),
                    sa.Column("input_id", sa.Integer, primary_key=True),
                    sa.Column("model_id", sa.String, primary_key=True),
                ],
            )
        ),
    }
)


pipeline = Pipeline(
    [
        UpdateExternalTable(
            output="input",
        ),
        UpdateExternalTable(
            output="models",
        ),
        BatchTransform(
            apply_model,
            inputs=["input", "models"],
            outputs=["output"],
            transform_keys=["pipeline_id", "input_id", "model_id"],
        ),
    ]
)

app = DatapipeApp(ds, catalog, pipeline)

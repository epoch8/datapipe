import pandas as pd

from datapipe.compute import Catalog, Pipeline, Table
from datapipe.step.batch_transform import BatchTransform
from datapipe.step.update_external_table import UpdateExternalTable

catalog = Catalog({"input_files": Table()})


def parse_and_upload(input_files_df: pd.DataFrame) -> None:
    pass


def process_in_dwh(partitions_df: pd.DataFrame) -> None:
    pass


pipeline = Pipeline(
    [
        UpdateExternalTable(output="input_files"),
        pau_transform := BatchTransform(
            parse_and_upload,
            inputs=["input_files"],
        ),
        BatchTransform(
            process_in_dwh,
            inputs=[pau_transform.meta],
        ),
    ]
)

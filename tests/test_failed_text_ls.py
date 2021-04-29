import json
import logging
import logging.config
import subprocess
from pathlib import Path

import pandas as pd
import threading
import time

from datapipe.compute import build_compute, run_steps
from datapipe.store.database import DBConn
from datapipe.metastore import MetaStore
from datapipe.dsl import Catalog, Table, TableStore, ExternalTable, Pipeline, BatchTransform, LabelStudioModeration
from datapipe.store.pandas import TableStoreJsonLine


def _convert_to_ls_input_data(input_df: pd.DataFrame):
    records = []
    for index, row in input_df.iterrows():
        records.append({
            "unique_id": index,
            "text": row["text"],
            "prediction": row["prediction"],
            "category": row["category"],
        })
    df = pd.DataFrame({"data": records}, index=input_df.index)
    return df


def _parse_annotation(input_texts_df: pd.DataFrame, annotation_df: pd.DataFrame):
    def _get_category(val):
        keys = [0, "result", 0, "value", "choices", 0]
        if not val:
            return ""
        for key in keys:
            if not val[key]:
                return ""
            val = val[key]
        return val

    if len(annotation_df) == 0:
        return input_texts_df
    input_texts_df["category"] = annotation_df["annotations"].apply(_get_category)
    return input_texts_df


def main(connection_string: str, schema: str, input_file: Path, ls_url: str):
    ms = MetaStore(dbconn=DBConn(connection_string, schema))
    input_fname = str(input_file)
    catalogue = Catalog({
        "input_texts": ExternalTable(
            store=TableStoreJsonLine(input_fname),
        ),
        "output_texts": ExternalTable(
            store=TableStoreJsonLine(input_fname.replace(".json", "-output.json")),
        )
    })
    pipeline = Pipeline([
        BatchTransform(
            _convert_to_ls_input_data,
            inputs=["input_texts"],
            outputs=["output_texts"],
        )
    ])
    steps = build_compute(ms, catalogue, pipeline)

    try:
        while True:
            run_steps(ms, steps)
    except KeyboardInterrupt:
        logging.info("Keyboard interrupt received, exiting")
    except Exception:
        raise


def cause_removing_failure():
    time.sleep(5)
    test_data_file = Path(__file__).parent.joinpath("data", "data.json")
    with open(test_data_file, "w") as target:
        target.write("{\"text\":\"3\\u043d\\u0434\\u0444\\u043b \\u0431\\u043b\\u0430\\u043d\\u043a\\u0438\",\"prediction\":\"\\u0414\\u0440\\u0443\\u0433\\u043e\\u0435\",\"id\":\"0\",\"category\":\"\"}")
    time.sleep(5)


if __name__ == "__main__":
    test_data_file = Path(__file__).parent.joinpath("data", "data.json")
    with open(test_data_file, "r") as src:
        test_data = src.read()
    threading.Thread(target=cause_removing_failure).start()
    try:
        main(
            connection_string="sqlite:///./text_classification.db",
            schema=None,
            input_file=Path(__file__).parent.joinpath("data", "data.json"),
            ls_url="http://localhost:8080",
        )
    finally:
        with open(test_data_file, "w") as target:
            target.write(test_data)
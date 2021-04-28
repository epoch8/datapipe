import json
import logging
import logging.config
import subprocess
from pathlib import Path

import pandas as pd

from datapipe.compute import build_compute, run_steps
from datapipe.store.database import DBConn
from datapipe.metastore import MetaStore
from datapipe.dsl import Catalog, Table, TableStore, ExternalTable, Pipeline, BatchTransform, LabelStudioModeration
from datapipe.store.pandas import TableStoreJsonLine


LS_LABEL_CONFIG_XML = """
    <View>
        <Text name="intro" value="Верна ли слдедующая категория для этого текста? Если нет - укажите новую" />
        <Text name="predictedCategory" value="$prediction" />
        <Text name="text" value="$text" />
        <Choices name="category" toName="text" choice="single-radio">
            <Choice value="Другое" />
            <Choice value="Пособия" />
            <Choice value="Есиа" />
            <Choice value="Выплата 3-7 лет" />
            <Choice value="Запись" />
            <Choice value="Вакцинация" />
            <Choice value="Справка" />
            <Choice value="Сертификат" />
            <Choice value="Пенсионный" />
            <Choice value="Ребенок" />
            <Choice value="Оплата" />
            <Choice value="Налог" />
            <Choice value="Голосование (ПОС)" />
            <Choice value="Выписка" />
            <Choice value="Маткап" />
            <Choice value="Решаем вместе (жалоба)" />
            <Choice value="Паспорт" />
            <Choice value="Электронный документ" />
            <Choice value="Возврат" />
            <Choice value="Загран-паспорт" />
        </Choices>
    </View>
"""
LS_PROJECT_CONFIG = {
    "title": "Text classification project",
    "description": "Text classification project",
    "label_config": LS_LABEL_CONFIG_XML,
    "expert_instruction": "",
    "show_instruction": False,
    "show_skip_button": True,
    "enable_empty_annotation": True,
    "show_annotation_history": False,
    "organization": 1,
    "color": "#FFFFFF",
    "maximum_annotations": 1,
    "is_published": False,
    "model_version": "",
    "is_draft": False,
    "min_annotations_to_start_training": 10,
    "show_collab_predictions": True,
    "sampling": "Sequential sampling",
    "show_ground_truth_first": True,
    "show_overlap_first": True,
    "overlap_cohort_percentage": 100,
    "task_data_login": None,
    "task_data_password": None,
    "control_weights": {}
}


def _initialize_logging():
    with open(Path(__file__).parent.parent.joinpath("logger.json"), "r") as src:
        logging.config.dictConfig(json.load(src))
    logging.root.setLevel("INFO")


def _run_labelstudio(data_dir: Path, host: str, port: int):
    data_dir.mkdir(exist_ok=True)
    return subprocess.Popen([
        "label-studio",
        "--database", str(data_dir.joinpath("ls.db")),
        "--internal-host", host,
        "--port", str(port),
        "--no-browser"
    ])


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
    #_initialize_logging()
    ms = MetaStore(dbconn=DBConn(connection_string, schema))
    input_fname = str(input_file)
    catalogue = Catalog({
        "input_texts": ExternalTable(
            store=TableStoreJsonLine(input_fname),
        ),
        "LS_data_raw": Table(
            store=TableStoreJsonLine(input_fname.replace(".json", "_ls_data_raw.json")),
        ),
        "annotation_raw": Table(
            store=TableStoreJsonLine(input_fname.replace(".json", "_annotation_raw.json"))
        ),
        "annotation_parsed": Table(
            store=TableStoreJsonLine(input_fname.replace(".json", "_annotation_parsed.json"))
        ),
    })
    pipeline = Pipeline([
        BatchTransform(
            _convert_to_ls_input_data,
            inputs=["input_texts"],
            outputs=["LS_data_raw"]
        ),
        LabelStudioModeration(
            ls_url=ls_url,
            project_setting=LS_PROJECT_CONFIG,
            inputs=["LS_data_raw"],
            outputs=["annotation_raw"],
        ),
        BatchTransform(
            _parse_annotation,
            inputs=["input_texts", "annotation_raw"],
            outputs=["annotation_parsed"],
        ),
    ])
    steps = build_compute(ms, catalogue, pipeline)

    try:
        while True:
            run_steps(ms, steps)
    except KeyboardInterrupt:
        logging.info("Keyboard interrupt received, exiting")
    except Exception:
        raise


if __name__ == "__main__":
    label_studio = _run_labelstudio(
        data_dir=Path(__file__).parent.joinpath("data"),
        host="localhost",
        port=8080,
    )
    try:
        main(
            connection_string="sqlite:///./text_classification.db",
            schema=None,
            input_file=Path(__file__).parent.joinpath("data", "data.json"),
            ls_url="http://localhost:8080",
        )
    finally:
        label_studio.terminate()

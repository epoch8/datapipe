import time
from subprocess import Popen
from pathlib import Path

import pandas as pd

from datapipe.compute import build_compute, run_steps
from datapipe.metastore import MetaStore
from datapipe.dsl import Catalog, Table, ExternalTable, Pipeline, BatchTransform, LabelStudioModeration
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
HOST = 'localhost'
LS_PORT = '8080'
DATA_DIR = Path('data/').absolute()


def convert_to_ls_input_data(input_df: pd.DataFrame):
    input_df['data'] = input_df.index.map(
        lambda id: {
            'unique_id': id,
            "text": str(input_df.loc[id, "text"]),
            "prediction": str(input_df.loc[id, "prediction"]),
            "category": str(input_df.loc[id, "category"]),
        }
    )
    return input_df[['data']]


def parse_annotation(input_texts_df: pd.DataFrame, annotation_df: pd.DataFrame):
    def get_category(val):
        keys = [0, "result", 0, "value", "choices", 0]
        if not val:
            return ""
        for key in keys:
            if not val[key]:
                return ""
            val = val[key]
        return val

    if len(annotation_df) > 0:
        input_texts_df["category"] = annotation_df["annotations"].apply(get_category)

    return input_texts_df


(DATA_DIR / 'xx_datatables').mkdir(exist_ok=True, parents=True)
ms = MetaStore('sqlite:///' + str(DATA_DIR / 'xx_datatables' / 'metadata.sqlite'))
catalogue = Catalog({
    "input_texts": ExternalTable(
        store=TableStoreJsonLine(DATA_DIR / '00_data.json'),
    ),
    "LS_data_raw": Table(
        store=TableStoreJsonLine(DATA_DIR / "01_ls_data_raw.json"),
    ),
    "annotation_raw": Table(
        store=TableStoreJsonLine(DATA_DIR / "02_annotation_raw.json"),
    ),
    "annotation_parsed": Table(
        store=TableStoreJsonLine(DATA_DIR / "03_annotation_parsed.json")
    ),
})
pipeline = Pipeline([
    BatchTransform(
        convert_to_ls_input_data,
        inputs=["input_texts"],
        outputs=["LS_data_raw"]
    ),
    LabelStudioModeration(
        ls_url=f'http://{HOST}:{LS_PORT}/',
        project_setting=LS_PROJECT_CONFIG,
        inputs=["LS_data_raw"],
        outputs=["annotation_raw"],
    ),
    BatchTransform(
        parse_annotation,
        inputs=["input_texts", "annotation_raw"],
        outputs=["annotation_parsed"],
    ),
])
steps = build_compute(ms, catalogue, pipeline)


if __name__ == "__main__":
    label_studio_service = Popen([
        'label-studio',
        '--database', str(DATA_DIR / 'xx_datatables' / 'ls.db'),
        '--internal-host', '0.0.0.0',
        '--port', LS_PORT,
        '--no-browser'
    ])
    try:
        while True:
            run_steps(ms, steps)
            time.sleep(5)
    finally:
        label_studio_service.terminate()

from datapipe.store.database import DBConn
import time
from subprocess import Popen
from pathlib import Path

import pandas as pd

from datapipe.compute import build_compute, run_steps
from datapipe.datatable import DataStore
from datapipe.compute import Catalog, Table, ExternalTable, Pipeline, BatchTransform, LabelStudioModeration
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
HOST = 'localhost'
LS_PORT = '8080'
DATA_DIR = Path('data/').absolute()


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
ds = DataStore(DBConn('sqlite:///' + str(DATA_DIR / 'xx_datatables' / 'metadata.sqlite')))
catalogue = Catalog({
    "00_input_texts": ExternalTable(
        store=TableStoreJsonLine(DATA_DIR / '00_data.json'),
    ),
    "01_annotation_raw": Table(
        store=TableStoreJsonLine(DATA_DIR / "01_annotation_raw.json"),
    ),
    "02_annotation_parsed": Table(
        store=TableStoreJsonLine(DATA_DIR / "02_annotation_parsed.json")
    ),
})
pipeline = Pipeline([
    LabelStudioModeration(
        ls_url=f'http://{HOST}:{LS_PORT}/',
        inputs=["00_input_texts"],
        outputs=["01_annotation_raw"],
        auth=('moderation@epoch8.co', 'qwerty123'),
        project_title='Text classification project',
        project_description='Text classification project!',
        project_label_config=LS_LABEL_CONFIG_XML,
        data=['text', 'prediction', 'category'],
        chunk_size=1000
    ),
    BatchTransform(
        parse_annotation,
        inputs=["00_input_texts", "01_annotation_raw"],
        outputs=["02_annotation_parsed"],
    ),
])
steps = build_compute(ds, catalogue, pipeline)


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
            run_steps(ds, steps)
            time.sleep(5)
    finally:
        label_studio_service.terminate()

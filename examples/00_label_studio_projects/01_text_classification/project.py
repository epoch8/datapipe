from datapipe.store.database import DBConn
import time
from pathlib import Path

import pandas as pd
from sqlalchemy import Column, String, JSON

from datapipe.compute import build_compute, run_steps
from datapipe.datatable import DataStore
from datapipe.compute import Catalog, Table, Pipeline
from datapipe.store.pandas import TableStoreJsonLine
from datapipe.core_steps import UpdateExternalTable, BatchTransform
from datapipe.label_studio.pipeline import LabelStudioStep


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
    "00_input_texts": Table(
        store=TableStoreJsonLine(DATA_DIR / '00_data.json'),
    ),
    "02_annotation_parsed": Table(
        store=TableStoreJsonLine(DATA_DIR / "02_annotation_parsed.json")
    ),
})
pipeline = Pipeline([
    UpdateExternalTable('00_input_texts'),
    LabelStudioStep(
        ls_url=f'http://{HOST}:{LS_PORT}/',
        input="00_input_texts",
        output="01_annotation_ls",
        auth=('moderation@epoch8.co', 'qwerty123'),
        project_identifier='Text classification project',
        project_description_at_create='Text classification project!',
        project_label_config_at_create=LS_LABEL_CONFIG_XML,
        data_sql_schema=[
            Column('id', String(), primary_key=True),
            Column('text', String()),
            Column('prediction', JSON()),
            Column('category', String()),
        ],
        page_chunk_size=100,
    ),
    BatchTransform(
        parse_annotation,
        inputs=["00_input_texts", "01_annotation_raw"],
        outputs=["02_annotation_parsed"],
    ),
])


steps = build_compute(ds, catalogue, pipeline)

while True:
    run_steps(ds, steps)
    time.sleep(5)

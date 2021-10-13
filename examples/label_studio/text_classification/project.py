import shutil
from datapipe.label_studio.store import TableStoreLabelStudio
from datapipe.store.database import DBConn
from subprocess import Popen
from pathlib import Path

import pandas as pd

from datapipe.datatable import DataStore
from datapipe.dsl import Catalog, ExternalTable, Table, Pipeline, BatchTransform, UpdateMetaTable
from datapipe.store.pandas import TableStoreJsonLine
from sqlalchemy.sql.schema import Column
from sqlalchemy.sql.sqltypes import String
from datapipe.cli import main


PROJECT_LABEL_CONFIG = """
    <View>
        <Text name="intro" value="Верна ли следующая категория для этого текста? Если нет - укажите новую" />
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
DATA_DIR = (Path(__file__).parent / 'data/').absolute()


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


for folder in ['02_annotation_raw', '03_texts_result', 'xx_datatables']:
    shutil.rmtree(DATA_DIR / folder, ignore_errors=True)
(DATA_DIR / 'xx_datatables').mkdir(exist_ok=True, parents=True)

ds = DataStore(DBConn('sqlite:///' + str(DATA_DIR / 'xx_datatables' / 'metadata.sqlite')))
catalog = Catalog({
    "00_input_texts": ExternalTable(
        store=TableStoreJsonLine(
            DATA_DIR / '00_data' / '00_data.json',
            primary_schema=[
                Column('id', String(), primary_key=True),
                Column('text', String()),
                Column('prediction', String()),
                Column('category', String())
            ],
        )
    ),
    '01_label_studio': Table(
        TableStoreLabelStudio(
            ls_url=f'http://{HOST}:{LS_PORT}/',
            auth=('moderation@epoch8.co', 'qwerty123'),
            project_title='Text Classification Project',
            project_label_config=PROJECT_LABEL_CONFIG,
            data_sql_schema=[
                Column('id', String(), primary_key=True),
                Column('text', String()),
                Column('prediction', String()),
            ],
            annotations_column='annotations',
            page_chunk_size=1000
        )
    ),
    "02_annotation_raw": Table(
        store=TableStoreJsonLine(
            DATA_DIR / "02_annotation_raw" / "02_annotation_raw.json",
            primary_schema=[
                Column('id', String(), primary_key=True),
            ],
        ),
    ),
    "03_texts_result": Table(
        store=TableStoreJsonLine(
            DATA_DIR / "03_texts_result" / "03_texts_result.json",
            primary_schema=[
                Column('id', String(), primary_key=True),
            ]
        ),
    ),
})


pipeline = Pipeline([
    BatchTransform(
        func=lambda df: df[['id', 'text', 'prediction', 'category']],
        inputs=['00_input_texts'],
        outputs=['01_label_studio'],
        chunk_size=1000
    ),
    UpdateMetaTable(
        outputs=['01_label_studio']
    ),
    BatchTransform(
        func=lambda df: df[['id', 'annotations']],
        inputs=["01_label_studio"],
        outputs=["02_annotation_raw"],
        chunk_size=5000
    ),
    BatchTransform(
        parse_annotation,
        inputs=["00_input_texts", "02_annotation_raw"],
        outputs=["03_texts_result"],
    ),
])

# Run "python project.py run-periodic 5"
if __name__ == "__main__":
    label_studio_service = Popen([
        'label-studio',
        '--database', str(DATA_DIR / 'xx_datatables' / 'ls.db'),
        '--internal-host', '0.0.0.0',
        '--port', LS_PORT,
        '--no-browser'
    ])
    try:
        main(ds, catalog, pipeline)
    finally:
        label_studio_service.terminate()

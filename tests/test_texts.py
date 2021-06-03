import numpy as np
import pandas as pd
from datapipe.texts import get_embedder_conversion, get_classifier_conversion

from datapipe.compute import build_compute, run_steps
from datapipe.metastore import MetaStore
from datapipe.dsl import Catalog, ExternalTable, Pipeline, BatchTransform, Table
from datapipe.store.pandas import TableStoreJsonLine

from .util import dbconn, tmp_dir



def make_file(file):
    with open(file, 'w') as out:
        out.write('{"id": "0", "text": "пастила Хрустящие кус без сах 35г Белев"}\n')
        out.write('{"id": "1", "text": "Скраб д/тела ECOLAB солевой Лифтинг"}\n')
        out.write('{"id": "2", "text": "БАРС ошейник инсектоакорицид. д/собак мелк.пород"}\n')
        out.write('{"id": "3", "text": "Тарелки глубокие для супа"}\n')
        out.write('{"id": "4", "text": "Ноутбук Lenovo"}\n')
        out.write('{"id": "5", "text": "Компьютерная мышь logitech"}\n')


def test_table_store_json_line_with_deleting(dbconn, tmp_dir):
    input_file = tmp_dir / "data.json"
    intermediate_file = tmp_dir / "intermediate.json"
    output_file = tmp_dir / "data_transformed.json"

    ms = MetaStore(dbconn)
    catalog = Catalog({
        "input_data": ExternalTable(
            store=TableStoreJsonLine(input_file),
        ),
        "with_embeddings": Table(
            store=TableStoreJsonLine(intermediate_file),
        ),
        "transfomed_data": Table(
            store=TableStoreJsonLine(output_file),
        )
    })
    pipeline = Pipeline([
        BatchTransform(
            get_embedder_conversion('http://c12n-common-embedder-v6.research.svc.cluster.local/v1/models/c12n-common-embedder-v6:predict',
                                    10,
                                    30),
            inputs=["input_data"],
            outputs=["with_embeddings"]
        ),
        BatchTransform(
            get_classifier_conversion('http://c12n-common-embedder-v6-ozon-search-space.research.svc.cluster.local/v1/models/c12n-common-embedder-v6-ozon-search-space:predict',
                                      10,
                                      30),
            inputs=["with_embeddings"],
            outputs=["transfomed_data"]
        ),
    ])

    # Create data, pipeline it
    make_file(input_file)

    steps = build_compute(ms, catalog, pipeline)
    run_steps(ms, steps)
    
    df_transformed = catalog.get_datatable(ms, 'transfomed_data').get_data()

    assert len(catalog.get_datatable(ms, 'input_data').get_data()) == 6
    assert len(df_transformed) == 6
    assert all(
        pd.read_csv("tmp-table-classification.csv")["classification"].apply(eval).apply(lambda data: data['category_name']).values == \
        [
            'Продукты питания->Хлеб и кондитерские изделия->Пастила',
            'Красота и здоровье->Косметика для ухода->Скраб для тела',
            'Зоотовары->Ветеринарная аптека->Капли ветеринарные нелицензированные',
            'Дом->Столовая посуда->Тарелка', 'Электроника->Компьютер->Ноутбук',
            'Электроника->Устройство ручного ввода->Мышь'
        ]
    )

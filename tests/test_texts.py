import json
import os
import shutil
import tempfile
import numpy as np
import pandas as pd
import requests_mock
from datapipe.texts import get_embedder_conversion, get_classifier_conversion
from datapipe.compute import build_compute, run_steps
from datapipe.metastore import MetaStore
from datapipe.dsl import Catalog, ExternalTable, Pipeline, BatchTransform, Table
from datapipe.store.pandas import TableStoreJsonLine
from .util import dbconn


def make_file(file):
    with open(file, 'w', encoding="utf-8") as out:
        out.write('{"id": "0", "text": "пастила Хрустящие кус без сах 35г Белев"}\n')
        out.write('{"id": "1", "text": "Скраб д/тела ECOLAB солевой Лифтинг"}\n')
        out.write('{"id": "2", "text": "БАРС ошейник инсектоакорицид. д/собак мелк.пород"}\n')
        out.write('{"id": "3", "text": "Тарелки глубокие для супа"}\n')
        out.write('{"id": "4", "text": "Ноутбук Lenovo"}\n')
        out.write('{"id": "5", "text": "Компьютерная мышь logitech"}\n')
        

def parse_embedder_result_mock(fname):
    with open(fname) as src:
        data = json.load(src)
    indices = sorted(data["text"].keys())
    #texts = [data["text"][i] for i in indices]
    embeddings = [data["embedding"][i] for i in indices]
    result = {
        "predictions": embeddings
    }
    return json.dumps(result).encode()
        

def parse_classifier_result_mock(fname):
    with open(fname) as src:
        data = json.load(src)
    indices = sorted(data["text"].keys())
    #texts = [data["text"][i] for i in indices]
    predictions = [data["classification"][i] for i in indices]
    result = {
        "predictions": predictions
    }
    return json.dumps(result).encode()


def test_table_classification_pipepline(dbconn):
    with requests_mock.Mocker() as m:
        data_directory = os.path.join(os.path.dirname(__file__), "data", "texts")
        embeddings_file = os.path.join(data_directory, "embeddings.json")
        classification_file = os.path.join(data_directory, "transformed.json")
        m.post('http://c12n-common-embedder-v6.research.svc.cluster.local/v1/models/c12n-common-embedder-v6:predict',
               content=parse_embedder_result_mock(embeddings_file))
        m.post('http://c12n-common-embedder-v6-ozon-search-space.research.svc.cluster.local/v1/models/c12n-common-embedder-v6-ozon-search-space:predict',
               content=parse_classifier_result_mock(classification_file))

        tmp_dir = tempfile.mkdtemp()

        input_file = os.path.join(tmp_dir, "data.json")
        intermediate_file = os.path.join(tmp_dir, "intermediate.json")
        output_file = os.path.join(tmp_dir, "data_transformed.json")

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
                get_embedder_conversion(
                    'http://c12n-common-embedder-v6.research.svc.cluster.local/v1/models/c12n-common-embedder-v6:predict',
                    10,
                    30),
                inputs=["input_data"],
                outputs=["with_embeddings"]
            ),
            BatchTransform(
                get_classifier_conversion(
                    'http://c12n-common-embedder-v6-ozon-search-space.research.svc.cluster.local/v1/models/c12n-common-embedder-v6-ozon-search-space:predict',
                    10,
                    30),
                inputs=["with_embeddings"],
                outputs=["transfomed_data"]
            ),
        ])

        make_file(input_file)

        steps = build_compute(ms, catalog, pipeline)
        run_steps(ms, steps)

        df_transformed = catalog.get_datatable(ms, 'transfomed_data').get_data()
        
        assert len(catalog.get_datatable(ms, 'input_data').get_data()) == 6
        assert len(df_transformed) == 6
        assert all(
            df_transformed["classification"].apply(lambda data: data['category_name']).values == \
            [
                'Продукты питания->Хлеб и кондитерские изделия->Пастила',
                'Красота и здоровье->Косметика для ухода->Скраб для тела',
                'Зоотовары->Ветеринарная аптека->Капли ветеринарные нелицензированные',
                'Дом->Столовая посуда->Тарелка', 'Электроника->Компьютер->Ноутбук',
                'Электроника->Устройство ручного ввода->Мышь'
            ]
        )

        shutil.rmtree(tmp_dir)

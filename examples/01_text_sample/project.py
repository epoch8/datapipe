import time
from subprocess import Popen
from pathlib import Path

import pandas as pd

from datapipe.compute import build_compute, run_steps
from datapipe.metastore import MetaStore
from datapipe.dsl import Catalog, Table, ExternalTable, Pipeline, BatchTransform, LabelStudioModeration
from datapipe.store.pandas import TableStoreJsonLine
from datapipe.texts import get_embedder_conversion, get_classifier_conversion


DATA_DIR = Path('data/').absolute()


if __name__ == "__main__":
    (DATA_DIR / 'xx_datatables').mkdir(exist_ok=True, parents=True)
    
    input_file = "data.json"
    intermediate_file = "intermediate.json"
    output_file = "data_transformed.json"

    ms = MetaStore('sqlite:///' + str(DATA_DIR / 'xx_datatables' / 'metadata.sqlite'))
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
    
    steps = build_compute(ms, catalog, pipeline)
    while True:
        run_steps(ms, steps)
        time.sleep(5)

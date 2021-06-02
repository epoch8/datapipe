from typing import Text
from datapipe.store.pandas import TableStoreJsonLine
from pathlib import Path

import pandas as pd

from datapipe.metastore import MetaStore
from datapipe.store.filedir import TableStoreFiledir, PILFile, TextFile
from datapipe.dsl import Catalog, ExternalTable, Table, Pipeline, BatchTransform
from datapipe.cli import main


CATALOG_DIR = Path('examples/PIPE-3-one-to-many/data/')


catalog = Catalog({
    'input_text_files': ExternalTable(
        metadata=['filename'],
        store=TableStoreFiledir(CATALOG_DIR / '00_input/{filename}.txt', TextFile()),
    ),
    'input_lines': Table(
        # id - неявное поле
        metadata=['filename', 'line_no'],
        store=TableStoreJsonLine(CATALOG_DIR / '01_intermediate/input_lines.jsonline'),
    ),
    'text_by_line_no': Table(
        metadata = ['line_no'],
        store=TableStoreFiledir(CATALOG_DIR / '02_output/{line_no}.txt', TextFile()),
    )
})


def split_lines(df: pd.DataFrame) -> pd.DataFrame:
    res = []
    for row in df.iterrows():
        lines = row['text'].split('\n')

        res.append(pd.DataFrame({
            'filename': row['filename'],
            'line_no': range(len(lines)),
            'text': lines
        }))
    
    return pd.concat(res)


def group_lines(df: pd.DataFrame) -> pd.DataFrame:
    return df.groupby('line_no').agg(text=lambda x: '\n'.join(x)).reset_index()


pipeline = Pipeline([
    BatchTransform(
        split_lines,
        # group_by=[<все поля метадаты>] - дефолт
        inputs=['input_text_files'],
        outputs=['input_lines'],
        chunk_size=100
    ),
    BatchTransform(
        group_lines,
        group_by=['line_no'],
        inputs=['input_lines'],
        outputs=['text_by_line_no'],
    )
])


ms = MetaStore('sqlite:///./test_data/metadata.sqlite')


if __name__ == '__main__':
    main(ms, catalog, pipeline)

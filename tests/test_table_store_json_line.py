import pandas as pd

from datapipe.compute import build_compute, run_steps
from datapipe.metastore import MetaStore
from datapipe.dsl import Catalog, ExternalTable, Pipeline, BatchTransform, Table
from datapipe.store.pandas import TableStoreJsonLine


def test_table_store_json_line_reading(tmp_dir):
    test_df = pd.DataFrame({
        # NOTE если записать сюда числа, даже строками, 
        # Pandas преобразует их в RangeIndex с dtype=int при чтении из JSON
        "id": ["id1", "id2", "id3"],
        "record": ["rec1", "rec2", "rec3"]
    })
    test_fname = tmp_dir / "table-pandas.json"
    test_df.to_json(test_fname, orient="records", lines=True)

    store = TableStoreJsonLine(
        filename=test_fname,
        index_columns=['id']
    )
    df = store.load_file()
    assert all(df.reset_index(drop=False)["id"].values == test_df["id"].values)
    assert all(df["record"].values == test_df["record"].values)


def make_file1(file):
    with open(file, 'w') as out:
        out.write('{"id": "0", "text": "text0"}\n')
        out.write('{"id": "1", "text": "text1"}\n')
        out.write('{"id": "2", "text": "text2"}\n')


def make_file2(file):
    with open(file, 'w') as out:
        out.write('{"id": "0", "text": "text0"}\n')
        out.write('{"id": "2", "text": "text2"}\n')


def test_table_store_json_line_with_deleting(dbconn, tmp_dir):
    input_file = tmp_dir / "data.json"

    ms = MetaStore(dbconn)
    catalog = Catalog({
        "input_data": ExternalTable(
            store=TableStoreJsonLine(tmp_dir / "data.json"),
        ),
        "transfomed_data": Table(
            store=TableStoreJsonLine(tmp_dir / "data_transformed.json"),
        )
    })
    pipeline = Pipeline([
        BatchTransform(
            lambda df: df,
            inputs=["input_data"],
            outputs=["transfomed_data"]
        )
    ])

    # Create data, pipeline it
    make_file1(input_file)

    steps = build_compute(ms, catalog, pipeline)
    run_steps(ms, steps)

    assert len(catalog.get_datatable(ms, 'input_data').get_data()) == 3
    assert len(catalog.get_datatable(ms, 'transfomed_data').get_data()) == 3

    # Remove {"id": "0"} from file, pipeline it
    make_file2(input_file)
    run_steps(ms, steps)

    # TODO: uncomment follow when we make files deletion
    # assert len(list(tmp_dir.glob('tbl2/*.png'))) == 2
    assert len(catalog.get_datatable(ms, 'input_data').get_data()) == 2
    assert len(catalog.get_datatable(ms, 'transfomed_data').get_data()) == 2

import pandas as pd
import pytest

from datapipe.compute import Catalog, Pipeline, Table, build_compute, run_steps
from datapipe.datatable import DataStore
from datapipe.step.batch_transform import BatchTransform
from datapipe.step.update_external_table import UpdateExternalTable
from datapipe.store.pandas import TableStoreExcel, TableStoreJsonLine
from datapipe.store.tests.abstract import AbstractBaseStoreTests


def test_table_store_json_line_reading(tmp_dir):
    test_df = pd.DataFrame({"id": ["0", "1", "2"], "record": ["rec1", "rec2", "rec3"]})
    test_fname = tmp_dir / "table-pandas.json"
    test_df.to_json(test_fname, orient="records", lines=True)

    store = TableStoreJsonLine(filename=test_fname)
    df = store.load_file()
    assert all(df.reset_index(drop=False)["id"].values == test_df["id"].values)
    assert all(df["record"].values == test_df["record"].values)


def make_file1(file):
    with open(file, "w") as out:
        out.write('{"id": "0", "text": "text0"}\n')
        out.write('{"id": "1", "text": "text1"}\n')
        out.write('{"id": "2", "text": "text2"}\n')


def make_file2(file):
    with open(file, "w") as out:
        out.write('{"id": "0", "text": "text0"}\n')
        out.write('{"id": "2", "text": "text2"}\n')


def test_table_store_json_line_with_deleting(dbconn, tmp_dir):
    input_file = tmp_dir / "data.json"

    ds = DataStore(dbconn, create_meta_table=True)
    catalog = Catalog(
        {
            "input_data": Table(
                store=TableStoreJsonLine(tmp_dir / "data.json"),
            ),
            "transfomed_data": Table(
                store=TableStoreJsonLine(tmp_dir / "data_transformed.json"),
            ),
        }
    )
    pipeline = Pipeline(
        [
            UpdateExternalTable("input_data"),
            BatchTransform(lambda df: df, inputs=["input_data"], outputs=["transfomed_data"]),
        ]
    )

    # Create data, pipeline it
    make_file1(input_file)

    steps = build_compute(ds, catalog, pipeline)
    run_steps(ds, steps)

    assert len(catalog.get_datatable(ds, "input_data").get_data()) == 3
    assert len(catalog.get_datatable(ds, "transfomed_data").get_data()) == 3

    # Remove {"id": "0"} from file, pipeline it
    make_file2(input_file)
    run_steps(ds, steps)

    # TODO: uncomment follow when we make files deletion
    # assert len(list(tmp_dir.glob('tbl2/*.png'))) == 2
    assert len(catalog.get_datatable(ds, "input_data").get_data()) == 2
    assert len(catalog.get_datatable(ds, "transfomed_data").get_data()) == 2


class TestTableStoreJsonLine(AbstractBaseStoreTests):
    @pytest.fixture
    def store_maker(self, tmp_dir):
        def make_db_store(data_schema):
            return TableStoreJsonLine(tmp_dir / "data.json", primary_schema=data_schema)

        return make_db_store


class TestTableStoreExcel(AbstractBaseStoreTests):
    @pytest.fixture
    def store_maker(self, tmp_dir):
        def make_db_store(data_schema):
            return TableStoreExcel(tmp_dir / "data.xlsx", primary_schema=data_schema)

        return make_db_store

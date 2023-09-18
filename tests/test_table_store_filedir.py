import json

import fsspec
import numpy as np
import pandas as pd
import pytest
from PIL import Image

from datapipe.store.filedir import JSONFile, PILFile, TableStoreFiledir

from .util import assert_df_equal, assert_ts_contains

TEST_DF = pd.DataFrame(
    {
        "id": ["aaa", "bbb"],
        "a": [1, 2],
        "b": [10, 20],
    }
)

TEST_JSONS = {
    "aaa": {"a": 1, "b": 10},
    "bbb": {"a": 2, "b": 20},
}


@pytest.fixture
def tmp_dir_with_json_data(tmp_dir):
    for fn, data in TEST_JSONS.items():
        with fsspec.open(f"{tmp_dir}/{fn}.json", "w+") as f:
            json.dump(data, f)
    yield tmp_dir


def get_test_df_filepath(test_df, tmp_dir_):
    test_df_filepath = test_df.copy()
    test_df_filepath["filepath"] = test_df_filepath["id"].map(
        lambda idx: f"{tmp_dir_}/{idx}.json"
    )
    return test_df_filepath


def test_read_json_rows(tmp_dir_with_json_data):
    ts = TableStoreFiledir(f"{tmp_dir_with_json_data}/{{id}}.json", adapter=JSONFile())

    assert_ts_contains(ts, TEST_DF)

    ts_with_filepath = TableStoreFiledir(
        f"{tmp_dir_with_json_data}/{{id}}.json",
        adapter=JSONFile(),
        add_filepath_column=True,
    )
    assert_ts_contains(
        ts_with_filepath, get_test_df_filepath(TEST_DF, tmp_dir_with_json_data)
    )


def test_read_json_rows_file_fs(tmp_dir_with_json_data):
    ts = TableStoreFiledir(
        f"file://{tmp_dir_with_json_data}/{{id}}.json", adapter=JSONFile()
    )

    assert_ts_contains(ts, TEST_DF)

    ts_with_filepath = TableStoreFiledir(
        f"file://{tmp_dir_with_json_data}/{{id}}.json",
        adapter=JSONFile(),
        add_filepath_column=True,
    )
    assert_ts_contains(
        ts_with_filepath,
        get_test_df_filepath(TEST_DF, f"file://{tmp_dir_with_json_data}"),
    )


def test_read_json_rows_gcs_fs():
    ts = TableStoreFiledir("gs://datapipe-test/{id}.json", adapter=JSONFile())

    assert_ts_contains(ts, TEST_DF)

    ts_with_filepath = TableStoreFiledir(
        "gs://datapipe-test/{id}.json", adapter=JSONFile(), add_filepath_column=True
    )
    assert_ts_contains(
        ts_with_filepath, get_test_df_filepath(TEST_DF, "gs://datapipe-test")
    )


def test_insert_json_rows(tmp_dir_with_json_data):
    ts = TableStoreFiledir(f"{tmp_dir_with_json_data}/{{id}}.json", adapter=JSONFile())

    ts.insert_rows(pd.DataFrame({"id": ["ccc"], "a": [3], "b": [30]}))

    with open(f"{tmp_dir_with_json_data}/ccc.json") as f:
        assert json.load(f) == {"a": 3, "b": 30}


@pytest.fixture
def tmp_dir_with_img_data(tmp_dir):
    with fsspec.open(f"{tmp_dir}/aaa.png", "wb+") as f:
        im = Image.fromarray(np.zeros((100, 100, 3), dtype="u8"), "RGB")
        im.save(f, format="png")

    yield tmp_dir


def test_read_png_rows(tmp_dir_with_img_data):
    ts = TableStoreFiledir(
        f"{tmp_dir_with_img_data}/{{id}}.png", adapter=PILFile("png")
    )

    rows = ts.read_rows(pd.DataFrame({"id": ["aaa"]}))

    assert_df_equal(rows[["id"]], pd.DataFrame({"id": ["aaa"]}))

    assert "image" in rows.columns


def test_read_rows_without_data(tmp_dir_with_img_data):
    ts = TableStoreFiledir(
        f"{tmp_dir_with_img_data}/{{id}}.png", adapter=PILFile("png"), read_data=False
    )

    df = pd.DataFrame({"id": ["aaa"]})
    rows = ts.read_rows(df)

    assert_df_equal(rows, df)

    assert "image" not in rows.columns


def test_insert_png_rows(tmp_dir_with_img_data):
    ts = TableStoreFiledir(
        f"{tmp_dir_with_img_data}/{{id}}.png", adapter=PILFile("png")
    )

    ts.insert_rows(
        pd.DataFrame(
            {
                "id": ["bbb"],
                "image": [Image.fromarray(np.zeros((100, 100, 3), "u8"), "RGB")],
            }
        )
    )

    assert fsspec.open(f"{tmp_dir_with_img_data}/bbb.png")


@pytest.fixture
def tmp_several_dirs_with_json_data(tmp_dir):
    for i in range(3):
        for j in range(3):
            with fsspec.open(
                f"{tmp_dir}/folder{i}/folder{j}/{i}{j}.json", "w", auto_mkdir=True
            ) as out:
                out.write(f'{{"a": {i}, "b": {j}}}')
        with fsspec.open(f"{tmp_dir}/folder{i}/{i}.json", "w", auto_mkdir=True) as out:
            out.write(f'{{"a": {i}, "b": -1}}')
    yield tmp_dir


TEST_DF_FOLDER0 = pd.DataFrame(
    {
        "id": ["00", "01", "02"],
        "a": [0, 0, 0],
        "b": [0, 1, 2],
    }
)


def test_read_json_rows_folders(tmp_several_dirs_with_json_data):
    ts = TableStoreFiledir(
        f"{tmp_several_dirs_with_json_data}/folder0/*/{{id}}.json", adapter=JSONFile()
    )
    assert_ts_contains(ts, TEST_DF_FOLDER0)

    ts_with_filepath = TableStoreFiledir(
        f"{tmp_several_dirs_with_json_data}/folder0/*/{{id}}.json",
        adapter=JSONFile(),
        add_filepath_column=True,
    )
    TEST_DF_FOLDER0_WITH_FILEPATH = TEST_DF_FOLDER0.copy()
    TEST_DF_FOLDER0_WITH_FILEPATH["filepath"] = TEST_DF_FOLDER0_WITH_FILEPATH["id"].map(
        lambda idx: (
            f"{tmp_several_dirs_with_json_data}/folder0/folder{idx[1]}/{idx}.json"
        )
    )
    assert_ts_contains(ts_with_filepath, TEST_DF_FOLDER0_WITH_FILEPATH)


TEST_DF_FOLDER_RECURSIVELY = pd.DataFrame(
    {
        "id": ["0", "00", "01", "02", "1", "10", "11", "12", "2", "20", "21", "22"],
        "a": [0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2],
        "b": [-1, 0, 1, 2, -1, 0, 1, 2, -1, 0, 1, 2],
    }
)


def test_read_json_rows_recursively(tmp_several_dirs_with_json_data):
    ts = TableStoreFiledir(
        f"{tmp_several_dirs_with_json_data}/**/{{id}}.json", adapter=JSONFile()
    )
    assert_ts_contains(ts, TEST_DF_FOLDER_RECURSIVELY)

    ts_with_filepath = TableStoreFiledir(
        f"{tmp_several_dirs_with_json_data}/**/{{id}}.json",
        adapter=JSONFile(),
        add_filepath_column=True,
    )
    TEST_DF_FOLDER_RECURSIVELY_WITH_FILEPATH = TEST_DF_FOLDER_RECURSIVELY.copy()
    TEST_DF_FOLDER_RECURSIVELY_WITH_FILEPATH[
        "filepath"
    ] = TEST_DF_FOLDER_RECURSIVELY_WITH_FILEPATH["id"].map(
        lambda idx: (
            f"{tmp_several_dirs_with_json_data}/folder{idx}/{idx}.json"
            if idx in ["0", "1", "2"]
            else f"{tmp_several_dirs_with_json_data}/folder{idx[0]}/folder{idx[1]}/{idx}.json"
        )
    )
    assert_ts_contains(ts_with_filepath, TEST_DF_FOLDER_RECURSIVELY_WITH_FILEPATH)


TEST_DF_SEVERAL_EXTENSIONS = pd.DataFrame(
    {
        "id": ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"],
        "a": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        "b": [-1, -1, -1, -1, -1, -1, -1, -1, -1, -1],
    }
)


@pytest.fixture
def tmp_several_extensions_with_json_data(tmp_dir):
    for i in range(10):
        ext = "json" if i < 5 else "txt"
        with fsspec.open(f"{tmp_dir}/folder0/folder1/{i}.{ext}", "w") as out:
            out.write(f'{{"a": {i}, "b": -1}}')
    yield tmp_dir


def test_read_json_rows_or_extensions(tmp_several_extensions_with_json_data):
    ts = TableStoreFiledir(
        f"{tmp_several_extensions_with_json_data}/folder0/folder1/{{id}}.(json|txt)",
        adapter=JSONFile(),
    )
    assert_ts_contains(ts, TEST_DF_SEVERAL_EXTENSIONS)

    ts2 = TableStoreFiledir(
        f"{tmp_several_extensions_with_json_data}/folder0/*/{{id}}.(json|txt)",
        adapter=JSONFile(),
    )
    assert_ts_contains(ts2, TEST_DF_SEVERAL_EXTENSIONS)

    ts3 = TableStoreFiledir(
        f"{tmp_several_extensions_with_json_data}/(folder0|folder1|folder2)/*/{{id}}.(json|txt)",
        adapter=JSONFile(),
    )
    assert_ts_contains(ts3, TEST_DF_SEVERAL_EXTENSIONS)

    ts4 = TableStoreFiledir(
        f"{tmp_several_extensions_with_json_data}/**/folder1/{{id}}.(json|txt)",
        adapter=JSONFile(),
    )
    assert_ts_contains(ts4, TEST_DF_SEVERAL_EXTENSIONS)

    ts5 = TableStoreFiledir(
        f"{tmp_several_extensions_with_json_data}/**/(folder0|folder1)/{{id}}.(json|txt)",
        adapter=JSONFile(),
    )
    assert_ts_contains(ts5, TEST_DF_SEVERAL_EXTENSIONS)


@pytest.fixture
def tmp_several_extensions_and_folders_with_json_data(tmp_dir):
    for i in range(10):
        ext = "json" if i < 5 else "txt"
        with fsspec.open(f"{tmp_dir}/folder{i%3}/{i}.{ext}", "w") as out:
            out.write(f'{{"a": {i}, "b": -1}}')
    yield tmp_dir


def test_read_json_rows_or_folders(tmp_several_extensions_and_folders_with_json_data):
    ts = TableStoreFiledir(
        f"{tmp_several_extensions_and_folders_with_json_data}/(folder0|folder1|folder2)/{{id}}.(json|txt)",
        adapter=JSONFile(),
    )
    assert_ts_contains(ts, TEST_DF_SEVERAL_EXTENSIONS)

import io
import importlib.util
import os
from typing import Any
from uuid import uuid4

import numpy as np
import pandas as pd
import pytest
from cv_pipeliner.core.data import BboxData, ImageData
from datapipe.store.filedir import TableStoreFiledir
from datapipe.tests.util import assert_df_equal
from PIL import Image
from sqlalchemy import Column, String

from datapipe_ml.utils.image_data_stores import (
    ConnectedImageDataTableStore,
    FiftyOneImagesDataTableStore,
    GetImageSizeFile,
    ImageDataFile,
    ImageDataTableStoreDB,
    NumpyDataFile,
    YOLOLabelsFile,
)


def _make_image_data() -> ImageData:
    return ImageData(
        bboxes_data=[
            BboxData(
                xmin=1,
                ymin=2,
                xmax=10,
                ymax=20,
                label="cat",
            )
        ]
    )


def _make_image_data_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"image_id": "image_1", "image_data": _make_image_data()},
            {
                "image_id": "image_2",
                "image_data": ImageData(
                    bboxes_data=[
                        BboxData(
                            xmin=3,
                            ymin=4,
                            xmax=30,
                            ymax=40,
                            label="dog",
                        )
                    ]
                ),
            },
        ]
    )


def _assert_image_data_labels(df: pd.DataFrame, expected: dict[str, str]) -> None:
    actual = {
        row["image_id"]: row["image_data"].bboxes_data[0].label
        for _, row in df.iterrows()
    }
    assert actual == expected


class _FakeSample:
    def __init__(self, filepath: str):
        self.filepath = filepath
        self._fields: dict[str, Any] = {}

    def __getitem__(self, key: str) -> Any:
        return self._fields[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self._fields[key] = value

    def __delitem__(self, key: str) -> None:
        del self._fields[key]

    def has_field(self, key: str | None) -> bool:
        return key is not None and key in self._fields

    def merge(self, other: "_FakeSample") -> None:
        self.filepath = other.filepath
        self._fields = other._fields.copy()


class _FakeDataset:
    persistent = False

    def __init__(self, name: str):
        self.name = name
        self.samples: list[_FakeSample] = []

    def add_samples(self, samples: list[_FakeSample], progress: bool = False) -> None:
        self.samples.extend(samples)

    def delete_samples(self, samples: list[_FakeSample]) -> None:
        sample_ids = {id(sample) for sample in samples}
        self.samples = [sample for sample in self.samples if id(sample) not in sample_ids]


class _FakeDatasetView:
    def __init__(self, dataset: _FakeDataset):
        self.samples = list(dataset.samples)

    def select_by(self, field: str, values: pd.Series) -> "_FakeDatasetView":
        selected_values = set(values.tolist())
        self.samples = [
            sample
            for sample in self.samples
            if (sample.filepath if field == "filepath" else sample[field]) in selected_values
        ]
        return self

    def __iter__(self):
        return iter(self.samples)


class _FakeFiftyOne:
    def __init__(self):
        self.datasets: dict[str, _FakeDataset] = {}

    def list_datasets(self) -> list[str]:
        return list(self.datasets)

    def load_dataset(self, name: str) -> _FakeDataset:
        return self.datasets[name]

    def Dataset(self, name: str) -> _FakeDataset:
        dataset = _FakeDataset(name)
        self.datasets[name] = dataset
        return dataset

    def DatasetView(self, dataset: _FakeDataset) -> _FakeDatasetView:
        return _FakeDatasetView(dataset)


class _FakeFiftyOneSession:
    def __init__(self):
        self.fiftyone = _FakeFiftyOne()

    def convert_image_data_to_fo_sample(
        self,
        image_data: ImageData,
        additional_info: dict[str, Any] | None = None,
        **kwargs,
    ) -> _FakeSample:
        sample = _FakeSample(str(image_data.image_path))
        sample["image_data"] = image_data
        for key, value in (additional_info or {}).items():
            sample[key] = value
        return sample

    def convert_sample_to_image_data(self, sample: _FakeSample, **kwargs) -> ImageData:
        return sample["image_data"]


def test_image_data_file_roundtrip():
    adapter = ImageDataFile()
    image_data = _make_image_data()
    out = io.StringIO()

    adapter.dump({"image_data": image_data}, out)
    loaded = adapter.load(io.StringIO(out.getvalue()))["image_data"]

    assert isinstance(loaded, ImageData)
    assert loaded.bboxes_data[0].coords == (1, 2, 10, 20)
    assert loaded.bboxes_data[0].label == "cat"


def test_image_data_file_table_store_filedir_ops(tmp_dir):
    store = TableStoreFiledir(
        filename_pattern=str(tmp_dir / "annotations" / "{image_id}.json"),
        adapter=ImageDataFile(),
        enable_rm=True,
    )
    df = _make_image_data_df()

    store.insert_rows(df)
    _assert_image_data_labels(store.read_rows(), {"image_1": "cat", "image_2": "dog"})

    updated_df = pd.DataFrame(
        [
            {
                "image_id": "image_1",
                "image_data": ImageData(
                    bboxes_data=[
                        BboxData(
                            xmin=5,
                            ymin=6,
                            xmax=50,
                            ymax=60,
                            label="tiger",
                        )
                    ]
                ),
            }
        ]
    )
    store.insert_rows(updated_df)
    _assert_image_data_labels(store.read_rows(pd.DataFrame([{"image_id": "image_1"}])), {"image_1": "tiger"})

    pseudo_df = next(store.read_rows_meta_pseudo_df())
    assert set(pseudo_df["image_id"]) == {"image_1", "image_2"}

    store.delete_rows(pd.DataFrame([{"image_id": "image_2"}]))
    _assert_image_data_labels(store.read_rows(), {"image_1": "tiger"})


def test_numpy_data_file_roundtrip():
    adapter = NumpyDataFile()
    ndarray = np.arange(6).reshape(2, 3)
    out = io.BytesIO()

    adapter.dump({"ndarray": ndarray}, out)
    out.seek(0)
    loaded = adapter.load(out)["ndarray"]

    np.testing.assert_array_equal(loaded, ndarray)


def test_numpy_data_file_table_store_filedir_ops(tmp_dir):
    store = TableStoreFiledir(
        filename_pattern=str(tmp_dir / "arrays" / "{array_id}.npy"),
        adapter=NumpyDataFile(),
        enable_rm=True,
    )
    df = pd.DataFrame(
        [
            {"array_id": "array_1", "ndarray": np.arange(6).reshape(2, 3)},
            {"array_id": "array_2", "ndarray": np.arange(4).reshape(2, 2)},
        ]
    )

    store.insert_rows(df)
    read_df = store.read_rows(pd.DataFrame([{"array_id": "array_1"}]))
    np.testing.assert_array_equal(read_df.loc[0, "ndarray"], df.loc[0, "ndarray"])

    updated_df = pd.DataFrame([{"array_id": "array_1", "ndarray": np.ones((2, 3), dtype=np.int64)}])
    store.insert_rows(updated_df)
    read_updated_df = store.read_rows(pd.DataFrame([{"array_id": "array_1"}]))
    np.testing.assert_array_equal(read_updated_df.loc[0, "ndarray"], updated_df.loc[0, "ndarray"])

    pseudo_df = next(store.read_rows_meta_pseudo_df())
    assert set(pseudo_df["array_id"]) == {"array_1", "array_2"}

    store.delete_rows(pd.DataFrame([{"array_id": "array_2"}]))
    assert store.read_rows()["array_id"].tolist() == ["array_1"]


def test_get_image_size_file_table_store_reads_image_sizes(tmp_dir):
    image_path = tmp_dir / "images" / "image_1.jpg"
    image_path.parent.mkdir(parents=True)
    Image.new("RGB", (32, 16), color="white").save(image_path)
    store = TableStoreFiledir(
        filename_pattern=str(tmp_dir / "images" / "{image_id}.jpg"),
        adapter=GetImageSizeFile(),
    )

    read_df = store.read_rows(pd.DataFrame([{"image_id": "image_1"}]))

    assert read_df.loc[0, "image_id"] == "image_1"
    assert read_df.loc[0, "image_size"] == (32, 16)


def test_yolo_labels_file_table_store_roundtrip(tmp_dir):
    image_path = tmp_dir / "images" / "image_1.jpg"
    image_path.parent.mkdir(parents=True)
    Image.new("RGB", (100, 50), color="white").save(image_path)
    store = TableStoreFiledir(
        filename_pattern=str(tmp_dir / "labels" / "{image_id}.txt"),
        adapter=YOLOLabelsFile(class_names=["cat", "dog"], img_format="jpg"),
        enable_rm=True,
    )
    image_data = ImageData(
        image_path=str(image_path),
        bboxes_data=[
            BboxData(
                xmin=10,
                ymin=5,
                xmax=30,
                ymax=25,
                label="cat",
            )
        ],
    )

    store.insert_rows(pd.DataFrame([{"image_id": "image_1", "image_data": image_data}]))
    read_df = store.read_rows(pd.DataFrame([{"image_id": "image_1"}]))
    read_image_data = read_df.loc[0, "image_data"]

    assert read_image_data.bboxes_data[0].label == "cat"
    assert read_image_data.bboxes_data[0].coords == (10, 5, 30, 25)

    store.delete_rows(pd.DataFrame([{"image_id": "image_1"}]))
    assert store.read_rows().empty


def test_image_data_table_store_db_table_store_ops(dbconn):
    store = ImageDataTableStoreDB(
        dbconn=dbconn,
        name="image_data",
        data_sql_schema=[Column("image_id", String, primary_key=True)],
        create_table=True,
    )
    df = _make_image_data_df()

    store.insert_rows(df)
    _assert_image_data_labels(store.read_rows(pd.DataFrame([{"image_id": "image_1"}])), {"image_1": "cat"})
    _assert_image_data_labels(store.read_rows(), {"image_1": "cat", "image_2": "dog"})

    updated_df = pd.DataFrame([{"image_id": "image_1", "image_data": ImageData(label="tiger")}])
    store.update_rows(updated_df)
    read_updated_df = store.read_rows(pd.DataFrame([{"image_id": "image_1"}]))
    assert read_updated_df.loc[0, "image_data"].label == "tiger"

    store.delete_rows(pd.DataFrame([{"image_id": "image_2"}]))
    assert store.read_rows()["image_id"].tolist() == ["image_1"]

    store.insert_rows(pd.DataFrame())
    store.update_rows(pd.DataFrame())
    assert store.read_rows()["image_id"].tolist() == ["image_1"]


def _make_connected_image_data_table_store(tmp_dir):
    for image_id in ["image_1", "image_2"]:
        image_path = tmp_dir / "images" / f"{image_id}.jpg"
        image_path.parent.mkdir(parents=True, exist_ok=True)
        Image.new("RGB", (32, 16), color="white").save(image_path)

    images_store = TableStoreFiledir(
        filename_pattern=str(tmp_dir / "images" / "{image_id}.jpg"),
        adapter=ImageDataFile(),
        read_data=False,
    )
    image_data_store = TableStoreFiledir(
        filename_pattern=str(tmp_dir / "annotations" / "{image_id}.json"),
        adapter=ImageDataFile(),
        enable_rm=True,
    )
    return ConnectedImageDataTableStore(
        images_table_store=images_store,
        images_data_table_store=image_data_store,
    )


def test_connected_image_data_table_store_sets_image_paths(tmp_dir):
    store = _make_connected_image_data_table_store(tmp_dir)

    store.insert_rows(pd.DataFrame([{"image_id": "image_1", "image_data": _make_image_data()}]))
    read_df = store.read_rows(pd.DataFrame([{"image_id": "image_1"}]))
    read_image_data = read_df.loc[0, "image_data"]

    image_path = tmp_dir / "images" / "image_1.jpg"
    assert str(read_image_data.image_path) == str(image_path.resolve())
    assert str(read_image_data.bboxes_data[0].image_path) == str(image_path.resolve())


def test_connected_image_data_table_store_table_store_ops(tmp_dir):
    store = _make_connected_image_data_table_store(tmp_dir)
    df = _make_image_data_df()

    store.insert_rows(df)
    _assert_image_data_labels(store.read_rows(pd.DataFrame([{"image_id": "image_1"}])), {"image_1": "cat"})
    _assert_image_data_labels(store.read_rows(), {"image_1": "cat", "image_2": "dog"})

    updated_df = pd.DataFrame([{"image_id": "image_1", "image_data": ImageData(label="tiger")}])
    store.update_rows(updated_df)
    read_updated_df = store.read_rows(pd.DataFrame([{"image_id": "image_1"}]))
    assert read_updated_df.loc[0, "image_data"].label == "tiger"
    assert str(read_updated_df.loc[0, "image_data"].image_path) == str((tmp_dir / "images" / "image_1.jpg").resolve())

    pseudo_df = next(store.read_rows_meta_pseudo_df())
    assert set(pseudo_df["image_id"]) == {"image_1", "image_2"}

    store.delete_rows(pd.DataFrame([{"image_id": "image_2"}]))
    assert store.read_rows()["image_id"].tolist() == ["image_1"]

    store.insert_rows(pd.DataFrame())
    store.update_rows(pd.DataFrame())
    assert store.read_rows()["image_id"].tolist() == ["image_1"]


def test_fiftyone_images_data_table_store_filepath_primary_key_table_store_ops():
    store = FiftyOneImagesDataTableStore(
        dataset="test_dataset",
        fo_session=_FakeFiftyOneSession(),
        rm_only_fo_fields=False,
    )
    df = pd.DataFrame(
        [
            {"filepath": "/tmp/image_1.jpg", "image_data": ImageData(image_path="/tmp/image_1.jpg", label="cat")},
            {"filepath": "/tmp/image_2.jpg", "image_data": ImageData(image_path="/tmp/image_2.jpg", label="dog")},
        ]
    )

    store.insert_rows(df)

    read_df = store.read_rows(pd.DataFrame([{"filepath": "/tmp/image_1.jpg"}]))
    assert_df_equal(read_df[["filepath"]], df.loc[[0], ["filepath"]], index_cols=store.primary_keys)
    assert read_df.loc[0, "image_data"].label == "cat"

    assert_df_equal(store.read_rows()[["filepath"]], df[["filepath"]], index_cols=store.primary_keys)

    updated_df = pd.DataFrame(
        [{"filepath": "/tmp/image_1.jpg", "image_data": ImageData(image_path="/tmp/image_1.jpg", label="tiger")}]
    )
    store.update_rows(updated_df)
    read_updated_df = store.read_rows(pd.DataFrame([{"filepath": "/tmp/image_1.jpg"}]))
    assert read_updated_df.loc[0, "image_data"].label == "tiger"

    store.delete_rows(pd.DataFrame([{"filepath": "/tmp/image_2.jpg"}]))
    assert_df_equal(store.read_rows()[["filepath"]], updated_df[["filepath"]], index_cols=store.primary_keys)


def test_fiftyone_images_data_table_store_uses_images_store_path(tmp_dir):
    image_path = tmp_dir / "images" / "image_1.jpg"
    image_path.parent.mkdir(parents=True)
    Image.new("RGB", (32, 16), color="white").save(image_path)
    images_store = TableStoreFiledir(
        filename_pattern=str(tmp_dir / "images" / "{image_id}.jpg"),
        adapter=ImageDataFile(),
        read_data=False,
    )
    store = FiftyOneImagesDataTableStore(
        dataset="test_dataset",
        fo_session=_FakeFiftyOneSession(),
        primary_schema=[Column("image_id", String, primary_key=True)],
        images_table_store=images_store,
        rm_only_fo_fields=False,
    )

    store.insert_rows(pd.DataFrame([{"image_id": "image_1", "image_data": _make_image_data()}]))
    read_df = store.read_rows(pd.DataFrame([{"image_id": "image_1"}]))
    read_image_data = read_df.loc[0, "image_data"]

    assert read_df.loc[0, "image_id"] == "image_1"
    assert str(read_image_data.image_path) == str(image_path.resolve())
    assert str(read_image_data.bboxes_data[0].image_path) == str(image_path.resolve())


def test_fiftyone_images_data_table_store_read_empty_idx():
    store = FiftyOneImagesDataTableStore(
        dataset="test_dataset",
        fo_session=_FakeFiftyOneSession(),
        rm_only_fo_fields=False,
    )

    empty_df = store.read_rows(pd.DataFrame(columns=store.primary_keys))

    assert empty_df.empty
    assert all(column in empty_df.columns for column in store.primary_keys)


def test_fiftyone_images_data_table_store_raises_when_dataset_missing_and_create_disabled():
    store = FiftyOneImagesDataTableStore(
        dataset="missing_dataset",
        fo_session=_FakeFiftyOneSession(),
        create_dataset_if_empty=False,
        rm_only_fo_fields=False,
    )

    with pytest.raises(ValueError, match="Dataset missing_dataset not found"):
        store.read_rows(pd.DataFrame([{"filepath": "/tmp/image.jpg"}]))


@pytest.mark.fiftyone
def test_fiftyone_images_data_table_store_real_fiftyone_table_store_ops(tmp_dir):
    if importlib.util.find_spec("fiftyone") is None:
        pytest.skip("fiftyone is not installed")

    from cv_pipeliner.utils.fiftyone import FifyOneSession

    image_1_path = tmp_dir / "image_1.jpg"
    image_2_path = tmp_dir / "image_2.jpg"
    Image.new("RGB", (32, 16), color="white").save(image_1_path)
    Image.new("RGB", (64, 32), color="white").save(image_2_path)

    dataset_name = f"datapipe_ml_test_{uuid4().hex}"
    database_name = f"datapipe_ml_test_{uuid4().hex}"
    database_uri = os.environ.get("FIFTYONE_DATABASE_URI")

    with FifyOneSession(database_uri=database_uri, database_name=database_name) as fo_session:
        store = FiftyOneImagesDataTableStore(
            dataset=dataset_name,
            fo_session=fo_session,
            fo_detections_label="detections",
            fo_classification_label="classification",
            rm_only_fo_fields=False,
        )
        df = pd.DataFrame(
            [
                {
                    "filepath": str(image_1_path.resolve()),
                    "image_data": ImageData(
                        image_path=str(image_1_path.resolve()),
                        label="cat",
                        bboxes_data=[BboxData(xmin=1, ymin=2, xmax=10, ymax=20, label="cat")],
                    ),
                },
                {
                    "filepath": str(image_2_path.resolve()),
                    "image_data": ImageData(
                        image_path=str(image_2_path.resolve()),
                        label="dog",
                        bboxes_data=[BboxData(xmin=2, ymin=3, xmax=20, ymax=25, label="dog")],
                    ),
                },
            ]
        )

        try:
            store.insert_rows(df)

            read_df = store.read_rows(pd.DataFrame([{"filepath": str(image_1_path.resolve())}]))
            assert_df_equal(read_df[["filepath"]], df.loc[[0], ["filepath"]], index_cols=store.primary_keys)
            assert read_df.loc[0, "image_data"].label == "cat"
            assert read_df.loc[0, "image_data"].bboxes_data[0].label == "cat"
            assert read_df.loc[0, "image_data"].bboxes_data[0].coords == (1, 2, 10, 20)

            updated_df = pd.DataFrame(
                [
                    {
                        "filepath": str(image_1_path.resolve()),
                        "image_data": ImageData(
                            image_path=str(image_1_path.resolve()),
                            label="tiger",
                            bboxes_data=[BboxData(xmin=3, ymin=4, xmax=12, ymax=22, label="tiger")],
                        ),
                    }
                ]
            )
            store.update_rows(updated_df)
            read_updated_df = store.read_rows(pd.DataFrame([{"filepath": str(image_1_path.resolve())}]))
            assert read_updated_df.loc[0, "image_data"].label == "tiger"
            assert read_updated_df.loc[0, "image_data"].bboxes_data[0].label == "tiger"
            assert read_updated_df.loc[0, "image_data"].bboxes_data[0].coords == (3, 4, 12, 22)

            store.delete_rows(pd.DataFrame([{"filepath": str(image_2_path.resolve())}]))
            assert_df_equal(store.read_rows()[["filepath"]], updated_df[["filepath"]], index_cols=store.primary_keys)
        finally:
            if dataset_name in fo_session.fiftyone.list_datasets():
                fo_session.fiftyone.delete_dataset(dataset_name)

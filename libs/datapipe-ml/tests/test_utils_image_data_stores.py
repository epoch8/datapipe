import io

import numpy as np
import pandas as pd
from cv_pipeliner.core.data import BboxData, ImageData
from datapipe.store.filedir import TableStoreFiledir
from PIL import Image
from sqlalchemy import Column, String

from datapipe_ml.utils.image_data_stores import (
    ConnectedImageDataTableStore,
    ImageDataFile,
    ImageDataTableStoreDB,
    NumpyDataFile,
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


def test_image_data_file_roundtrip():
    adapter = ImageDataFile()
    image_data = _make_image_data()
    out = io.StringIO()

    adapter.dump({"image_data": image_data}, out)
    loaded = adapter.load(io.StringIO(out.getvalue()))["image_data"]

    assert isinstance(loaded, ImageData)
    assert loaded.bboxes_data[0].coords == (1, 2, 10, 20)
    assert loaded.bboxes_data[0].label == "cat"


def test_numpy_data_file_roundtrip():
    adapter = NumpyDataFile()
    ndarray = np.arange(6).reshape(2, 3)
    out = io.BytesIO()

    adapter.dump({"ndarray": ndarray}, out)
    out.seek(0)
    loaded = adapter.load(out)["ndarray"]

    np.testing.assert_array_equal(loaded, ndarray)


def test_image_data_table_store_db_roundtrip(dbconn):
    store = ImageDataTableStoreDB(
        dbconn=dbconn,
        name="image_data",
        data_sql_schema=[Column("image_id", String, primary_key=True)],
        create_table=True,
    )
    image_data = _make_image_data()

    store.insert_rows(pd.DataFrame([{"image_id": "image_1", "image_data": image_data}]))
    read_df = store.read_rows(pd.DataFrame([{"image_id": "image_1"}]))

    assert len(read_df) == 1
    assert isinstance(read_df.loc[0, "image_data"], ImageData)
    assert read_df.loc[0, "image_data"].bboxes_data[0].label == "cat"
    assert image_data.bboxes_data[0].label == "cat"


def test_connected_image_data_table_store_sets_image_paths(tmp_dir):
    image_path = tmp_dir / "images" / "image_1.jpg"
    image_path.parent.mkdir(parents=True)
    Image.new("RGB", (32, 16), color="white").save(image_path)

    images_store = TableStoreFiledir(
        filename_pattern=str(tmp_dir / "images" / "{image_id}.jpg"),
        adapter=ImageDataFile(),
        read_data=False,
    )
    image_data_store = TableStoreFiledir(
        filename_pattern=str(tmp_dir / "annotations" / "{image_id}.json"),
        adapter=ImageDataFile(),
    )
    store = ConnectedImageDataTableStore(
        images_table_store=images_store,
        images_data_table_store=image_data_store,
    )

    store.insert_rows(pd.DataFrame([{"image_id": "image_1", "image_data": _make_image_data()}]))
    read_df = store.read_rows(pd.DataFrame([{"image_id": "image_1"}]))
    read_image_data = read_df.loc[0, "image_data"]

    assert str(read_image_data.image_path) == str(image_path.resolve())
    assert str(read_image_data.bboxes_data[0].image_path) == str(image_path.resolve())

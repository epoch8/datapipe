import tempfile
from pathlib import Path

import pandas as pd
import pytest
from datapipe.compute import Catalog, DataStore, Pipeline, Table
from datapipe.store.database import DBConn, TableStoreDB
from datapipe.store.filedir import JSONFile, PILFile, TableStoreFiledir
from datapipe.store.table_store import TableStore, TableStoreCaps
from datapipe.types import DataDF, DataSchema, IndexDF, MetaSchema
from fastapi.testclient import TestClient
from PIL import Image
from sqlalchemy import Column, Integer, String

from datapipe_app import DatapipeAPI
from datapipe_app.app import models
from datapipe_app.pipeline.table_data import get_table_data, get_table_store_schema


class _OpaqueStore(TableStore):
    """Store without a dedicated browse path — should fall back to meta PKs."""

    caps = TableStoreCaps(
        supports_delete=True,
        supports_get_schema=False,
        supports_read_all_rows=False,
        supports_read_nonexistent_rows=False,
        supports_read_meta_pseudo_df=False,
    )

    def __init__(self) -> None:
        self._rows: dict[int, dict] = {}

    def get_primary_schema(self) -> DataSchema:
        return [Column("id", Integer(), primary_key=True)]

    def get_meta_schema(self) -> MetaSchema:
        return []

    def insert_rows(self, df: DataDF) -> None:
        for row in df.to_dict(orient="records"):
            self._rows[int(row["id"])] = row

    def delete_rows(self, idx: IndexDF) -> None:
        for row_id in idx["id"].tolist():
            self._rows.pop(int(row_id), None)

    def read_rows(self, idx: IndexDF | None = None) -> DataDF:
        raise AssertionError("opaque store browse must not call read_rows")


@pytest.fixture
def filedir_app():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        dbconn = DBConn(f"sqlite+pysqlite3:///{tmp}/store.sqlite")

        json_dir = tmp / "json"
        json_dir.mkdir()
        img_dir = tmp / "img"
        img_dir.mkdir()

        catalog = Catalog(
            {
                "docs": Table(
                    store=TableStoreFiledir(
                        str(json_dir / "{id}.json"),
                        adapter=JSONFile(),
                        add_filepath_column=True,
                    )
                ),
                "images": Table(
                    store=TableStoreFiledir(
                        str(img_dir / "{id}.png"),
                        adapter=PILFile("png"),
                        add_filepath_column=True,
                    )
                ),
                "opaque": Table(store=_OpaqueStore()),
                "db_rows": Table(
                    store=TableStoreDB(
                        name="db_rows",
                        dbconn=dbconn,
                        data_sql_schema=[
                            Column("id", Integer(), primary_key=True),
                            Column("v", String()),
                        ],
                        create_table=True,
                    )
                ),
            }
        )
        ds = DataStore(dbconn, create_meta_table=True)
        docs = catalog.get_datatable(ds, "docs")
        images = catalog.get_datatable(ds, "images")
        opaque = catalog.get_datatable(ds, "opaque")
        db_rows = catalog.get_datatable(ds, "db_rows")

        docs.store_chunk(
            pd.DataFrame(
                [
                    {"id": "a", "name": "alpha", "n": 1},
                    {"id": "b", "name": "beta", "n": 2},
                ]
            )
        )
        img = Image.new("RGB", (4, 4), color=(255, 0, 0))
        images.store_chunk(pd.DataFrame([{"id": "i1", "image": img}]))
        opaque.store_chunk(pd.DataFrame([{"id": 7}, {"id": 8}]))
        db_rows.store_chunk(pd.DataFrame([{"id": 1, "v": "x"}]))

        api = DatapipeAPI(ds, catalog, Pipeline([]), pipeline_id="table_data_test")
        yield api, docs, images, opaque, db_rows


def test_get_table_data_filedir_json_includes_payload(filedir_app):
    api, *_ = filedir_app
    resp = get_table_data(
        api.ds,
        api.catalog,
        models.GetDataRequest(table="docs", page=0, page_size=10, include_total=True),
    )
    assert resp.total == 2
    by_id = {row["id"]: row for row in resp.data}
    assert by_id["a"]["name"] == "alpha"
    assert by_id["a"]["n"] == 1
    assert "filepath" in by_id["a"]


def test_get_table_data_filedir_pil_skips_image_blob(filedir_app):
    api, *_ = filedir_app
    resp = get_table_data(
        api.ds,
        api.catalog,
        models.GetDataRequest(table="images", page=0, page_size=10, include_total=True),
    )
    assert resp.total == 1
    row = resp.data[0]
    assert row["id"] == "i1"
    assert "filepath" in row
    assert "image" not in row


def test_get_table_data_opaque_falls_back_to_meta_pk(filedir_app):
    api, *_ = filedir_app
    resp = get_table_data(
        api.ds,
        api.catalog,
        models.GetDataRequest(table="opaque", page=0, page_size=10, include_total=True),
    )
    assert resp.total == 2
    assert sorted(row["id"] for row in resp.data) == [7, 8]
    assert all(set(row.keys()) == {"id"} for row in resp.data)


def test_get_table_data_db_unchanged(filedir_app):
    api, *_ = filedir_app
    resp = get_table_data(
        api.ds,
        api.catalog,
        models.GetDataRequest(table="db_rows", page=0, page_size=10, include_total=True),
    )
    assert resp.total == 1
    assert resp.data[0]["id"] == 1
    assert resp.data[0]["v"] == "x"


def test_get_table_store_schema_non_db_uses_primary_keys(filedir_app):
    _, docs, *_ = filedir_app
    schema = get_table_store_schema(docs.table_store)
    assert [c.name for c in schema] == ["id"]


def test_v1alpha3_get_table_data_filedir_http(filedir_app):
    api, *_ = filedir_app
    client = TestClient(api)
    res = client.post(
        "/api/v1alpha3/get-table-data",
        json={"table": "docs", "page": 0, "page_size": 10, "include_total": True},
    )
    assert res.status_code == 200
    body = res.json()
    assert body["total"] == 2
    assert {row["id"] for row in body["data"]} == {"a", "b"}
    assert any(row.get("name") == "alpha" for row in body["data"])


def test_v1alpha2_filedir_still_not_implemented(filedir_app):
    api, *_ = filedir_app
    client = TestClient(api)
    res = client.post(
        "/api/v1alpha2/get-table-data",
        json={"table": "docs", "page": 0, "page_size": 10},
    )
    assert res.status_code == 500
    assert res.json()["detail"] == "Not implemented"

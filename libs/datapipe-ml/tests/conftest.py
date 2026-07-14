import os

os.environ["SQLALCHEMY_WARN_20"] = "1"

from tests.helpers.test_env import load_test_env

load_test_env()

import tempfile
from pathlib import Path

import pytest
from sqlalchemy import Column, create_engine, text
from sqlalchemy.sql.sqltypes import JSON, String

from tests.fixtures.smoke_data import SmokeDataset, make_smoke_dataset
from tests.helpers.dbconn import get_sqlite_dbconnstr


@pytest.fixture(
    params=[
        pytest.param("local", id="local-path"),
        pytest.param("file", id="file-url"),
    ]
)
def storage_case(request):
    selected = os.environ.get("DATAPIPE_ML_STORAGE_CASES")
    if selected:
        allowed = {item.strip() for item in selected.split(",") if item.strip()}
        if request.param not in allowed:
            pytest.skip(f"Storage case {request.param!r} is not selected")
    return request.param


@pytest.fixture
def storage_workdir(tmp_path: Path, storage_case: str) -> str:
    path = (tmp_path / "datapipe_ml").resolve()
    path.mkdir(parents=True, exist_ok=True)
    if storage_case == "file":
        return f"file://{path}"
    return str(path)


@pytest.fixture
def tmp_dir() -> Path:
    with tempfile.TemporaryDirectory() as d:
        d = Path(d)
        yield d


@pytest.fixture
def dbconn():
    from datapipe.store.database import DBConn

    if os.environ.get("TEST_DB_ENV") == "postgres":
        pg_host = os.getenv("POSTGRES_HOST", "localhost")
        pg_port = os.getenv("POSTGRES_PORT", "5432")
        DBCONNSTR = f"postgresql://postgres:password@{pg_host}:{pg_port}/postgres"
        DB_TEST_SCHEMA = "test"
    else:
        DBCONNSTR = get_sqlite_dbconnstr()
        DB_TEST_SCHEMA = None

    if DB_TEST_SCHEMA:
        eng = create_engine(DBCONNSTR)

        try:
            with eng.begin() as conn:
                conn.execute(text(f"DROP SCHEMA {DB_TEST_SCHEMA} CASCADE"))
        except Exception:
            pass

        with eng.begin() as conn:
            conn.execute(text(f"CREATE SCHEMA {DB_TEST_SCHEMA}"))

        yield DBConn(DBCONNSTR, DB_TEST_SCHEMA)

        with eng.begin() as conn:
            conn.execute(text(f"DROP SCHEMA {DB_TEST_SCHEMA} CASCADE"))

    else:
        yield DBConn(DBCONNSTR, DB_TEST_SCHEMA)


@pytest.fixture
def datapipe_dir(tmp_dir: Path) -> Path:
    d = tmp_dir / "datapipe_ml"
    d.mkdir(parents=True, exist_ok=True)
    return d


@pytest.fixture
def base_datastore(dbconn):
    from datapipe.datatable import DataStore

    return DataStore(dbconn, create_meta_table=True)


@pytest.fixture
def smoke_images_dir(tmp_dir: Path) -> Path:
    return tmp_dir / "images"


@pytest.fixture
def smoke_dataset(smoke_images_dir: Path) -> SmokeDataset:
    return make_smoke_dataset(smoke_images_dir)


@pytest.fixture
def base_catalog_factory(dbconn):
    def build():
        from datapipe.compute import Catalog, Table
        from datapipe.store.database import TableStoreDB

        return Catalog(
            {
                "image": Table(
                    store=TableStoreDB(
                        dbconn=dbconn,
                        name="image",
                        data_sql_schema=[
                            Column("image_id", String, primary_key=True),
                            Column("image__image_path", String),
                        ],
                        create_table=True,
                    )
                ),
                "image__ground_truth": Table(
                    store=TableStoreDB(
                        dbconn=dbconn,
                        name="image__ground_truth",
                        data_sql_schema=[
                            Column("image_id", String, primary_key=True),
                            Column("bboxes", JSON),
                            Column("labels", JSON),
                            Column("masks", JSON),
                        ],
                        create_table=True,
                    )
                ),
                "image__ground_truth_for_keypoints": Table(
                    store=TableStoreDB(
                        dbconn=dbconn,
                        name="image__ground_truth_for_keypoints",
                        data_sql_schema=[
                            Column("image_id", String, primary_key=True),
                            Column("bboxes", JSON),
                            Column("labels", JSON),
                            Column("keypoints", JSON),
                            Column("keypoints_visibility", JSON),
                            Column("flip_idx", JSON),
                        ],
                        create_table=True,
                    )
                ),
                "image__ground_truth_for_classification": Table(
                    store=TableStoreDB(
                        dbconn=dbconn,
                        name="image__ground_truth_for_classification",
                        data_sql_schema=[
                            Column("image_id", String, primary_key=True),
                            Column("label", String),
                        ],
                        create_table=True,
                    )
                ),
                "subset__has__image": Table(
                    store=TableStoreDB(
                        dbconn=dbconn,
                        name="subset__has__image",
                        data_sql_schema=[
                            Column("image_id", String, primary_key=True),
                            Column("subset_id", String, primary_key=True),
                        ],
                        create_table=True,
                    )
                ),
            }
        )

    return build


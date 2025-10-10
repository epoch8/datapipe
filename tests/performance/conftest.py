"""
Shared fixtures for performance tests.
"""
import os
import time

import pandas as pd
import pytest
from sqlalchemy import Column, Integer, String, create_engine, text

from datapipe.compute import ComputeInput
from datapipe.datatable import DataStore
from datapipe.step.batch_transform import BatchTransformStep
from datapipe.store.database import DBConn, TableStoreDB


@pytest.fixture
def dbconn():
    """Database connection fixture for performance tests."""
    if os.environ.get("TEST_DB_ENV") == "sqlite":
        DBCONNSTR = "sqlite+pysqlite3:///:memory:"
        DB_TEST_SCHEMA = None
    else:
        pg_host = os.getenv("POSTGRES_HOST", "localhost")
        pg_port = os.getenv("POSTGRES_PORT", "5432")
        DBCONNSTR = f"postgresql://postgres:password@{pg_host}:{pg_port}/postgres"
        DB_TEST_SCHEMA = "test"

    if DB_TEST_SCHEMA:
        eng = create_engine(DBCONNSTR)

        try:
            with eng.begin() as conn:
                conn.execute(text(f"DROP SCHEMA {DB_TEST_SCHEMA} CASCADE"))
        except Exception:
            pass

        with eng.begin() as conn:
            conn.execute(text(f"CREATE SCHEMA {DB_TEST_SCHEMA}"))

    dbconn = DBConn(DBCONNSTR, DB_TEST_SCHEMA)
    yield dbconn


def fast_bulk_insert(dbconn: DBConn, table_name: str, data: pd.DataFrame):
    """
    Быстрая вставка данных используя bulk insert (PostgreSQL).

    Это намного быстрее чем обычный store_chunk, т.к. минует метаданные.
    """
    # Используем to_sql с method='multi' для быстрой вставки
    with dbconn.con.begin() as con:
        data.to_sql(
            table_name,
            con=con,
            schema=dbconn.schema,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=10000,
        )


@pytest.fixture
def fast_data_loader(dbconn: DBConn):
    """
    Фикстура для быстрой загрузки больших объемов тестовых данных.
    """
    def prepare_large_dataset(
        ds: DataStore,
        table_name: str,
        num_records: int,
        primary_key: str = "id"
    ) -> "DataTable":  # noqa: F821
        """
        Быстрая подготовка большого объема тестовых данных.

        Использует прямую вставку в data таблицу и метатаблицу для скорости.
        """
        # Создать таблицу
        store = TableStoreDB(
            dbconn,
            table_name,
            [
                Column(primary_key, String, primary_key=True),
                Column("value", Integer),
                Column("category", String),
            ],
            create_table=True,
        )
        dt = ds.create_table(table_name, store)

        # Генерировать данные партиями для эффективности памяти
        batch_size = 50000
        now = time.time()

        print(f"\nPreparing {num_records:,} records for {table_name}...")
        start_time = time.time()

        for i in range(0, num_records, batch_size):
            chunk_size = min(batch_size, num_records - i)

            # Генерация данных
            data = pd.DataFrame({
                primary_key: [f"id_{j:010d}" for j in range(i, i + chunk_size)],
                "value": range(i, i + chunk_size),
                "category": [f"cat_{j % 100}" for j in range(i, i + chunk_size)],
            })

            # Быстрая вставка в data таблицу
            fast_bulk_insert(dbconn, table_name, data)

            # Вставка метаданных (необходимо для работы offset)
            meta_data = data[[primary_key]].copy()
            meta_data['hash'] = 0  # Упрощенный hash
            meta_data['create_ts'] = now
            meta_data['update_ts'] = now
            meta_data['process_ts'] = None
            meta_data['delete_ts'] = None

            fast_bulk_insert(dbconn, f"{table_name}_meta", meta_data)

            if (i + chunk_size) % 100000 == 0:
                elapsed = time.time() - start_time
                rate = (i + chunk_size) / elapsed
                print(f"  Inserted {i + chunk_size:,} records ({rate:,.0f} rec/sec)")

        total_time = time.time() - start_time
        print(f"Data preparation completed in {total_time:.2f}s ({num_records/total_time:,.0f} rec/sec)")

        return dt

    return prepare_large_dataset


@pytest.fixture
def perf_pipeline_factory(dbconn: DBConn):
    """
    Фабрика для создания пайплайнов в нагрузочных тестах.
    """
    def create_pipeline(
        ds: DataStore,
        name: str,
        source_dt,
        target_dt,
        use_offset: bool
    ) -> BatchTransformStep:
        """
        Создать пайплайн для тестирования производительности.
        """
        def copy_transform(df):
            # Простая трансформация - копирование с небольшим изменением
            result = df.copy()
            result['value'] = result['value'] * 2
            return result

        return BatchTransformStep(
            ds=ds,
            name=name,
            func=copy_transform,
            input_dts=[ComputeInput(dt=source_dt, join_type="full")],
            output_dts=[target_dt],
            transform_keys=["id"],
            use_offset_optimization=use_offset,
        )

    return create_pipeline

"""
Тесты для build_changed_idx_sql_v2 - новой оптимизированной версии с offset'ами
"""
import time

import pandas as pd
from sqlalchemy import Column, Integer, String

from datapipe.compute import ComputeInput, Table
from datapipe.datatable import DataStore
from datapipe.meta.sql_meta import TransformMetaTable, build_changed_idx_sql_v2
from datapipe.store.database import DBConn, TableStoreDB


def test_build_changed_idx_sql_v2_basic(dbconn: DBConn):
    """Тест базовой работы build_changed_idx_sql_v2"""
    ds = DataStore(dbconn, create_meta_table=True)

    # Создаем входную таблицу
    input_table_store = TableStoreDB(
        dbconn,
        "test_input",
        [
            Column("id", String, primary_key=True),
            Column("value", String),
        ],
        create_table=True,
    )
    input_dt = ds.create_table("test_input", input_table_store)

    # Создаем TransformMetaTable
    transform_meta = TransformMetaTable(
        dbconn,
        "test_transform_meta",
        [Column("id", String, primary_key=True)],
        create_table=True,
    )

    # Добавляем данные во входную таблицу
    now = time.time()
    input_dt.store_chunk(
        pd.DataFrame({"id": ["1", "2", "3"], "value": ["a", "b", "c"]}), now=now
    )

    # Устанавливаем offset
    ds.offset_table.update_offset("test_transform", "test_input", now - 10)

    # Создаем ComputeInput
    compute_input = ComputeInput(dt=input_dt, join_type="full")

    # Строим SQL с использованием v2
    transform_keys, sql = build_changed_idx_sql_v2(
        ds=ds,
        meta_table=transform_meta,
        input_dts=[compute_input],
        transform_keys=["id"],
        offset_table=ds.offset_table,
        transformation_id="test_transform",
    )

    # Проверяем, что SQL компилируется
    assert transform_keys == ["id"]
    assert sql is not None

    # Выполняем SQL и проверяем результат
    with dbconn.con.begin() as con:
        result = con.execute(sql).fetchall()
        # Должны получить записи, добавленные после offset
        assert len(result) == 3


def test_build_changed_idx_sql_v2_with_offset_filters_new_records(dbconn: DBConn):
    """Тест что offset фильтрует старые записи"""
    ds = DataStore(dbconn, create_meta_table=True)

    input_table_store = TableStoreDB(
        dbconn,
        "test_input2",
        [
            Column("id", String, primary_key=True),
            Column("value", String),
        ],
        create_table=True,
    )
    input_dt = ds.create_table("test_input2", input_table_store)

    transform_meta = TransformMetaTable(
        dbconn,
        "test_transform_meta2",
        [Column("id", String, primary_key=True)],
        create_table=True,
    )

    # Добавляем старые данные
    old_time = time.time() - 100
    input_dt.store_chunk(pd.DataFrame({"id": ["1", "2"], "value": ["a", "b"]}), now=old_time)

    # Устанавливаем offset после старых данных
    ds.offset_table.update_offset("test_transform2", "test_input2", old_time + 10)

    # Добавляем новые данные
    new_time = time.time()
    input_dt.store_chunk(pd.DataFrame({"id": ["3", "4"], "value": ["c", "d"]}), now=new_time)

    compute_input = ComputeInput(dt=input_dt, join_type="full")

    transform_keys, sql = build_changed_idx_sql_v2(
        ds=ds,
        meta_table=transform_meta,
        input_dts=[compute_input],
        transform_keys=["id"],
        offset_table=ds.offset_table,
        transformation_id="test_transform2",
    )

    # Выполняем SQL
    with dbconn.con.begin() as con:
        result = con.execute(sql).fetchall()
        ids = sorted([row[1] for row in result])  # row[0] is _datapipe_dummy
        # Должны получить только новые записи (3, 4), старые (1, 2) отфильтрованы offset'ом
        assert ids == ["3", "4"]


def test_build_changed_idx_sql_v2_with_error_records(dbconn: DBConn):
    """Тест что ошибочные записи попадают в выборку"""
    ds = DataStore(dbconn, create_meta_table=True)

    input_table_store = TableStoreDB(
        dbconn,
        "test_input3",
        [
            Column("id", String, primary_key=True),
            Column("value", String),
        ],
        create_table=True,
    )
    input_dt = ds.create_table("test_input3", input_table_store)

    transform_meta = TransformMetaTable(
        dbconn,
        "test_transform_meta3",
        [Column("id", String, primary_key=True)],
        create_table=True,
    )

    # Добавляем данные
    now = time.time()
    input_dt.store_chunk(pd.DataFrame({"id": ["1", "2", "3"], "value": ["a", "b", "c"]}), now=now)

    # Устанавливаем offset
    ds.offset_table.update_offset("test_transform3", "test_input3", now + 10)

    # Добавляем ошибочную запись в transform_meta
    transform_meta.insert_rows(pd.DataFrame({"id": ["error_id"]}))
    # Оставляем is_success=False и process_ts=0 (дефолтные значения)

    compute_input = ComputeInput(dt=input_dt, join_type="full")

    transform_keys, sql = build_changed_idx_sql_v2(
        ds=ds,
        meta_table=transform_meta,
        input_dts=[compute_input],
        transform_keys=["id"],
        offset_table=ds.offset_table,
        transformation_id="test_transform3",
    )

    # Выполняем SQL
    with dbconn.con.begin() as con:
        result = con.execute(sql).fetchall()
        ids = sorted([row[1] for row in result])
        # Должны получить только error_id (новые записи отфильтрованы offset'ом)
        assert ids == ["error_id"]


def test_build_changed_idx_sql_v2_multiple_inputs(dbconn: DBConn):
    """Тест с несколькими входными таблицами"""
    ds = DataStore(dbconn, create_meta_table=True)

    # Создаем две входных таблицы
    input1_store = TableStoreDB(
        dbconn,
        "test_input_a",
        [Column("id", String, primary_key=True), Column("value", String)],
        create_table=True,
    )
    input1_dt = ds.create_table("test_input_a", input1_store)

    input2_store = TableStoreDB(
        dbconn,
        "test_input_b",
        [Column("id", String, primary_key=True), Column("value", String)],
        create_table=True,
    )
    input2_dt = ds.create_table("test_input_b", input2_store)

    transform_meta = TransformMetaTable(
        dbconn,
        "test_transform_multi",
        [Column("id", String, primary_key=True)],
        create_table=True,
    )

    # Добавляем данные в обе таблицы
    now = time.time()
    input1_dt.store_chunk(pd.DataFrame({"id": ["1", "2"], "value": ["a", "b"]}), now=now)
    input2_dt.store_chunk(pd.DataFrame({"id": ["3", "4"], "value": ["c", "d"]}), now=now)

    # Устанавливаем offset для обеих таблиц
    ds.offset_table.update_offset("test_transform_multi", "test_input_a", now - 10)
    ds.offset_table.update_offset("test_transform_multi", "test_input_b", now - 10)

    compute_inputs = [
        ComputeInput(dt=input1_dt, join_type="full"),
        ComputeInput(dt=input2_dt, join_type="full"),
    ]

    transform_keys, sql = build_changed_idx_sql_v2(
        ds=ds,
        meta_table=transform_meta,
        input_dts=compute_inputs,
        transform_keys=["id"],
        offset_table=ds.offset_table,
        transformation_id="test_transform_multi",
    )

    # Выполняем SQL
    with dbconn.con.begin() as con:
        result = con.execute(sql).fetchall()
        ids = sorted([row[1] for row in result])
        # Должны получить UNION записей из обеих таблиц
        assert ids == ["1", "2", "3", "4"]


def test_build_changed_idx_sql_v2_no_offset_processes_all(dbconn: DBConn):
    """Тест что при отсутствии offset обрабатываются все данные"""
    ds = DataStore(dbconn, create_meta_table=True)

    input_table_store = TableStoreDB(
        dbconn,
        "test_input_no_offset",
        [Column("id", String, primary_key=True), Column("value", String)],
        create_table=True,
    )
    input_dt = ds.create_table("test_input_no_offset", input_table_store)

    transform_meta = TransformMetaTable(
        dbconn,
        "test_transform_no_offset",
        [Column("id", String, primary_key=True)],
        create_table=True,
    )

    # Добавляем данные
    now = time.time()
    input_dt.store_chunk(pd.DataFrame({"id": ["1", "2", "3"], "value": ["a", "b", "c"]}), now=now)

    # НЕ устанавливаем offset - первый запуск

    compute_input = ComputeInput(dt=input_dt, join_type="full")

    transform_keys, sql = build_changed_idx_sql_v2(
        ds=ds,
        meta_table=transform_meta,
        input_dts=[compute_input],
        transform_keys=["id"],
        offset_table=ds.offset_table,
        transformation_id="test_transform_no_offset",
    )

    # Выполняем SQL
    with dbconn.con.begin() as con:
        result = con.execute(sql).fetchall()
        ids = sorted([row[1] for row in result])
        # При offset=None (дефолт 0.0) должны получить все записи
        assert ids == ["1", "2", "3"]

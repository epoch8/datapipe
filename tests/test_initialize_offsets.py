"""
Тесты для Phase 4: Инициализация offset'ов из существующих TransformMetaTable

Проверяет функцию initialize_offsets_from_transform_meta() которая устанавливает
начальные offset'ы на основе уже обработанных данных.
"""
import time

import pandas as pd
from sqlalchemy import Column, Integer, String

from datapipe.compute import ComputeInput
from datapipe.datatable import DataStore
from datapipe.meta.sql_meta import initialize_offsets_from_transform_meta
from datapipe.step.batch_transform import BatchTransformStep
from datapipe.store.database import DBConn, TableStoreDB


def test_initialize_offsets_from_empty_transform_meta(dbconn: DBConn):
    """
    Тест инициализации offset'ов когда TransformMetaTable пустая.

    Ожидаемое поведение: offset'ы не устанавливаются (нет данных для инициализации)
    """
    ds = DataStore(dbconn, create_meta_table=True)

    input_store = TableStoreDB(
        dbconn,
        "init_empty_input",
        [Column("id", String, primary_key=True), Column("value", Integer)],
        create_table=True,
    )
    input_dt = ds.create_table("init_empty_input", input_store)

    output_store = TableStoreDB(
        dbconn,
        "init_empty_output",
        [Column("id", String, primary_key=True), Column("result", Integer)],
        create_table=True,
    )
    output_dt = ds.create_table("init_empty_output", output_store)

    def transform_func(df):
        return df.rename(columns={"value": "result"})

    step = BatchTransformStep(
        ds=ds,
        name="init_empty_transform",
        func=transform_func,
        input_dts=[ComputeInput(dt=input_dt, join_type="full")],
        output_dts=[output_dt],
        transform_keys=["id"],
        use_offset_optimization=False,  # Не используем автообновление
    )

    # Инициализируем offset'ы (TransformMetaTable пустая)
    result = initialize_offsets_from_transform_meta(ds, step)

    # Проверяем что offset'ы не установлены
    assert result == {}
    offsets = ds.offset_table.get_offsets_for_transformation(step.get_name())
    assert len(offsets) == 0


def test_initialize_offsets_from_existing_data(dbconn: DBConn):
    """
    Тест инициализации offset'ов из существующих обработанных данных.

    Сценарий:
    1. Обрабатываем данные старым методом (v1, без offset'ов)
    2. Инициализируем offset'ы на основе уже обработанных данных
    3. Проверяем что offset'ы установлены корректно
    4. Добавляем новые данные и проверяем что обрабатываются только они
    """
    ds = DataStore(dbconn, create_meta_table=True)

    input_store = TableStoreDB(
        dbconn,
        "init_existing_input",
        [Column("id", String, primary_key=True), Column("value", Integer)],
        create_table=True,
    )
    input_dt = ds.create_table("init_existing_input", input_store)

    output_store = TableStoreDB(
        dbconn,
        "init_existing_output",
        [Column("id", String, primary_key=True), Column("result", Integer)],
        create_table=True,
    )
    output_dt = ds.create_table("init_existing_output", output_store)

    def transform_func(df):
        return df.rename(columns={"value": "result"})

    # Создаем step БЕЗ offset optimization
    step = BatchTransformStep(
        ds=ds,
        name="init_existing_transform",
        func=transform_func,
        input_dts=[ComputeInput(dt=input_dt, join_type="full")],
        output_dts=[output_dt],
        transform_keys=["id"],
        use_offset_optimization=False,  # Используем старый метод
    )

    # ========== Шаг 1: Обрабатываем данные старым методом ==========
    now1 = time.time()
    input_dt.store_chunk(
        pd.DataFrame({"id": ["A", "B", "C"], "value": [10, 20, 30]}), now=now1
    )

    step.run_full(ds)

    # Проверяем что данные обработались
    output_data = output_dt.get_data()
    assert len(output_data) == 3

    # ========== Шаг 2: Инициализируем offset'ы ==========
    result = initialize_offsets_from_transform_meta(ds, step)

    # Проверяем что offset был установлен
    assert len(result) == 1
    assert "init_existing_input" in result
    assert result["init_existing_input"] >= now1

    # Проверяем что offset сохранен в БД
    offsets = ds.offset_table.get_offsets_for_transformation(step.get_name())
    assert len(offsets) == 1
    assert offsets["init_existing_input"] >= now1

    # ========== Шаг 3: Включаем offset optimization ==========
    step.use_offset_optimization = True

    # ========== Шаг 4: Добавляем новые данные ==========
    time.sleep(0.01)
    now2 = time.time()
    input_dt.store_chunk(
        pd.DataFrame({"id": ["D", "E"], "value": [40, 50]}), now=now2
    )

    # ========== Шаг 5: Проверяем что видны только новые записи ==========
    changed_count = step.get_changed_idx_count(ds)
    assert changed_count == 2, f"Expected 2 new records, got {changed_count}"

    # Запускаем обработку с offset optimization
    step.run_full(ds)

    # Проверяем что обработались все 5 записей
    output_data = output_dt.get_data()
    assert len(output_data) == 5
    assert sorted(output_data["id"].tolist()) == ["A", "B", "C", "D", "E"]


def test_initialize_offsets_with_multiple_inputs(dbconn: DBConn):
    """
    Тест инициализации offset'ов для трансформации с несколькими входными таблицами.
    """
    ds = DataStore(dbconn, create_meta_table=True)

    # Первая входная таблица
    input1_store = TableStoreDB(
        dbconn,
        "init_multi_input1",
        [Column("id", String, primary_key=True), Column("value1", Integer)],
        create_table=True,
    )
    input1_dt = ds.create_table("init_multi_input1", input1_store)

    # Вторая входная таблица
    input2_store = TableStoreDB(
        dbconn,
        "init_multi_input2",
        [Column("id", String, primary_key=True), Column("value2", Integer)],
        create_table=True,
    )
    input2_dt = ds.create_table("init_multi_input2", input2_store)

    # Выходная таблица
    output_store = TableStoreDB(
        dbconn,
        "init_multi_output",
        [Column("id", String, primary_key=True), Column("sum", Integer)],
        create_table=True,
    )
    output_dt = ds.create_table("init_multi_output", output_store)

    def transform_func(df1, df2):
        merged = df1.merge(df2, on="id", how="outer")
        merged["sum"] = merged["value1"].fillna(0) + merged["value2"].fillna(0)
        return merged[["id", "sum"]].astype({"sum": int})

    step = BatchTransformStep(
        ds=ds,
        name="init_multi_transform",
        func=transform_func,
        input_dts=[
            ComputeInput(dt=input1_dt, join_type="full"),
            ComputeInput(dt=input2_dt, join_type="full"),
        ],
        output_dts=[output_dt],
        transform_keys=["id"],
        use_offset_optimization=False,
    )

    # Добавляем данные в обе таблицы с разными timestamp
    now1 = time.time()
    input1_dt.store_chunk(pd.DataFrame({"id": ["X", "Y"], "value1": [1, 2]}), now=now1)

    time.sleep(0.01)
    now2 = time.time()
    input2_dt.store_chunk(pd.DataFrame({"id": ["X", "Y"], "value2": [10, 20]}), now=now2)

    # Обрабатываем
    step.run_full(ds)

    # Инициализируем offset'ы
    result = initialize_offsets_from_transform_meta(ds, step)

    # Проверяем что offset'ы установлены для обеих таблиц
    assert len(result) == 2
    assert "init_multi_input1" in result
    assert "init_multi_input2" in result

    # Проверяем что offset'ы корректные (учитывают MIN(process_ts))
    assert result["init_multi_input1"] >= now1
    assert result["init_multi_input2"] >= now2


def test_initialize_offsets_conservative_approach(dbconn: DBConn):
    """
    Тест что инициализация использует консервативный подход:
    MAX(input.update_ts) <= MIN(transform.process_ts)

    Это гарантирует что мы не пропустим данные которые еще не были обработаны.
    """
    ds = DataStore(dbconn, create_meta_table=True)

    input_store = TableStoreDB(
        dbconn,
        "init_conservative_input",
        [Column("id", String, primary_key=True), Column("value", Integer)],
        create_table=True,
    )
    input_dt = ds.create_table("init_conservative_input", input_store)

    output_store = TableStoreDB(
        dbconn,
        "init_conservative_output",
        [Column("id", String, primary_key=True), Column("result", Integer)],
        create_table=True,
    )
    output_dt = ds.create_table("init_conservative_output", output_store)

    def transform_func(df):
        return df.rename(columns={"value": "result"})

    step = BatchTransformStep(
        ds=ds,
        name="init_conservative_transform",
        func=transform_func,
        input_dts=[ComputeInput(dt=input_dt, join_type="full")],
        output_dts=[output_dt],
        transform_keys=["id"],
        use_offset_optimization=False,
    )

    # Добавляем первую партию данных
    now1 = time.time()
    input_dt.store_chunk(
        pd.DataFrame({"id": ["1", "2"], "value": [10, 20]}), now=now1
    )

    # Обрабатываем первую партию
    step.run_full(ds)

    # Добавляем вторую партию данных (с более поздним timestamp)
    time.sleep(0.01)
    now2 = time.time()
    input_dt.store_chunk(
        pd.DataFrame({"id": ["3", "4"], "value": [30, 40]}), now=now2
    )

    # НЕ обрабатываем вторую партию!
    # Инициализируем offset'ы
    result = initialize_offsets_from_transform_meta(ds, step)

    # Offset должен быть <= now1 (только для первой партии)
    # Потому что MIN(process_ts) соответствует первой партии
    # Это гарантирует что вторая партия (с now2) НЕ будет пропущена
    assert result["init_conservative_input"] <= now1 + 0.1  # небольшой запас на overhead

    # Включаем offset optimization и проверяем что видны обе непроцессированные записи
    step.use_offset_optimization = True
    changed_count = step.get_changed_idx_count(ds)
    assert changed_count == 2  # Должны видеть записи 3 и 4

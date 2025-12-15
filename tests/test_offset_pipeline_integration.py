"""
Интеграционный тест для пайплайна с offset-оптимизацией.

Проверяет полный цикл работы простого пайплайна копирования данных:
- Добавление новых записей
- Изменение существующих записей
- Удаление записей
- Смешанные операции
"""
import time

import pandas as pd
import pytest
from sqlalchemy import Column, Integer, String

from datapipe.compute import ComputeInput
from datapipe.datatable import DataStore, DataTable
from datapipe.step.batch_transform import BatchTransformStep
from datapipe.store.database import DBConn, TableStoreDB


@pytest.fixture
def setup_copy_pipeline(dbconn: DBConn):
    """
    Фикстура для создания простого пайплайна копирования данных.

    Возвращает:
        (ds, source_dt, target_dt, step) - готовый пайплайн для тестирования
    """
    ds = DataStore(dbconn, create_meta_table=True)

    # Создаем source таблицу
    source_store = TableStoreDB(
        dbconn,
        "pipeline_source",
        [
            Column("id", String, primary_key=True),
            Column("name", String),
            Column("value", Integer),
        ],
        create_table=True,
    )
    source_dt = ds.create_table("pipeline_source", source_store)

    # Создаем target таблицу
    target_store = TableStoreDB(
        dbconn,
        "pipeline_target",
        [
            Column("id", String, primary_key=True),
            Column("name", String),
            Column("value", Integer),
        ],
        create_table=True,
    )
    target_dt = ds.create_table("pipeline_target", target_store)

    # Функция копирования (просто возвращает данные как есть)
    def copy_transform(df):
        return df[["id", "name", "value"]]

    # Создаем step с offset-оптимизацией
    step = BatchTransformStep(
        ds=ds,
        name="copy_pipeline",
        func=copy_transform,
        input_dts=[ComputeInput(dt=source_dt, join_type="full")],
        output_dts=[target_dt],
        transform_keys=["id"],
        use_offset_optimization=True,
    )

    return ds, source_dt, target_dt, step


def test_offset_pipeline_insert_records(setup_copy_pipeline):
    """
    Тест добавления новых записей в пайплайн с offset-оптимизацией.

    Проверяет:
    1. Начальное добавление данных
    2. Добавление новых записей (только новые обрабатываются)
    3. Корректность данных в target
    4. Обновление offset'ов
    """
    ds, source_dt, target_dt, step = setup_copy_pipeline

    # ========== Добавляем начальные данные ==========
    now1 = time.time()
    initial_data = pd.DataFrame({
        "id": ["A", "B", "C"],
        "name": ["Alice", "Bob", "Charlie"],
        "value": [10, 20, 30],
    })
    source_dt.store_chunk(initial_data, now=now1)

    # Запускаем трансформацию
    step.run_full(ds)

    # Проверяем что данные скопировались в target
    target_data = target_dt.get_data().sort_values("id").reset_index(drop=True)
    expected = initial_data.sort_values("id").reset_index(drop=True)
    pd.testing.assert_frame_equal(target_data, expected)

    # Проверяем что offset установлен
    offsets = ds.offset_table.get_offsets_for_transformation(step.get_name())
    assert len(offsets) == 1
    assert "pipeline_source" in offsets
    assert offsets["pipeline_source"] >= now1

    # ========== Добавляем новые записи ==========
    time.sleep(0.01)
    now2 = time.time()
    new_data = pd.DataFrame({
        "id": ["D", "E"],
        "name": ["David", "Eve"],
        "value": [40, 50],
    })
    source_dt.store_chunk(new_data, now=now2)

    # Проверяем что changed_count показывает только новые записи
    changed_count = step.get_changed_idx_count(ds)
    assert changed_count == 2, f"Expected 2 new records, got {changed_count}"

    # Запускаем трансформацию
    step.run_full(ds)

    # Проверяем что в target теперь все 5 записей
    target_data = target_dt.get_data().sort_values("id").reset_index(drop=True)
    assert len(target_data) == 5
    expected_all = pd.concat([initial_data, new_data]).sort_values("id").reset_index(drop=True)
    pd.testing.assert_frame_equal(target_data, expected_all)

    # Проверяем что offset обновился
    offsets = ds.offset_table.get_offsets_for_transformation(step.get_name())
    assert offsets["pipeline_source"] >= now2


def test_offset_pipeline_update_records(setup_copy_pipeline):
    """
    Тест изменения существующих записей в пайплайне с offset-оптимизацией.

    Проверяет:
    1. Обновление существующих записей
    2. Только измененные записи обрабатываются
    3. Корректность обновленных данных в target
    4. Обновление offset'ов
    """
    ds, source_dt, target_dt, step = setup_copy_pipeline

    # Добавляем начальные данные
    now1 = time.time()
    initial_data = pd.DataFrame({
        "id": ["A", "B", "C"],
        "name": ["Alice", "Bob", "Charlie"],
        "value": [10, 20, 30],
    })
    source_dt.store_chunk(initial_data, now=now1)
    step.run_full(ds)

    # ========== Изменяем существующие записи ==========
    time.sleep(0.01)
    now2 = time.time()
    updated_data = pd.DataFrame({
        "id": ["B", "C"],
        "name": ["Bob Updated", "Charlie Updated"],
        "value": [200, 300],
    })
    source_dt.store_chunk(updated_data, now=now2)

    # Проверяем что changed_count показывает измененные записи
    changed_count = step.get_changed_idx_count(ds)
    assert changed_count == 2, f"Expected 2 updated records, got {changed_count}"

    # Запускаем трансформацию
    step.run_full(ds)

    # Проверяем что в target обновились записи B и C
    target_data = target_dt.get_data().sort_values("id").reset_index(drop=True)
    assert len(target_data) == 3

    # Проверяем конкретные значения
    row_a = target_data[target_data["id"] == "A"].iloc[0]
    assert row_a["name"] == "Alice"
    assert row_a["value"] == 10

    row_b = target_data[target_data["id"] == "B"].iloc[0]
    assert row_b["name"] == "Bob Updated"
    assert row_b["value"] == 200

    row_c = target_data[target_data["id"] == "C"].iloc[0]
    assert row_c["name"] == "Charlie Updated"
    assert row_c["value"] == 300

    # Проверяем что offset обновился
    offsets = ds.offset_table.get_offsets_for_transformation(step.get_name())
    assert offsets["pipeline_source"] >= now2


def test_offset_pipeline_delete_records(setup_copy_pipeline):
    """
    Тест удаления записей в пайплайне с offset-оптимизацией.

    Проверяет:
    1. Удаление записей из source
    2. Только удаленные записи обрабатываются
    3. Записи удаляются из target
    4. Обновление offset'ов
    """
    ds, source_dt, target_dt, step = setup_copy_pipeline

    # Добавляем начальные данные
    now1 = time.time()
    initial_data = pd.DataFrame({
        "id": ["A", "B", "C", "D"],
        "name": ["Alice", "Bob", "Charlie", "David"],
        "value": [10, 20, 30, 40],
    })
    source_dt.store_chunk(initial_data, now=now1)
    step.run_full(ds)

    # Проверяем что все данные в target
    assert len(target_dt.get_data()) == 4

    # ========== Удаляем записи ==========
    time.sleep(0.01)
    now2 = time.time()
    delete_idx = pd.DataFrame({"id": ["A", "C"]})
    source_dt.delete_by_idx(delete_idx, now=now2)

    # Проверяем что changed_count показывает удаленные записи
    changed_count = step.get_changed_idx_count(ds)
    assert changed_count == 2, f"Expected 2 deleted records, got {changed_count}"

    # Запускаем трансформацию
    step.run_full(ds)

    # Проверяем что в target остались только B и D
    target_data = target_dt.get_data().sort_values("id").reset_index(drop=True)
    assert len(target_data) == 2
    assert sorted(target_data["id"].tolist()) == ["B", "D"]

    # Проверяем что значения корректные
    row_b = target_data[target_data["id"] == "B"].iloc[0]
    assert row_b["name"] == "Bob"
    assert row_b["value"] == 20

    row_d = target_data[target_data["id"] == "D"].iloc[0]
    assert row_d["name"] == "David"
    assert row_d["value"] == 40


def test_offset_pipeline_mixed_operations(setup_copy_pipeline):
    """
    Тест смешанных операций (добавление + изменение + удаление) в одном батче.

    Проверяет:
    1. Одновременное добавление новых, изменение существующих и удаление записей
    2. Все операции обрабатываются корректно в одном запуске
    3. Финальное состояние target корректно
    """
    ds, source_dt, target_dt, step = setup_copy_pipeline

    # Добавляем начальные данные
    now1 = time.time()
    initial_data = pd.DataFrame({
        "id": ["1", "2", "3"],
        "name": ["One", "Two", "Three"],
        "value": [100, 200, 300],
    })
    source_dt.store_chunk(initial_data, now=now1)
    step.run_full(ds)

    # Проверяем начальное состояние
    assert len(target_dt.get_data()) == 3

    # ========== Одновременно: добавляем "4", изменяем "2", удаляем "3" ==========
    time.sleep(0.01)
    now2 = time.time()

    # Добавляем новую запись
    source_dt.store_chunk(
        pd.DataFrame({"id": ["4"], "name": ["Four"], "value": [400]}), now=now2
    )

    # Изменяем существующую
    source_dt.store_chunk(
        pd.DataFrame({"id": ["2"], "name": ["Two Updated"], "value": [222]}), now=now2
    )

    # Удаляем
    delete_idx = pd.DataFrame({"id": ["3"]})
    source_dt.delete_by_idx(delete_idx, now=now2)

    # Проверяем что changed_count = 3 (новая + измененная + удаленная)
    changed_count = step.get_changed_idx_count(ds)
    assert changed_count == 3, f"Expected 3 changed records, got {changed_count}"

    # Запускаем трансформацию
    step.run_full(ds)

    # Проверяем результат: должны остаться "1", "2", "4"
    target_data = target_dt.get_data().sort_values("id").reset_index(drop=True)
    assert len(target_data) == 3
    assert sorted(target_data["id"].tolist()) == ["1", "2", "4"]

    # Проверяем значения каждой записи
    row_1 = target_data[target_data["id"] == "1"].iloc[0]
    assert row_1["name"] == "One"
    assert row_1["value"] == 100

    row_2 = target_data[target_data["id"] == "2"].iloc[0]
    assert row_2["name"] == "Two Updated"  # изменился
    assert row_2["value"] == 222

    row_4 = target_data[target_data["id"] == "4"].iloc[0]
    assert row_4["name"] == "Four"  # новый
    assert row_4["value"] == 400

    # Проверяем что offset обновился
    offsets = ds.offset_table.get_offsets_for_transformation(step.get_name())
    assert offsets["pipeline_source"] >= now2

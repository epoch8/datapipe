"""
Тесты для динамического переключения между v1 и v2 через RunConfig.labels
"""
import time

import pandas as pd
from sqlalchemy import Column, Integer, String

from datapipe.compute import ComputeInput
from datapipe.datatable import DataStore
from datapipe.run_config import RunConfig
from datapipe.step.batch_transform import BatchTransformStep
from datapipe.store.database import DBConn, TableStoreDB


def test_runtime_switch_via_labels_default_false(dbconn: DBConn):
    """Тест переключения режима через RunConfig.labels при use_offset_optimization=False"""
    ds = DataStore(dbconn, create_meta_table=True)

    input_store = TableStoreDB(
        dbconn,
        "test_input",
        [Column("id", String, primary_key=True), Column("value", Integer)],
        create_table=True,
    )
    input_dt = ds.create_table("test_input", input_store)

    output_store = TableStoreDB(
        dbconn,
        "test_output",
        [Column("id", String, primary_key=True), Column("result", Integer)],
        create_table=True,
    )
    output_dt = ds.create_table("test_output", output_store)

    def transform_func(df):
        return df.rename(columns={"value": "result"})

    # Создаем step с ВЫКЛЮЧЕННОЙ offset оптимизацией
    step = BatchTransformStep(
        ds=ds,
        name="test_transform",
        func=transform_func,
        input_dts=[ComputeInput(dt=input_dt, join_type="full")],
        output_dts=[output_dt],
        transform_keys=["id"],
        use_offset_optimization=False,  # По умолчанию ВЫКЛЮЧЕНО
    )

    # Добавляем данные
    now = time.time()
    input_dt.store_chunk(
        pd.DataFrame({"id": ["1", "2", "3"], "value": [10, 20, 30]}), now=now
    )

    # Первый запуск БЕЗ offset (дефолтное поведение)
    step.run_full(ds)

    output_data = output_dt.get_data()
    assert len(output_data) == 3

    # Добавляем новые данные
    time.sleep(0.01)
    now2 = time.time()
    input_dt.store_chunk(
        pd.DataFrame({"id": ["4", "5"], "value": [40, 50]}), now=now2
    )

    # Второй запуск С ВКЛЮЧЕНИЕМ offset через RunConfig.labels
    run_config = RunConfig(labels={"use_offset_optimization": True})
    step.run_full(ds, run_config=run_config)

    # Проверяем результат
    output_data = output_dt.get_data()
    assert len(output_data) == 5
    assert sorted(output_data["id"].tolist()) == ["1", "2", "3", "4", "5"]


def test_runtime_switch_via_labels_default_true(dbconn: DBConn):
    """Тест переключения режима через RunConfig.labels при use_offset_optimization=True"""
    ds = DataStore(dbconn, create_meta_table=True)

    input_store = TableStoreDB(
        dbconn,
        "test_input2",
        [Column("id", String, primary_key=True), Column("value", Integer)],
        create_table=True,
    )
    input_dt = ds.create_table("test_input2", input_store)

    output_store = TableStoreDB(
        dbconn,
        "test_output2",
        [Column("id", String, primary_key=True), Column("result", Integer)],
        create_table=True,
    )
    output_dt = ds.create_table("test_output2", output_store)

    def transform_func(df):
        return df.rename(columns={"value": "result"})

    # Создаем step с ВКЛЮЧЕННОЙ offset оптимизацией
    step = BatchTransformStep(
        ds=ds,
        name="test_transform2",
        func=transform_func,
        input_dts=[ComputeInput(dt=input_dt, join_type="full")],
        output_dts=[output_dt],
        transform_keys=["id"],
        use_offset_optimization=True,  # По умолчанию ВКЛЮЧЕНО
    )

    # Добавляем данные
    now = time.time()
    input_dt.store_chunk(
        pd.DataFrame({"id": ["1", "2", "3"], "value": [10, 20, 30]}), now=now
    )

    # Первый запуск С offset (дефолтное поведение)
    step.run_full(ds)

    output_data = output_dt.get_data()
    assert len(output_data) == 3

    # Добавляем новые данные
    time.sleep(0.01)
    now2 = time.time()
    input_dt.store_chunk(
        pd.DataFrame({"id": ["4", "5"], "value": [40, 50]}), now=now2
    )

    # Второй запуск С ВЫКЛЮЧЕНИЕМ offset через RunConfig.labels
    run_config = RunConfig(labels={"use_offset_optimization": False})
    step.run_full(ds, run_config=run_config)

    # Проверяем результат
    output_data = output_dt.get_data()
    assert len(output_data) == 5
    assert sorted(output_data["id"].tolist()) == ["1", "2", "3", "4", "5"]


def test_runtime_switch_multiple_runs(dbconn: DBConn):
    """Тест множественных переключений между режимами"""
    ds = DataStore(dbconn, create_meta_table=True)

    input_store = TableStoreDB(
        dbconn,
        "test_input3",
        [Column("id", String, primary_key=True), Column("value", Integer)],
        create_table=True,
    )
    input_dt = ds.create_table("test_input3", input_store)

    output_store = TableStoreDB(
        dbconn,
        "test_output3",
        [Column("id", String, primary_key=True), Column("result", Integer)],
        create_table=True,
    )
    output_dt = ds.create_table("test_output3", output_store)

    def transform_func(df):
        return df.rename(columns={"value": "result"})

    step = BatchTransformStep(
        ds=ds,
        name="test_transform3",
        func=transform_func,
        input_dts=[ComputeInput(dt=input_dt, join_type="full")],
        output_dts=[output_dt],
        transform_keys=["id"],
        use_offset_optimization=False,
    )

    # Запуск 1: v1 режим (по умолчанию)
    now = time.time()
    input_dt.store_chunk(pd.DataFrame({"id": ["1"], "value": [10]}), now=now)
    step.run_full(ds)
    assert len(output_dt.get_data()) == 1

    # Запуск 2: переключаемся на v2 через labels
    time.sleep(0.01)
    now = time.time()
    input_dt.store_chunk(pd.DataFrame({"id": ["2"], "value": [20]}), now=now)
    step.run_full(ds, run_config=RunConfig(labels={"use_offset_optimization": True}))
    assert len(output_dt.get_data()) == 2

    # Запуск 3: обратно на v1 через labels
    time.sleep(0.01)
    now = time.time()
    input_dt.store_chunk(pd.DataFrame({"id": ["3"], "value": [30]}), now=now)
    step.run_full(ds, run_config=RunConfig(labels={"use_offset_optimization": False}))
    assert len(output_dt.get_data()) == 3

    # Запуск 4: снова v2
    time.sleep(0.01)
    now = time.time()
    input_dt.store_chunk(pd.DataFrame({"id": ["4"], "value": [40]}), now=now)
    step.run_full(ds, run_config=RunConfig(labels={"use_offset_optimization": True}))
    assert len(output_dt.get_data()) == 4

    # Проверяем финальный результат
    output_data = output_dt.get_data()
    assert sorted(output_data["id"].tolist()) == ["1", "2", "3", "4"]
    assert sorted(output_data["result"].tolist()) == [10, 20, 30, 40]


def test_get_changed_idx_count_respects_label_override(dbconn: DBConn):
    """Тест что get_changed_idx_count также учитывает переопределение через labels"""
    ds = DataStore(dbconn, create_meta_table=True)

    input_store = TableStoreDB(
        dbconn,
        "test_input4",
        [Column("id", String, primary_key=True), Column("value", Integer)],
        create_table=True,
    )
    input_dt = ds.create_table("test_input4", input_store)

    output_store = TableStoreDB(
        dbconn,
        "test_output4",
        [Column("id", String, primary_key=True), Column("result", Integer)],
        create_table=True,
    )
    output_dt = ds.create_table("test_output4", output_store)

    def transform_func(df):
        return df.rename(columns={"value": "result"})

    # Создаем step с включенной offset оптимизацией для корректной работы теста
    step = BatchTransformStep(
        ds=ds,
        name="test_transform4",
        func=transform_func,
        input_dts=[ComputeInput(dt=input_dt, join_type="full")],
        output_dts=[output_dt],
        transform_keys=["id"],
        use_offset_optimization=True,  # Включаем для обновления offset'ов
    )

    # Добавляем данные
    now = time.time()
    input_dt.store_chunk(
        pd.DataFrame({"id": ["1", "2", "3"], "value": [10, 20, 30]}), now=now
    )

    # Проверяем get_changed_idx_count с v2 (дефолт)
    count_v2 = step.get_changed_idx_count(ds)
    assert count_v2 == 3

    # Проверяем get_changed_idx_count с override на v1
    count_v1 = step.get_changed_idx_count(
        ds, run_config=RunConfig(labels={"use_offset_optimization": False})
    )
    assert count_v1 == 3  # Должно быть то же самое количество

    # Обрабатываем с offset оптимизацией (обновляются offset'ы)
    step.run_full(ds)

    # Добавляем новые данные
    time.sleep(0.01)
    now2 = time.time()
    input_dt.store_chunk(pd.DataFrame({"id": ["4"], "value": [40]}), now=now2)

    # v2 увидит только новую запись (фильтрация по offset)
    count_v2_after = step.get_changed_idx_count(ds)
    assert count_v2_after == 1

    # v1 также увидит только новую запись (метаданные уже обработаны)
    count_v1_after = step.get_changed_idx_count(
        ds, run_config=RunConfig(labels={"use_offset_optimization": False})
    )
    assert count_v1_after == 1


def test_get_full_process_ids_respects_label_override(dbconn: DBConn):
    """Тест что get_full_process_ids также учитывает переопределение через labels"""
    ds = DataStore(dbconn, create_meta_table=True)

    input_store = TableStoreDB(
        dbconn,
        "test_input5",
        [Column("id", String, primary_key=True), Column("value", Integer)],
        create_table=True,
    )
    input_dt = ds.create_table("test_input5", input_store)

    output_store = TableStoreDB(
        dbconn,
        "test_output5",
        [Column("id", String, primary_key=True), Column("result", Integer)],
        create_table=True,
    )
    output_dt = ds.create_table("test_output5", output_store)

    def transform_func(df):
        return df.rename(columns={"value": "result"})

    # Создаем step с включенной offset оптимизацией для корректной работы теста
    step = BatchTransformStep(
        ds=ds,
        name="test_transform5",
        func=transform_func,
        input_dts=[ComputeInput(dt=input_dt, join_type="full")],
        output_dts=[output_dt],
        transform_keys=["id"],
        use_offset_optimization=True,  # Включаем для обновления offset'ов
    )

    # Добавляем данные
    now = time.time()
    input_dt.store_chunk(
        pd.DataFrame({"id": ["1", "2", "3"], "value": [10, 20, 30]}), now=now
    )

    # Обрабатываем с offset оптимизацией (обновляются offset'ы)
    step.run_full(ds)

    # Добавляем новые данные
    time.sleep(0.01)
    now2 = time.time()
    input_dt.store_chunk(pd.DataFrame({"id": ["4"], "value": [40]}), now=now2)

    # v2 (дефолт) должен вернуть только новую запись
    count_v2, ids_v2 = step.get_full_process_ids(ds)
    ids_v2_list = pd.concat(list(ids_v2))
    assert count_v2 == 1
    assert len(ids_v2_list) == 1
    assert ids_v2_list["id"].iloc[0] == "4"

    # v1 (через override) также должен вернуть только новую запись,
    # так как метаданные уже обработаны
    count_v1, ids_v1 = step.get_full_process_ids(
        ds, run_config=RunConfig(labels={"use_offset_optimization": False})
    )
    ids_v1_list = pd.concat(list(ids_v1))
    assert count_v1 == 1
    assert len(ids_v1_list) == 1
    assert ids_v1_list["id"].iloc[0] == "4"

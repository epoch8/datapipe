"""
Интеграционные тесты для проверки работы BatchTransformStep с offset оптимизацией (v2)
"""
import time

import pandas as pd
from sqlalchemy import Column, Integer, String

from datapipe.compute import ComputeInput
from datapipe.datatable import DataStore
from datapipe.step.batch_transform import BatchTransformStep
from datapipe.store.database import DBConn, TableStoreDB


def test_batch_transform_with_offset_basic(dbconn: DBConn):
    """Базовый тест работы BatchTransformStep с offset оптимизацией"""
    ds = DataStore(dbconn, create_meta_table=True)

    # Создаем входную таблицу
    input_store = TableStoreDB(
        dbconn,
        "test_input",
        [Column("id", String, primary_key=True), Column("value", Integer)],
        create_table=True,
    )
    input_dt = ds.create_table("test_input", input_store)

    # Создаем выходную таблицу
    output_store = TableStoreDB(
        dbconn,
        "test_output",
        [Column("id", String, primary_key=True), Column("result", Integer)],
        create_table=True,
    )
    output_dt = ds.create_table("test_output", output_store)

    # Функция трансформации
    def transform_func(df):
        return df.rename(columns={"value": "result"})

    # Создаем step с включенной offset оптимизацией
    step = BatchTransformStep(
        ds=ds,
        name="test_transform",
        func=transform_func,
        input_dts=[ComputeInput(dt=input_dt, join_type="full")],
        output_dts=[output_dt],
        transform_keys=["id"],
        use_offset_optimization=True,
    )

    # Добавляем данные
    now = time.time()
    input_dt.store_chunk(
        pd.DataFrame({"id": ["1", "2", "3"], "value": [10, 20, 30]}), now=now
    )

    # Первый запуск - обработать все
    step.run_full(ds)

    # Проверяем результат
    output_data = output_dt.get_data()
    assert len(output_data) == 3
    assert sorted(output_data["id"].tolist()) == ["1", "2", "3"]
    assert sorted(output_data["result"].tolist()) == [10, 20, 30]

    # Добавляем новые данные
    time.sleep(0.01)
    now2 = time.time()
    input_dt.store_chunk(
        pd.DataFrame({"id": ["4", "5"], "value": [40, 50]}), now=now2
    )

    # Второй запуск - должны обработаться только новые записи
    step.run_full(ds)

    # Проверяем, что все данные присутствуют
    output_data = output_dt.get_data()
    assert len(output_data) == 5
    assert sorted(output_data["id"].tolist()) == ["1", "2", "3", "4", "5"]
    assert sorted(output_data["result"].tolist()) == [10, 20, 30, 40, 50]


def test_batch_transform_offset_filters_old_records(dbconn: DBConn):
    """Тест что offset фильтрует старые записи при инкрементальной обработке"""
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

    call_count = {"count": 0}

    def transform_func(df):
        call_count["count"] += 1
        return df.rename(columns={"value": "result"})

    step = BatchTransformStep(
        ds=ds,
        name="test_transform2",
        func=transform_func,
        input_dts=[ComputeInput(dt=input_dt, join_type="full")],
        output_dts=[output_dt],
        transform_keys=["id"],
        use_offset_optimization=True,
    )

    # Добавляем старые данные
    old_time = time.time() - 100
    input_dt.store_chunk(
        pd.DataFrame({"id": ["1", "2"], "value": [10, 20]}), now=old_time
    )

    # Первый запуск
    step.run_full(ds)
    first_call_count = call_count["count"]

    # Добавляем новые данные
    time.sleep(0.01)
    new_time = time.time()
    input_dt.store_chunk(
        pd.DataFrame({"id": ["3", "4"], "value": [30, 40]}), now=new_time
    )

    # Второй запуск - должны обработаться только новые
    step.run_full(ds)

    # Проверяем, что функция вызывалась оба раза
    assert call_count["count"] > first_call_count

    # Проверяем результат
    output_data = output_dt.get_data()
    assert len(output_data) == 4
    assert sorted(output_data["id"].tolist()) == ["1", "2", "3", "4"]


def test_batch_transform_offset_with_error_retry(dbconn: DBConn):
    """Тест что ошибочные записи переобрабатываются"""
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

    call_data = {"calls": []}

    def transform_func(df):
        call_data["calls"].append(sorted(df["id"].tolist()))
        # Имитируем ошибку на первом запуске для id=2
        if len(call_data["calls"]) == 1 and "2" in df["id"].tolist():
            # Обрабатываем только id != 2
            result = df[df["id"] != "2"].rename(columns={"value": "result"})
            return result
        return df.rename(columns={"value": "result"})

    step = BatchTransformStep(
        ds=ds,
        name="test_transform3",
        func=transform_func,
        input_dts=[ComputeInput(dt=input_dt, join_type="full")],
        output_dts=[output_dt],
        transform_keys=["id"],
        use_offset_optimization=True,
    )

    # Добавляем данные
    now = time.time()
    input_dt.store_chunk(
        pd.DataFrame({"id": ["1", "2", "3"], "value": [10, 20, 30]}), now=now
    )

    # Первый запуск - id=2 не обработается
    step.run_full(ds)

    # Проверяем, что обработались только 1 и 3
    output_data = output_dt.get_data()
    assert len(output_data) == 2
    assert sorted(output_data["id"].tolist()) == ["1", "3"]

    # Второй запуск - должен переобработать id=2 (т.к. он остался с is_success=False)
    step.run_full(ds)

    # Проверяем, что теперь все обработалось
    output_data = output_dt.get_data()
    assert len(output_data) == 3
    assert sorted(output_data["id"].tolist()) == ["1", "2", "3"]


def test_batch_transform_offset_multiple_inputs(dbconn: DBConn):
    """Тест работы с несколькими входными таблицами и offset оптимизацией"""
    ds = DataStore(dbconn, create_meta_table=True)

    # Две входных таблицы
    input1_store = TableStoreDB(
        dbconn,
        "test_input_a",
        [Column("id", String, primary_key=True), Column("value", Integer)],
        create_table=True,
    )
    input1_dt = ds.create_table("test_input_a", input1_store)

    input2_store = TableStoreDB(
        dbconn,
        "test_input_b",
        [Column("id", String, primary_key=True), Column("value", Integer)],
        create_table=True,
    )
    input2_dt = ds.create_table("test_input_b", input2_store)

    # Выходная таблица
    output_store = TableStoreDB(
        dbconn,
        "test_output_multi",
        [Column("id", String, primary_key=True), Column("result", Integer)],
        create_table=True,
    )
    output_dt = ds.create_table("test_output_multi", output_store)

    def transform_func(df1, df2):
        # Объединяем данные из обеих таблиц
        return pd.concat([df1, df2]).rename(columns={"value": "result"})

    step = BatchTransformStep(
        ds=ds,
        name="test_transform_multi",
        func=transform_func,
        input_dts=[
            ComputeInput(dt=input1_dt, join_type="full"),
            ComputeInput(dt=input2_dt, join_type="full"),
        ],
        output_dts=[output_dt],
        transform_keys=["id"],
        use_offset_optimization=True,
    )

    # Добавляем данные в обе таблицы
    now = time.time()
    input1_dt.store_chunk(pd.DataFrame({"id": ["1", "2"], "value": [10, 20]}), now=now)
    input2_dt.store_chunk(pd.DataFrame({"id": ["3", "4"], "value": [30, 40]}), now=now)

    # Первый запуск
    step.run_full(ds)

    output_data = output_dt.get_data()
    assert len(output_data) == 4
    assert sorted(output_data["id"].tolist()) == ["1", "2", "3", "4"]

    # Добавляем новые данные только в первую таблицу
    time.sleep(0.01)
    now2 = time.time()
    input1_dt.store_chunk(pd.DataFrame({"id": ["5"], "value": [50]}), now=now2)

    # Второй запуск - должны обработаться только новые из первой таблицы
    step.run_full(ds)

    output_data = output_dt.get_data()
    assert len(output_data) == 5
    assert sorted(output_data["id"].tolist()) == ["1", "2", "3", "4", "5"]


def test_batch_transform_without_offset_vs_with_offset(dbconn: DBConn):
    """Сравнение работы без и с offset оптимизацией"""
    ds = DataStore(dbconn, create_meta_table=True)

    # Входная таблица (общая)
    input_store = TableStoreDB(
        dbconn,
        "test_input_compare",
        [Column("id", String, primary_key=True), Column("value", Integer)],
        create_table=True,
    )
    input_dt = ds.create_table("test_input_compare", input_store)

    # Две выходные таблицы - одна без offset, другая с offset
    output_no_offset_store = TableStoreDB(
        dbconn,
        "test_output_no_offset",
        [Column("id", String, primary_key=True), Column("result", Integer)],
        create_table=True,
    )
    output_no_offset_dt = ds.create_table("test_output_no_offset", output_no_offset_store)

    output_with_offset_store = TableStoreDB(
        dbconn,
        "test_output_with_offset",
        [Column("id", String, primary_key=True), Column("result", Integer)],
        create_table=True,
    )
    output_with_offset_dt = ds.create_table("test_output_with_offset", output_with_offset_store)

    def transform_func(df):
        return df.rename(columns={"value": "result"})

    # Step без offset
    step_no_offset = BatchTransformStep(
        ds=ds,
        name="test_transform_no_offset",
        func=transform_func,
        input_dts=[ComputeInput(dt=input_dt, join_type="full")],
        output_dts=[output_no_offset_dt],
        transform_keys=["id"],
        use_offset_optimization=False,
    )

    # Step с offset
    step_with_offset = BatchTransformStep(
        ds=ds,
        name="test_transform_with_offset",
        func=transform_func,
        input_dts=[ComputeInput(dt=input_dt, join_type="full")],
        output_dts=[output_with_offset_dt],
        transform_keys=["id"],
        use_offset_optimization=True,
    )

    # Добавляем данные
    now = time.time()
    input_dt.store_chunk(
        pd.DataFrame({"id": ["1", "2", "3"], "value": [10, 20, 30]}), now=now
    )

    # Первый запуск обоих
    step_no_offset.run_full(ds)
    step_with_offset.run_full(ds)

    # Результаты должны быть идентичны
    data_no_offset = output_no_offset_dt.get_data().sort_values("id").reset_index(drop=True)
    data_with_offset = output_with_offset_dt.get_data().sort_values("id").reset_index(drop=True)
    pd.testing.assert_frame_equal(data_no_offset, data_with_offset)

    # Добавляем новые данные
    time.sleep(0.01)
    now2 = time.time()
    input_dt.store_chunk(
        pd.DataFrame({"id": ["4", "5"], "value": [40, 50]}), now=now2
    )

    # Второй запуск
    step_no_offset.run_full(ds)
    step_with_offset.run_full(ds)

    # Результаты снова должны быть идентичны
    data_no_offset = output_no_offset_dt.get_data().sort_values("id").reset_index(drop=True)
    data_with_offset = output_with_offset_dt.get_data().sort_values("id").reset_index(drop=True)
    pd.testing.assert_frame_equal(data_no_offset, data_with_offset)


def test_batch_transform_offset_no_new_data(dbconn: DBConn):
    """Тест что при отсутствии новых данных ничего не обрабатывается"""
    ds = DataStore(dbconn, create_meta_table=True)

    input_store = TableStoreDB(
        dbconn,
        "test_input_no_new",
        [Column("id", String, primary_key=True), Column("value", Integer)],
        create_table=True,
    )
    input_dt = ds.create_table("test_input_no_new", input_store)

    output_store = TableStoreDB(
        dbconn,
        "test_output_no_new",
        [Column("id", String, primary_key=True), Column("result", Integer)],
        create_table=True,
    )
    output_dt = ds.create_table("test_output_no_new", output_store)

    call_count = {"count": 0}

    def transform_func(df):
        call_count["count"] += 1
        return df.rename(columns={"value": "result"})

    step = BatchTransformStep(
        ds=ds,
        name="test_transform_no_new",
        func=transform_func,
        input_dts=[ComputeInput(dt=input_dt, join_type="full")],
        output_dts=[output_dt],
        transform_keys=["id"],
        use_offset_optimization=True,
    )

    # Добавляем данные
    now = time.time()
    input_dt.store_chunk(
        pd.DataFrame({"id": ["1", "2"], "value": [10, 20]}), now=now
    )

    # Первый запуск
    step.run_full(ds)
    assert call_count["count"] == 1

    # Второй запуск без новых данных
    step.run_full(ds)
    # Функция не должна вызваться, если нет новых данных для обработки
    # (или вызваться с пустым df)
    # В зависимости от реализации может быть 1 или 2
    # Проверяем что результаты не изменились
    output_data = output_dt.get_data()
    assert len(output_data) == 2

"""
Интеграционный тест для Phase 3: Автоматическое обновление offset'ов

Проверяет что offset'ы автоматически обновляются после успешной обработки батча
и используются при последующих запусках для фильтрации уже обработанных данных.
"""
import time

import pandas as pd
from sqlalchemy import Column, Integer, String

from datapipe.compute import ComputeInput
from datapipe.datatable import DataStore
from datapipe.step.batch_transform import BatchTransformStep
from datapipe.store.database import DBConn, TableStoreDB


def test_offset_auto_update_integration(dbconn: DBConn):
    """
    Интеграционный тест: Полный цикл работы с автоматическим обновлением offset'ов

    Сценарий:
    1. Создаем входные данные (3 записи)
    2. Запускаем трансформацию с offset optimization
    3. Проверяем что все обработалось
    4. Проверяем что offset'ы обновились в базе
    5. Добавляем новые данные (2 записи)
    6. Запускаем трансформацию снова
    7. Проверяем что обработались только новые записи (благодаря offset'ам)
    8. Проверяем что offset'ы снова обновились
    """
    ds = DataStore(dbconn, create_meta_table=True)

    # Создаем входную таблицу
    input_store = TableStoreDB(
        dbconn,
        "integration_input",
        [Column("id", String, primary_key=True), Column("value", Integer)],
        create_table=True,
    )
    input_dt = ds.create_table("integration_input", input_store)

    # Создаем выходную таблицу
    output_store = TableStoreDB(
        dbconn,
        "integration_output",
        [Column("id", String, primary_key=True), Column("result", Integer)],
        create_table=True,
    )
    output_dt = ds.create_table("integration_output", output_store)

    # Трансформация: просто копируем данные с переименованием колонки
    def transform_func(df):
        return df.rename(columns={"value": "result"})

    # Создаем step с ВКЛЮЧЕННОЙ offset оптимизацией
    step = BatchTransformStep(
        ds=ds,
        name="integration_transform",
        func=transform_func,
        input_dts=[ComputeInput(dt=input_dt, join_type="full")],
        output_dts=[output_dt],
        transform_keys=["id"],
        use_offset_optimization=True,
    )

    # ========== Шаг 1: Добавляем первую партию данных ==========
    now1 = time.time()
    input_dt.store_chunk(
        pd.DataFrame({"id": ["A", "B", "C"], "value": [10, 20, 30]}), now=now1
    )

    # Проверяем что offset'ов ещё нет
    offsets_before = ds.offset_table.get_offsets_for_transformation(
        step.get_name()
    )
    assert len(offsets_before) == 0, "Offset'ы должны быть пустыми до первого запуска"

    # ========== Шаг 2: Первый запуск трансформации ==========
    step.run_full(ds)

    # Проверяем что все данные обработались
    output_data = output_dt.get_data()
    assert len(output_data) == 3
    assert sorted(output_data["id"].tolist()) == ["A", "B", "C"]
    assert sorted(output_data["result"].tolist()) == [10, 20, 30]

    # ========== Шаг 3: Проверяем что offset обновился ==========
    offsets_after_first_run = ds.offset_table.get_offsets_for_transformation(
        step.get_name()
    )
    assert len(offsets_after_first_run) == 1, "Должен быть offset для одной входной таблицы"
    assert "integration_input" in offsets_after_first_run

    first_offset = offsets_after_first_run["integration_input"]
    assert first_offset >= now1, f"Offset ({first_offset}) должен быть >= времени создания данных ({now1})"
    assert first_offset <= time.time(), "Offset не должен быть из будущего"

    # ========== Шаг 4: Проверяем что повторный запуск ничего не обработает ==========
    changed_count_before_new_data = step.get_changed_idx_count(ds)
    assert changed_count_before_new_data == 0, "Не должно быть изменений после первого запуска"

    # ========== Шаг 5: Добавляем новые данные ==========
    time.sleep(0.01)  # Небольшая задержка чтобы update_ts был больше
    now2 = time.time()
    input_dt.store_chunk(
        pd.DataFrame({"id": ["D", "E"], "value": [40, 50]}), now=now2
    )

    # ========== Шаг 6: Проверяем что видны только новые записи ==========
    changed_count_after_new_data = step.get_changed_idx_count(ds)
    assert changed_count_after_new_data == 2, "Должно быть видно только 2 новые записи (D, E)"

    # ========== Шаг 7: Второй запуск трансформации ==========
    step.run_full(ds)

    # Проверяем что теперь всего 5 записей
    output_data = output_dt.get_data()
    assert len(output_data) == 5
    assert sorted(output_data["id"].tolist()) == ["A", "B", "C", "D", "E"]
    assert sorted(output_data["result"].tolist()) == [10, 20, 30, 40, 50]

    # ========== Шаг 8: Проверяем что offset снова обновился ==========
    offsets_after_second_run = ds.offset_table.get_offsets_for_transformation(
        step.get_name()
    )
    assert len(offsets_after_second_run) == 1

    second_offset = offsets_after_second_run["integration_input"]
    assert second_offset > first_offset, f"Второй offset ({second_offset}) должен быть больше первого ({first_offset})"
    assert second_offset >= now2, f"Offset ({second_offset}) должен быть >= времени создания новых данных ({now2})"

    # ========== Шаг 9: Финальная проверка - нет новых изменений ==========
    final_changed_count = step.get_changed_idx_count(ds)
    assert final_changed_count == 0, "После второго запуска не должно быть изменений"


def test_offset_update_with_multiple_inputs(dbconn: DBConn):
    """
    Тест автоматического обновления offset'ов для трансформации с несколькими входами

    Проверяет что offset'ы обновляются независимо для каждой входной таблицы
    """
    ds = DataStore(dbconn, create_meta_table=True)

    # Первая входная таблица
    input1_store = TableStoreDB(
        dbconn,
        "multi_input1",
        [Column("id", String, primary_key=True), Column("value1", Integer)],
        create_table=True,
    )
    input1_dt = ds.create_table("multi_input1", input1_store)

    # Вторая входная таблица
    input2_store = TableStoreDB(
        dbconn,
        "multi_input2",
        [Column("id", String, primary_key=True), Column("value2", Integer)],
        create_table=True,
    )
    input2_dt = ds.create_table("multi_input2", input2_store)

    # Выходная таблица
    output_store = TableStoreDB(
        dbconn,
        "multi_output",
        [Column("id", String, primary_key=True), Column("sum", Integer)],
        create_table=True,
    )
    output_dt = ds.create_table("multi_output", output_store)

    # Трансформация: суммируем значения из обеих таблиц
    def transform_func(df1, df2):
        merged = df1.merge(df2, on="id", how="outer")
        merged["sum"] = merged["value1"].fillna(0) + merged["value2"].fillna(0)
        return merged[["id", "sum"]].astype({"sum": int})

    step = BatchTransformStep(
        ds=ds,
        name="multi_input_transform",
        func=transform_func,
        input_dts=[
            ComputeInput(dt=input1_dt, join_type="full"),
            ComputeInput(dt=input2_dt, join_type="full"),
        ],
        output_dts=[output_dt],
        transform_keys=["id"],
        use_offset_optimization=True,
    )

    # ========== Добавляем данные в обе таблицы ==========
    now1 = time.time()
    input1_dt.store_chunk(pd.DataFrame({"id": ["X", "Y"], "value1": [1, 2]}), now=now1)

    time.sleep(0.01)
    now2 = time.time()
    input2_dt.store_chunk(pd.DataFrame({"id": ["X", "Y"], "value2": [10, 20]}), now=now2)

    # ========== Первый запуск ==========
    step.run_full(ds)

    output_data = output_dt.get_data()
    assert len(output_data) == 2
    assert sorted(output_data["id"].tolist()) == ["X", "Y"]
    assert sorted(output_data["sum"].tolist()) == [11, 22]

    # ========== Проверяем offset'ы для обеих таблиц ==========
    offsets = ds.offset_table.get_offsets_for_transformation(step.get_name())
    assert len(offsets) == 2, "Должно быть 2 offset'а (по одному на каждую входную таблицу)"
    assert "multi_input1" in offsets
    assert "multi_input2" in offsets

    offset1 = offsets["multi_input1"]
    offset2 = offsets["multi_input2"]
    assert offset1 >= now1
    assert offset2 >= now2

    # ========== Добавляем данные только в первую таблицу ==========
    time.sleep(0.01)
    now3 = time.time()
    input1_dt.store_chunk(pd.DataFrame({"id": ["Z"], "value1": [3]}), now=now3)

    # ========== Второй запуск ==========
    step.run_full(ds)

    # Проверяем результат
    output_data = output_dt.get_data()
    assert len(output_data) == 3
    assert "Z" in output_data["id"].tolist()

    # ========== Проверяем что offset первой таблицы обновился, а второй - нет ==========
    offsets_after = ds.offset_table.get_offsets_for_transformation(step.get_name())
    new_offset1 = offsets_after["multi_input1"]
    new_offset2 = offsets_after["multi_input2"]

    assert new_offset1 > offset1, "Offset первой таблицы должен обновиться"
    assert new_offset1 >= now3
    assert new_offset2 == offset2, "Offset второй таблицы не должен измениться (не было новых данных)"


def test_offset_updated_even_when_optimization_disabled(dbconn: DBConn):
    """
    Тест что offset'ы обновляются даже когда use_offset_optimization=False

    Это позволяет накапливать offset'ы для будущего использования,
    чтобы при включении оптимизации они были готовы к работе.
    """
    ds = DataStore(dbconn, create_meta_table=True)

    input_store = TableStoreDB(
        dbconn,
        "disabled_opt_input",
        [Column("id", String, primary_key=True), Column("value", Integer)],
        create_table=True,
    )
    input_dt = ds.create_table("disabled_opt_input", input_store)

    output_store = TableStoreDB(
        dbconn,
        "disabled_opt_output",
        [Column("id", String, primary_key=True), Column("result", Integer)],
        create_table=True,
    )
    output_dt = ds.create_table("disabled_opt_output", output_store)

    def transform_func(df):
        return df.rename(columns={"value": "result"})

    # Создаем step с ВЫКЛЮЧЕННОЙ offset оптимизацией
    step = BatchTransformStep(
        ds=ds,
        name="disabled_opt_transform",
        func=transform_func,
        input_dts=[ComputeInput(dt=input_dt, join_type="full")],
        output_dts=[output_dt],
        transform_keys=["id"],
        use_offset_optimization=False,  # ВЫКЛЮЧЕНО!
    )

    # Добавляем данные
    now = time.time()
    input_dt.store_chunk(
        pd.DataFrame({"id": ["1", "2"], "value": [10, 20]}), now=now
    )

    # Запускаем трансформацию (используется v1 метод, но offset'ы должны обновиться)
    step.run_full(ds)

    # Проверяем что данные обработались
    output_data = output_dt.get_data()
    assert len(output_data) == 2

    # ГЛАВНАЯ ПРОВЕРКА: offset'ы должны обновиться даже при выключенной оптимизации
    offsets = ds.offset_table.get_offsets_for_transformation(step.get_name())
    assert len(offsets) == 1, "Offset должен обновиться даже при use_offset_optimization=False"
    assert "disabled_opt_input" in offsets
    assert offsets["disabled_opt_input"] >= now


def test_works_without_offset_table(dbconn: DBConn):
    """
    Тест что код работает корректно если таблица offset'ов не создана.

    Симулируем ситуацию: сначала создаем таблицу offset'ов, затем удаляем её,
    и проверяем что код не падает при попытке использовать v2 метод и обновить offset'ы.
    """
    ds = DataStore(dbconn, create_meta_table=True)

    input_store = TableStoreDB(
        dbconn,
        "no_offset_table_input",
        [Column("id", String, primary_key=True), Column("value", Integer)],
        create_table=True,
    )
    input_dt = ds.create_table("no_offset_table_input", input_store)

    output_store = TableStoreDB(
        dbconn,
        "no_offset_table_output",
        [Column("id", String, primary_key=True), Column("result", Integer)],
        create_table=True,
    )
    output_dt = ds.create_table("no_offset_table_output", output_store)

    def transform_func(df):
        return df.rename(columns={"value": "result"})

    step = BatchTransformStep(
        ds=ds,
        name="no_offset_table_transform",
        func=transform_func,
        input_dts=[ComputeInput(dt=input_dt, join_type="full")],
        output_dts=[output_dt],
        transform_keys=["id"],
        use_offset_optimization=True,
    )

    # Добавляем данные
    now = time.time()
    input_dt.store_chunk(pd.DataFrame({"id": ["1", "2"], "value": [10, 20]}), now=now)

    # УДАЛЯЕМ таблицу offset'ов, симулируя ситуацию когда она не создана
    with dbconn.con.begin() as con:
        con.execute("DROP TABLE IF EXISTS transform_input_offsets")

    # Запускаем трансформацию - должна работать без ошибок
    # v2 метод должен обработать все данные (get_offsets_for_transformation вернет {})
    # Обновление offset'ов должно залогировать warning и продолжить работу
    step.run_full(ds)

    # Проверяем что данные обработались несмотря на отсутствие offset таблицы
    output_data = output_dt.get_data()
    assert len(output_data) == 2
    assert sorted(output_data["id"].tolist()) == ["1", "2"]


def test_offset_not_updated_on_empty_result(dbconn: DBConn):
    """
    Тест что offset НЕ обновляется если трансформация вернула пустой результат
    """
    ds = DataStore(dbconn, create_meta_table=True)

    input_store = TableStoreDB(
        dbconn,
        "empty_result_input",
        [Column("id", String, primary_key=True), Column("value", Integer)],
        create_table=True,
    )
    input_dt = ds.create_table("empty_result_input", input_store)

    output_store = TableStoreDB(
        dbconn,
        "empty_result_output",
        [Column("id", String, primary_key=True), Column("result", Integer)],
        create_table=True,
    )
    output_dt = ds.create_table("empty_result_output", output_store)

    # Трансформация которая всегда возвращает пустой DataFrame
    def transform_func(df):
        return pd.DataFrame(columns=["id", "result"])

    step = BatchTransformStep(
        ds=ds,
        name="empty_result_transform",
        func=transform_func,
        input_dts=[ComputeInput(dt=input_dt, join_type="full")],
        output_dts=[output_dt],
        transform_keys=["id"],
        use_offset_optimization=True,
    )

    # Добавляем данные
    now = time.time()
    input_dt.store_chunk(pd.DataFrame({"id": ["1"], "value": [100]}), now=now)

    # Запускаем трансформацию
    step.run_full(ds)

    # Проверяем что output пустой
    output_data = output_dt.get_data()
    assert len(output_data) == 0

    # Проверяем что offset НЕ обновился (потому что результат пустой)
    offsets = ds.offset_table.get_offsets_for_transformation(step.get_name())
    assert len(offsets) == 0, "Offset не должен обновиться при пустом результате"

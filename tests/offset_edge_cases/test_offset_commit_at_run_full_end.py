"""
Тесты для нового механизма коммита offset'ов в конце run_full.

Покрываемые сценарии:
1. Одна трансформация - offset коммитится в конце run_full
2. Цепочка трансформаций - offset'ы коммитятся независимо для каждой трансформации
3. Многотабличная трансформация - offset'ы обновляются для всех входных таблиц
4. Частичная обработка (сбой mid-batch) - offset НЕ обновляется
5. Пустой результат - offset НЕ обновляется
6. run_changelist - offset НЕ обновляется
"""
import time

import pandas as pd
import pytest
from sqlalchemy import Column, Integer, String

from datapipe.compute import ComputeInput
from datapipe.datatable import DataStore
from datapipe.step.batch_transform import BatchTransformStep
from datapipe.store.database import DBConn, TableStoreDB
from datapipe.types import ChangeList


def test_single_transform_offset_committed_at_end(dbconn: DBConn):
    """
    Тест: Одна трансформация, offset коммитится в конце run_full.

    Сценарий:
    1. Создать 12 записей с разными update_ts
    2. Запустить run_full (batch_size=5) - 3 батча
    3. Проверить что offset = MAX(update_ts) из всех обработанных записей
    4. Повторный запуск - выборка пустая
    """
    ds = DataStore(dbconn, create_meta_table=True)

    # Входная таблица
    input_store = TableStoreDB(
        dbconn,
        "single_input",
        [
            Column("id", String, primary_key=True),
            Column("value", Integer),
        ],
        create_table=True,
    )
    input_dt = ds.create_table("single_input", input_store)

    # Выходная таблица
    output_store = TableStoreDB(
        dbconn,
        "single_output",
        [
            Column("id", String, primary_key=True),
            Column("value", Integer),
        ],
        create_table=True,
    )
    output_dt = ds.create_table("single_output", output_store)

    def copy_func(df):
        return df[["id", "value"]]

    step = BatchTransformStep(
        ds=ds,
        name="single_transform",
        func=copy_func,
        input_dts=[ComputeInput(dt=input_dt, join_type="full")],
        output_dts=[output_dt],
        transform_keys=["id"],
        use_offset_optimization=True,
        chunk_size=5,  # 3 батча по 5, 5, 2
    )

    # Создаем 12 записей с разными update_ts
    t1 = time.time()
    df1 = pd.DataFrame({"id": [f"rec_{i:02d}" for i in range(5)], "value": list(range(5))})
    input_dt.store_chunk(df1, now=t1)

    time.sleep(0.01)
    t2 = time.time()
    df2 = pd.DataFrame({"id": [f"rec_{i:02d}" for i in range(5, 10)], "value": list(range(5, 10))})
    input_dt.store_chunk(df2, now=t2)

    time.sleep(0.01)
    t3 = time.time()
    df3 = pd.DataFrame({"id": [f"rec_{i:02d}" for i in range(10, 12)], "value": list(range(10, 12))})
    input_dt.store_chunk(df3, now=t3)

    # Запускаем run_full
    step.run_full(ds)

    # Проверяем что все записи обработаны
    output_data = output_dt.get_data()
    assert len(output_data) == 12, "Все 12 записей должны быть обработаны"

    # Проверяем offset - должен быть MAX(update_ts) = t3
    offsets = ds.offset_table.get_offsets_for_transformation(step.get_name())
    assert "single_input" in offsets
    offset_value = offsets["single_input"]
    assert offset_value >= t3, f"Offset должен быть >= {t3}, получено {offset_value}"

    # Повторный запуск - выборка должна быть пустой
    idx_count, idx_gen = step.get_full_process_ids(ds=ds, run_config=None)
    assert idx_count == 0, "После полной обработки выборка должна быть пустой"


def test_transform_chain_independent_offsets(dbconn: DBConn):
    """
    Тест: Цепочка трансформаций, offset'ы коммитятся независимо.

    Сценарий:
    1. Создать цепочку: input -> transform1 -> intermediate -> transform2 -> output
    2. Запустить transform1 полностью
    3. Проверить что offset для input обновлен
    4. Запустить transform2 полностью
    5. Проверить что offset для intermediate обновлен
    6. Offset'ы независимы
    """
    ds = DataStore(dbconn, create_meta_table=True)

    # Входная таблица
    input_store = TableStoreDB(
        dbconn,
        "chain_input",
        [
            Column("id", String, primary_key=True),
            Column("value", Integer),
        ],
        create_table=True,
    )
    input_dt = ds.create_table("chain_input", input_store)

    # Промежуточная таблица
    intermediate_store = TableStoreDB(
        dbconn,
        "chain_intermediate",
        [
            Column("id", String, primary_key=True),
            Column("value", Integer),
        ],
        create_table=True,
    )
    intermediate_dt = ds.create_table("chain_intermediate", intermediate_store)

    # Выходная таблица
    output_store = TableStoreDB(
        dbconn,
        "chain_output",
        [
            Column("id", String, primary_key=True),
            Column("value", Integer),
        ],
        create_table=True,
    )
    output_dt = ds.create_table("chain_output", output_store)

    def copy_func(df):
        return df[["id", "value"]]

    # Первая трансформация
    step1 = BatchTransformStep(
        ds=ds,
        name="chain_transform1",
        func=copy_func,
        input_dts=[ComputeInput(dt=input_dt, join_type="full")],
        output_dts=[intermediate_dt],
        transform_keys=["id"],
        use_offset_optimization=True,
        chunk_size=5,
    )

    # Вторая трансформация
    step2 = BatchTransformStep(
        ds=ds,
        name="chain_transform2",
        func=copy_func,
        input_dts=[ComputeInput(dt=intermediate_dt, join_type="full")],
        output_dts=[output_dt],
        transform_keys=["id"],
        use_offset_optimization=True,
        chunk_size=5,
    )

    # Создаем данные в input
    t1 = time.time()
    df1 = pd.DataFrame({"id": [f"rec_{i:02d}" for i in range(7)], "value": list(range(7))})
    input_dt.store_chunk(df1, now=t1)

    # Запускаем первую трансформацию
    step1.run_full(ds)

    # Проверяем offset для input
    offsets1 = ds.offset_table.get_offsets_for_transformation(step1.get_name())
    assert "chain_input" in offsets1
    offset1_value = offsets1["chain_input"]
    assert offset1_value >= t1

    # Проверяем что intermediate заполнена
    intermediate_data = intermediate_dt.get_data()
    assert len(intermediate_data) == 7

    # Запускаем вторую трансформацию
    step2.run_full(ds)

    # Проверяем offset для intermediate
    offsets2 = ds.offset_table.get_offsets_for_transformation(step2.get_name())
    assert "chain_intermediate" in offsets2
    offset2_value = offsets2["chain_intermediate"]

    # Получаем update_ts intermediate таблицы
    intermediate_meta = intermediate_dt.meta_table.get_metadata()
    max_intermediate_ts = intermediate_meta["update_ts"].max()
    assert offset2_value >= max_intermediate_ts

    # Проверяем что output заполнена
    output_data = output_dt.get_data()
    assert len(output_data) == 7

    # Offset'ы независимы - у каждой трансформации свой набор offset'ов
    assert offsets1.keys() != offsets2.keys(), "Offset'ы должны быть независимы для разных трансформаций"


def test_multi_table_transform_all_offsets_updated(dbconn: DBConn):
    """
    Тест: Многотабличная трансформация, offset'ы для всех входных таблиц.

    Сценарий:
    1. Создать трансформацию с 2 входными таблицами
    2. Добавить данные в обе таблицы
    3. Запустить run_full
    4. Проверить что offset'ы обновлены для ОБЕИХ входных таблиц
    """
    ds = DataStore(dbconn, create_meta_table=True)

    # Первая входная таблица
    input1_store = TableStoreDB(
        dbconn,
        "multi_input1",
        [
            Column("id", String, primary_key=True),
            Column("value1", Integer),
        ],
        create_table=True,
    )
    input1_dt = ds.create_table("multi_input1", input1_store)

    # Вторая входная таблица
    input2_store = TableStoreDB(
        dbconn,
        "multi_input2",
        [
            Column("id", String, primary_key=True),
            Column("value2", Integer),
        ],
        create_table=True,
    )
    input2_dt = ds.create_table("multi_input2", input2_store)

    # Выходная таблица
    output_store = TableStoreDB(
        dbconn,
        "multi_output",
        [
            Column("id", String, primary_key=True),
            Column("value1", Integer),
            Column("value2", Integer),
        ],
        create_table=True,
    )
    output_dt = ds.create_table("multi_output", output_store)

    def merge_func(df1, df2):
        # Объединяем по id
        result = df1.merge(df2, on="id", how="outer")
        return result[["id", "value1", "value2"]].fillna(0).astype({"value1": int, "value2": int})

    step = BatchTransformStep(
        ds=ds,
        name="multi_transform",
        func=merge_func,
        input_dts=[
            ComputeInput(dt=input1_dt, join_type="full"),
            ComputeInput(dt=input2_dt, join_type="full"),
        ],
        output_dts=[output_dt],
        transform_keys=["id"],
        use_offset_optimization=True,
        chunk_size=5,
    )

    # Создаем данные в первой таблице
    t1 = time.time()
    df1 = pd.DataFrame({"id": [f"rec_{i:02d}" for i in range(6)], "value1": list(range(6))})
    input1_dt.store_chunk(df1, now=t1)

    time.sleep(0.01)

    # Создаем данные во второй таблице
    t2 = time.time()
    df2 = pd.DataFrame({"id": [f"rec_{i:02d}" for i in range(3, 9)], "value2": list(range(3, 9))})
    input2_dt.store_chunk(df2, now=t2)

    # Запускаем трансформацию
    step.run_full(ds)

    # Проверяем что данные обработаны
    output_data = output_dt.get_data()
    assert len(output_data) > 0, "Должны быть обработанные записи"

    # Проверяем offset'ы для ОБЕИХ входных таблиц
    offsets = ds.offset_table.get_offsets_for_transformation(step.get_name())
    assert "multi_input1" in offsets, "Offset для первой входной таблицы должен быть установлен"
    assert "multi_input2" in offsets, "Offset для второй входной таблицы должен быть установлен"

    offset1_value = offsets["multi_input1"]
    offset2_value = offsets["multi_input2"]

    assert offset1_value >= t1, f"Offset для input1 должен быть >= {t1}"
    assert offset2_value >= t2, f"Offset для input2 должен быть >= {t2}"


def test_partial_processing_offset_not_updated(dbconn: DBConn):
    """
    Тест: Частичная обработка (сбой mid-batch), offset НЕ обновляется.

    Сценарий:
    1. Создать 12 записей
    2. Симулировать обработку только первого батча (без run_full)
    3. Проверить что offset НЕ обновился
    4. Запустить полный run_full
    5. Проверить что offset обновился до максимума
    """
    ds = DataStore(dbconn, create_meta_table=True)

    # Входная таблица
    input_store = TableStoreDB(
        dbconn,
        "partial_input",
        [
            Column("id", String, primary_key=True),
            Column("value", Integer),
        ],
        create_table=True,
    )
    input_dt = ds.create_table("partial_input", input_store)

    # Выходная таблица
    output_store = TableStoreDB(
        dbconn,
        "partial_output",
        [
            Column("id", String, primary_key=True),
            Column("value", Integer),
        ],
        create_table=True,
    )
    output_dt = ds.create_table("partial_output", output_store)

    def copy_func(df):
        return df[["id", "value"]]

    step = BatchTransformStep(
        ds=ds,
        name="partial_transform",
        func=copy_func,
        input_dts=[ComputeInput(dt=input_dt, join_type="full")],
        output_dts=[output_dt],
        transform_keys=["id"],
        use_offset_optimization=True,
        chunk_size=5,
    )

    # Создаем записи
    t1 = time.time()
    df1 = pd.DataFrame({"id": [f"rec_{i:02d}" for i in range(12)], "value": list(range(12))})
    input_dt.store_chunk(df1, now=t1)

    # Получаем первый батч и обрабатываем его напрямую (симуляция частичной обработки)
    idx_count, idx_gen = step.get_full_process_ids(ds=ds, run_config=None)
    assert idx_count == 3, "Должно быть 3 батча"

    # Обрабатываем только первый батч (без вызова run_full)
    first_batch = next(idx_gen)
    step.process_batch(ds, first_batch, run_config=None)

    # Проверяем что offset НЕ обновился (нет вызова run_full)
    offsets_after_partial = ds.offset_table.get_offsets_for_transformation(step.get_name())
    assert len(offsets_after_partial) == 0, "Offset не должен обновиться при частичной обработке"

    # Теперь запускаем полный run_full
    step.run_full(ds)

    # Проверяем что offset обновился
    offsets_after_full = ds.offset_table.get_offsets_for_transformation(step.get_name())
    assert "partial_input" in offsets_after_full
    offset_value = offsets_after_full["partial_input"]
    assert offset_value >= t1

    # Проверяем что все записи обработаны (не дублируются)
    output_data = output_dt.get_data()
    assert len(output_data) == 12, "Все 12 записей должны быть обработаны ровно один раз"


def test_empty_result_offset_not_updated(dbconn: DBConn):
    """
    Тест: Пустой результат трансформации, offset НЕ обновляется.

    Сценарий:
    1. Создать данные
    2. Трансформация возвращает пустой DataFrame
    3. Запустить run_full
    4. Проверить что offset НЕ обновился
    """
    ds = DataStore(dbconn, create_meta_table=True)

    # Входная таблица
    input_store = TableStoreDB(
        dbconn,
        "empty_input",
        [
            Column("id", String, primary_key=True),
            Column("value", Integer),
        ],
        create_table=True,
    )
    input_dt = ds.create_table("empty_input", input_store)

    # Выходная таблица
    output_store = TableStoreDB(
        dbconn,
        "empty_output",
        [
            Column("id", String, primary_key=True),
            Column("result", Integer),
        ],
        create_table=True,
    )
    output_dt = ds.create_table("empty_output", output_store)

    # Трансформация всегда возвращает пустой DataFrame
    def empty_func(df):
        return pd.DataFrame(columns=["id", "result"])

    step = BatchTransformStep(
        ds=ds,
        name="empty_transform",
        func=empty_func,
        input_dts=[ComputeInput(dt=input_dt, join_type="full")],
        output_dts=[output_dt],
        transform_keys=["id"],
        use_offset_optimization=True,
        chunk_size=5,
    )

    # Создаем данные
    now = time.time()
    df = pd.DataFrame({"id": [f"rec_{i:02d}" for i in range(7)], "value": list(range(7))})
    input_dt.store_chunk(df, now=now)

    # Запускаем run_full
    step.run_full(ds)

    # Проверяем что output пустой
    output_data = output_dt.get_data()
    assert len(output_data) == 0, "Output должен быть пустым"

    # Проверяем что offset НЕ обновился
    offsets = ds.offset_table.get_offsets_for_transformation(step.get_name())
    assert len(offsets) == 0, "Offset не должен обновиться при пустом результате"


def test_run_changelist_does_not_update_offset(dbconn: DBConn):
    """
    Тест: run_changelist НЕ обновляет offset.

    Сценарий:
    1. Создать 3 записи (rec_1, rec_2, rec_3) с update_ts = T1, T2, T3
    2. Запустить run_full - offset = T3
    3. Создать новые записи (rec_4, rec_5) с update_ts = T4, T5
    4. Запустить run_changelist только для rec_5
    5. Проверить что offset остался = T3 (НЕ перепрыгнул на T5)
    6. Запустить run_full
    7. Проверить что rec_4 обработана (не пропущена)
    """
    ds = DataStore(dbconn, create_meta_table=True)

    # Входная таблица
    input_store = TableStoreDB(
        dbconn,
        "changelist_input",
        [
            Column("id", String, primary_key=True),
            Column("value", Integer),
        ],
        create_table=True,
    )
    input_dt = ds.create_table("changelist_input", input_store)

    # Выходная таблица
    output_store = TableStoreDB(
        dbconn,
        "changelist_output",
        [
            Column("id", String, primary_key=True),
            Column("value", Integer),
        ],
        create_table=True,
    )
    output_dt = ds.create_table("changelist_output", output_store)

    def copy_func(df):
        return df[["id", "value"]]

    step = BatchTransformStep(
        ds=ds,
        name="changelist_transform",
        func=copy_func,
        input_dts=[ComputeInput(dt=input_dt, join_type="full")],
        output_dts=[output_dt],
        transform_keys=["id"],
        use_offset_optimization=True,
        chunk_size=5,
    )

    # Создаем начальные записи
    t1 = time.time()
    df1 = pd.DataFrame({"id": ["rec_1"], "value": [1]})
    input_dt.store_chunk(df1, now=t1)

    time.sleep(0.01)
    t2 = time.time()
    df2 = pd.DataFrame({"id": ["rec_2"], "value": [2]})
    input_dt.store_chunk(df2, now=t2)

    time.sleep(0.01)
    t3 = time.time()
    df3 = pd.DataFrame({"id": ["rec_3"], "value": [3]})
    input_dt.store_chunk(df3, now=t3)

    # Запускаем run_full
    step.run_full(ds)

    # Проверяем offset
    offsets = ds.offset_table.get_offsets_for_transformation(step.get_name())
    offset_after_full = offsets[input_dt.name]
    assert offset_after_full >= t3

    # Создаем новые записи
    time.sleep(0.01)
    t4 = time.time()
    df4 = pd.DataFrame({"id": ["rec_4"], "value": [4]})
    input_dt.store_chunk(df4, now=t4)

    time.sleep(0.01)
    t5 = time.time()
    df5 = pd.DataFrame({"id": ["rec_5"], "value": [5]})
    input_dt.store_chunk(df5, now=t5)

    # Запускаем run_changelist ТОЛЬКО для rec_5
    change_list = ChangeList()
    change_list.append(input_dt.name, df5[["id"]])
    step.run_changelist(ds, change_list)

    # Проверяем что rec_5 обработана
    output_data_after_changelist = output_dt.get_data()
    assert "rec_5" in output_data_after_changelist["id"].values

    # КРИТИЧНО: Проверяем что offset НЕ изменился (остался T3)
    offsets_after_changelist = ds.offset_table.get_offsets_for_transformation(step.get_name())
    offset_after_changelist = offsets_after_changelist[input_dt.name]
    assert offset_after_changelist == offset_after_full, (
        f"Offset не должен измениться после run_changelist. "
        f"Было: {offset_after_full}, стало: {offset_after_changelist}"
    )

    # Запускаем run_full - rec_4 должна быть обработана
    step.run_full(ds)

    # Проверяем что rec_4 обработана
    output_data_final = output_dt.get_data()
    assert len(output_data_final) == 5, "Все 5 записей должны быть обработаны"
    assert "rec_4" in output_data_final["id"].values, "rec_4 не должна быть пропущена"

    # Проверяем что offset обновился до максимума обработанных записей
    # После run_full обрабатываются rec_4 (t4) и rec_5 (t5 - уже была обработана в changelist, но обновилась)
    # Offset должен быть >= t4 (так как rec_4 точно обработана в этом run_full)
    offsets_final = ds.offset_table.get_offsets_for_transformation(step.get_name())
    offset_final = offsets_final[input_dt.name]
    assert offset_final >= t4, "Offset должен обновиться после run_full (минимум до t4)"
    # Offset может быть >= t5 если rec_5 тоже переобработалась
    assert offset_final >= offset_after_full, "Offset должен увеличиться по сравнению с предыдущим значением"


def test_multiple_batches_offset_is_max(dbconn: DBConn):
    """
    Тест: При обработке нескольких батчей offset = MAX(update_ts) из всех батчей.

    Сценарий:
    1. Создать 3 группы записей с разными update_ts: T1, T2, T3
    2. Размер батча = 5, будет 3 батча
    3. Запустить run_full
    4. Проверить что offset = MAX(T1, T2, T3) = T3
    """
    ds = DataStore(dbconn, create_meta_table=True)

    # Входная таблица
    input_store = TableStoreDB(
        dbconn,
        "maxbatch_input",
        [
            Column("id", String, primary_key=True),
            Column("value", Integer),
        ],
        create_table=True,
    )
    input_dt = ds.create_table("maxbatch_input", input_store)

    # Выходная таблица
    output_store = TableStoreDB(
        dbconn,
        "maxbatch_output",
        [
            Column("id", String, primary_key=True),
            Column("value", Integer),
        ],
        create_table=True,
    )
    output_dt = ds.create_table("maxbatch_output", output_store)

    def copy_func(df):
        return df[["id", "value"]]

    step = BatchTransformStep(
        ds=ds,
        name="maxbatch_transform",
        func=copy_func,
        input_dts=[ComputeInput(dt=input_dt, join_type="full")],
        output_dts=[output_dt],
        transform_keys=["id"],
        use_offset_optimization=True,
        chunk_size=5,
    )

    # Создаем записи с разными update_ts
    # Группа 1: T1
    t1 = time.time()
    df1 = pd.DataFrame({"id": [f"rec_t1_{i:02d}" for i in range(4)], "value": list(range(4))})
    input_dt.store_chunk(df1, now=t1)

    time.sleep(0.01)

    # Группа 2: T2
    t2 = time.time()
    df2 = pd.DataFrame({"id": [f"rec_t2_{i:02d}" for i in range(5)], "value": list(range(5))})
    input_dt.store_chunk(df2, now=t2)

    time.sleep(0.01)

    # Группа 3: T3
    t3 = time.time()
    df3 = pd.DataFrame({"id": [f"rec_t3_{i:02d}" for i in range(3)], "value": list(range(3))})
    input_dt.store_chunk(df3, now=t3)

    # Запускаем run_full
    step.run_full(ds)

    # Проверяем что все записи обработаны
    output_data = output_dt.get_data()
    assert len(output_data) == 12, "Все 12 записей должны быть обработаны"

    # Проверяем offset - должен быть MAX(T1, T2, T3) = T3
    offsets = ds.offset_table.get_offsets_for_transformation(step.get_name())
    offset_value = offsets[input_dt.name]
    assert offset_value >= t3, f"Offset должен быть >= MAX(T1,T2,T3) = {t3}, получено {offset_value}"

    # Проверяем что offset не меньше минимального
    assert offset_value >= t1, f"Offset должен быть >= T1 = {t1}"

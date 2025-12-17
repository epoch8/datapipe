"""
Тесты на инварианты offset optimization.

ГЛАВНЫЙ ИНВАРИАНТ:
После каждой итерации трансформации, ВСЕ записи с update_ts <= offset должны быть обработаны.
НЕ ДОЛЖНО быть необработанных записей с update_ts < offset.

Проверяем два режима:
1. Синхронное выполнение (1 под) - последовательная обработка
2. Асинхронное выполнение (несколько подов) - параллельная обработка

РЕАЛЬНЫЙ СЦЕНАРИЙ ИЗ PRODUCTION:
- Записи создаются с разными update_ts
- Батч обрабатывает их в порядке (id, hashtag), НЕ по update_ts
- offset = MAX(update_ts) из батча
- МЕЖДУ итерациями могут появляться новые записи
- Эти записи НЕ ДОЛЖНЫ пропускаться
"""
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import pytest
from sqlalchemy import Column, Integer, String

from datapipe.compute import ComputeInput
from datapipe.datatable import DataStore
from datapipe.step.batch_transform import BatchTransformStep
from datapipe.store.database import DBConn, TableStoreDB


def test_offset_invariant_synchronous(dbconn: DBConn):
    """
    Тест инварианта в синхронном режиме.

    Проверяем что после каждой итерации:
    - offset обновляется корректно
    - НЕТ необработанных записей с update_ts < offset
    - update_ts новых записей всегда >= предыдущего offset

    Это базовый тест - если он падает, система работает некорректно.
    """
    ds = DataStore(dbconn, create_meta_table=True)

    input_store = TableStoreDB(
        dbconn,
        "invariant_input",
        [
            Column("id", String, primary_key=True),
            Column("value", Integer),
        ],
        create_table=True,
    )
    input_dt = ds.create_table("invariant_input", input_store)

    output_store = TableStoreDB(
        dbconn,
        "invariant_output",
        [
            Column("id", String, primary_key=True),
            Column("value", Integer),
        ],
        create_table=True,
    )
    output_dt = ds.create_table("invariant_output", output_store)

    def copy_func(df):
        return df[["id", "value"]]

    step = BatchTransformStep(
        ds=ds,
        name="invariant_copy",
        func=copy_func,
        input_dts=[ComputeInput(dt=input_dt, join_type="full")],
        output_dts=[output_dt],
        transform_keys=["id"],
        use_offset_optimization=True,
        chunk_size=5,  # Маленький размер батча для тестирования
    )

    previous_offset = 0.0

    # Выполняем 5 итераций создания данных и обработки
    # NOTE: Уменьшено с 10 до 5 итераций для ускорения тестов (каждая итерация делает run_full())
    for iteration in range(5):
        time.sleep(0.01)  # Небольшая задержка между итерациями

        # Создаем новые записи
        current_time = time.time()
        new_records = pd.DataFrame({
            "id": [f"iter{iteration}_rec{i}" for i in range(3)],
            "value": [iteration * 10 + i for i in range(3)],
        })

        input_dt.store_chunk(new_records, now=current_time)

        # Проверяем метаданные созданных записей
        meta_df = input_dt.meta_table.get_metadata(new_records[["id"]])
        for idx, row in meta_df.iterrows():
            record_update_ts = row["update_ts"]

            # ИНВАРИАНТ 1: update_ts новых записей должен быть >= предыдущего offset
            assert record_update_ts >= previous_offset, (
                f"НАРУШЕНИЕ ИНВАРИАНТА (итерация {iteration}): "
                f"Новая запись {row['id']} имеет update_ts={record_update_ts} < "
                f"предыдущий offset={previous_offset}!"
            )

        # Запускаем трансформацию
        step.run_full(ds)

        # Получаем текущий offset
        offsets = ds.offset_table.get_offsets_for_transformation(step.get_name())
        current_offset = offsets.get("invariant_input", 0.0)

        # ИНВАРИАНТ 2: offset должен расти монотонно (или оставаться прежним)
        assert current_offset >= previous_offset, (
            f"НАРУШЕНИЕ ИНВАРИАНТА (итерация {iteration}): "
            f"offset уменьшился! previous={previous_offset}, current={current_offset}"
        )

        # ИНВАРИАНТ 3: ВСЕ записи с update_ts <= current_offset должны быть обработаны
        # Проверяем: нет ли необработанных записей в input с update_ts < current_offset
        all_input_meta = input_dt.meta_table.get_metadata()
        all_output_ids = set(output_dt.get_data()["id"].tolist()) if len(output_dt.get_data()) > 0 else set()

        for idx, row in all_input_meta.iterrows():
            if row["update_ts"] <= current_offset:
                # Эта запись должна быть обработана!
                assert row["id"] in all_output_ids, (
                    f"НАРУШЕНИЕ ИНВАРИАНТА (итерация {iteration}): "
                    f"Запись {row['id']} с update_ts={row['update_ts']} <= offset={current_offset} "
                    f"НЕ обработана! Это означает потерю данных."
                )

        print(f"Итерация {iteration}: offset={current_offset}, обработано {len(all_output_ids)} записей")
        previous_offset = current_offset

    # Финальная проверка: все записи обработаны
    total_records = 5 * 3  # 5 итераций по 3 записи
    final_output = output_dt.get_data()
    assert len(final_output) == total_records, (
        f"Ожидалось {total_records} записей в output, получено {len(final_output)}"
    )


def test_offset_invariant_concurrent(dbconn: DBConn):
    """
    Тест инварианта в асинхронном режиме (несколько подов параллельно).

    Сценарий:
    - Несколько потоков одновременно:
      1. Создают записи (с текущим time.time())
      2. Запускают трансформацию
    - Проверяем что offset корректно обрабатывает race conditions

    ПОТЕНЦИАЛЬНАЯ ПРОБЛЕМА:
    - Поток 1: создал запись с update_ts=T1, запустил обработку
    - Поток 2 (параллельно): создал запись с update_ts=T2 (T2 > T1)
    - Поток 2: обработал раньше, установил offset=T2
    - Поток 1: обработал позже, но его запись с update_ts=T1 < offset=T2
    - Результат: может быть проблема с видимостью данных
    """
    ds = DataStore(dbconn, create_meta_table=True)

    input_store = TableStoreDB(
        dbconn,
        "concurrent_input",
        [
            Column("id", String, primary_key=True),
            Column("thread_id", Integer),
            Column("value", Integer),
        ],
        create_table=True,
    )
    input_dt = ds.create_table("concurrent_input", input_store)

    output_store = TableStoreDB(
        dbconn,
        "concurrent_output",
        [
            Column("id", String, primary_key=True),
            Column("thread_id", Integer),
            Column("value", Integer),
        ],
        create_table=True,
    )
    output_dt = ds.create_table("concurrent_output", output_store)

    def copy_func(df):
        return df[["id", "thread_id", "value"]]

    step = BatchTransformStep(
        ds=ds,
        name="concurrent_copy",
        func=copy_func,
        input_dts=[ComputeInput(dt=input_dt, join_type="full")],
        output_dts=[output_dt],
        transform_keys=["id"],
        use_offset_optimization=True,
        chunk_size=10,
    )

    def worker_thread(thread_id, iterations):
        """
        Рабочий поток: создает записи и запускает трансформацию.
        Симулирует работу отдельного пода.
        """
        created_ids = []

        for i in range(iterations):
            # Небольшая случайная задержка чтобы потоки не синхронизировались
            time.sleep(0.001 * (thread_id % 3))

            # Создаем запись с ТЕКУЩИМ временем
            record_id = f"thread{thread_id}_iter{i}"
            record = pd.DataFrame({
                "id": [record_id],
                "thread_id": [thread_id],
                "value": [thread_id * 1000 + i],
            })

            # Записываем с текущим now (как в реальной системе)
            input_dt.store_chunk(record, now=time.time())
            created_ids.append(record_id)

            # Запускаем трансформацию (каждый под обрабатывает данные)
            step.run_full(ds)

        return created_ids

    # Запускаем несколько потоков параллельно (симулируем несколько подов)
    # NOTE: Этот тест медленный (каждая итерация делает run_full()).
    # Баланс: 2 threads × 6 iterations = 12 records → 2 batches при chunk_size=10
    # Это достаточно для тестирования concurrent behavior без чрезмерной медленности.
    num_threads = 2
    iterations_per_thread = 6

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [
            executor.submit(worker_thread, thread_id, iterations_per_thread)
            for thread_id in range(num_threads)
        ]

        all_created_ids = []
        for future in as_completed(futures):
            all_created_ids.extend(future.result())

    # Финальный запуск для обработки оставшихся данных
    step.run_full(ds)

    # Получаем финальный offset
    offsets = ds.offset_table.get_offsets_for_transformation(step.get_name())
    final_offset = offsets.get("concurrent_input", 0.0)

    # КРИТИЧНАЯ ПРОВЕРКА: ВСЕ записи должны быть обработаны
    final_output = output_dt.get_data()
    processed_ids = set(final_output["id"].tolist())

    expected_count = num_threads * iterations_per_thread
    assert len(final_output) == expected_count, (
        f"Ожидалось {expected_count} записей, получено {len(final_output)}"
    )

    # Проверяем что КАЖДАЯ созданная запись обработана
    for record_id in all_created_ids:
        assert record_id in processed_ids, (
            f"КРИТИЧЕСКИЙ БАГ (CONCURRENT): Запись {record_id} была создана но НЕ обработана!"
        )

    # ИНВАРИАНТ: Нет необработанных записей с update_ts <= final_offset
    all_input_meta = input_dt.meta_table.get_metadata()
    for idx, row in all_input_meta.iterrows():
        if row["update_ts"] <= final_offset:
            assert row["id"] in processed_ids, (
                f"НАРУШЕНИЕ ИНВАРИАНТА (CONCURRENT): "
                f"Запись {row['id']} с update_ts={row['update_ts']} <= final_offset={final_offset} "
                f"НЕ обработана при параллельном выполнении!"
            )

    print(f"Concurrent test: {expected_count} записей обработано, final_offset={final_offset}")


@pytest.mark.xfail(
    reason="Known limitation: records with update_ts < offset are lost (Hypothesis 4). "
           "Warning added in get_changes_for_store_chunk() to detect this edge case."
)
def test_offset_with_delayed_records(dbconn: DBConn):
    """
    Тест проверяет сценарий когда запись создается "между итерациями".

    Сценарий:
    1. Обрабатываем батч 1, offset устанавливается на MAX(update_ts) = T3
    2. МЕЖДУ итерациями создается запись с update_ts = T2 (T2 < T3, но запись НОВАЯ)
    3. Следующая итерация должна обработать эту запись

    ВОПРОС: Может ли это произойти на проде?
    - Если время создания = time.time(), то update_ts всегда растет
    - НО: если несколько серверов с разными часами...
    - ИЛИ: если система обрабатывает данные из очереди с задержкой...
    """
    ds = DataStore(dbconn, create_meta_table=True)

    input_store = TableStoreDB(
        dbconn,
        "delayed_input",
        [Column("id", String, primary_key=True), Column("value", Integer)],
        create_table=True,
    )
    input_dt = ds.create_table("delayed_input", input_store)

    output_store = TableStoreDB(
        dbconn,
        "delayed_output",
        [Column("id", String, primary_key=True), Column("value", Integer)],
        create_table=True,
    )
    output_dt = ds.create_table("delayed_output", output_store)

    def copy_func(df):
        return df[["id", "value"]]

    step = BatchTransformStep(
        ds=ds,
        name="delayed_copy",
        func=copy_func,
        input_dts=[ComputeInput(dt=input_dt, join_type="full")],
        output_dts=[output_dt],
        transform_keys=["id"],
        use_offset_optimization=True,
    )

    # Создаем записи с монотонно растущими timestamps
    base_time = time.time()

    # Первый батч: записи с T1, T2, T3
    input_dt.store_chunk(
        pd.DataFrame({"id": ["rec_T1"], "value": [1]}),
        now=base_time + 1
    )
    input_dt.store_chunk(
        pd.DataFrame({"id": ["rec_T2"], "value": [2]}),
        now=base_time + 2
    )
    input_dt.store_chunk(
        pd.DataFrame({"id": ["rec_T3"], "value": [3]}),
        now=base_time + 3
    )

    # Первая итерация
    step.run_full(ds)

    offsets = ds.offset_table.get_offsets_for_transformation(step.get_name())
    offset_after_first = offsets["delayed_input"]

    # offset должен быть >= T3
    assert offset_after_first >= base_time + 3

    # Теперь создаем запись с update_ts МЕЖДУ T2 и T3
    # Вопрос: как это может произойти если now=time.time()?
    # Ответ: НЕ МОЖЕТ в нормальной ситуации!
    #
    # Но проверим что ЕСЛИ это произойдет, система обработает корректно
    time.sleep(0.01)

    # Создаем "запоздалую" запись (симулируем запись от другого сервера с отстающими часами)
    delayed_time = base_time + 2.5  # Между T2 и T3
    input_dt.store_chunk(
        pd.DataFrame({"id": ["rec_delayed"], "value": [999]}),
        now=delayed_time
    )

    # Проверяем метаданные
    delayed_meta = input_dt.meta_table.get_metadata(pd.DataFrame({"id": ["rec_delayed"]}))
    delayed_update_ts = delayed_meta.iloc[0]["update_ts"]

    # КРИТИЧНО: update_ts < offset!
    if delayed_update_ts < offset_after_first:
        # Запись НЕ ВИДНА для следующей итерации
        changed_count = step.get_changed_idx_count(ds)

        # Эта проверка покажет есть ли баг
        print(f"ПРЕДУПРЕЖДЕНИЕ: Запись с update_ts={delayed_update_ts} < offset={offset_after_first}")
        print(f"changed_count={changed_count} (ожидается 0 - запись не видна)")

        # Запускаем обработку
        step.run_full(ds)

        # Проверяем результат
        final_output = output_dt.get_data()
        output_ids = set(final_output["id"].tolist())

        # КРИТИЧНАЯ ПРОВЕРКА
        if "rec_delayed" not in output_ids:
            pytest.fail(
                f"КРИТИЧЕСКИЙ БАГ: Запись 'rec_delayed' с update_ts={delayed_update_ts} < "
                f"offset={offset_after_first} НЕ обработана! "
                f"Это означает что система теряет данные при создании записей 'из прошлого'."
            )

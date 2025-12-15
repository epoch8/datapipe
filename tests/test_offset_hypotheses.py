"""
Раздельные тесты для гипотез 1 и 2.

ВАЖНО: Эти тесты независимы и проверяют РАЗНЫЕ проблемы!

Гипотеза 1: Строгое неравенство update_ts > offset
Гипотеза 2: ORDER BY по transform_keys, а не по update_ts
"""
import time

import pandas as pd
import pytest
from sqlalchemy import Column, Integer, String

from datapipe.compute import ComputeInput
from datapipe.datatable import DataStore
from datapipe.step.batch_transform import BatchTransformStep
from datapipe.store.database import DBConn, TableStoreDB


def test_hypothesis_1_strict_inequality_loses_records_with_equal_update_ts(dbconn: DBConn):
    """
    Тест ТОЛЬКО для гипотезы 1: Строгое неравенство update_ts > offset.

    Сценарий:
    - ВСЕ записи имеют ОДИНАКОВЫЙ update_ts
    - Записи сортируются по id (это не важно для этого теста)
    - Первый батч: обрабатываем 5 записей с update_ts=T1
    - offset = MAX(T1) = T1
    - Второй запуск: WHERE update_ts > T1 пропускает записи с update_ts == T1

    Результат: Все необработанные записи с update_ts == offset ПОТЕРЯНЫ!

    === КАК ЭТО МОЖЕТ ПРОИЗОЙТИ В PRODUCTION ===

    1. **Bulk insert / Batch processing**:
       - Приложение получает пакет данных (например, 1000 записей из внешнего API)
       - Все записи вставляются одним вызовом store_chunk(df, now=current_time)
       - Результат: 1000 записей с ОДИНАКОВЫМ update_ts

    2. **Миграция данных**:
       - Перенос исторических данных из старой системы
       - Данные импортируются пакетами с одним timestamp
       - Результат: Тысячи записей с одинаковым update_ts

    3. **Реальный production кейс (из hashtag_issue.md)**:
       - Трансформация extract_hashtags создала записи пакетами
       - Каждый пост может иметь несколько хештегов → несколько записей с одним update_ts
       - Пример: пост с 5 хештегами → 5 записей с одинаковым update_ts
       - При chunk_size=10 часть записей попадает в первый батч, часть остается
       - offset устанавливается на update_ts первого батча
       - Оставшиеся записи с тем же update_ts ТЕРЯЮТСЯ!

    4. **High-load scenario**:
       - При высокой нагрузке записи могут создаваться очень быстро
       - Точность timestamp может быть до секунды или миллисекунды
       - В рамках одной миллисекунды может быть создано 10-100+ записей
       - Результат: Множество записей с одинаковым update_ts

    Этот тест должен ПРОЙТИ при исправлении:
    - ✅ update_ts > offset → update_ts >= offset

    Этот тест НЕ должен зависеть от:
    - ❌ Порядка сортировки (ORDER BY id vs ORDER BY update_ts)
    """
    ds = DataStore(dbconn, create_meta_table=True)

    input_store = TableStoreDB(
        dbconn,
        "hyp1_input",
        [Column("id", String, primary_key=True), Column("value", Integer)],
        create_table=True,
    )
    input_dt = ds.create_table("hyp1_input", input_store)

    output_store = TableStoreDB(
        dbconn,
        "hyp1_output",
        [Column("id", String, primary_key=True), Column("value", Integer)],
        create_table=True,
    )
    output_dt = ds.create_table("hyp1_output", output_store)

    def copy_func(df):
        return df[["id", "value"]]

    step = BatchTransformStep(
        ds=ds,
        name="hyp1_copy",
        func=copy_func,
        input_dts=[ComputeInput(dt=input_dt, join_type="full")],
        output_dts=[output_dt],
        transform_keys=["id"],
        use_offset_optimization=True,
        chunk_size=5,
    )

    # Создаем 12 записей с ОДИНАКОВЫМ update_ts
    # Симулируем bulk insert или batch processing
    base_time = time.time()
    same_timestamp = base_time + 1

    records_df = pd.DataFrame({
        "id": [f"rec_{i:02d}" for i in range(12)],
        "value": list(range(12)),
    })

    # Одним вызовом store_chunk - как в production при bulk insert
    input_dt.store_chunk(records_df, now=same_timestamp)
    time.sleep(0.001)

    # Проверяем данные
    all_meta = input_dt.meta_table.get_metadata()
    print(f"\n=== ПОДГОТОВКА ===")
    print(f"Всего записей: {len(all_meta)}")
    print(f"Все записи имеют update_ts = {same_timestamp:.2f}")
    print("(Симуляция bulk insert или batch processing)")

    # ПЕРВЫЙ ЗАПУСК: обрабатываем только первый батч (5 записей)
    (idx_count, idx_gen) = step.get_full_process_ids(ds=ds, run_config=None)
    print(f"\nБатчей доступно: {idx_count}")

    first_batch_idx = next(idx_gen)
    idx_gen.close()
    print(f"Обрабатываем первый батч, размер: {len(first_batch_idx)}")
    step.run_idx(ds=ds, idx=first_batch_idx, run_config=None)

    # Проверяем offset
    offsets = ds.offset_table.get_offsets_for_transformation(step.get_name())
    offset_after_first = offsets["hyp1_input"]

    output_after_first = output_dt.get_data()
    processed_ids = set(output_after_first["id"].tolist())

    print(f"\n=== ПОСЛЕ ПЕРВОГО ЗАПУСКА ===")
    print(f"Обработано: {len(output_after_first)} записей")
    print(f"offset = {offset_after_first:.2f}")
    print(f"Обработанные id: {sorted(processed_ids)}")

    # ВТОРОЙ ЗАПУСК: с учетом offset
    (idx_count_second, idx_gen_second) = step.get_full_process_ids(ds=ds, run_config=None)
    print(f"\n=== ВТОРОЙ ЗАПУСК ===")
    print(f"Батчей доступно: {idx_count_second}")

    if idx_count_second > 0:
        for idx in idx_gen_second:
            print(f"Обрабатываем батч, размер: {len(idx)}")
            step.run_idx(ds=ds, idx=idx, run_config=None)
        idx_gen_second.close()

    # ПРОВЕРКА: ВСЕ записи должны быть обработаны
    final_output = output_dt.get_data()
    final_processed_ids = set(final_output["id"].tolist())
    all_input_ids = set(all_meta["id"].tolist())
    lost_records = all_input_ids - final_processed_ids

    if lost_records:
        lost_meta = all_meta[all_meta["id"].isin(lost_records)]
        print(f"\n=== 🚨 ПОТЕРЯННЫЕ ЗАПИСИ (БАГ!) ===")
        for idx, row in lost_meta.sort_values("id").iterrows():
            print(f"  id={row['id']:10} update_ts={row['update_ts']:.2f} (== offset={offset_after_first:.2f})")

        pytest.fail(
            f"ГИПОТЕЗА 1 ПОДТВЕРЖДЕНА: {len(lost_records)} записей с update_ts == offset ПОТЕРЯНЫ!\n"
            f"Ожидалось: {len(all_input_ids)} записей\n"
            f"Получено:  {len(final_output)} записей\n"
            f"Потеряно:  {len(lost_records)} записей\n"
            f"Потерянные id: {sorted(lost_records)}\n\n"
            f"Причина: Строгое неравенство 'update_ts > offset' пропускает записи с update_ts == offset\n"
            f"Исправление: datapipe/meta/sql_meta.py - заменить '>' на '>='"
        )

    print(f"\n=== ✅ ВСЕ ЗАПИСИ ОБРАБОТАНЫ ===")
    print(f"Всего записей: {len(all_input_ids)}")
    print(f"Обработано:    {len(final_output)}")


def test_hypothesis_2_order_by_transform_keys_with_mixed_update_ts(dbconn: DBConn):
    """
    Тест ТОЛЬКО для гипотезы 2: ORDER BY по transform_keys, а не по update_ts.

    Сценарий:
    - Записи имеют РАЗНЫЕ update_ts
    - Записи сортируются по id (transform_keys), НЕ по update_ts
    - В батч попадают записи с разными update_ts (например: T1, T1, T3, T3, T3)
    - offset = MAX(T1, T1, T3, T3, T3) = T3
    - Но есть запись с id ПОСЛЕ последней обработанной, но с update_ts < T3
    - Второй запуск: WHERE update_ts > T3 пропускает эту запись

    ВАЖНО: Этот тест должен ПАДАТЬ даже при исправлении гипотезы 1 (> на >=)!
    Для этого мы НЕ должны иметь записей с update_ts == offset в необработанных данных.

    Этот тест должен ПРОЙТИ при исправлении:
    - ✅ ORDER BY transform_keys → ORDER BY update_ts
    - ИЛИ другой способ обеспечить что offset не превышает MAX(update_ts обработанных записей)

    Этот тест НЕ должен пройти при исправлении:
    - ❌ update_ts > offset → update_ts >= offset (гипотеза 1)
    """
    ds = DataStore(dbconn, create_meta_table=True)

    input_store = TableStoreDB(
        dbconn,
        "hyp2_input",
        [Column("id", String, primary_key=True), Column("value", Integer)],
        create_table=True,
    )
    input_dt = ds.create_table("hyp2_input", input_store)

    output_store = TableStoreDB(
        dbconn,
        "hyp2_output",
        [Column("id", String, primary_key=True), Column("value", Integer)],
        create_table=True,
    )
    output_dt = ds.create_table("hyp2_output", output_store)

    def copy_func(df):
        return df[["id", "value"]]

    step = BatchTransformStep(
        ds=ds,
        name="hyp2_copy",
        func=copy_func,
        input_dts=[ComputeInput(dt=input_dt, join_type="full")],
        output_dts=[output_dt],
        transform_keys=["id"],
        use_offset_optimization=True,
        chunk_size=5,
    )

    # Создаем записи с РАЗНЫМИ update_ts в "неправильном" порядке id
    base_time = time.time()

    # Группа 1: T1 - ранний timestamp
    t1 = base_time + 1
    input_dt.store_chunk(
        pd.DataFrame({"id": ["rec_00", "rec_01"], "value": [0, 1]}),
        now=t1
    )
    time.sleep(0.001)

    # Группа 2: T3 - ПОЗДНИЙ timestamp (специально создаем "дыру")
    t3 = base_time + 3
    input_dt.store_chunk(
        pd.DataFrame({"id": ["rec_02", "rec_03", "rec_04"], "value": [2, 3, 4]}),
        now=t3
    )
    time.sleep(0.001)

    # Группа 3: T2 - СРЕДНИЙ timestamp (но id ПОСЛЕ первого батча)
    t2 = base_time + 2
    input_dt.store_chunk(
        pd.DataFrame({"id": ["rec_05", "rec_06", "rec_07"], "value": [5, 6, 7]}),
        now=t2  # Старый timestamp, но id ПОСЛЕ rec_04!
    )
    time.sleep(0.001)

    # Группа 4: T4 - Еще более поздний timestamp
    t4 = base_time + 4
    input_dt.store_chunk(
        pd.DataFrame({"id": ["rec_08", "rec_09", "rec_10"], "value": [8, 9, 10]}),
        now=t4
    )

    # Проверяем данные
    all_meta = input_dt.meta_table.get_metadata()
    print(f"\n=== ПОДГОТОВКА ===")
    print(f"Всего записей: {len(all_meta)}")
    print("Распределение по update_ts (сортировка по id):")
    for idx, row in all_meta.sort_values("id").iterrows():
        ts_label = "T1" if abs(row["update_ts"] - t1) < 0.01 else \
                   "T2" if abs(row["update_ts"] - t2) < 0.01 else \
                   "T3" if abs(row["update_ts"] - t3) < 0.01 else "T4"
        print(f"  id={row['id']:10} update_ts={ts_label} ({row['update_ts']:.2f})")

    # ПЕРВЫЙ ЗАПУСК: обрабатываем только первый батч (5 записей)
    # Батч будет: rec_00(T1), rec_01(T1), rec_02(T3), rec_03(T3), rec_04(T3)
    # offset = MAX(T1, T1, T3, T3, T3) = T3
    (idx_count, idx_gen) = step.get_full_process_ids(ds=ds, run_config=None)
    print(f"\nБатчей доступно: {idx_count}")

    first_batch_idx = next(idx_gen)
    idx_gen.close()
    print(f"Обрабатываем первый батч, размер: {len(first_batch_idx)}")
    step.run_idx(ds=ds, idx=first_batch_idx, run_config=None)

    # Проверяем offset
    offsets = ds.offset_table.get_offsets_for_transformation(step.get_name())
    offset_after_first = offsets["hyp2_input"]

    output_after_first = output_dt.get_data()
    processed_ids = set(output_after_first["id"].tolist())

    print(f"\n=== ПОСЛЕ ПЕРВОГО ЗАПУСКА ===")
    print(f"Обработано: {len(output_after_first)} записей")
    print(f"offset = {offset_after_first:.2f} (должно быть T3 = {t3:.2f})")
    print(f"Обработанные id: {sorted(processed_ids)}")

    # Проверяем необработанные записи
    all_input_ids = set(all_meta["id"].tolist())
    unprocessed_ids = all_input_ids - processed_ids

    if unprocessed_ids:
        print(f"\n=== НЕОБРАБОТАННЫЕ ЗАПИСИ ===")
        unprocessed_meta = all_meta[all_meta["id"].isin(unprocessed_ids)]
        for idx, row in unprocessed_meta.sort_values("id").iterrows():
            # ВАЖНО: Проверяем СТРОГО меньше, не <=
            below_offset = row["update_ts"] < offset_after_first
            status = "БУДЕТ ПОТЕРЯНА!" if below_offset else "будет обработана"
            print(
                f"  id={row['id']:10} update_ts={row['update_ts']:.2f} "
                f"< offset={offset_after_first:.2f} ? {below_offset} → {status}"
            )

    # ВТОРОЙ ЗАПУСК: с учетом offset
    (idx_count_second, idx_gen_second) = step.get_full_process_ids(ds=ds, run_config=None)
    print(f"\n=== ВТОРОЙ ЗАПУСК ===")
    print(f"Батчей доступно: {idx_count_second}")

    if idx_count_second > 0:
        for idx in idx_gen_second:
            print(f"Обрабатываем батч, размер: {len(idx)}")
            step.run_idx(ds=ds, idx=idx, run_config=None)
        idx_gen_second.close()

    # ПРОВЕРКА: ВСЕ записи должны быть обработаны
    final_output = output_dt.get_data()
    final_processed_ids = set(final_output["id"].tolist())
    lost_records = all_input_ids - final_processed_ids

    if lost_records:
        lost_meta = all_meta[all_meta["id"].isin(lost_records)]
        print(f"\n=== 🚨 ПОТЕРЯННЫЕ ЗАПИСИ (БАГ!) ===")
        for idx, row in lost_meta.sort_values("id").iterrows():
            print(
                f"  id={row['id']:10} update_ts={row['update_ts']:.2f} "
                f"< offset={offset_after_first:.2f}"
            )

        pytest.fail(
            f"ГИПОТЕЗА 2 ПОДТВЕРЖДЕНА: {len(lost_records)} записей ПОТЕРЯНЫ из-за ORDER BY по transform_keys!\n"
            f"Ожидалось: {len(all_input_ids)} записей\n"
            f"Получено:  {len(final_output)} записей\n"
            f"Потеряно:  {len(lost_records)} записей\n"
            f"Потерянные id: {sorted(lost_records)}\n\n"
            f"Причина: Батчи сортируются ORDER BY transform_keys (id), но offset = MAX(update_ts).\n"
            f"         Записи с id ПОСЛЕ последней обработанной, но с update_ts < offset ПОТЕРЯНЫ.\n"
            f"Исправление: Либо сортировать по update_ts, либо пересмотреть логику offset."
        )

    print(f"\n=== ✅ ВСЕ ЗАПИСИ ОБРАБОТАНЫ ===")
    print(f"Всего записей: {len(all_input_ids)}")
    print(f"Обработано:    {len(final_output)}")


def test_antiregression_no_infinite_loop_with_equal_update_ts(dbconn: DBConn):
    """
    Анти-регрессионный тест: Проверяет что после исправления > на >= не возникает зацикливание.

    ВАЖНО: Этот тест должен ПРОХОДИТЬ (не xfail) и после исправления тоже должен проходить!

    Сценарий:
    1. Создаем 12 записей с ОДИНАКОВЫМ update_ts (bulk insert)
    2. Первый запуск: обрабатываем первый батч (5 записей)
       - offset = T1
       - Проверяем что обработано ровно 5 записей
    3. Второй запуск: обрабатываем следующий батч (5 записей с update_ts == T1)
       - Проверяем что обработано ровно 5 НОВЫХ записей (не те же самые!)
       - Проверяем что offset НЕ изменился (всё ещё T1)
    4. Третий запуск: обрабатываем последний батч (2 записи)
       - Проверяем что обработано 2 новых записи
    5. Добавляем НОВЫЕ записи с update_ts > T1
       - Проверяем что новые записи будут обработаны

    Критично:
    - Каждый запуск должен обрабатывать НОВЫЕ записи, не зацикливаться на одних и тех же
    - После исправления >= система должна корректно обрабатывать записи с update_ts == offset
    - process_ts должен обновляться для обработанных записей
    """
    ds = DataStore(dbconn, create_meta_table=True)

    input_store = TableStoreDB(
        dbconn,
        "antiregr_input",
        [Column("id", String, primary_key=True), Column("value", Integer)],
        create_table=True,
    )
    input_dt = ds.create_table("antiregr_input", input_store)

    output_store = TableStoreDB(
        dbconn,
        "antiregr_output",
        [Column("id", String, primary_key=True), Column("value", Integer)],
        create_table=True,
    )
    output_dt = ds.create_table("antiregr_output", output_store)

    def copy_func(df):
        return df[["id", "value"]]

    step = BatchTransformStep(
        ds=ds,
        name="antiregr_copy",
        func=copy_func,
        input_dts=[ComputeInput(dt=input_dt, join_type="full")],
        output_dts=[output_dt],
        transform_keys=["id"],
        use_offset_optimization=True,
        chunk_size=5,
    )

    # Создаем 12 записей с ОДИНАКОВЫМ update_ts (bulk insert)
    # ВАЖНО: НЕ передаем now= чтобы store_chunk использовал текущее время
    # Это соответствует production поведению: данные создаются "сейчас",
    # а обработка происходит позже, поэтому process_ts >= update_ts
    records_df = pd.DataFrame({
        "id": [f"rec_{i:02d}" for i in range(12)],
        "value": list(range(12)),
    })

    input_dt.store_chunk(records_df)
    time.sleep(0.01)  # Даем время чтобы process_ts > update_ts при обработке

    print(f"\n=== ПОДГОТОВКА ===")
    print(f"Создано 12 записей с одинаковым update_ts")

    # ========== ПЕРВЫЙ ЗАПУСК: 5 записей ==========
    (idx_count_1, idx_gen_1) = step.get_full_process_ids(ds=ds, run_config=None)
    print(f"\n=== ПЕРВЫЙ ЗАПУСК ===")
    print(f"Батчей доступно: {idx_count_1}")

    first_batch_idx = next(idx_gen_1)
    idx_gen_1.close()
    print(f"Обрабатываем батч, размер: {len(first_batch_idx)}")
    step.run_idx(ds=ds, idx=first_batch_idx, run_config=None)

    output_1 = output_dt.get_data()
    processed_ids_1 = set(output_1["id"].tolist())
    offsets_1 = ds.offset_table.get_offsets_for_transformation(step.get_name())
    offset_1 = offsets_1["antiregr_input"]

    print(f"Обработано: {len(output_1)} записей")
    print(f"offset = {offset_1:.2f}")
    print(f"Обработанные id: {sorted(processed_ids_1)}")

    assert len(output_1) == 5, f"Ожидалось 5 записей, получено {len(output_1)}"
    # Сохраняем offset первого батча для последующих проверок
    first_batch_offset = offset_1

    # ========== ВТОРОЙ ЗАПУСК: следующие 5 записей (с update_ts == offset!) ==========
    (idx_count_2, idx_gen_2) = step.get_full_process_ids(ds=ds, run_config=None)
    print(f"\n=== ВТОРОЙ ЗАПУСК ===")
    print(f"Батчей доступно: {idx_count_2}")

    if idx_count_2 == 0:
        pytest.fail(
            "БАГ: Нет батчей для обработки во втором запуске!\n"
            "Это означает что записи с update_ts == offset НЕ попали в выборку.\n"
            "Проблема: Строгое неравенство update_ts > offset"
        )

    second_batch_idx = next(idx_gen_2)
    idx_gen_2.close()
    print(f"Обрабатываем батч, размер: {len(second_batch_idx)}")
    step.run_idx(ds=ds, idx=second_batch_idx, run_config=None)

    output_2 = output_dt.get_data()
    processed_ids_2 = set(output_2["id"].tolist())
    new_ids_2 = processed_ids_2 - processed_ids_1
    offsets_2 = ds.offset_table.get_offsets_for_transformation(step.get_name())
    offset_2 = offsets_2["antiregr_input"]

    print(f"Всего обработано: {len(output_2)} записей")
    print(f"Новых записей: {len(new_ids_2)}")
    print(f"Новые id: {sorted(new_ids_2)}")
    print(f"offset = {offset_2:.2f}")

    # Критичная проверка: должны обработать НОВЫЕ записи, не зациклиться на старых
    assert len(new_ids_2) == 5, (
        f"Ожидалось 5 НОВЫХ записей, получено {len(new_ids_2)}!\n"
        f"Возможно зацикливание: обрабатываем те же записи снова и снова."
    )
    assert len(output_2) == 10, f"Всего должно быть 10 записей, получено {len(output_2)}"
    assert abs(offset_2 - first_batch_offset) < 0.01, (
        f"offset НЕ должен измениться! "
        f"Был {first_batch_offset:.2f}, стал {offset_2:.2f}"
    )

    # Проверяем что это действительно ДРУГИЕ записи
    intersection = processed_ids_1 & new_ids_2
    assert len(intersection) == 0, (
        f"ЗАЦИКЛИВАНИЕ: Повторно обрабатываем те же записи: {sorted(intersection)}"
    )

    # ========== ТРЕТИЙ ЗАПУСК: последние 2 записи ==========
    (idx_count_3, idx_gen_3) = step.get_full_process_ids(ds=ds, run_config=None)
    print(f"\n=== ТРЕТИЙ ЗАПУСК ===")
    print(f"Батчей доступно: {idx_count_3}")

    if idx_count_3 > 0:
        third_batch_idx = next(idx_gen_3)
        idx_gen_3.close()
        print(f"Обрабатываем батч, размер: {len(third_batch_idx)}")
        step.run_idx(ds=ds, idx=third_batch_idx, run_config=None)

    output_3 = output_dt.get_data()
    processed_ids_3 = set(output_3["id"].tolist())
    new_ids_3 = processed_ids_3 - processed_ids_2

    print(f"Всего обработано: {len(output_3)} записей")
    print(f"Новых записей: {len(new_ids_3)}")
    print(f"Новые id: {sorted(new_ids_3)}")

    assert len(output_3) == 12, f"Всего должно быть 12 записей, получено {len(output_3)}"
    assert len(new_ids_3) == 2, f"Ожидалось 2 новых записи, получено {len(new_ids_3)}"

    # ========== ДОБАВЛЯЕМ НОВЫЕ ЗАПИСИ с update_ts > offset ==========
    # Ждем чтобы гарантировать что новые записи будут иметь update_ts > offset
    time.sleep(0.02)
    new_records_df = pd.DataFrame({
        "id": [f"new_{i:02d}" for i in range(5)],
        "value": list(range(100, 105)),
    })

    input_dt.store_chunk(new_records_df)  # now=None, используем текущее время
    time.sleep(0.01)

    print(f"\n=== ДОБАВИЛИ 5 НОВЫХ ЗАПИСЕЙ с update_ts > {first_batch_offset:.2f} ===")

    # ========== ЧЕТВЕРТЫЙ ЗАПУСК: новые записи ==========
    (idx_count_4, idx_gen_4) = step.get_full_process_ids(ds=ds, run_config=None)
    print(f"\n=== ЧЕТВЕРТЫЙ ЗАПУСК ===")
    print(f"Батчей доступно: {idx_count_4}")

    if idx_count_4 == 0:
        pytest.fail(
            "БАГ: Нет батчей для обработки новых записей!\n"
            "Новые записи с update_ts > offset должны обрабатываться."
        )

    fourth_batch_idx = next(idx_gen_4)
    idx_gen_4.close()
    print(f"Обрабатываем батч, размер: {len(fourth_batch_idx)}")
    step.run_idx(ds=ds, idx=fourth_batch_idx, run_config=None)

    output_4 = output_dt.get_data()
    processed_ids_4 = set(output_4["id"].tolist())
    new_ids_4 = processed_ids_4 - processed_ids_3
    offsets_4 = ds.offset_table.get_offsets_for_transformation(step.get_name())
    offset_4 = offsets_4["antiregr_input"]

    print(f"Всего обработано: {len(output_4)} записей")
    print(f"Новых записей: {len(new_ids_4)}")
    print(f"Новые id: {sorted(new_ids_4)}")
    print(f"offset = {offset_4:.2f}")

    assert len(output_4) == 17, f"Всего должно быть 17 записей (12 старых + 5 новых), получено {len(output_4)}"
    assert len(new_ids_4) == 5, f"Ожидалось 5 новых записей, получено {len(new_ids_4)}"
    assert offset_4 > first_batch_offset, (
        f"offset должен обновиться для новых записей! "
        f"Был {first_batch_offset:.2f}, остался {offset_4:.2f}"
    )

    # Проверяем что новые записи действительно новые
    assert all(id.startswith("new_") for id in new_ids_4), (
        f"Новые записи должны начинаться с 'new_', получено: {sorted(new_ids_4)}"
    )

    print(f"\n=== ✅ ВСЕ ПРОВЕРКИ ПРОШЛИ ===")
    print("1. Нет зацикливания на одних и тех же записях")
    print("2. Каждый запуск обрабатывает НОВЫЕ записи")
    print("3. Записи с update_ts == offset корректно обрабатываются")
    print("4. Новые записи с update_ts > offset корректно обрабатываются")
    print("5. offset корректно обновляется")

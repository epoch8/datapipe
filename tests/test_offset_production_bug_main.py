"""
🚨 КРИТИЧЕСКИЙ PRODUCTION БАГ: Offset Optimization теряет данные

ПРОБЛЕМА В PRODUCTION:
- Дата: 08.12.2025
- Потеряно: 48,915 из 82,000 записей (60%)
- Причина: Строгое неравенство в WHERE update_ts > offset

КОРНЕВАЯ ПРИЧИНА:
Код: datapipe/meta/sql_meta.py:967
  tbl.c.update_ts > offset  # ❌ ОШИБКА! Должно быть >=

МЕХАНИЗМ БАГА:
1. Батч обрабатывает записи в ORDER BY (id, hashtag) - НЕ по времени!
2. Батч содержит записи с РАЗНЫМИ update_ts
3. offset = MAX(update_ts) из обработанного батча
4. Следующий запуск: WHERE update_ts > offset (строгое неравенство!)
5. Записи с update_ts == offset но не вошедшие в батч ТЕРЯЮТСЯ

ВИЗУАЛИЗАЦИЯ ПРОБЛЕМЫ:
┌─────────────────────────────────────────────────────────────────────┐
│ Временная шкала (update_ts):                                       │
│                                                                     │
│ T1 (16:21)  T2 (17:00)  T3 (18:00)  T4 (19:00)  T5 (20:29)        │
│     │           │           │           │           │               │
│     ▼           ▼           ▼           ▼           ▼               │
│  rec_00      rec_08      rec_13      rec_18      rec_22            │
│  rec_01      rec_09      rec_14      rec_19      rec_23            │
│  ...         rec_10      rec_15      rec_20      rec_24            │
│  rec_07      rec_11      rec_16      rec_21                        │
│              rec_12      rec_17                                     │
│                                                                     │
│ ORDER BY id (сортировка для обработки):                            │
│ rec_00 → rec_01 → ... → rec_07 → rec_08 → rec_09 → rec_10 → ...   │
│   T1      T1           T1        T2        T2        T2            │
│                                                                     │
│ ┌──────────────────────────┐                                       │
│ │ БАТЧ 1 (chunk_size=10)   │                                       │
│ │ rec_00 до rec_09         │                                       │
│ │ update_ts: T1...T1, T2   │                                       │
│ └──────────────────────────┘                                       │
│           ↓                                                         │
│    offset = MAX(T1, T2) = T2                                       │
│                                                                     │
│ Следующий запуск:                                                  │
│ WHERE update_ts > T2  ← СТРОГОЕ НЕРАВЕНСТВО!                      │
│                                                                     │
│ 🚨 ПОТЕРЯНЫ:                                                       │
│    rec_10, rec_11, rec_12  (update_ts = T2 == offset)             │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘

РЕШЕНИЕ:
Заменить > на >= в sql_meta.py:967:
  tbl.c.update_ts >= offset  # ✅ ПРАВИЛЬНО
"""

import time
from typing import List, Tuple

import pandas as pd
import pytest
from sqlalchemy import Column, Integer, String

from datapipe.compute import ComputeInput
from datapipe.datatable import DataStore
from datapipe.step.batch_transform import BatchTransformStep
from datapipe.store.database import DBConn, TableStoreDB


# ============================================================================
# ПОДГОТОВКА ТЕСТОВЫХ ДАННЫХ
# ============================================================================

def prepare_test_data() -> List[Tuple[str, str, float]]:
    """
    Подготовка тестовых данных для воспроизведения production бага.

    Данные имитируют накопление записей в течение нескольких часов
    с РАЗНЫМИ update_ts (как в реальной системе).

    Returns:
        List[(record_id, label, update_ts_offset)]

    Временная шкала:
        T1 = base_time + 1  (16:21 в production)
        T2 = base_time + 2  (17:00)
        T3 = base_time + 3  (18:00)
        T4 = base_time + 4  (19:00)
        T5 = base_time + 5  (20:29 в production)
    """
    # Формат: (id, label для логов, смещение timestamp от base_time)
    test_data = [
        # Группа 1: T1 (16:21) - 8 записей
        ("rec_00", "T1", 1.0),
        ("rec_01", "T1", 1.0),
        ("rec_02", "T1", 1.0),
        ("rec_03", "T1", 1.0),
        ("rec_04", "T1", 1.0),
        ("rec_05", "T1", 1.0),
        ("rec_06", "T1", 1.0),
        ("rec_07", "T1", 1.0),

        # Группа 2: T2 (17:00) - 5 записей
        # ⚠️ КРИТИЧНО: rec_08 и rec_09 войдут в ПЕРВЫЙ батч
        #              rec_10, rec_11, rec_12 останутся на ВТОРОЙ батч
        ("rec_08", "T2", 2.0),
        ("rec_09", "T2", 2.0),
        ("rec_10", "T2", 2.0),  # 🚨 БУДЕТ ПОТЕРЯНА
        ("rec_11", "T2", 2.0),  # 🚨 БУДЕТ ПОТЕРЯНА
        ("rec_12", "T2", 2.0),  # 🚨 БУДЕТ ПОТЕРЯНА

        # Группа 3: T3 (18:00) - 5 записей
        ("rec_13", "T3", 3.0),
        ("rec_14", "T3", 3.0),
        ("rec_15", "T3", 3.0),
        ("rec_16", "T3", 3.0),
        ("rec_17", "T3", 3.0),

        # Группа 4: T4 (19:00) - 4 записи
        ("rec_18", "T4", 4.0),
        ("rec_19", "T4", 4.0),
        ("rec_20", "T4", 4.0),
        ("rec_21", "T4", 4.0),

        # Группа 5: T5 (20:29) - 3 записи
        ("rec_22", "T5", 5.0),
        ("rec_23", "T5", 5.0),
        ("rec_24", "T5", 5.0),
    ]

    return test_data


def print_test_data_visualization(test_data: List[Tuple[str, str, float]], base_time: float):
    """Визуализация тестовых данных для отладки"""
    print("\n" + "=" * 80)
    print("ПОДГОТОВЛЕННЫЕ ТЕСТОВЫЕ ДАННЫЕ")
    print("=" * 80)
    print("\nВсего записей:", len(test_data))
    print("\nРаспределение по временным меткам:")

    # Группируем по timestamp
    by_timestamp = {}
    for record_id, label, offset in test_data:
        ts = base_time + offset
        if ts not in by_timestamp:
            by_timestamp[ts] = []
        by_timestamp[ts].append((record_id, label))

    for ts in sorted(by_timestamp.keys()):
        records = by_timestamp[ts]
        label = records[0][1]
        ids = [r[0] for r in records]
        print(f"  {label}: {len(records)} записей - {', '.join(ids)}")

    print("\nОжидаемое распределение по батчам (chunk_size=10, ORDER BY id):")
    print("  Батч 1: rec_00 до rec_09  (10 записей)")
    print("          update_ts: T1(8 записей), T2(2 записи)")
    print("          offset после батча = MAX(T1, T2) = T2")
    print()
    print("  🚨 КРИТИЧНО: Следующий запуск WHERE update_ts > T2")
    print("     ПРОПУСТИТ: rec_10, rec_11, rec_12 (update_ts == T2)")
    print()


# ============================================================================
# PRODUCTION БАГ ТЕСТ
# ============================================================================

def test_production_bug_offset_loses_records_with_equal_update_ts(dbconn: DBConn):
    """
    🚨 ВОСПРОИЗВОДИТ PRODUCTION БАГ: 48,915 записей потеряно (60%)

    Сценарий (упрощенная версия production):
    1. Накапливается 25 записей с разными update_ts (chunk_size=10)
    2. ПЕРВЫЙ запуск обрабатывает ТОЛЬКО первый батч (10 записей)
    3. offset = MAX(update_ts) из этих 10 = T2
    4. ВТОРОЙ запуск: WHERE update_ts > T2 (строгое неравенство!)
    5. Записи с update_ts == T2 но не вошедшие в первый батч ПОТЕРЯНЫ

    В production:
    - 82,000 записей накоплено
    - chunk_size=1000
    - Потеряно 48,915 записей (60%)

    Механизм тот же - строгое неравенство в фильтре offset.
    """
    # ========== SETUP ==========
    ds = DataStore(dbconn, create_meta_table=True)

    input_store = TableStoreDB(
        dbconn,
        "production_bug_input",
        [
            Column("id", String, primary_key=True),
            Column("value", Integer),
        ],
        create_table=True,
    )
    input_dt = ds.create_table("production_bug_input", input_store)

    output_store = TableStoreDB(
        dbconn,
        "production_bug_output",
        [
            Column("id", String, primary_key=True),
            Column("value", Integer),
        ],
        create_table=True,
    )
    output_dt = ds.create_table("production_bug_output", output_store)

    def copy_func(df):
        """Простая функция копирования (как copy_to_online в production)"""
        return df[["id", "value"]]

    step = BatchTransformStep(
        ds=ds,
        name="production_bug_copy",
        func=copy_func,
        input_dts=[ComputeInput(dt=input_dt, join_type="full")],
        output_dts=[output_dt],
        transform_keys=["id"],
        use_offset_optimization=True,
        chunk_size=10,  # Маленький для быстрого теста (в production=1000)
    )

    # ========== ПОДГОТОВКА ДАННЫХ ==========
    base_time = time.time()
    test_data = prepare_test_data()

    # Визуализация данных
    print_test_data_visualization(test_data, base_time)

    # Загружаем данные группами по timestamp
    for record_id, label, offset in test_data:
        ts = base_time + offset
        input_dt.store_chunk(
            pd.DataFrame({"id": [record_id], "value": [int(offset * 100)]}),
            now=ts
        )
        time.sleep(0.001)  # Небольшая задержка для корректности timestamp

    # Проверяем метаданные
    all_meta = input_dt.meta_table.get_metadata()
    print(f"\n✓ Всего записей загружено: {len(all_meta)}")

    # ========== ПЕРВЫЙ ЗАПУСК (только 1 батч) ==========
    print("\n" + "=" * 80)
    print("ПЕРВЫЙ ЗАПУСК ТРАНСФОРМАЦИИ (обработка только 1 батча)")
    print("=" * 80)

    # Имитируем отдельный запуск джобы: обрабатываем ТОЛЬКО первый батч
    (idx_count, idx_gen) = step.get_full_process_ids(ds=ds, run_config=None)
    print(f"Батчей доступно для обработки: {idx_count}")

    # Обрабатываем ТОЛЬКО первый батч (как если бы джоба завершилась после него)
    first_batch_idx = next(idx_gen)
    idx_gen.close()  # Закрываем генератор, чтобы освободить соединение с БД
    print(f"Обрабатываем первый батч, размер: {len(first_batch_idx)}")
    step.run_idx(ds=ds, idx=first_batch_idx, run_config=None)

    # Проверяем offset после первого запуска
    offsets = ds.offset_table.get_offsets_for_transformation(step.get_name())
    offset_after_first = offsets["production_bug_input"]

    output_after_first = output_dt.get_data()

    print(f"\n✓ Обработано записей: {len(output_after_first)}")
    print(f"✓ offset установлен на: {offset_after_first:.2f}")

    # Показываем какие записи обработаны
    processed_ids = sorted(output_after_first["id"].tolist())
    print(f"✓ Обработанные id: {', '.join(processed_ids[:5])}...{', '.join(processed_ids[-2:])}")

    # ========== АНАЛИЗ ==========
    print("\n" + "=" * 80)
    print("АНАЛИЗ: Какие записи останутся необработанными?")
    print("=" * 80)

    # Проверяем что обработан только один батч
    if len(output_after_first) >= len(test_data):
        pytest.fail(
            f"ОШИБКА В ТЕСТЕ: Обработано {len(output_after_first)} записей, "
            f"ожидалось ~10 (один батч). Тест не симулирует отдельные запуски."
        )

    print(f"✓ Обработан только один батч: {len(output_after_first)} из {len(test_data)} записей")

    # Находим записи которые будут потеряны
    all_ids = set([rec[0] for rec in test_data])
    processed_ids_set = set(output_after_first["id"].tolist())
    unprocessed_ids = all_ids - processed_ids_set

    # Проверяем какие из необработанных записей имеют update_ts <= offset
    lost_records = []
    for record_id, label, offset_val in test_data:
        if record_id in unprocessed_ids:
            ts = base_time + offset_val
            if ts <= offset_after_first:
                lost_records.append((record_id, label, ts))

    if lost_records:
        print(f"\n🚨 ОБНАРУЖЕНЫ ЗАПИСИ КОТОРЫЕ БУДУТ ПОТЕРЯНЫ: {len(lost_records)}")
        print("   Эти записи имеют update_ts <= offset, но НЕ обработаны!")
        for record_id, label, ts in lost_records:
            status = "==" if abs(ts - offset_after_first) < 0.01 else "<"
            print(f"   {record_id:10} ({label}) update_ts {status} offset")

    # ========== ВТОРОЙ ЗАПУСК ==========
    print("\n" + "=" * 80)
    print("ВТОРОЙ ЗАПУСК ТРАНСФОРМАЦИИ (имитация повторного запуска джобы)")
    print("=" * 80)

    # Получаем батчи для второго запуска (с учетом offset)
    (idx_count_second, idx_gen_second) = step.get_full_process_ids(ds=ds, run_config=None)
    print(f"Батчей доступно для обработки: {idx_count_second}")

    if idx_count_second > 0:
        # Обрабатываем оставшиеся батчи
        for idx in idx_gen_second:
            print(f"Обрабатываем батч, размер: {len(idx)}")
            step.run_idx(ds=ds, idx=idx, run_config=None)
        idx_gen_second.close()  # Закрываем генератор после использования

    # ========== ПРОВЕРКА РЕЗУЛЬТАТА ==========
    final_output = output_dt.get_data()
    final_processed_ids = set(final_output["id"].tolist())

    print(f"\nФинальный результат:")
    print(f"  Всего записей в input:  {len(test_data)}")
    print(f"  Обработано в output:    {len(final_output)}")
    print(f"  ПОТЕРЯНО:               {len(all_ids) - len(final_processed_ids)}")

    # КРИТИЧНАЯ ПРОВЕРКА: Все ли записи обработаны?
    if len(final_output) < len(test_data):
        # Находим потерянные записи
        lost_ids = all_ids - final_processed_ids
        lost_records_final = []
        for record_id, label, offset_val in test_data:
            if record_id in lost_ids:
                lost_records_final.append((record_id, label, base_time + offset_val))

        print("\n" + "=" * 80)
        print("🚨 КРИТИЧЕСКИЙ БАГ ВОСПРОИЗВЕДЕН!")
        print("=" * 80)
        print(f"\nПотерянные записи ({len(lost_records_final)}):")
        for record_id, label, ts in lost_records_final:
            print(f"  {record_id:10} ({label}) update_ts={ts:.2f} {'==' if abs(ts - offset_after_first) < 0.01 else '<='} offset={offset_after_first:.2f}")

        # Группируем по timestamp
        by_label = {}
        for record_id, label, ts in lost_records_final:
            if label not in by_label:
                by_label[label] = []
            by_label[label].append(record_id)

        print(f"\nРаспределение потерянных по временной метке:")
        for label in sorted(by_label.keys()):
            ids = by_label[label]
            print(f"  {label}: {len(ids)} записей - {', '.join(ids)}")

        pytest.fail(
            f"\n🚨 КРИТИЧЕСКИЙ БАГ В OFFSET OPTIMIZATION!\n"
            f"{'=' * 50}\n"
            f"Всего записей:      {len(test_data)}\n"
            f"Обработано:         {len(final_output)}\n"
            f"ПОТЕРЯНО:           {len(lost_records_final)} ({len(lost_records_final)*100/len(test_data):.1f}%)\n"
            f"offset после 1-го:  {offset_after_first:.2f}\n\n"
            f"МЕХАНИЗМ БАГА:\n"
            f"1. Первый батч (10 записей) содержал записи с РАЗНЫМИ update_ts\n"
            f"2. offset установлен на MAX(update_ts) = {offset_after_first:.2f}\n"
            f"3. Записи с update_ts == offset НО не вошедшие в первый батч ПОТЕРЯНЫ!\n"
            f"4. Причина: WHERE update_ts > offset (строгое >) вместо >=\n\n"
            f"В PRODUCTION: 82,000 записей, chunk_size=1000, потеряно 48,915 (60%)\n"
            f"{'=' * 50}"
        )

    print("\n✅ Все записи обработаны корректно")


if __name__ == "__main__":
    # Для ручного запуска и отладки
    from datapipe.store.database import DBConn
    from sqlalchemy import create_engine, text

    DBCONNSTR = "postgresql://postgres:password@localhost:5432/postgres"
    DB_TEST_SCHEMA = "test_production_bug"

    eng = create_engine(DBCONNSTR)
    try:
        with eng.begin() as conn:
            conn.execute(text(f"DROP SCHEMA {DB_TEST_SCHEMA} CASCADE"))
    except Exception:
        pass

    with eng.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA {DB_TEST_SCHEMA}"))

    test_dbconn = DBConn(DBCONNSTR, DB_TEST_SCHEMA)

    print("Запуск теста воспроизведения production бага...")
    test_production_bug_offset_loses_records_with_equal_update_ts(test_dbconn)

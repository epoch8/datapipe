"""
Тест воспроизводит РЕАЛЬНЫЙ баг из production.

ПРОБЛЕМА:
При первом запуске трансформации с offset optimization,
если записи обрабатываются в порядке ORDER BY (id, hashtag) а не по update_ts,
и в батч попадают записи с РАЗНЫМИ update_ts (созданные в разное время),
то offset устанавливается на MAX(update_ts) из батча.

Все записи с id ПОСЛЕ последней обработанной, но с update_ts < offset,
будут ПРОПУЩЕНЫ при следующих запусках!

РЕАЛЬНЫЙ СЦЕНАРИЙ ИЗ PRODUCTION (hashtag_issue.md):
- 16:21 - Пост b927ca71 создан, хештеги извлечены
- 20:29 - Пост e26f9c4b создан
- copy_to_online ПЕРВЫЙ РАЗ обрабатывает:
  - Батч содержит (в порядке id): b927ca71(16:21), e26f9c4b(20:29), ...
  - offset = MAX(16:21, 20:29) = 20:29
- Следующий запуск: WHERE update_ts > 20:29
  - Пропускаются ВСЕ записи с id > e26f9c4b и update_ts < 20:29!
  - Результат: 60% данных потеряно

Этот тест воспроизводит эту проблему.
"""
import time

import pandas as pd
import pytest
from sqlalchemy import Column, Integer, String

from datapipe.compute import ComputeInput
from datapipe.datatable import DataStore
from datapipe.step.batch_transform import BatchTransformStep
from datapipe.store.database import DBConn, TableStoreDB


@pytest.mark.xfail(reason="Test uses run_idx() which no longer commits offsets (offsets only commit at end of run_full)")
def test_first_run_with_mixed_update_ts_and_order_by_id(dbconn: DBConn):
    """
    Воспроизводит ТОЧНЫЙ сценарий production бага.

    Симуляция накопления данных за несколько часов,
    затем первый запуск copy_to_online который обрабатывает
    записи в порядке (id, hashtag), НЕ по update_ts.

    Результат: данные с id после "границы батча" но с старым update_ts ТЕРЯЮТСЯ.
    """
    ds = DataStore(dbconn, create_meta_table=True)

    input_store = TableStoreDB(
        dbconn,
        "first_run_input",
        [
            Column("id", String, primary_key=True),
            Column("hashtag", String, primary_key=True),
            Column("value", Integer),
        ],
        create_table=True,
    )
    input_dt = ds.create_table("first_run_input", input_store)

    output_store = TableStoreDB(
        dbconn,
        "first_run_output",
        [
            Column("id", String, primary_key=True),
            Column("hashtag", String, primary_key=True),
            Column("value", Integer),
        ],
        create_table=True,
    )
    output_dt = ds.create_table("first_run_output", output_store)

    def copy_func(df):
        return df[["id", "hashtag", "value"]]

    step = BatchTransformStep(
        ds=ds,
        name="first_run_copy",
        func=copy_func,
        input_dts=[ComputeInput(dt=input_dt, join_type="full")],
        output_dts=[output_dt],
        transform_keys=["id", "hashtag"],
        use_offset_optimization=True,
        chunk_size=10,  # Маленький размер для демонстрации
    )

    # ========== Симулируем накопление данных за несколько часов ==========
    base_time = time.time()

    # Имитируем посты которые приходили в течение 4 часов
    # 16:21 - Пост b927ca71 (UUID начинается с 'b')
    t_16_21 = base_time + 1
    input_dt.store_chunk(
        pd.DataFrame({
            "id": ["b927ca71-0001", "b927ca71-0001"],
            "hashtag": ["322", "anime"],
            "value": [1, 2],
        }),
        now=t_16_21
    )

    time.sleep(0.001)

    # 17:00 - Еще посты с разными id, но старыми timestamps
    t_17_00 = base_time + 2
    input_dt.store_chunk(
        pd.DataFrame({
            "id": ["a111aaaa-0002", "c222cccc-0003", "d333dddd-0004"],
            "hashtag": ["test1", "test2", "test3"],
            "value": [3, 4, 5],
        }),
        now=t_17_00
    )

    time.sleep(0.001)

    # 18:00 - Больше постов
    t_18_00 = base_time + 3
    input_dt.store_chunk(
        pd.DataFrame({
            "id": ["e444eeee-0005", "f555ffff-0006", "g666gggg-0007"],
            "hashtag": ["hash1", "hash2", "hash3"],
            "value": [6, 7, 8],
        }),
        now=t_18_00
    )

    time.sleep(0.001)

    # 20:29 - Новый пост e26f9c4b (UUID начинается с 'e')
    t_20_29 = base_time + 4
    input_dt.store_chunk(
        pd.DataFrame({
            "id": ["e26f9c4b-0008"],
            "hashtag": ["looky"],
            "value": [9],
        }),
        now=t_20_29
    )

    time.sleep(0.001)

    # 20:30 - Еще несколько постов ПОСЛЕ e26f9c4b, но с РАЗНЫМИ timestamps
    # Эти посты критичны - у них id > e26f9c4b, но update_ts может быть старым!
    t_20_30 = base_time + 5
    input_dt.store_chunk(
        pd.DataFrame({
            "id": ["h777hhhh-0009", "i888iiii-0010", "j999jjjj-0011"],
            "hashtag": ["new1", "new2", "new3"],
            "value": [10, 11, 12],
        }),
        now=t_20_30  # Новое время!
    )

    time.sleep(0.001)

    # Критично: добавляем записи с id МЕЖДУ уже созданными, но со СТАРЫМ timestamp
    # Симулируем ситуацию где записи приходят не в порядке id
    t_19_00 = base_time + 2.5  # Старый timestamp (между 17:00 и 18:00)
    input_dt.store_chunk(
        pd.DataFrame({
            "id": ["f111ffff-late1", "f222ffff-late2"],  # id в середине диапазона
            "hashtag": ["late1", "late2"],
            "value": [98, 99],
        }),
        now=t_19_00  # СТАРЫЙ timestamp!
    )

    time.sleep(0.001)

    # Добавляем еще записей для полного второго батча (чтобы было 20+ записей → 2 батча)
    t_20_31 = base_time + 5.1
    input_dt.store_chunk(
        pd.DataFrame({
            "id": ["k111kkkk-0012", "l222llll-0013", "m333mmmm-0014",
                   "n444nnnn-0015", "o555oooo-0016", "p666pppp-0017"],
            "hashtag": ["extra1", "extra2", "extra3", "extra4", "extra5", "extra6"],
            "value": [12, 13, 14, 15, 16, 17],
        }),
        now=t_20_31
    )

    # ========== Проверяем накопленные данные ==========
    all_meta = input_dt.meta_table.get_metadata()
    print(f"\nВсего записей накоплено: {len(all_meta)}")
    print("Распределение по update_ts:")
    for idx, row in all_meta.sort_values("id").iterrows():
        print(f"  id={row['id'][:15]:15} hashtag={row['hashtag']:10} update_ts={row['update_ts']:.2f}")

    # ========== ПЕРВЫЙ ЗАПУСК copy_to_online ==========
    # Имитируем прерывание: обработаем только первые chunk_size=10 записей
    (idx_count, idx_gen) = step.get_full_process_ids(ds=ds, run_config=None)
    print(f"\nБатчей доступно для обработки: {idx_count}")

    # Обрабатываем ТОЛЬКО первый батч (как если бы джоба прервалась)
    first_batch_idx = next(idx_gen)
    idx_gen.close()  # Закрываем генератор
    print(f"Обрабатываем первый батч, размер: {len(first_batch_idx)}")
    step.run_idx(ds=ds, idx=first_batch_idx, run_config=None)

    # Получаем offset после первого запуска
    offsets = ds.offset_table.get_offsets_for_transformation(step.get_name())
    offset_after_first = offsets["first_run_input"]

    # Проверяем что обработано
    output_after_first = output_dt.get_data()
    processed_ids = set(output_after_first["id"].tolist())

    print(f"\n=== ПОСЛЕ ПЕРВОГО ЗАПУСКА ===")
    print(f"Обработано записей: {len(output_after_first)}")
    print(f"offset установлен на: {offset_after_first:.2f}")
    print(f"Обработанные id: {sorted(processed_ids)}")

    # ========== КРИТИЧНО: Какие записи НЕ обработаны? ==========
    all_input_ids = set(all_meta["id"].tolist())
    unprocessed_ids = all_input_ids - processed_ids

    if unprocessed_ids:
        print(f"\n=== НЕОБРАБОТАННЫЕ ЗАПИСИ ===")
        unprocessed_meta = all_meta[all_meta["id"].isin(unprocessed_ids)]
        for idx, row in unprocessed_meta.sort_values("id").iterrows():
            below_offset = row["update_ts"] < offset_after_first
            status = "БУДЕТ ПОТЕРЯНА!" if below_offset else "будет обработана"
            print(
                f"  id={row['id'][:15]:15} update_ts={row['update_ts']:.2f} "
                f"< offset={offset_after_first:.2f} ? {below_offset} → {status}"
            )

    # ========== ВТОРОЙ ЗАПУСК ==========
    # Получаем оставшиеся батчи (с учетом offset)
    (idx_count_second, idx_gen_second) = step.get_full_process_ids(ds=ds, run_config=None)
    print(f"\n=== ВТОРОЙ ЗАПУСК ===")
    print(f"Батчей доступно для обработки: {idx_count_second}")

    if idx_count_second > 0:
        # Обрабатываем оставшиеся батчи
        for idx in idx_gen_second:
            print(f"Обрабатываем батч, размер: {len(idx)}")
            step.run_idx(ds=ds, idx=idx, run_config=None)
        idx_gen_second.close()

    # ========== КРИТИЧНАЯ ПРОВЕРКА: ВСЕ записи должны быть обработаны ==========
    final_output = output_dt.get_data()
    final_processed_ids = set(final_output["id"].tolist())

    # Список потерянных записей
    lost_records = all_input_ids - final_processed_ids

    if lost_records:
        lost_meta = all_meta[all_meta["id"].isin(lost_records)]
        print(f"\n=== 🚨 ПОТЕРЯННЫЕ ЗАПИСИ (БАГ!) ===")
        for idx, row in lost_meta.sort_values("id").iterrows():
            print(
                f"  id={row['id'][:15]:15} hashtag={row['hashtag']:10} "
                f"update_ts={row['update_ts']:.2f} < offset={offset_after_first:.2f}"
            )

        pytest.fail(
            f"КРИТИЧЕСКИЙ БАГ ВОСПРОИЗВЕДЕН: {len(lost_records)} записей ПОТЕРЯНЫ!\n"
            f"Ожидалось: {len(all_input_ids)} записей\n"
            f"Получено:  {len(final_output)} записей\n"
            f"Потеряно:  {len(lost_records)} записей\n"
            f"Потерянные id: {sorted(lost_records)}\n\n"
            f"Это ТОЧНО воспроизводит production баг где было потеряно 60% данных!"
        )

    print(f"\n=== ФИНАЛЬНЫЙ РЕЗУЛЬТАТ ===")
    print(f"Всего записей в input: {len(all_input_ids)}")
    print(f"Обработано в output:   {len(final_output)}")
    print(f"✅ Все записи обработаны корректно!")


def test_first_run_invariant_all_records_below_offset_must_be_processed(dbconn: DBConn):
    """
    Проверяет инвариант для первого запуска:
    После первого запуска ВСЕ записи с update_ts <= offset должны быть обработаны.

    Это более общий тест который проверяет что независимо от:
    - Порядка создания записей
    - Порядка их id
    - Размера батча

    После установки offset НЕ ДОЛЖНО остаться необработанных записей с update_ts < offset.
    """
    ds = DataStore(dbconn, create_meta_table=True)

    input_store = TableStoreDB(
        dbconn,
        "invariant_input",
        [Column("id", String, primary_key=True), Column("value", Integer)],
        create_table=True,
    )
    input_dt = ds.create_table("invariant_input", input_store)

    output_store = TableStoreDB(
        dbconn,
        "invariant_output",
        [Column("id", String, primary_key=True), Column("value", Integer)],
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
        chunk_size=5,
    )

    # Создаем записи с разными update_ts и разными id (не в порядке времени)
    base_time = time.time()

    records = [
        ("z999", base_time + 1),   # Поздний id, ранний timestamp
        ("a111", base_time + 5),   # Ранний id, поздний timestamp
        ("m555", base_time + 2),   # Средний id, средний timestamp
        ("b222", base_time + 3),
        ("y888", base_time + 1.5),
        ("c333", base_time + 4),
        ("x777", base_time + 2.5),
    ]

    for record_id, timestamp in records:
        input_dt.store_chunk(
            pd.DataFrame({"id": [record_id], "value": [int(timestamp)]}),
            now=timestamp
        )
        time.sleep(0.001)

    # Первый запуск
    step.run_full(ds)

    # Получаем offset
    offsets = ds.offset_table.get_offsets_for_transformation(step.get_name())
    current_offset = offsets["invariant_input"]

    # ИНВАРИАНТ: ВСЕ записи с update_ts <= current_offset должны быть обработаны
    all_meta = input_dt.meta_table.get_metadata()
    output_data = output_dt.get_data()
    processed_ids = set(output_data["id"].tolist())

    violations = []
    for idx, row in all_meta.iterrows():
        if row["update_ts"] <= current_offset:
            if row["id"] not in processed_ids:
                violations.append(row)

    if violations:
        print(f"\n🚨 НАРУШЕНИЕ ИНВАРИАНТА!")
        print(f"offset = {current_offset}")
        print(f"Необработанные записи с update_ts <= offset:")
        for row in violations:
            print(f"  id={row['id']} update_ts={row['update_ts']}")

        pytest.fail(
            f"НАРУШЕНИЕ ИНВАРИАНТА: {len(violations)} записей с update_ts <= offset НЕ обработаны!\n"
            f"Это означает потерю данных."
        )

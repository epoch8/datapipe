"""
Тесты для воспроизведения production бага с offset optimization.

Production issue:
- После деплоя offset optimization было потеряно 60% данных (48,915 из 82,000 записей)
- Корневая причина: update_ts vs process_ts расхождение при чтении таблиц
- Промежуточные трансформации обновляют process_ts но не update_ts
- Offset фильтрация использует update_ts, пропуская "устаревшие" записи

Эти тесты ДОЛЖНЫ ПАДАТЬ на текущей реализации (демонстрируя баг).
После фикса они должны проходить.
"""
import time

import pandas as pd
import pytest
from sqlalchemy import Column, Integer, String

from datapipe.compute import ComputeInput
from datapipe.datatable import DataStore
from datapipe.step.batch_transform import BatchTransformStep, DatatableBatchTransformStep
from datapipe.store.database import DBConn, TableStoreDB


@pytest.mark.xfail(reason="Reproduces production bug: offset skips records after intermediate transformation")
def test_offset_skips_records_with_intermediate_transformation(dbconn: DBConn):
    """
    Воспроизводит основной баг из production.

    Сценарий:
    1. Создается запись в таблице (update_ts=T1, process_ts=T1)
    2. Промежуточная трансформация (DatatableBatchTransform) ЧИТАЕТ запись
       - Обновляет process_ts=T2
       - НЕ обновляет update_ts (остается T1)
    3. Создается новая запись (update_ts=T3)
    4. Финальная трансформация с offset обрабатывает батч
       - Батч содержит: старую запись (update_ts=T1) и новую (update_ts=T3)
       - После обработки: offset = MAX(T1, T3) = T3
    5. Следующий запуск: WHERE update_ts > T3
       - Старые записи с update_ts=T1 пропускаются!

    Реальный пример из production:
    - 16:21 - Пост создается (update_ts=16:21, process_ts=16:21)
    - 20:04 - hashtag_statistics_aggregation читает (update_ts=16:21, process_ts=20:04)
    - 20:29 - Новый пост (update_ts=20:29)
    - copy_to_online устанавливает offset=20:29
    - Следующий запуск пропускает все с update_ts < 20:29
    """
    ds = DataStore(dbconn, create_meta_table=True)

    # ========== Создаем таблицу данных (как post_hashtag_lower) ==========
    data_store = TableStoreDB(
        dbconn,
        "data_table",
        [
            Column("id", String, primary_key=True),
            Column("hashtag", String, primary_key=True),
            Column("value", Integer),
        ],
        create_table=True,
    )
    data_dt = ds.create_table("data_table", data_store)

    # ========== Создаем таблицу агрегации (как hashtag_lower) ==========
    stats_store = TableStoreDB(
        dbconn,
        "stats_table",
        [
            Column("hashtag", String, primary_key=True),
            Column("count", Integer),
        ],
        create_table=True,
    )
    stats_dt = ds.create_table("stats_table", stats_store)

    # ========== Создаем таблицу назначения (как post_hashtag_lower_online) ==========
    target_store = TableStoreDB(
        dbconn,
        "target_table",
        [
            Column("id", String, primary_key=True),
            Column("hashtag", String, primary_key=True),
            Column("value", Integer),
        ],
        create_table=True,
    )
    target_dt = ds.create_table("target_table", target_store)

    # ========== Промежуточная трансформация (читает, но не изменяет данные) ==========
    def stats_aggregation(ds, idx, input_dts, run_config=None, kwargs=None):
        """Агрегирует статистику - аналог hashtag_statistics_aggregation"""
        df = input_dts[0].get_data(idx)
        result = df.groupby("hashtag").size().reset_index(name="count")
        return [(result, None)]

    intermediate_step = DatatableBatchTransformStep(
        ds=ds,
        name="stats_aggregation",
        func=stats_aggregation,
        input_dts=[ComputeInput(dt=data_dt, join_type="full")],
        output_dts=[stats_dt],
        transform_keys=["hashtag"],
        use_offset_optimization=True,
    )

    # ========== Финальная трансформация (копирует в target) ==========
    def copy_transform(df):
        """Копирует данные - аналог copy_to_online"""
        return df[["id", "hashtag", "value"]]

    final_step = BatchTransformStep(
        ds=ds,
        name="copy_to_target",
        func=copy_transform,
        input_dts=[ComputeInput(dt=data_dt, join_type="full")],
        output_dts=[target_dt],
        transform_keys=["id", "hashtag"],
        use_offset_optimization=True,
    )

    # ========== Шаг 1: Создаем первую запись ==========
    t1 = time.time()
    data_dt.store_chunk(
        pd.DataFrame({
            "id": ["post1"],
            "hashtag": ["test"],
            "value": [100],
        }),
        now=t1
    )

    # Проверяем метаданные
    meta1 = data_dt.meta_table.get_metadata()
    assert len(meta1) == 1
    initial_update_ts = meta1.iloc[0]["update_ts"]
    initial_process_ts = meta1.iloc[0]["process_ts"]
    assert initial_update_ts == initial_process_ts  # Изначально равны

    # ========== Шаг 2: Промежуточная трансформация ЧИТАЕТ данные ==========
    time.sleep(0.01)
    intermediate_step.run_full(ds)

    # Проверяем что process_ts обновился, но update_ts НЕТ
    meta2 = data_dt.meta_table.get_metadata()
    after_read_update_ts = meta2.iloc[0]["update_ts"]
    after_read_process_ts = meta2.iloc[0]["process_ts"]

    # КРИТИЧНАЯ ПРОВЕРКА: update_ts НЕ изменился, process_ts изменился
    assert after_read_update_ts == initial_update_ts, "update_ts не должен измениться при чтении"
    assert after_read_process_ts > initial_process_ts, "process_ts должен обновиться после чтения"

    # ========== Шаг 3: Создаем новую запись (с более поздним update_ts) ==========
    time.sleep(0.01)
    t3 = time.time()
    data_dt.store_chunk(
        pd.DataFrame({
            "id": ["post2"],
            "hashtag": ["demo"],
            "value": [200],
        }),
        now=t3
    )

    # ========== Шаг 4: Финальная трансформация обрабатывает ОБЕ записи ==========
    final_step.run_full(ds)

    # Проверяем что обе записи скопировались
    target_data_1 = target_dt.get_data()
    assert len(target_data_1) == 2, "Обе записи должны быть скопированы"
    assert set(target_data_1["id"].tolist()) == {"post1", "post2"}

    # ========== Шаг 5: Добавляем еще одну запись ==========
    time.sleep(0.01)
    t5 = time.time()
    data_dt.store_chunk(
        pd.DataFrame({
            "id": ["post3"],
            "hashtag": ["new"],
            "value": [300],
        }),
        now=t5
    )

    # ========== Шаг 6: Финальная трансформация запускается снова ==========
    # Из-за offset optimization должна обработать ТОЛЬКО post3
    # НО: если в батч попадет post1 (у которой старый update_ts),
    # offset обновится на MAX(update_ts), и post1 будет потеряна при следующем запуске

    changed_count = final_step.get_changed_idx_count(ds)
    # Ожидаем что видна только 1 новая запись (post3)
    assert changed_count == 1, f"Должна быть видна только новая запись, получено: {changed_count}"

    final_step.run_full(ds)

    # Проверяем что все 3 записи в target
    target_data_2 = target_dt.get_data()
    assert len(target_data_2) == 3, f"Все 3 записи должны быть в target, получено: {len(target_data_2)}"
    assert set(target_data_2["id"].tolist()) == {"post1", "post2", "post3"}

    # ========== Шаг 7: Симулируем что промежуточная трансформация снова читает старые данные ==========
    # Это может произойти если она запускается без offset или перезапускается
    time.sleep(0.01)

    # Обновляем process_ts для post1 снова (симулируем повторное чтение)
    meta_post1 = data_dt.meta_table.get_metadata(pd.DataFrame({"id": ["post1"], "hashtag": ["test"]}))
    current_update_ts_post1 = meta_post1.iloc[0]["update_ts"]

    # После повторного чтения process_ts должен обновиться
    intermediate_step.run_full(ds)

    meta_post1_after = data_dt.meta_table.get_metadata(pd.DataFrame({"id": ["post1"], "hashtag": ["test"]}))
    new_process_ts_post1 = meta_post1_after.iloc[0]["process_ts"]
    new_update_ts_post1 = meta_post1_after.iloc[0]["update_ts"]

    # update_ts по-прежнему старый!
    assert new_update_ts_post1 == current_update_ts_post1

    # ========== Шаг 8: Добавляем еще записи ==========
    time.sleep(0.01)
    t8 = time.time()
    data_dt.store_chunk(
        pd.DataFrame({
            "id": ["post4"],
            "hashtag": ["another"],
            "value": [400],
        }),
        now=t8
    )

    # ========== Шаг 9: Финальная трансформация ==========
    # ПРОБЛЕМА: Если в changed records попадут post1 (update_ts старый) и post4 (update_ts новый)
    # То offset = MAX(старый, новый) = новый
    # И при следующем запуске post1 будет пропущена

    final_step.run_full(ds)

    # Финальная проверка: ВСЕ записи должны быть в target
    final_target_data = target_dt.get_data()
    assert len(final_target_data) == 4, (
        f"БАГ ВОСПРОИЗВЕДЕН: Ожидалось 4 записи в target, получено {len(final_target_data)}. "
        f"Пропущенные записи с старым update_ts!"
    )
    assert set(final_target_data["id"].tolist()) == {"post1", "post2", "post3", "post4"}


@pytest.mark.xfail(reason="Reproduces production bug: ORDER BY non-temporal causes data loss")
def test_offset_with_non_temporal_ordering(dbconn: DBConn):
    """
    Воспроизводит Сценарий 2 из production: ORDER BY (id, hashtag) вместо update_ts.

    Сценарий:
    1. В таблице есть записи с разными update_ts и id
    2. Запрос использует ORDER BY id, hashtag LIMIT N
    3. Обработаны записи в порядке id (не по времени!)
    4. offset = MAX(update_ts) из обработанных записей
    5. Добавляется новая запись с id в середине диапазона
    6. Следующий запрос: WHERE update_ts > offset
       - Запись отфильтрована по update_ts
       - Но она также "пропущена" в сортировке по id

    Реальный пример из production:
    Данные: [
      {id: "a1", hashtag: "test", update_ts: 19:00},
      {id: "b2", hashtag: "demo", update_ts: 18:00},
      {id: "c3", hashtag: "foo", update_ts: 17:00},
      {id: "z9", hashtag: "bar", update_ts: 20:00}
    ]
    Запрос 1: ORDER BY id, hashtag LIMIT 3
      → a1(19:00), b2(18:00), c3(17:00)
      → offset = 19:00
    Новая запись: {id: "d4", hashtag: "new", update_ts: 18:30}
    Запрос 2: WHERE update_ts > 19:00
      → d4 пропущена (18:30 < 19:00)!
    """
    ds = DataStore(dbconn, create_meta_table=True)

    input_store = TableStoreDB(
        dbconn,
        "input_ordering",
        [
            Column("id", String, primary_key=True),
            Column("hashtag", String, primary_key=True),
            Column("value", Integer),
        ],
        create_table=True,
    )
    input_dt = ds.create_table("input_ordering", input_store)

    output_store = TableStoreDB(
        dbconn,
        "output_ordering",
        [
            Column("id", String, primary_key=True),
            Column("hashtag", String, primary_key=True),
            Column("value", Integer),
        ],
        create_table=True,
    )
    output_dt = ds.create_table("output_ordering", output_store)

    def copy_func(df):
        return df[["id", "hashtag", "value"]]

    step = BatchTransformStep(
        ds=ds,
        name="copy_ordering",
        func=copy_func,
        input_dts=[ComputeInput(dt=input_dt, join_type="full")],
        output_dts=[output_dt],
        transform_keys=["id", "hashtag"],
        use_offset_optimization=True,
        chunk_size=3,  # Ограничиваем размер батча чтобы обработать только первые 3
    )

    # ========== Шаг 1: Создаем записи с разными timestamp в несортированном порядке ==========
    base_time = time.time()

    # a1 - update_ts будет средним
    input_dt.store_chunk(
        pd.DataFrame({"id": ["a1"], "hashtag": ["test"], "value": [1]}),
        now=base_time + 2  # T=2 (средний)
    )

    time.sleep(0.01)

    # b2 - update_ts будет ранним
    input_dt.store_chunk(
        pd.DataFrame({"id": ["b2"], "hashtag": ["demo"], "value": [2]}),
        now=base_time + 1  # T=1 (ранний)
    )

    time.sleep(0.01)

    # c3 - update_ts будет самым ранним
    input_dt.store_chunk(
        pd.DataFrame({"id": ["c3"], "hashtag": ["foo"], "value": [3]}),
        now=base_time + 0  # T=0 (самый ранний)
    )

    time.sleep(0.01)

    # z9 - update_ts будет самым поздним
    input_dt.store_chunk(
        pd.DataFrame({"id": ["z9"], "hashtag": ["bar"], "value": [4]}),
        now=base_time + 3  # T=3 (самый поздний)
    )

    # ========== Шаг 2: Первый запуск - обрабатываем только первые 3 (по ORDER BY id) ==========
    # Из-за ORDER BY id, hashtag получим: a1, b2, c3
    # update_ts этих записей: T=2, T=1, T=0
    # offset = MAX(2, 1, 0) = 2

    step.run_full(ds)

    # Проверяем что обработались первые 3 записи (по id)
    output_data_1 = output_dt.get_data()
    output_ids_1 = sorted(output_data_1["id"].tolist())
    assert len(output_data_1) == 3
    assert output_ids_1 == ["a1", "b2", "c3"], f"Ожидались a1,b2,c3, получено: {output_ids_1}"

    # Проверяем offset
    offsets = ds.offset_table.get_offsets_for_transformation(step.get_name())
    offset_value = offsets["input_ordering"]

    # offset должен быть максимальным из обработанных (T=2)
    assert offset_value >= base_time + 2

    # ========== Шаг 3: Добавляем запись d4 с промежуточным timestamp ==========
    time.sleep(0.01)
    # update_ts между T=1 и T=2, но меньше offset=T=2
    input_dt.store_chunk(
        pd.DataFrame({"id": ["d4"], "hashtag": ["new"], "value": [5]}),
        now=base_time + 1.5  # T=1.5 (между b2 и a1)
    )

    # ========== Шаг 4: Проверяем видимость новых записей ==========
    # Должны быть видны: z9 (T=3 > offset=2) и d4 (T=1.5 < offset=2 - НЕ ВИДНА!)
    changed_count = step.get_changed_idx_count(ds)

    # На самом деле будет видна только z9, так как d4 отфильтрована по offset
    # Это и есть баг!

    # ========== Шаг 5: Второй запуск ==========
    step.run_full(ds)

    # ========== КРИТИЧНАЯ ПРОВЕРКА: Все записи должны быть в output ==========
    final_output = output_dt.get_data()
    final_ids = sorted(final_output["id"].tolist())

    # Эта проверка должна падать
    assert len(final_output) == 5, (
        f"БАГ ВОСПРОИЗВЕДЕН: Ожидалось 5 записей, получено {len(final_output)}. "
        f"Запись d4 с промежуточным update_ts пропущена!"
    )
    assert final_ids == ["a1", "b2", "c3", "d4", "z9"], (
        f"Ожидались все записи включая d4, получено: {final_ids}"
    )


@pytest.mark.xfail(reason="Reproduces production bug: process_ts vs update_ts divergence")
def test_process_ts_vs_update_ts_divergence(dbconn: DBConn):
    """
    Проверяет семантику update_ts vs process_ts.

    Проблема:
    - update_ts обновляется только когда ДАННЫЕ изменяются
    - process_ts обновляется когда запись ОБРАБАТЫВАЕТСЯ (читается)
    - Offset фильтрация использует update_ts
    - Если трансформация только читает (не изменяет), process_ts != update_ts
    - Следующая трансформация с offset может пропустить "обработанные но не измененные" записи

    Этот тест проверяет базовый инвариант:
    - После создания: update_ts == process_ts ✓
    - После чтения: update_ts < process_ts (расхождение!)
    - Offset использует update_ts → данные теряются
    """
    ds = DataStore(dbconn, create_meta_table=True)

    # Создаем таблицу
    table_store = TableStoreDB(
        dbconn,
        "timestamps_table",
        [
            Column("id", String, primary_key=True),
            Column("value", Integer),
        ],
        create_table=True,
    )
    table_dt = ds.create_table("timestamps_table", table_store)

    # Создаем выходную таблицу для чтения (не изменяет данные)
    read_output_store = TableStoreDB(
        dbconn,
        "read_output",
        [Column("id", String, primary_key=True), Column("count", Integer)],
        create_table=True,
    )
    read_output_dt = ds.create_table("read_output", read_output_store)

    # Трансформация которая ЧИТАЕТ но НЕ ИЗМЕНЯЕТ входные данные
    def read_only_transform(ds, idx, input_dts, run_config=None, kwargs=None):
        """Просто считает записи - не изменяет источник"""
        df = input_dts[0].get_data(idx)
        result = pd.DataFrame({"id": ["summary"], "count": [len(df)]})
        return [(result, None)]

    read_step = DatatableBatchTransformStep(
        ds=ds,
        name="read_only",
        func=read_only_transform,
        input_dts=[ComputeInput(dt=table_dt, join_type="full")],
        output_dts=[read_output_dt],
        transform_keys=["id"],
        use_offset_optimization=True,
    )

    # Создаем выходную таблицу для копирования
    copy_output_store = TableStoreDB(
        dbconn,
        "copy_output",
        [Column("id", String, primary_key=True), Column("value", Integer)],
        create_table=True,
    )
    copy_output_dt = ds.create_table("copy_output", copy_output_store)

    # Трансформация которая копирует данные
    def copy_transform(df):
        return df[["id", "value"]]

    copy_step = BatchTransformStep(
        ds=ds,
        name="copy_step",
        func=copy_transform,
        input_dts=[ComputeInput(dt=table_dt, join_type="full")],
        output_dts=[copy_output_dt],
        transform_keys=["id"],
        use_offset_optimization=True,
    )

    # ========== Шаг 1: Создаем записи ==========
    t1 = time.time()
    table_dt.store_chunk(
        pd.DataFrame({"id": ["rec1", "rec2"], "value": [10, 20]}),
        now=t1
    )

    # Проверяем что после создания update_ts == process_ts
    meta_after_create = table_dt.meta_table.get_metadata()
    for idx, row in meta_after_create.iterrows():
        assert row["update_ts"] == row["process_ts"], (
            f"После создания update_ts должен равняться process_ts"
        )
        create_update_ts = row["update_ts"]
        create_process_ts = row["process_ts"]

    # ========== Шаг 2: Трансформация ЧИТАЕТ данные ==========
    time.sleep(0.01)
    read_step.run_full(ds)

    # Проверяем что process_ts обновился, но update_ts НЕТ
    meta_after_read = table_dt.meta_table.get_metadata()
    for idx, row in meta_after_read.iterrows():
        assert row["update_ts"] == create_update_ts, (
            f"После чтения update_ts НЕ должен измениться"
        )
        assert row["process_ts"] > create_process_ts, (
            f"После чтения process_ts должен обновиться"
        )
        # КРИТИЧНО: Теперь update_ts < process_ts (расхождение!)
        assert row["update_ts"] < row["process_ts"], (
            "РАСХОЖДЕНИЕ: update_ts < process_ts после чтения данных"
        )

    # ========== Шаг 3: Добавляем новую запись с более поздним timestamp ==========
    time.sleep(0.01)
    t3 = time.time()
    table_dt.store_chunk(
        pd.DataFrame({"id": ["rec3"], "value": [30]}),
        now=t3
    )

    # ========== Шаг 4: Копируем все записи ==========
    copy_step.run_full(ds)

    # Проверяем что все 3 записи скопировались
    copy_output_1 = copy_output_dt.get_data()
    assert len(copy_output_1) == 3
    assert set(copy_output_1["id"].tolist()) == {"rec1", "rec2", "rec3"}

    # Проверяем offset - будет MAX(update_ts) = t3
    offsets = ds.offset_table.get_offsets_for_transformation(copy_step.get_name())
    offset_after_copy = offsets["timestamps_table"]
    assert offset_after_copy >= t3

    # ========== Шаг 5: Читаем старые записи снова ==========
    time.sleep(0.01)
    read_step.run_full(ds)

    # process_ts для rec1, rec2 снова обновился
    meta_after_read2 = table_dt.meta_table.get_metadata()
    rec1_meta = meta_after_read2[meta_after_read2["id"] == "rec1"].iloc[0]

    # update_ts все еще старый (t1), но process_ts свежий
    assert rec1_meta["update_ts"] == create_update_ts
    assert rec1_meta["process_ts"] > create_process_ts

    # ========== Шаг 6: Добавляем еще одну запись ==========
    time.sleep(0.01)
    t6 = time.time()
    table_dt.store_chunk(
        pd.DataFrame({"id": ["rec4"], "value": [40]}),
        now=t6
    )

    # ========== Шаг 7: Копируем снова ==========
    # ПРОБЛЕМА: Если offset основан на update_ts,
    # а rec1, rec2 имеют старый update_ts (t1 < offset),
    # они могут быть пропущены при определенных условиях

    copy_step.run_full(ds)

    # ========== КРИТИЧНАЯ ПРОВЕРКА: Все 4 записи должны быть скопированы ==========
    final_copy_output = copy_output_dt.get_data()
    assert len(final_copy_output) == 4, (
        f"БАГ: Ожидалось 4 записи, получено {len(final_copy_output)}. "
        f"Записи с update_ts < process_ts могут быть пропущены!"
    )
    assert set(final_copy_output["id"].tolist()) == {"rec1", "rec2", "rec3", "rec4"}


@pytest.mark.xfail(reason="Reproduces production bug: full chain like in production")
def test_copy_to_online_with_stats_aggregation_chain(dbconn: DBConn):
    """
    Интеграционный тест: полная цепочка как в production.

    Цепочка:
    1. post_hashtag_scraping_lower создает записи в post_hashtag_lower
    2. hashtag_statistics_aggregation ЧИТАЕТ post_hashtag_lower (агрегирует статистику)
       - Обновляет process_ts для прочитанных записей
       - НЕ обновляет update_ts (данные не изменились)
    3. copy_to_online копирует из post_hashtag_lower в post_hashtag_lower_online
       - Использует offset на основе update_ts
       - Пропускает записи с "устаревшим" update_ts

    Реальный сценарий из production:
    - 16:21 - Пост создан, хештеги извлечены
    - 20:04 - hashtag_statistics_aggregation прочитал (process_ts=20:04, update_ts=16:21)
    - 20:29 - Новый пост создан (update_ts=20:29)
    - copy_to_online обработал обе записи, offset=20:29
    - Следующий запуск copy_to_online пропустил все записи с update_ts<20:29
    - Результат: 60% данных потеряно
    """
    ds = DataStore(dbconn, create_meta_table=True)

    # ========== Таблица: post_hashtag_lower (offline) ==========
    post_hashtag_store = TableStoreDB(
        dbconn,
        "post_hashtag_lower",
        [
            Column("id", String, primary_key=True),
            Column("hashtag", String, primary_key=True),
            Column("created_at", Integer),
        ],
        create_table=True,
    )
    post_hashtag_dt = ds.create_table("post_hashtag_lower", post_hashtag_store)

    # ========== Таблица: hashtag_lower (статистика) ==========
    hashtag_stats_store = TableStoreDB(
        dbconn,
        "hashtag_lower",
        [
            Column("hashtag", String, primary_key=True),
            Column("post_count", Integer),
            Column("last_seen", Integer),
        ],
        create_table=True,
    )
    hashtag_stats_dt = ds.create_table("hashtag_lower", hashtag_stats_store)

    # ========== Таблица: post_hashtag_lower_online ==========
    post_hashtag_online_store = TableStoreDB(
        dbconn,
        "post_hashtag_lower_online",
        [
            Column("id", String, primary_key=True),
            Column("hashtag", String, primary_key=True),
            Column("created_at", Integer),
        ],
        create_table=True,
    )
    post_hashtag_online_dt = ds.create_table("post_hashtag_lower_online", post_hashtag_online_store)

    # ========== Трансформация 1: hashtag_statistics_aggregation ==========
    def aggregate_hashtag_stats(ds, idx, input_dts, run_config=None, kwargs=None):
        """Агрегирует статистику по хештегам"""
        df = input_dts[0].get_data(idx)
        stats = df.groupby("hashtag").agg({
            "id": "count",
            "created_at": "max"
        }).reset_index()
        stats.columns = ["hashtag", "post_count", "last_seen"]
        return [(stats, None)]

    stats_step = DatatableBatchTransformStep(
        ds=ds,
        name="hashtag_statistics_aggregation",
        func=aggregate_hashtag_stats,
        input_dts=[ComputeInput(dt=post_hashtag_dt, join_type="full")],
        output_dts=[hashtag_stats_dt],
        transform_keys=["hashtag"],
        use_offset_optimization=True,
    )

    # ========== Трансформация 2: copy_to_online ==========
    def copy_to_online(df):
        """Копирует данные в online БД"""
        return df[["id", "hashtag", "created_at"]]

    copy_step = BatchTransformStep(
        ds=ds,
        name="copy_to_online",
        func=copy_to_online,
        input_dts=[ComputeInput(dt=post_hashtag_dt, join_type="full")],
        output_dts=[post_hashtag_online_dt],
        transform_keys=["id", "hashtag"],
        use_offset_optimization=True,
    )

    # ========== Симуляция production сценария ==========

    # Время 16:21 - Пост b927ca71 создан с 5 хештегами
    t_16_21 = time.time()
    post_hashtag_dt.store_chunk(
        pd.DataFrame({
            "id": ["b927ca71"] * 5,
            "hashtag": ["322", "anime", "looky", "test77", "ошош"],
            "created_at": [int(t_16_21)] * 5,
        }),
        now=t_16_21
    )

    # Проверяем метаданные после создания
    meta_after_create = post_hashtag_dt.meta_table.get_metadata()
    assert len(meta_after_create) == 5
    initial_update_ts = meta_after_create.iloc[0]["update_ts"]

    # Время 20:04 - hashtag_statistics_aggregation обрабатывает
    time.sleep(0.01)
    stats_step.run_full(ds)

    # Проверяем что статистика создана
    stats_data = hashtag_stats_dt.get_data()
    assert len(stats_data) == 5

    # КРИТИЧНО: process_ts обновился, update_ts НЕТ
    meta_after_stats = post_hashtag_dt.meta_table.get_metadata()
    for idx, row in meta_after_stats.iterrows():
        assert row["update_ts"] == initial_update_ts, "update_ts не должен измениться"
        assert row["process_ts"] > initial_update_ts, "process_ts должен обновиться"

    # Время 20:29 - Новый пост e26f9c4b создан
    time.sleep(0.01)
    t_20_29 = time.time()
    post_hashtag_dt.store_chunk(
        pd.DataFrame({
            "id": ["e26f9c4b"],
            "hashtag": ["demo"],
            "created_at": [int(t_20_29)],
        }),
        now=t_20_29
    )

    # Время 20:30 - copy_to_online запускается ВПЕРВЫЕ
    time.sleep(0.01)
    copy_step.run_full(ds)

    # Проверяем что ВСЕ 6 хештегов скопированы
    online_data_1 = post_hashtag_online_dt.get_data()
    assert len(online_data_1) == 6, f"Ожидалось 6 записей, получено {len(online_data_1)}"

    # Проверяем offset
    offsets = ds.offset_table.get_offsets_for_transformation(copy_step.get_name())
    offset_after_first_copy = offsets["post_hashtag_lower"]

    # Offset будет MAX(update_ts) из обработанных записей
    # Это будет t_20_29 (самый новый update_ts)
    assert offset_after_first_copy >= t_20_29

    # Время 09:00 (следующий день) - Еще посты создаются
    time.sleep(0.01)
    t_09_00 = time.time()
    post_hashtag_dt.store_chunk(
        pd.DataFrame({
            "id": ["f79ec772", "f79ec772"],
            "hashtag": ["новый", "хештег"],
            "created_at": [int(t_09_00)] * 2,
        }),
        now=t_09_00
    )

    # hashtag_statistics_aggregation снова обрабатывает (включая старые записи)
    time.sleep(0.01)
    stats_step.run_full(ds)

    # Проверяем что process_ts для старых записей снова обновился
    meta_old_posts = post_hashtag_dt.meta_table.get_metadata(
        pd.DataFrame({
            "id": ["b927ca71"] * 5,
            "hashtag": ["322", "anime", "looky", "test77", "ошош"],
        })
    )
    for idx, row in meta_old_posts.iterrows():
        # update_ts все еще старый!
        assert row["update_ts"] == initial_update_ts
        # process_ts свежий
        assert row["process_ts"] > offset_after_first_copy

    # copy_to_online запускается снова
    time.sleep(0.01)
    copy_step.run_full(ds)

    # ========== КРИТИЧНАЯ ПРОВЕРКА: Все 8 записей должны быть в online ==========
    final_online_data = post_hashtag_online_dt.get_data()

    # БАГ: Старые записи (b927ca71) с update_ts < offset будут пропущены
    # если они попадут в батч вместе с новыми записями
    assert len(final_online_data) == 8, (
        f"БАГ ВОСПРОИЗВЕДЕН: Ожидалось 8 записей в online, получено {len(final_online_data)}. "
        f"Это симулирует потерю 60% данных из production!"
    )

    # Проверяем что все посты присутствуют
    online_post_ids = set(final_online_data["id"].unique())
    expected_ids = {"b927ca71", "e26f9c4b", "f79ec772"}
    assert online_post_ids == expected_ids, (
        f"Ожидались посты {expected_ids}, получено {online_post_ids}"
    )

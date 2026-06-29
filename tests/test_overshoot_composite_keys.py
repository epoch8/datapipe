"""
Failing test для демонстрации overshoot проблемы с composite keys
"""
import time
import pandas as pd
from sqlalchemy import Column, Integer, String, Float

from datapipe.compute import ComputeInput
from datapipe.datatable import DataStore
from datapipe.step.batch_transform import BatchTransformStep
from datapipe.store.database import DBConn, TableStoreDB


def test_offset_overshoot_with_composite_keys(dbconn: DBConn):
    """
    Демонстрирует проблему overshoot при использовании composite keys.

    Сценарий:
    - PK входной таблицы: (id, profile_id, post_id)
    - transform_keys: (profile_id, post_id) - без id
    - Для пары (p1, post1) есть несколько событий с разными id
    - max_records_per_run ограничивает батч

    Проблема:
    - Офсет вычисляется по ВСЕМ событиям обработанных пар
    - А не только по событиям из батча
    - Результат: overshoot - офсет прыгает через необработанные данные
    """
    ds = DataStore(dbconn, create_meta_table=True)

    # Входная таблица с composite PK
    input_store = TableStoreDB(
        dbconn,
        "amplitude_events",
        [
            Column("id", Integer, primary_key=True),
            Column("profile_id", String, primary_key=True),
            Column("post_id", String, primary_key=True),
            Column("event_type", String),
        ],
        create_table=True,
    )
    input_dt = ds.create_table("amplitude_events", input_store)

    # Выходная таблица
    output_store = TableStoreDB(
        dbconn,
        "post_deepview",
        [
            Column("profile_id", String, primary_key=True),
            Column("post_id", String, primary_key=True),
            Column("deepview_count", Integer),
        ],
        create_table=True,
    )
    output_dt = ds.create_table("post_deepview", output_store)

    # Transform функция: считает deepview для каждой пары
    def parse_post_deepview(df):
        result = df.groupby(['profile_id', 'post_id']).agg({
            'event_type': 'count'
        }).reset_index()
        result = result.rename(columns={'event_type': 'deepview_count'})
        return result

    step = BatchTransformStep(
        ds=ds,
        name="parse_post_deepview",
        func=parse_post_deepview,
        input_dts=[ComputeInput(dt=input_dt)],  # БЕЗ join_type="full"!
        output_dts=[output_dt],
        transform_keys=["profile_id", "post_id"],  # БЕЗ id!
        use_offset_optimization=True,
        max_records_per_run=5,  # Ограничиваем батч
    )

    # ========== ДАННЫЕ ==========

    # Timestamp для разных батчей
    old_ts = time.time() - 100
    middle_ts = old_ts + 10
    new_ts = middle_ts + 10

    # БАТЧ 1: Старые события (update_ts = old_ts)
    # 3 события для пары (p1, post1) - id=1,2,3
    # 2 события для пары (p2, post2) - id=4,5
    input_dt.store_chunk(
        pd.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "profile_id": ["p1", "p1", "p1", "p2", "p2"],
            "post_id": ["post1", "post1", "post1", "post2", "post2"],
            "event_type": ["deepPostView"] * 5,
        }),
        now=old_ts,
    )

    # БАТЧ 2: Средние события (update_ts = middle_ts)
    # 2 события для пары (p1, post1) - id=6,7
    # 1 событие для пары (p3, post3) - id=8
    input_dt.store_chunk(
        pd.DataFrame({
            "id": [6, 7, 8],
            "profile_id": ["p1", "p1", "p3"],
            "post_id": ["post1", "post1", "post3"],
            "event_type": ["deepPostView"] * 3,
        }),
        now=middle_ts,
    )

    # БАТЧ 3: Новые события (update_ts = new_ts)
    # 3 события для пары (p1, post1) - id=9,10,11
    input_dt.store_chunk(
        pd.DataFrame({
            "id": [9, 10, 11],
            "profile_id": ["p1", "p1", "p1"],
            "post_id": ["post1", "post1", "post1"],
            "event_type": ["deepPostView"] * 3,
        }),
        now=new_ts,
    )

    print("\n" + "=" * 80)
    print("ДАННЫЕ В amplitude_events:")
    print("=" * 80)
    all_data = input_dt.get_data().sort_values("id")
    print(all_data.to_string())
    print(f"\nВсего событий: {len(all_data)}")
    print(f"Для пары (p1, post1): {len(all_data[all_data['profile_id'] == 'p1'])} событий")
    print(f"  - id=1,2,3 с update_ts={old_ts}")
    print(f"  - id=6,7 с update_ts={middle_ts}")
    print(f"  - id=9,10,11 с update_ts={new_ts}")

    # ========== ПЕРВЫЙ ЗАПУСК ==========
    print("\n" + "=" * 80)
    print("ПЕРВЫЙ ЗАПУСК (max_records_per_run=5):")
    print("=" * 80)

    # Проверяем changed_idx_count
    changed_count = step.get_changed_idx_count(ds)
    print(f"\nChanged idx count: {changed_count}")

    step.run_full(ds)

    # Проверяем что обработали
    output = output_dt.get_data()
    print(f"\nОбработанные пары:")
    print(output.to_string())

    # Проверяем офсет
    offsets = ds.offset_table.get_offsets_for_transformation(step.get_name())
    offset_value = offsets.get("amplitude_events")

    print(f"\nОфсет после первого запуска: {offset_value}")
    if offset_value:
        print(f"Офсет (datetime): {pd.Timestamp(offset_value, unit='s')}")
        print(f"old_ts (datetime): {pd.Timestamp(old_ts, unit='s')}")
        print(f"middle_ts (datetime): {pd.Timestamp(middle_ts, unit='s')}")
        print(f"new_ts (datetime): {pd.Timestamp(new_ts, unit='s')}")

    # ========== ПРОВЕРКА ==========
    print("\n" + "=" * 80)
    print("ПРОВЕРКА:")
    print("=" * 80)

    # Changed idx query вернул первые 5 событий (LIMIT 5):
    # id=1,2,3,4,5 с update_ts=old_ts
    # Это агрегируется в 2 пары: (p1, post1), (p2, post2)

    # Ожидаемое поведение (ПРАВИЛЬНО):
    # offset = max(update_ts из батча) = old_ts
    # Потому что в батче были только события с update_ts=old_ts

    # Текущее поведение (БАГ - OVERSHOOT):
    # _get_max_update_ts_for_batch делает:
    #   SELECT MAX(update_ts) WHERE (profile_id, post_id) IN (('p1', 'post1'), ('p2', 'post2'))
    # Для пары (p1, post1) возвращает MAX по ВСЕМ её событиям
    # Включая id=9,10,11 с update_ts=new_ts которые НЕ были в батче!
    # offset = new_ts (OVERSHOOT!)

    if offset_value:
        # Проверяем наличие overshoot
        has_overshoot = offset_value > old_ts + 1

        if has_overshoot:
            print(f"\n❌ БАГ ПОДТВЕРЖДЁН!")
            print(f"Офсет = {offset_value}")
            print(f"Это близко к new_ts = {new_ts}")
            print(f"\nПроблема:")
            print(f"  - Changed idx query вернул события id=1-5 (update_ts={old_ts})")
            print(f"  - Но офсет взял MAX по ВСЕМ событиям пар (p1, post1) и (p2, post2)")
            print(f"  - Включая события id=9,10,11 (update_ts={new_ts}) которые НЕ были в батче!")
            print(f"\nПоследствия:")
            print(f"  - События id=6,7,8 (update_ts={middle_ts}) ПОТЕРЯНЫ")
            print(f"  - Следующий changed_idx query: WHERE update_ts > {offset_value}")
            print(f"  - События с update_ts={middle_ts} не попадут в запрос!")
        else:
            print(f"\n✅ БАГ ИСПРАВЛЕН!")
            print(f"Офсет = {offset_value}")
            print(f"Это соответствует old_ts = {old_ts}")
            print(f"\nПравильное поведение (strict mode):")
            print(f"  - Changed idx query вернул события id=1-5 (update_ts={old_ts})")
            print(f"  - Офсет вычислен только по этим 5 событиям: offset = {offset_value}")
            print(f"  - События id=6,7,8 с update_ts={middle_ts} будут обработаны в следующем запуске")
            print(f"  - Никакие данные не потеряны!")

        # Это failing тест - он должен упасть при overshoot
        assert offset_value <= old_ts + 1, (
            f"Офсет {offset_value} слишком большой (overshoot)! "
            f"Должен быть <= {old_ts + 1} (max update_ts из обработанного батча)"
        )
    else:
        print(f"\n✅ Офсет не обновился (limit_hit guard сработал)")
        print(f"Это текущее поведение после коммита 10f7326")
        print(f"Предотвращает overshoot, но создаёт deadlock")


if __name__ == "__main__":
    # Для локального запуска
    from datapipe.store.database import DBConn
    import os

    db_env = os.getenv("TEST_DB_ENV", "sqlite")
    if db_env == "sqlite":
        try:
            import pysqlite3  # noqa: F401
            DBCONNSTR = "sqlite+pysqlite3:///:memory:"
        except ImportError:
            DBCONNSTR = "sqlite+pysqlite:///:memory:"
        dbconn = DBConn(DBCONNSTR, None)
    else:
        pg_host = os.getenv("POSTGRES_HOST", "localhost")
        pg_port = os.getenv("POSTGRES_PORT", "5432")
        DBCONNSTR = f"postgresql://postgres:password@{pg_host}:{pg_port}/postgres"
        dbconn = DBConn(DBCONNSTR, "test")

    test_offset_overshoot_with_composite_keys(dbconn)

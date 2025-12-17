"""
Тест для проверки что offset'ы создаются для JoinSpec таблиц (с join_keys).

Воспроизводит баг где offset создавался только для главной таблицы (posts),
но не для справочной таблицы (profiles) с join_keys.
"""

import time

import pandas as pd
from sqlalchemy import Column, Integer, String

from datapipe.compute import ComputeInput
from datapipe.datatable import DataStore
from datapipe.step.batch_transform import BatchTransformStep
from datapipe.store.database import DBConn, TableStoreDB


def test_offset_created_for_joinspec_tables(dbconn: DBConn):
    """
    Проверяет что offset создается для таблиц с join_keys (JoinSpec).

    Сценарий:
    1. Создаём posts и profiles (profiles с join_keys={'user_id': 'id'})
    2. Запускаем трансформацию с offset optimization
    3. Проверяем что offset создан ДЛЯ ОБЕИХ таблиц: posts И profiles
    """
    ds = DataStore(dbconn, create_meta_table=True)

    # 1. Создать posts таблицу (используем String для id чтобы совпадать с мета-таблицей)
    posts_store = TableStoreDB(
        dbconn,
        "posts",
        [
            Column("id", String, primary_key=True),
            Column("user_id", String),
            Column("content", String),
        ],
        create_table=True,
    )
    posts = ds.create_table("posts", posts_store)

    # 2. Создать profiles таблицу (справочник)
    profiles_store = TableStoreDB(
        dbconn,
        "profiles",
        [Column("id", String, primary_key=True), Column("username", String)],
        create_table=True,
    )
    profiles = ds.create_table("profiles", profiles_store)

    # 3. Создать output таблицу (id - primary key, остальное - данные)
    output_store = TableStoreDB(
        dbconn,
        "posts_with_username",
        [
            Column("id", String, primary_key=True),
            Column("user_id", String),  # Обычная колонка, не primary key
            Column("content", String),
            Column("username", String),
        ],
        create_table=True,
    )
    output_dt = ds.create_table("posts_with_username", output_store)

    # 4. Добавить данные
    process_ts = time.time()

    # 3 поста от 2 пользователей
    posts_df = pd.DataFrame([
        {"id": "1", "user_id": "1", "content": "Post 1"},
        {"id": "2", "user_id": "1", "content": "Post 2"},
        {"id": "3", "user_id": "2", "content": "Post 3"},
    ])
    posts.store_chunk(posts_df, now=process_ts)

    # 2 профиля
    profiles_df = pd.DataFrame([
        {"id": "1", "username": "alice"},
        {"id": "2", "username": "bob"},
    ])
    profiles.store_chunk(profiles_df, now=process_ts)

    # 5. Создать трансформацию с join_keys
    def transform_func(posts_df, profiles_df):
        # JOIN posts + profiles
        result = posts_df.merge(profiles_df, left_on="user_id", right_on="id", suffixes=("", "_profile"))
        return result[["id", "user_id", "content", "username"]]

    step = BatchTransformStep(
        ds=ds,
        name="test_transform",
        func=transform_func,
        input_dts=[
            ComputeInput(dt=posts, join_type="full"),  # Главная таблица
            ComputeInput(dt=profiles, join_type="inner", join_keys={"user_id": "id"}),  # JoinSpec таблица
        ],
        output_dts=[output_dt],
        transform_keys=["id"],  # Primary key первой таблицы (posts)
        use_offset_optimization=True,  # ВАЖНО: используем offset optimization
    )

    # 6. Запустить трансформацию
    print("\n🚀 Running initial transformation...")
    step.run_full(ds)

    # Проверяем результаты трансформации
    output_data = output_dt.get_data()
    print(f"✅ Output rows created: {len(output_data)}")
    print(f"Output data:\n{output_data}")

    # 7. Проверить что offset'ы созданы для ОБЕИХ таблиц
    print("\n🔍 Checking offsets...")
    # Используем step.get_name() чтобы получить имя с хэшем
    transform_name = step.get_name()
    print(f"🔑 Transform name with hash: {transform_name}")
    offsets = ds.offset_table.get_offsets_for_transformation(transform_name)

    print(f"📊 Offsets created: {offsets}")

    # КРИТИЧЕСКИ ВАЖНО: offset должен быть для posts И для profiles!
    assert "posts" in offsets, "Offset for 'posts' table not found!"
    assert "profiles" in offsets, "Offset for 'profiles' table not found! (БАГ!)"

    # Оба offset'а должны быть >= process_ts
    assert offsets["posts"] >= process_ts, f"posts offset {offsets['posts']} < process_ts {process_ts}"
    assert offsets["profiles"] >= process_ts, f"profiles offset {offsets['profiles']} < process_ts {process_ts}"

    # Проверяем что были созданы 3 записи в output
    output_data = output_dt.get_data()
    assert len(output_data) == 3, f"Expected 3 output rows, got {len(output_data)}"

    # 8. Добавим новые данные и проверим инкрементальную обработку
    time.sleep(0.01)  # Небольшая задержка для различения timestamp'ов
    process_ts2 = time.time()

    # Добавляем 1 новый пост
    new_posts_df = pd.DataFrame([
        {"id": "4", "user_id": "1", "content": "New Post 4"},
    ])
    posts.store_chunk(new_posts_df, now=process_ts2)

    # Добавляем 1 новый профиль
    new_profiles_df = pd.DataFrame([
        {"id": "3", "username": "charlie"},
    ])
    profiles.store_chunk(new_profiles_df, now=process_ts2)

    # 9. Запускаем инкрементальную обработку
    step.run_full(ds)

    # 10. Проверяем что offset'ы обновились
    new_offsets = ds.offset_table.get_offsets_for_transformation(transform_name)

    print(f"\n📊 New offsets after incremental run: {new_offsets}")

    # Оба offset'а должны обновиться до process_ts2
    assert new_offsets["posts"] >= process_ts2, f"posts offset not updated: {new_offsets['posts']} < {process_ts2}"
    assert new_offsets["profiles"] >= process_ts2, f"profiles offset not updated: {new_offsets['profiles']} < {process_ts2}"

    # Проверяем что теперь 4 записи в output (3 старых + 1 новый пост)
    output_data = output_dt.get_data()
    assert len(output_data) == 4, f"Expected 4 output rows, got {len(output_data)}"

    print("\n✅ SUCCESS: Offsets created and updated for both posts AND profiles (including JoinSpec table)!")


def test_joinspec_update_ts_from_meta_table_not_null(dbconn: DBConn):
    """
    Проблема: Для join_keys (reverse join) update_ts подставляется как NULL
    из primary_data_tbl, что приводит к переобработке на каждом запуске.

    Сценарий:
    1. Создать основную таблицу (posts) и справочную таблицу (profiles)
    2. Связать через join_keys
    3. Изменить справочную таблицу
    4. Обработать через reverse join
    5. Повторно запустить - выборка должна быть ПУСТОЙ (нет новых изменений)

    Ожидание: update_ts должен браться из мета-таблицы справочника (tbl.c.update_ts),
    а не подставляться как NULL из primary_data_tbl

    Код исправления в sql_meta.py (строки 930-936):
    - update_ts берется из tbl.c.update_ts вместо NULL
    - update_ts добавлен в GROUP BY
    """
    ds = DataStore(dbconn, create_meta_table=True)

    # Создаем основную таблицу (posts)
    posts_store = TableStoreDB(
        dbconn,
        "posts_table",
        [
            Column("post_id", String, primary_key=True),
            Column("user_id", String),
            Column("content", String),
        ],
        create_table=True,
    )
    posts_dt = ds.create_table("posts_table", posts_store)

    # Создаем справочную таблицу (profiles)
    profiles_store = TableStoreDB(
        dbconn,
        "profiles_table",
        [
            Column("id", String, primary_key=True),
            Column("name", String),
        ],
        create_table=True,
    )
    profiles_dt = ds.create_table("profiles_table", profiles_store)

    # Создаем выходную таблицу
    output_store = TableStoreDB(
        dbconn,
        "enriched_posts",
        [
            Column("post_id", String, primary_key=True),
            Column("user_id", String),
            Column("content", String),
        ],
        create_table=True,
    )
    output_dt = ds.create_table("enriched_posts", output_store)

    def join_func(posts_df, profiles_df):
        # Обогащаем посты данными из profiles (хотя в этом тесте просто возвращаем posts)
        return posts_df[["post_id", "user_id", "content"]]

    step = BatchTransformStep(
        ds=ds,
        name="join_test",
        func=join_func,
        input_dts=[
            ComputeInput(dt=posts_dt, join_type="full"),  # Основная таблица
            ComputeInput(
                dt=profiles_dt,
                join_type="full",
                join_keys={"user_id": "id"},  # Reverse join
            ),
        ],
        output_dts=[output_dt],
        transform_keys=["post_id"],
        use_offset_optimization=True,
        chunk_size=10,
    )

    # 1. Создать данные в основной таблице
    t1 = time.time()
    posts_df = pd.DataFrame({
        "post_id": ["post_1", "post_2"],
        "user_id": ["user_1", "user_2"],
        "content": ["Hello", "World"],
    })
    posts_dt.store_chunk(posts_df, now=t1)

    # 2. Создать данные в справочной таблице
    time.sleep(0.01)
    t2 = time.time()
    profiles_df = pd.DataFrame({
        "id": ["user_1", "user_2"],
        "name": ["Alice", "Bob"],
    })
    profiles_dt.store_chunk(profiles_df, now=t2)

    # 3. Первый прогон - должен обработать все записи
    step.run_full(ds)

    output_data = output_dt.get_data()
    assert len(output_data) == 2
    assert set(output_data["post_id"]) == {"post_1", "post_2"}

    # 4. Проверить offset
    offsets = ds.offset_table.get_offsets_for_transformation(step.get_name())
    assert "posts_table" in offsets
    assert "profiles_table" in offsets
    posts_offset = offsets["posts_table"]
    profiles_offset = offsets["profiles_table"]

    # 5. Повторный прогон - выборка должна быть ПУСТОЙ (нет новых изменений)
    # Получаем количество батчей для обработки
    idx_count, idx_gen = step.get_full_process_ids(ds=ds, run_config=None)

    assert idx_count == 0, (
        f"После первого успешного run_full выборка должна быть пустой. "
        f"Получено {idx_count} батчей для обработки. "
        f"Это указывает на то, что offset-оптимизация для JoinSpec НЕ работает: "
        f"записи с join_keys переобрабатываются на каждом запуске из-за update_ts = NULL."
    )

    print("\n✅ SUCCESS: JoinSpec update_ts correctly taken from meta table, no reprocessing!")

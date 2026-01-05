import time

import pandas as pd
from sqlalchemy import Column, String

from datapipe.compute import ComputeInput
from datapipe.datatable import DataStore
from datapipe.step.batch_transform import BatchTransformStep
from datapipe.store.database import DBConn, TableStoreDB
from datapipe.tests.util import assert_datatable_equal
from datapipe.types import DataField


def test_transform_key_mapping(dbconn: DBConn):
    """
    Проверяет что трансформация с keys (InputSpec) корректно отрабатывает.

    Сценарий:
    1. Создаём posts и profiles (profiles с key_mapping={'user_id': 'id'})
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
        [
            Column("id", String, primary_key=True),
            Column("username", String),
        ],
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
    posts_df = pd.DataFrame(
        [
            {"id": "1", "user_id": "1", "content": "Post 1"},
            {"id": "2", "user_id": "1", "content": "Post 2"},
            {"id": "3", "user_id": "2", "content": "Post 3"},
        ]
    )
    posts.store_chunk(posts_df, now=process_ts)

    # 2 профиля
    profiles_df = pd.DataFrame(
        [
            {"id": "1", "username": "alice"},
            {"id": "2", "username": "bob"},
        ]
    )
    profiles.store_chunk(profiles_df, now=process_ts)

    # 5. Создать трансформацию с key_mapping
    def transform_func(posts_df, profiles_df):
        # JOIN posts + profiles
        result = posts_df.merge(profiles_df, left_on="user_id", right_on="id", suffixes=("", "_profile"))
        return result[["id", "user_id", "content", "username"]]

    step = BatchTransformStep(
        ds=ds,
        name="test_transform",
        func=transform_func,
        input_dts=[
            ComputeInput(
                dt=posts,
                join_type="full",
                keys={
                    "post_id": "id",
                    "user_id": DataField("user_id"),
                },
            ),
            ComputeInput(
                dt=profiles,
                join_type="inner",
                keys={
                    "user_id": "id",
                },
            ),
        ],
        output_dts=[output_dt],
        transform_keys=["post_id", "user_id"],
    )

    # 6. Запустить трансформацию
    print("\n🚀 Running initial transformation...")
    step.run_full(ds)

    # Проверяем результаты трансформации
    assert_datatable_equal(
        output_dt,
        pd.DataFrame(
            [
                {"id": "1", "user_id": "1", "content": "Post 1", "username": "alice"},
                {"id": "2", "user_id": "1", "content": "Post 2", "username": "alice"},
                {"id": "3", "user_id": "2", "content": "Post 3", "username": "bob"},
            ]
        ),
    )

    # 8. Добавим новые данные и проверим инкрементальную обработку
    time.sleep(0.01)  # Небольшая задержка для различения timestamp'ов
    process_ts2 = time.time()

    # Добавляем 1 новый пост
    new_posts_df = pd.DataFrame(
        [
            {"id": "4", "user_id": "1", "content": "New Post 4"},
        ]
    )
    posts.store_chunk(new_posts_df, now=process_ts2)

    # Добавляем 1 новый профиль
    new_profiles_df = pd.DataFrame(
        [
            {"id": "3", "username": "charlie"},
        ]
    )
    profiles.store_chunk(new_profiles_df, now=process_ts2)

    # 9. Запускаем инкрементальную обработку
    step.run_full(ds)

    assert_datatable_equal(
        output_dt,
        pd.DataFrame(
            [
                {"id": "1", "user_id": "1", "content": "Post 1", "username": "alice"},
                {"id": "2", "user_id": "1", "content": "Post 2", "username": "alice"},
                {"id": "3", "user_id": "2", "content": "Post 3", "username": "bob"},
                {"id": "4", "user_id": "1", "content": "New Post 4", "username": "alice"},
            ]
        ),
    )

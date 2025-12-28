"""
Тест для reverse_join когда FK находятся в meta table, а не в data table.

Проблема 1:
_build_reverse_join_cte всегда делает JOIN с primary_data_tbl,
но если FK есть в primary_meta_tbl, то JOIN с data table не нужен.

Проблема 2:
Строки 956-963 делают literal(None) для колонок которые не в primary_data_cols,
но если колонка в meta_cols справочной таблицы, нужно брать из tbl.c[k].

Сценарий:
- posts.meta содержит [id, author_id, update_ts, delete_ts] (FK в META!)
- posts.data содержит [id, content] (минимум)
- users.meta содержит [id, name, update_ts, delete_ts]
- При изменении user должен сработать reverse_join для posts
"""

import time

import pandas as pd
from sqlalchemy import Column, String

from datapipe.compute import ComputeInput
from datapipe.datatable import DataStore
from datapipe.step.batch_transform import BatchTransformStep
from datapipe.store.database import DBConn, MetaKey, TableStoreDB


def test_reverse_join_fk_in_meta_simple(dbconn: DBConn):
    """
    Простой тест: posts с author_id в meta, users с name в meta.
    """
    ds = DataStore(dbconn, create_meta_table=True)

    # 1. Posts таблица: author_id в МЕТА через MetaKey
    post_store = TableStoreDB(
        dbconn,
        "posts",
        [
            Column("id", String, primary_key=True),
            Column("author_id", String, MetaKey()),  # FK в meta table!
            Column("content", String),  # В data table
        ],
        create_table=True,
    )
    post_dt = ds.create_table("posts", post_store)

    # 2. Users таблица: name в мета через MetaKey
    user_store = TableStoreDB(
        dbconn,
        "users",
        [
            Column("id", String, primary_key=True),
            Column("name", String, MetaKey()),  # В meta table!
            Column("email", String),  # В data table
        ],
        create_table=True,
    )
    user_dt = ds.create_table("users", user_store)

    # 3. Output таблица
    output_store = TableStoreDB(
        dbconn,
        "posts_with_author",
        [
            Column("id", String, primary_key=True),
            Column("author_name", String),
            Column("content", String),
        ],
        create_table=True,
    )
    output_dt = ds.create_table("posts_with_author", output_store)

    # 4. Трансформация с join_keys
    def transform_func(post_df, user_df):
        result = post_df.merge(
            user_df[["id", "name"]],
            left_on="author_id",
            right_on="id",
            how="left",
            suffixes=("", "_user"),
        )
        return result[["id", "content", "name"]].rename(columns={"name": "author_name"})

    step = BatchTransformStep(
        ds=ds,
        name="test_reverse_join_simple",
        func=transform_func,
        input_dts=[
            ComputeInput(dt=post_dt, join_type="full"),
            # КРИТИЧНО: join_keys связывают posts.meta.author_id ← users.meta.id
            ComputeInput(
                dt=user_dt,
                join_type="full",
                join_keys={"author_id": "id"},  # author_id в posts.META!
            ),
        ],
        output_dts=[output_dt],
        transform_keys=["id"],
        use_offset_optimization=True,
        chunk_size=10,
    )

    # === НАЧАЛЬНЫЕ ДАННЫЕ ===

    t1 = time.time()

    # Создать пользователей
    users_df = pd.DataFrame(
        [
            {"id": "u1", "name": "Alice", "email": "alice@example.com"},
            {"id": "u2", "name": "Bob", "email": "bob@example.com"},
        ]
    )
    user_dt.store_chunk(users_df, now=t1)

    # Создать посты (author_id в meta через idx_columns)
    posts_df = pd.DataFrame(
        [
            {"id": "p1", "author_id": "u1", "content": "Hello World"},
            {"id": "p2", "author_id": "u2", "content": "Test Post"},
        ]
    )
    post_dt.store_chunk(posts_df, now=t1)

    # === ПЕРВАЯ ОБРАБОТКА ===

    time.sleep(0.01)
    step.run_full(ds)

    output_data = output_dt.get_data()
    assert len(output_data) == 2, f"Expected 2 records in output, got {len(output_data)}"

    p1 = output_data[output_data["id"] == "p1"].iloc[0]
    assert p1["author_name"] == "Alice", f"p1.author_name should be Alice, got {p1['author_name']}"

    # === ИЗМЕНЕНИЕ USER (должен сработать reverse_join) ===

    time.sleep(0.01)
    t2 = time.time()

    # Изменить Alice → Alicia
    user_dt.store_chunk(
        pd.DataFrame([{"id": "u1", "name": "Alicia", "email": "alice@example.com"}]),
        now=t2,
    )

    # === ВТОРАЯ ОБРАБОТКА ===

    time.sleep(0.01)

    # Проверяем что reverse_join сработал (users.meta изменился → posts должны пересчитаться)
    idx_count, idx_gen = step.get_full_process_ids(ds, run_config=None)
    print(f"\n=== Записей для обработки после изменения user: {idx_count} ===")

    # Посмотрим на SQL запрос
    sql_keys, sql = step._build_changed_idx_sql(ds, run_config=None)
    print(f"\n=== SQL запрос для changed_idx ===")
    print(sql)

    # Должен быть минимум p1 (зависит от u1 который изменился)
    assert idx_count >= 1, (
        f"БАГ: должно быть минимум 1 запись для обработки (p1 зависит от u1), got {idx_count}. "
        "Это означает что reverse_join не сработал!"
    )

    step.run_full(ds)

    # === ПРОВЕРКА ===

    output_data_after = output_dt.get_data()
    assert len(output_data_after) == 2, f"Expected 2 records, got {len(output_data_after)}"

    # p1: author=u1 (Alicia) - должно обновиться через reverse_join
    p1_after = output_data_after[output_data_after["id"] == "p1"].iloc[0]
    assert p1_after["author_name"] == "Alicia", (
        f"БАГ: p1.author_name должно быть 'Alicia', got '{p1_after['author_name']}'. "
        "Проблема 1: reverse_join делает JOIN с posts.data, хотя author_id в posts.meta. "
        "Проблема 2: колонка 'name' из users.meta заменяется на literal(None)."
    )

    # p2: author=u2 (Bob) - без изменений
    p2_after = output_data_after[output_data_after["id"] == "p2"].iloc[0]
    assert p2_after["author_name"] == "Bob", f"p2.author_name should be Bob, got {p2_after['author_name']}"

    print("✓ Reverse_join корректно работает с FK в meta table")

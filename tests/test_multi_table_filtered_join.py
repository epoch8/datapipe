"""
Тесты для проверки мульти-табличных трансформаций с filtered join оптимизацией.

Тесты проверяют:
1. Что filtered join вызывается корректно при наличии join_keys
2. Что join выполняется по правильным ключам
3. Что результаты v1 (FULL OUTER JOIN) и v2 (offset-based) идентичны
"""
import time

import pandas as pd
from sqlalchemy import Column, Integer, String

from datapipe.compute import ComputeInput
from datapipe.datatable import DataStore
from datapipe.step.batch_transform import BatchTransformStep
from datapipe.store.database import DBConn, TableStoreDB


def test_filtered_join_is_called(dbconn: DBConn):
    """
    Тест 1: Проверяет что filtered join вызывается и читает только нужные данные.

    Сценарий:
    - Основная таблица users с user_id
    - Справочник profiles с большим количеством записей
    - join_keys указывает связь user_id -> id
    - Проверяем что из profiles читаются только записи для существующих пользователей
    """
    ds = DataStore(dbconn, create_meta_table=True)

    # Основная таблица users
    users_store = TableStoreDB(
        dbconn,
        "users",
        [
            Column("user_id", String, primary_key=True),
            Column("name", String),
        ],
        create_table=True,
    )
    users_dt = ds.create_table("users", users_store)

    # Справочная таблица profiles (много записей)
    profiles_store = TableStoreDB(
        dbconn,
        "profiles",
        [
            Column("id", String, primary_key=True),
            Column("description", String),
        ],
        create_table=True,
    )
    profiles_dt = ds.create_table("profiles", profiles_store)

    # Выходная таблица
    output_store = TableStoreDB(
        dbconn,
        "user_profiles",
        [
            Column("user_id", String, primary_key=True),
            Column("name", String),
            Column("description", String),
        ],
        create_table=True,
    )
    output_dt = ds.create_table("user_profiles", output_store)

    # Функция трансформации с отслеживанием вызовов
    transform_calls = []

    def transform_func(users_df, profiles_df):
        transform_calls.append({
            "users_count": len(users_df),
            "profiles_count": len(profiles_df),
            "profiles_ids": sorted(profiles_df["id"].tolist()) if not profiles_df.empty else []
        })
        # Merge by user_id and id
        merged = users_df.merge(
            profiles_df,
            left_on="user_id",
            right_on="id",
            how="left"
        )
        return merged[["user_id", "name", "description"]].fillna("")

    # ОТСЛЕЖИВАНИЕ ВЫЗОВОВ get_data для profiles_dt
    get_data_calls = []
    original_get_data = profiles_dt.get_data

    def tracked_get_data(idx=None, **kwargs):
        if idx is not None:
            get_data_calls.append({
                "idx_columns": list(idx.columns),
                "idx_values": sorted(idx["id"].tolist()) if "id" in idx.columns else [],
                "idx_length": len(idx)
            })
        result = original_get_data(idx=idx, **kwargs)
        return result

    profiles_dt.get_data = tracked_get_data  # type: ignore[method-assign]

    # Создаем step с filtered join
    step = BatchTransformStep(
        ds=ds,
        name="test_filtered_join",
        func=transform_func,
        input_dts=[
            ComputeInput(dt=users_dt, join_type="full"),
            ComputeInput(
                dt=profiles_dt,
                join_type="full",
                join_keys={"user_id": "id"}  # user_id из idx -> id из profiles
            ),
        ],
        output_dts=[output_dt],
        transform_keys=["user_id"],
        chunk_size=10,
    )

    # Добавляем данные: только 3 пользователя
    now = time.time()
    users_dt.store_chunk(
        pd.DataFrame({
            "user_id": ["u1", "u2", "u3"],
            "name": ["Alice", "Bob", "Charlie"]
        }),
        now=now
    )

    # Добавляем много профилей (100 записей), но только 3 связаны с пользователями
    profiles_data = []
    for i in range(100):
        profiles_data.append({
            "id": f"p{i}",
            "description": f"Profile {i}"
        })
    # Добавляем профили для наших пользователей
    profiles_data.extend([
        {"id": "u1", "description": "Alice's profile"},
        {"id": "u2", "description": "Bob's profile"},
        {"id": "u3", "description": "Charlie's profile"},
    ])

    profiles_dt.store_chunk(pd.DataFrame(profiles_data), now=now)

    # Запускаем трансформацию
    step.run_full(ds)

    # Проверяем что трансформация вызвалась
    assert len(transform_calls) > 0

    # ПРОВЕРКА 1: get_data был вызван с filtered idx
    assert len(get_data_calls) > 0, "get_data should be called with filtered idx"

    first_get_data_call = get_data_calls[0]
    assert "id" in first_get_data_call["idx_columns"], (
        f"idx should contain 'id' column for filtered join, "
        f"but got columns: {first_get_data_call['idx_columns']}"
    )
    assert first_get_data_call["idx_length"] == 3, (
        f"Filtered idx should contain 3 records (u1, u2, u3), "
        f"but got {first_get_data_call['idx_length']}"
    )
    assert first_get_data_call["idx_values"] == ["u1", "u2", "u3"], (
        f"Filtered idx should contain correct user_ids mapped to profile ids, "
        f"but got {first_get_data_call['idx_values']}"
    )

    # ПРОВЕРКА 2: filtered join должен читать только 3 профиля,
    # а не все 103 записи из таблицы profiles
    first_call = transform_calls[0]
    assert first_call["users_count"] == 3, "Should process 3 users"
    assert first_call["profiles_count"] == 3, (
        f"Filtered join should read only 3 profiles (for u1, u2, u3), "
        f"but got {first_call['profiles_count']}"
    )
    assert first_call["profiles_ids"] == ["u1", "u2", "u3"], (
        "Should read profiles only for existing users"
    )

    # Проверяем результат
    output_data = output_dt.get_data().sort_values("user_id").reset_index(drop=True)
    assert len(output_data) == 3
    assert output_data["user_id"].tolist() == ["u1", "u2", "u3"]
    assert output_data["description"].tolist() == [
        "Alice's profile",
        "Bob's profile",
        "Charlie's profile"
    ]


def test_join_keys_correctness(dbconn: DBConn):
    """
    Тест 2: Проверяет что join происходит по правильным ключам.

    Сценарий:
    - Основная таблица orders с order_id и customer_id как часть primary key
    - Справочник customers с id и другими полями
    - join_keys: {"customer_id": "id"}
    - Проверяем что данные джойнятся правильно
    """
    ds = DataStore(dbconn, create_meta_table=True)

    # Таблица заказов с customer_id в primary key
    orders_store = TableStoreDB(
        dbconn,
        "orders",
        [
            Column("order_id", String, primary_key=True),
            Column("customer_id", String, primary_key=True),
            Column("amount", Integer),
        ],
        create_table=True,
    )
    orders_dt = ds.create_table("orders", orders_store)

    # Таблица покупателей
    customers_store = TableStoreDB(
        dbconn,
        "customers",
        [
            Column("id", String, primary_key=True),
            Column("name", String),
        ],
        create_table=True,
    )
    customers_dt = ds.create_table("customers", customers_store)

    # Выходная таблица
    output_store = TableStoreDB(
        dbconn,
        "enriched_orders",
        [
            Column("order_id", String, primary_key=True),
            Column("customer_id", String, primary_key=True),
            Column("customer_name", String),
            Column("amount", Integer),
        ],
        create_table=True,
    )
    output_dt = ds.create_table("enriched_orders", output_store)

    def transform_func(orders_df, customers_df):
        # Join по customer_id = id
        merged = orders_df.merge(
            customers_df,
            left_on="customer_id",
            right_on="id",
            how="left"
        )
        return merged[["order_id", "customer_id", "name", "amount"]].rename(columns={"name": "customer_name"})

    # Step с join_keys
    step = BatchTransformStep(
        ds=ds,
        name="test_join_keys",
        func=transform_func,
        input_dts=[
            ComputeInput(dt=orders_dt, join_type="full"),
            ComputeInput(
                dt=customers_dt,
                join_type="full",
                join_keys={"customer_id": "id"}
            ),
        ],
        output_dts=[output_dt],
        transform_keys=["order_id", "customer_id"],
    )

    # Добавляем данные
    now = time.time()
    orders_dt.store_chunk(
        pd.DataFrame({
            "order_id": ["o1", "o2", "o3"],
            "customer_id": ["c1", "c2", "c1"],
            "amount": [100, 200, 150]
        }),
        now=now
    )

    customers_dt.store_chunk(
        pd.DataFrame({
            "id": ["c1", "c2", "c3"],  # c3 не используется
            "name": ["John", "Jane", "Bob"]
        }),
        now=now
    )

    # Запускаем
    step.run_full(ds)

    # Проверяем результат
    output_data = output_dt.get_data().sort_values("order_id").reset_index(drop=True)
    assert len(output_data) == 3

    # КЛЮЧЕВАЯ ПРОВЕРКА: join keys работают правильно
    expected = pd.DataFrame({
        "order_id": ["o1", "o2", "o3"],
        "customer_id": ["c1", "c2", "c1"],
        "customer_name": ["John", "Jane", "John"],
        "amount": [100, 200, 150]
    })
    pd.testing.assert_frame_equal(output_data, expected)


def test_v1_vs_v2_results_identical(dbconn: DBConn):
    """
    Тест 3: Сравнивает результаты v1 (FULL OUTER JOIN) и v2 (offset-based).

    Сценарий:
    - Две входные таблицы с мульти-табличной трансформацией
    - Запускаем одну и ту же трансформацию с v1 и v2
    - Проверяем что результаты идентичны
    """
    ds = DataStore(dbconn, create_meta_table=True)

    # Входные таблицы
    table_a_store = TableStoreDB(
        dbconn,
        "table_a",
        [
            Column("id", String, primary_key=True),
            Column("value_a", Integer),
        ],
        create_table=True,
    )
    table_a_dt = ds.create_table("table_a", table_a_store)

    table_b_store = TableStoreDB(
        dbconn,
        "table_b",
        [
            Column("id", String, primary_key=True),
            Column("value_b", Integer),
        ],
        create_table=True,
    )
    table_b_dt = ds.create_table("table_b", table_b_store)

    # Две выходные таблицы - одна для v1, другая для v2
    output_v1_store = TableStoreDB(
        dbconn,
        "output_v1",
        [
            Column("id", String, primary_key=True),
            Column("sum_value", Integer),
        ],
        create_table=True,
    )
    output_v1_dt = ds.create_table("output_v1", output_v1_store)

    output_v2_store = TableStoreDB(
        dbconn,
        "output_v2",
        [
            Column("id", String, primary_key=True),
            Column("sum_value", Integer),
        ],
        create_table=True,
    )
    output_v2_dt = ds.create_table("output_v2", output_v2_store)

    def transform_func(df_a, df_b):
        # Объединяем по id
        merged = df_a.merge(df_b, on="id", how="outer")
        merged["sum_value"] = merged["value_a"].fillna(0) + merged["value_b"].fillna(0)
        return merged[["id", "sum_value"]].astype({"sum_value": int})

    # Step v1 (без offset)
    step_v1 = BatchTransformStep(
        ds=ds,
        name="test_transform_v1",
        func=transform_func,
        input_dts=[
            ComputeInput(dt=table_a_dt, join_type="full"),
            ComputeInput(dt=table_b_dt, join_type="full"),
        ],
        output_dts=[output_v1_dt],
        transform_keys=["id"],
        use_offset_optimization=False,
    )

    # Step v2 (с offset)
    step_v2 = BatchTransformStep(
        ds=ds,
        name="test_transform_v2",
        func=transform_func,
        input_dts=[
            ComputeInput(dt=table_a_dt, join_type="full"),
            ComputeInput(dt=table_b_dt, join_type="full"),
        ],
        output_dts=[output_v2_dt],
        transform_keys=["id"],
        use_offset_optimization=True,
    )

    # Добавляем данные
    now = time.time()
    table_a_dt.store_chunk(
        pd.DataFrame({
            "id": ["1", "2", "3"],
            "value_a": [10, 20, 30]
        }),
        now=now
    )
    table_b_dt.store_chunk(
        pd.DataFrame({
            "id": ["2", "3", "4"],
            "value_b": [100, 200, 300]
        }),
        now=now
    )

    # Запускаем обе трансформации
    step_v1.run_full(ds)
    step_v2.run_full(ds)

    # Сравниваем результаты
    result_v1 = output_v1_dt.get_data().sort_values("id").reset_index(drop=True)
    result_v2 = output_v2_dt.get_data().sort_values("id").reset_index(drop=True)

    # КЛЮЧЕВАЯ ПРОВЕРКА: результаты v1 и v2 должны быть идентичны
    pd.testing.assert_frame_equal(result_v1, result_v2)

    # Проверяем корректность результатов
    expected = pd.DataFrame({
        "id": ["1", "2", "3", "4"],
        "sum_value": [10, 120, 230, 300]
    })
    pd.testing.assert_frame_equal(result_v1, expected)

    # === Инкрементальная обработка ===
    # Добавляем новые данные
    time.sleep(0.01)
    now2 = time.time()
    table_a_dt.store_chunk(
        pd.DataFrame({
            "id": ["5"],
            "value_a": [50]
        }),
        now=now2
    )

    # Запускаем снова
    step_v1.run_full(ds)
    step_v2.run_full(ds)

    # Снова сравниваем
    result_v1 = output_v1_dt.get_data().sort_values("id").reset_index(drop=True)
    result_v2 = output_v2_dt.get_data().sort_values("id").reset_index(drop=True)

    pd.testing.assert_frame_equal(result_v1, result_v2)

    # Проверяем что добавилась новая запись
    assert len(result_v1) == 5
    assert "5" in result_v1["id"].values


def test_three_tables_filtered_join(dbconn: DBConn):
    """
    Тест 4: Проверяет работу filtered join с ТРЕМЯ таблицами.

    Сценарий:
    - Основная таблица posts с post_id, user_id, category_id
    - Справочник users с user_id
    - Справочник categories с category_id
    - join_keys для обоих справочников
    - Проверяем что все три таблицы корректно джойнятся и filtered join работает
    """
    ds = DataStore(dbconn, create_meta_table=True)

    # Основная таблица: посты
    posts_store = TableStoreDB(
        dbconn,
        "posts",
        [
            Column("id", String, primary_key=True),
            Column("user_id", String),
            Column("category_id", String),
            Column("content", String),
        ],
        create_table=True,
    )
    posts_dt = ds.create_table("posts", posts_store)

    # Справочник 1: пользователи
    users_store = TableStoreDB(
        dbconn,
        "users",
        [
            Column("id", String, primary_key=True),
            Column("username", String),
        ],
        create_table=True,
    )
    users_dt = ds.create_table("users", users_store)

    # Справочник 2: категории
    categories_store = TableStoreDB(
        dbconn,
        "categories",
        [
            Column("id", String, primary_key=True),
            Column("category_name", String),
        ],
        create_table=True,
    )
    categories_dt = ds.create_table("categories", categories_store)

    # Выходная таблица
    output_store = TableStoreDB(
        dbconn,
        "enriched_posts",
        [
            Column("id", String, primary_key=True),
            Column("content", String),
            Column("username", String),
            Column("category_name", String),
        ],
        create_table=True,
    )
    output_dt = ds.create_table("enriched_posts", output_store)

    # Отслеживание вызовов get_data для проверки filtered join
    users_get_data_calls = []
    categories_get_data_calls = []

    original_users_get_data = users_dt.get_data
    original_categories_get_data = categories_dt.get_data

    def tracked_users_get_data(idx=None, **kwargs):
        if idx is not None:
            users_get_data_calls.append({
                "idx_columns": list(idx.columns),
                "idx_values": sorted(idx["id"].tolist()) if "id" in idx.columns else [],
                "idx_length": len(idx)
            })
        return original_users_get_data(idx=idx, **kwargs)

    def tracked_categories_get_data(idx=None, **kwargs):
        if idx is not None:
            categories_get_data_calls.append({
                "idx_columns": list(idx.columns),
                "idx_values": sorted(idx["id"].tolist()) if "id" in idx.columns else [],
                "idx_length": len(idx)
            })
        return original_categories_get_data(idx=idx, **kwargs)

    users_dt.get_data = tracked_users_get_data  # type: ignore[method-assign]
    categories_dt.get_data = tracked_categories_get_data  # type: ignore[method-assign]

    # Функция трансформации: join всех трёх таблиц
    def transform_func(posts_df, users_df, categories_df):
        # Join с users
        result = posts_df.merge(
            users_df,
            left_on="user_id",
            right_on="id",
            how="left",
            suffixes=("", "_user")
        )
        # Join с categories
        result = result.merge(
            categories_df,
            left_on="category_id",
            right_on="id",
            how="left",
            suffixes=("", "_cat")
        )
        return result[["id", "content", "username", "category_name"]]

    # Step с двумя filtered joins
    # join_keys работает только с v2 (offset optimization)
    step = BatchTransformStep(
        ds=ds,
        name="test_three_tables",
        func=transform_func,
        input_dts=[
            ComputeInput(dt=posts_dt, join_type="full"),  # Основная таблица
            ComputeInput(
                dt=users_dt,
                join_type="full",
                join_keys={"user_id": "id"}  # Filtered join: posts.user_id -> users.id
            ),
            ComputeInput(
                dt=categories_dt,
                join_type="full",
                join_keys={"category_id": "id"}  # Filtered join: posts.category_id -> categories.id
            ),
        ],
        output_dts=[output_dt],
        transform_keys=["id"],
        use_offset_optimization=True,  # join_keys требует v2
    )

    # Добавляем данные
    now = time.time()

    # 3 поста от 2 пользователей в 2 категориях
    posts_dt.store_chunk(
        pd.DataFrame({
            "id": ["p1", "p2", "p3"],
            "user_id": ["u1", "u2", "u1"],
            "category_id": ["cat1", "cat2", "cat1"],
            "content": ["Post 1", "Post 2", "Post 3"]
        }),
        now=now
    )

    # 100 пользователей (но только u1, u2 используются в постах)
    users_data = [{"id": f"u{i}", "username": f"User{i}"} for i in range(3, 100)]
    users_data.extend([
        {"id": "u1", "username": "Alice"},
        {"id": "u2", "username": "Bob"},
    ])
    users_dt.store_chunk(pd.DataFrame(users_data), now=now)

    # 50 категорий (но только cat1, cat2 используются в постах)
    categories_data = [{"id": f"cat{i}", "category_name": f"Category {i}"} for i in range(3, 50)]
    categories_data.extend([
        {"id": "cat1", "category_name": "Tech"},
        {"id": "cat2", "category_name": "News"},
    ])
    categories_dt.store_chunk(pd.DataFrame(categories_data), now=now)

    # Запускаем трансформацию
    step.run_full(ds)

    # ПРОВЕРКА 1: Filtered join для users должен читать только u1, u2
    assert len(users_get_data_calls) > 0, "users get_data should be called with filtered idx"
    users_call = users_get_data_calls[0]
    assert "id" in users_call["idx_columns"]
    assert users_call["idx_length"] == 2, f"Should filter to 2 users, got {users_call['idx_length']}"
    assert sorted(users_call["idx_values"]) == ["u1", "u2"], (
        f"Should filter users to u1, u2, got {users_call['idx_values']}"
    )

    # ПРОВЕРКА 2: Filtered join для categories должен читать только cat1, cat2
    assert len(categories_get_data_calls) > 0, "categories get_data should be called with filtered idx"
    categories_call = categories_get_data_calls[0]
    assert "id" in categories_call["idx_columns"]
    assert categories_call["idx_length"] == 2, (
        f"Should filter to 2 categories, got {categories_call['idx_length']}"
    )
    assert sorted(categories_call["idx_values"]) == ["cat1", "cat2"], (
        f"Should filter categories to cat1, cat2, got {categories_call['idx_values']}"
    )

    # ПРОВЕРКА 3: Результат должен содержать правильные данные
    output_data = output_dt.get_data().sort_values("id").reset_index(drop=True)
    assert len(output_data) == 3

    expected = pd.DataFrame({
        "id": ["p1", "p2", "p3"],
        "content": ["Post 1", "Post 2", "Post 3"],
        "username": ["Alice", "Bob", "Alice"],
        "category_name": ["Tech", "News", "Tech"]
    })
    pd.testing.assert_frame_equal(output_data, expected)

    print("\n✅ Three tables filtered join test passed!")
    print(f"   - Posts: 3 records")
    print(f"   - Users: filtered from {len(users_data)} to 2 records (u1, u2)")
    print(f"   - Categories: filtered from {len(categories_data)} to 2 records (cat1, cat2)")
    print(f"   - Output: 3 enriched posts with correct joins")

    # === ДОПОЛНИТЕЛЬНАЯ ПРОВЕРКА: сравнение v1 vs v2 ===
    # Создаём отдельную выходную таблицу для v1
    output_v1_store = TableStoreDB(
        dbconn,
        "enriched_posts_v1",
        [
            Column("id", String, primary_key=True),
            Column("content", String),
            Column("username", String),
            Column("category_name", String),
        ],
        create_table=True,
    )
    output_v1_dt = ds.create_table("enriched_posts_v1", output_v1_store)

    # Step v1 БЕЗ join_keys (обычный FULL OUTER JOIN всех таблиц)
    step_v1 = BatchTransformStep(
        ds=ds,
        name="test_three_tables_v1",
        func=transform_func,
        input_dts=[
            ComputeInput(dt=posts_dt, join_type="full"),
            ComputeInput(dt=users_dt, join_type="full"),  # БЕЗ join_keys
            ComputeInput(dt=categories_dt, join_type="full"),  # БЕЗ join_keys
        ],
        output_dts=[output_v1_dt],
        transform_keys=["id"],
        use_offset_optimization=False,  # v1
    )

    # Запускаем v1
    step_v1.run_full(ds)

    # Сравниваем результаты v1 и v2
    result_v1 = output_v1_dt.get_data().sort_values("id").reset_index(drop=True)
    result_v2 = output_data  # Уже отсортирован

    # КЛЮЧЕВАЯ ПРОВЕРКА: результаты v1 и v2 должны быть идентичны
    pd.testing.assert_frame_equal(result_v1, result_v2)

    print("\n✅ V1 vs V2 comparison PASSED!")
    print(f"   - V1 (FULL OUTER JOIN): {len(result_v1)} rows")
    print(f"   - V2 (offset + filtered join): {len(result_v2)} rows")
    print(f"   - Results are identical ✓")

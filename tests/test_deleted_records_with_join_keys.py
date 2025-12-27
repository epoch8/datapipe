"""
Тест для воспроизведения бага с удаленными записями при использовании join_keys.

Проблема:
При использовании join_keys и FK в data table, удаленные записи не попадают
в processed_idx из-за INNER JOIN, что приводит к тому что они не удаляются
из output таблицы.

Сценарий:
1. Создать user и subscription таблицы
2. subscription имеет ДВА join_keys для filtered join:
   - join_keys={"follower_id": "id"} (кто подписывается)
   - join_keys={"following_id": "id"} (на кого подписываются)
3. Создать 3 subscription:
   - sub1: будет удалена
   - sub2: будет изменена
   - sub3: без изменений
4. Запустить трансформацию → все записи в output
5. Удалить sub1, изменить sub2
6. Запустить трансформацию → проверить что:
   - sub1 удалена из output (удаленная запись)
   - sub2 обновлена и присутствует (измененная запись)
   - sub3 присутствует без изменений (неизмененная запись)
"""

import time

import pandas as pd
from sqlalchemy import Column, String

from datapipe.compute import ComputeInput
from datapipe.datatable import DataStore
from datapipe.step.batch_transform import BatchTransformStep
from datapipe.store.database import DBConn, TableStoreDB


def test_deleted_records_with_join_keys(dbconn: DBConn):
    """
    Воспроизводит баг: удаленные записи не попадают в processed_idx
    при использовании join_keys, и не удаляются из output.
    """
    ds = DataStore(dbconn, create_meta_table=True)

    # 1. Создать user таблицу
    user_store = TableStoreDB(
        dbconn,
        "users",
        [Column("id", String, primary_key=True), Column("name", String)],
        create_table=True,
    )
    user_dt = ds.create_table("users", user_store)

    # 2. Создать subscription таблицу
    subscription_store = TableStoreDB(
        dbconn,
        "subscriptions",
        [
            Column("id", String, primary_key=True),
            Column("follower_id", String),  # FK в data table!
            Column("following_id", String),
        ],
        create_table=True,
    )
    subscription_dt = ds.create_table("subscriptions", subscription_store)

    # 3. Создать output таблицу
    output_store = TableStoreDB(
        dbconn,
        "enriched_subscriptions",
        [
            Column("id", String, primary_key=True),
            Column("follower_name", String),
            Column("following_name", String),
        ],
        create_table=True,
    )
    output_dt = ds.create_table("enriched_subscriptions", output_store)

    # 4. Трансформация с ДВА join_keys (получает ТРИ DataFrame)
    def transform_func(subscription_df, followers_df, followings_df):
        # JOIN для follower (кто подписывается)
        result = subscription_df.merge(
            followers_df,
            left_on="follower_id",
            right_on="id",
            how="left",
            suffixes=("", "_follower"),
        )
        # JOIN для following (на кого подписываются)
        result = result.merge(
            followings_df,
            left_on="following_id",
            right_on="id",
            how="left",
            suffixes=("", "_following"),
        )
        # После merge колонки будут: id, follower_id, following_id, id_follower, name, id_following, name_following
        # name - это имя follower, name_following - имя following
        return result[["id", "name", "name_following"]].rename(
            columns={
                "name": "follower_name",
                "name_following": "following_name",
            }
        )

    step = BatchTransformStep(
        ds=ds,
        name="test_join_keys_delete",
        func=transform_func,
        input_dts=[
            ComputeInput(dt=subscription_dt, join_type="full"),
            # ← КРИТИЧНО: ДВА отдельных ComputeInput для user_dt с разными join_keys!
            ComputeInput(
                dt=user_dt,
                join_type="full",
                join_keys={"follower_id": "id"},  # Follower
            ),
            ComputeInput(
                dt=user_dt,
                join_type="full",
                join_keys={"following_id": "id"},  # Following
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
            {"id": "u1", "name": "Alice"},
            {"id": "u2", "name": "Bob"},
            {"id": "u3", "name": "Charlie"},
        ]
    )
    user_dt.store_chunk(users_df, now=t1)

    # Создать подписки
    subscriptions_df = pd.DataFrame(
        [
            {"id": "sub1", "follower_id": "u1", "following_id": "u2"},  # Будет удалена
            {"id": "sub2", "follower_id": "u2", "following_id": "u3"},  # Будет изменена
            {"id": "sub3", "follower_id": "u3", "following_id": "u1"},  # Без изменений
        ]
    )
    subscription_dt.store_chunk(subscriptions_df, now=t1)

    # === ПЕРВАЯ ОБРАБОТКА ===

    time.sleep(0.01)
    step.run_full(ds)

    # Проверяем что все 3 записи в output
    output_data = output_dt.get_data()
    assert len(output_data) == 3, f"Expected 3 records in output, got {len(output_data)}"
    assert set(output_data["id"]) == {"sub1", "sub2", "sub3"}

    # === ИЗМЕНЕНИЯ ===

    time.sleep(0.01)
    t2 = time.time()

    # Удаляем sub1
    subscription_dt.delete_by_idx(pd.DataFrame({"id": ["sub1"]}), now=t2)

    # Изменяем sub2 (новый follower)
    time.sleep(0.01)
    t3 = time.time()
    subscription_dt.store_chunk(
        pd.DataFrame(
            [
                {"id": "sub2", "follower_id": "u1", "following_id": "u3"}  # follower u2 → u1
            ]
        ),
        now=t3,
    )

    # === ВТОРАЯ ОБРАБОТКА ===

    time.sleep(0.01)
    step.run_full(ds)

    # === ПРОВЕРКА ===

    output_data_after = output_dt.get_data()
    output_ids = set(output_data_after["id"])

    # ОЖИДАНИЕ: sub1 должна быть удалена
    assert "sub1" not in output_ids, (
        f"БАГ: sub1 должна быть удалена из output, но она есть! "
        f"Это происходит потому что при INNER JOIN с data table удаленные записи "
        f"не попадают в processed_idx. Output IDs: {output_ids}"
    )

    # ОЖИДАНИЕ: sub2 должна быть обновлена
    assert "sub2" in output_ids, f"sub2 должна быть в output"
    sub2_data = output_data_after[output_data_after["id"] == "sub2"].iloc[0]
    assert sub2_data["follower_name"] == "Alice", (
        f"sub2 должна иметь обновленного follower (Alice), " f"got {sub2_data['follower_name']}"
    )
    assert sub2_data["following_name"] == "Charlie", (
        f"sub2 должна иметь following (Charlie), " f"got {sub2_data['following_name']}"
    )

    # ОЖИДАНИЕ: sub3 без изменений
    assert "sub3" in output_ids, f"sub3 должна быть в output"

    # ОЖИДАНИЕ: Ровно 2 записи в output
    assert len(output_data_after) == 2, f"Expected 2 records in output (sub2, sub3), got {len(output_data_after)}"

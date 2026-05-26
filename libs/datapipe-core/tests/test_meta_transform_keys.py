import time

import pandas as pd
from sqlalchemy import Column, String

from datapipe.compute import Catalog, ComputeInput, ComputeOutput, Table
from datapipe.datatable import DataStore
from datapipe.step.batch_transform import BatchTransform, BatchTransformStep
from datapipe.store.database import DBConn, TableStoreDB
from datapipe.tests.util import assert_datatable_equal
from datapipe.types import DataField, InputSpec, OutputSpec


def test_transform_keys(dbconn: DBConn):
    """
    Проверяет что трансформация с keys (InputSpec) корректно отрабатывает.

    Сценарий:
    1. Создаём posts и profiles (profiles с keys={'user_id': 'id'})
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

    # 5. Создать трансформацию с keys
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
                dt=posts,  # [id, user_id, content]
                join_type="full",
                keys={
                    "post_id": "id",
                    "user_id": DataField("user_id"),
                },
            ),
            ComputeInput(
                dt=profiles,  # [id, username]
                join_type="inner",
                keys={
                    "user_id": "id",
                },
            ),
        ],
        # post, profiles -> output_dt

        # 1 post [id, user_id, content] -> mapping [post_id, user_id, content]
        # 2 profiles [id, username] -> mapping [user_id, username]

        # output_dt 1x2 = [post_id, user_id, content, username] -> get output [post_id, username] -> mapping [id, user_name]
        output_dts=[
            ComputeOutput(
                dt=output_dt,  # [id, user_id, content, username]
                keys={
                    "post_id": "id",
                }
            ),
        ],
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

    # 8. Изменение lookup-таблицы должно пересчитать все связанные posts.
    time.sleep(0.01)  # Небольшая задержка для различения timestamp'ов
    process_ts2 = time.time()

    profiles.store_chunk(pd.DataFrame([{"id": "1", "username": "alice-updated"}]), now=process_ts2)
    step.run_full(ds)

    assert_datatable_equal(
        output_dt,
        pd.DataFrame(
            [
                {"id": "1", "user_id": "1", "content": "Post 1", "username": "alice-updated"},
                {"id": "2", "user_id": "1", "content": "Post 2", "username": "alice-updated"},
                {"id": "3", "user_id": "2", "content": "Post 3", "username": "bob"},
            ]
        ),
    )

    # 11. Удаление lookup-записи должно удалить все output rows для связанных posts.
    time.sleep(0.01)  # Небольшая задержка для различения timestamp'ов
    profiles.delete_by_idx(pd.DataFrame([{"id": "1"}]), now=time.time())
    step.run_full(ds)

    assert_datatable_equal(
        output_dt,
        pd.DataFrame(
            [
                {"id": "3", "user_id": "2", "content": "Post 3", "username": "bob"},
            ]
        ),
    )

    # 9. Добавим новые данные и проверим инкрементальную обработку
    time.sleep(0.01)  # Небольшая задержка для различения timestamp'ов
    process_ts3 = time.time()

    # Добавляем 1 новый пост
    new_posts_df = pd.DataFrame(
        [
            {"id": "4", "user_id": "1", "content": "New Post 4"},
        ]
    )
    posts.store_chunk(new_posts_df, now=process_ts3)

    # Добавляем 1 новый профиль без связанных posts. Он не должен создать partial transform task.
    new_profiles_df = pd.DataFrame(
        [
            {"id": "3", "username": "charlie"},
        ]
    )
    profiles.store_chunk(new_profiles_df, now=process_ts3)

    # 10. Запускаем инкрементальную обработку
    step.run_full(ds)

    assert_datatable_equal(
        output_dt,
        pd.DataFrame(
            [
                {"id": "3", "user_id": "2", "content": "Post 3", "username": "bob"},
            ]
        ),
    )


def test_transform_output_spec_keys(dbconn: DBConn):
    ds = DataStore(dbconn, create_meta_table=True)

    catalog = Catalog(
        {
            "posts": Table(
                store=TableStoreDB(
                    dbconn,
                    "posts",
                    [
                        Column("id", String, primary_key=True),
                        Column("user_id", String),
                        Column("content", String),
                    ],
                    create_table=True,
                )
            ),
            "profiles": Table(
                store=TableStoreDB(
                    dbconn,
                    "profiles",
                    [
                        Column("id", String, primary_key=True),
                        Column("username", String),
                    ],
                    create_table=True,
                )
            ),
            "posts_with_username": Table(
                store=TableStoreDB(
                    dbconn,
                    "posts_with_username",
                    [
                        Column("id", String, primary_key=True),
                        Column("user_id", String),
                        Column("content", String),
                        Column("username", String),
                    ],
                    create_table=True,
                )
            ),
        }
    )

    def transform_func(posts_df, profiles_df):
        result = posts_df.merge(profiles_df, left_on="user_id", right_on="id", suffixes=("", "_profile"))
        return result[["id", "user_id", "content", "username"]]

    step = BatchTransform(
        func=transform_func,
        inputs=[
            InputSpec(
                table="posts",
                keys={
                    "post_id": "id",
                    "user_id": DataField("user_id"),
                },
            ),
            InputSpec(
                table="profiles",
                keys={
                    "user_id": "id",
                },
            ),
        ],
        outputs=[OutputSpec(table="posts_with_username", keys={"post_id": "id"})],
        transform_keys=["post_id", "user_id"],
    ).build_compute(ds, catalog)[0]

    assert isinstance(step, BatchTransformStep)
    assert len(step.output_specs) == 1
    assert step.output_specs[0].keys == {"post_id": "id"}


def test_transform_keys_with_input_table_as_output(dbconn: DBConn):
    ds = DataStore(dbconn, create_meta_table=True)

    users = ds.create_table(
        "users",
        TableStoreDB(
            dbconn,
            "users",
            [
                Column("id", String, primary_key=True),
                Column("name", String),
            ],
            create_table=True,
        ),
    )
    scores = ds.create_table(
        "scores",
        TableStoreDB(
            dbconn,
            "scores",
            [
                Column("id", String, primary_key=True),
                Column("score", String),
                Column("user_name", String),
            ],
            create_table=True,
        ),
    )

    process_ts = time.time()
    users.store_chunk(
        pd.DataFrame(
            [
                {"id": "u1", "name": "Alice"},
                {"id": "u2", "name": "Bob"},
            ]
        ),
        now=process_ts,
    )
    scores.store_chunk(
        pd.DataFrame(
            [
                {"id": "u1", "score": "10", "user_name": ""},
                {"id": "u2", "score": "20", "user_name": ""},
            ]
        ),
        now=process_ts,
    )

    def transform_func(users_df, scores_df):
        df = scores_df[["id", "score"]].merge(users_df, on="id")
        return df.rename(columns={"name": "user_name"})[["id", "score", "user_name"]]

    step = BatchTransformStep(
        ds=ds,
        name="test_input_table_as_output",
        func=transform_func,
        input_dts=[
            ComputeInput(
                dt=users,
                join_type="full",
                keys={
                    "user_id": "id",
                },
            ),
            ComputeInput(
                dt=scores,
                join_type="inner",
                keys={
                    "user_id": "id",
                },
            ),
        ],
        output_dts=[
            ComputeOutput(
                dt=scores,
                keys={
                    "user_id": "id",
                },
            )
        ],
        transform_keys=["user_id"],
    )

    step.run_full(ds)

    assert_datatable_equal(
        scores,
        pd.DataFrame(
            [
                {"id": "u1", "score": "10", "user_name": "Alice"},
                {"id": "u2", "score": "20", "user_name": "Bob"},
            ]
        ),
    )

    time.sleep(0.01)
    users.store_chunk(pd.DataFrame([{"id": "u1", "name": "Alice Updated"}]), now=time.time())
    step.run_full(ds)

    assert_datatable_equal(
        scores,
        pd.DataFrame(
            [
                {"id": "u1", "score": "10", "user_name": "Alice Updated"},
                {"id": "u2", "score": "20", "user_name": "Bob"},
            ]
        ),
    )


def test_batch_transform_outputs_with_different_key_mappings(dbconn: DBConn):
    ds = DataStore(dbconn, create_meta_table=True)

    catalog = Catalog(
        {
            "posts": Table(
                store=TableStoreDB(
                    dbconn,
                    "posts",
                    [
                        Column("id", String, primary_key=True),
                        Column("user_id", String),
                        Column("content", String),
                    ],
                    create_table=True,
                )
            ),
            "profiles": Table(
                store=TableStoreDB(
                    dbconn,
                    "profiles",
                    [
                        Column("id", String, primary_key=True),
                        Column("username", String),
                    ],
                    create_table=True,
                )
            ),
            "post_cards": Table(
                store=TableStoreDB(
                    dbconn,
                    "post_cards",
                    [
                        Column("id", String, primary_key=True),
                        Column("username", String),
                        Column("content", String),
                    ],
                    create_table=True,
                )
            ),
            "user_cards": Table(
                store=TableStoreDB(
                    dbconn,
                    "user_cards",
                    [
                        Column("id", String, primary_key=True),
                        Column("username", String),
                        Column("posts_count", String),
                    ],
                    create_table=True,
                )
            ),
        }
    )

    posts = catalog.get_datatable(ds, "posts")
    profiles = catalog.get_datatable(ds, "profiles")
    post_cards = catalog.get_datatable(ds, "post_cards")
    user_cards = catalog.get_datatable(ds, "user_cards")

    process_ts = time.time()
    posts.store_chunk(
        pd.DataFrame(
            [
                {"id": "p1", "user_id": "u1", "content": "Post 1"},
                {"id": "p2", "user_id": "u1", "content": "Post 2"},
                {"id": "p3", "user_id": "u2", "content": "Post 3"},
            ]
        ),
        now=process_ts,
    )
    profiles.store_chunk(
        pd.DataFrame(
            [
                {"id": "u1", "username": "alice"},
                {"id": "u2", "username": "bob"},
            ]
        ),
        now=process_ts,
    )

    def transform_func(posts_df, profiles_df):
        df = posts_df.merge(profiles_df, left_on="user_id", right_on="id", suffixes=("_post", "_profile"))
        post_cards_df = df.rename(columns={"id_post": "id"})[["id", "username", "content"]]
        user_cards_df = (
            df.groupby(["id_profile", "username"], as_index=False)
            .agg(posts_count=("id_post", "count"))
            .rename(columns={"id_profile": "id"})
        )
        user_cards_df["posts_count"] = user_cards_df["posts_count"].astype(str)
        return post_cards_df, user_cards_df

    step = BatchTransform(
        func=transform_func,
        inputs=[
            InputSpec(
                table="posts",
                keys={
                    "post_id": "id",
                    "user_id": DataField("user_id"),
                },
            ),
            InputSpec(
                table="profiles",
                keys={
                    "user_id": "id",
                },
            ),
        ],
        outputs=[
            OutputSpec(table="post_cards", keys={"post_id": "id"}),
            OutputSpec(table="user_cards", keys={"user_id": "id"}),
        ],
        transform_keys=["post_id", "user_id"],
    ).build_compute(ds, catalog)[0]

    step.run_full(ds)

    assert_datatable_equal(
        post_cards,
        pd.DataFrame(
            [
                {"id": "p1", "username": "alice", "content": "Post 1"},
                {"id": "p2", "username": "alice", "content": "Post 2"},
                {"id": "p3", "username": "bob", "content": "Post 3"},
            ]
        ),
    )
    assert_datatable_equal(
        user_cards,
        pd.DataFrame(
            [
                {"id": "u1", "username": "alice", "posts_count": "2"},
                {"id": "u2", "username": "bob", "posts_count": "1"},
            ]
        ),
    )

    time.sleep(0.01)
    profiles.delete_by_idx(pd.DataFrame([{"id": "u1"}]), now=time.time())
    step.run_full(ds)

    assert_datatable_equal(
        post_cards,
        pd.DataFrame(
            [
                {"id": "p3", "username": "bob", "content": "Post 3"},
            ]
        ),
    )
    assert_datatable_equal(
        user_cards,
        pd.DataFrame(
            [
                {"id": "u2", "username": "bob", "posts_count": "1"},
            ]
        ),
    )


def test_transform_keys_with_composite_aliases_and_multiple_outputs(dbconn: DBConn):
    ds = DataStore(dbconn, create_meta_table=True)

    events = ds.create_table(
        "events",
        TableStoreDB(
            dbconn,
            "events",
            [
                Column("event_id", String, primary_key=True),
                Column("tenant_id", String, primary_key=True),
                Column("user_id", String),
                Column("payload", String),
            ],
            create_table=True,
        ),
    )
    tenants = ds.create_table(
        "tenants",
        TableStoreDB(
            dbconn,
            "tenants",
            [
                Column("id", String, primary_key=True),
                Column("tenant_name", String),
            ],
            create_table=True,
        ),
    )
    enriched_events = ds.create_table(
        "enriched_events",
        TableStoreDB(
            dbconn,
            "enriched_events",
            [
                Column("event_pk", String, primary_key=True),
                Column("tenant_pk", String, primary_key=True),
                Column("user_id", String),
                Column("payload", String),
                Column("tenant_name", String),
            ],
            create_table=True,
        ),
    )
    event_summaries = ds.create_table(
        "event_summaries",
        TableStoreDB(
            dbconn,
            "event_summaries",
            [
                Column("summary_event_id", String, primary_key=True),
                Column("summary_tenant_id", String, primary_key=True),
                Column("summary", String),
            ],
            create_table=True,
        ),
    )

    process_ts = time.time()
    events.store_chunk(
        pd.DataFrame(
            [
                {"event_id": "e1", "tenant_id": "t1", "user_id": "u1", "payload": "payload-1"},
                {"event_id": "e2", "tenant_id": "t1", "user_id": "u2", "payload": "payload-2"},
                {"event_id": "e3", "tenant_id": "t2", "user_id": "u3", "payload": "payload-3"},
            ]
        ),
        now=process_ts,
    )
    tenants.store_chunk(
        pd.DataFrame(
            [
                {"id": "t1", "tenant_name": "tenant-one"},
                {"id": "t2", "tenant_name": "tenant-two"},
            ]
        ),
        now=process_ts,
    )

    def transform_func(events_df, tenants_df):
        df = events_df.merge(tenants_df, left_on="tenant_id", right_on="id")
        enriched_df = df.rename(columns={"event_id": "event_pk", "tenant_id": "tenant_pk"})[
            ["event_pk", "tenant_pk", "user_id", "payload", "tenant_name"]
        ]
        summary_df = df.rename(columns={"event_id": "summary_event_id", "tenant_id": "summary_tenant_id"})[
            ["summary_event_id", "summary_tenant_id"]
        ].copy()
        summary_df["summary"] = [
            f"{payload}@{tenant_name}" for payload, tenant_name in zip(df["payload"], df["tenant_name"])
        ]
        return enriched_df, summary_df

    step = BatchTransformStep(
        ds=ds,
        name="test_composite_aliases",
        func=transform_func,
        input_dts=[
            ComputeInput(
                dt=events,
                join_type="full",
                keys={
                    "task_event_id": "event_id",
                    "task_tenant_id": "tenant_id",
                },
            ),
            ComputeInput(
                dt=tenants,
                join_type="inner",
                keys={
                    "task_tenant_id": "id",
                },
            ),
        ],
        output_dts=[
            ComputeOutput(
                dt=enriched_events,
                keys={
                    "task_event_id": "event_pk",
                    "task_tenant_id": "tenant_pk",
                },
            ),
            ComputeOutput(
                dt=event_summaries,
                keys={
                    "task_event_id": "summary_event_id",
                    "task_tenant_id": "summary_tenant_id",
                },
            ),
        ],
        transform_keys=["task_event_id", "task_tenant_id"],
    )

    step.run_full(ds)

    assert_datatable_equal(
        enriched_events,
        pd.DataFrame(
            [
                {
                    "event_pk": "e1",
                    "tenant_pk": "t1",
                    "user_id": "u1",
                    "payload": "payload-1",
                    "tenant_name": "tenant-one",
                },
                {
                    "event_pk": "e2",
                    "tenant_pk": "t1",
                    "user_id": "u2",
                    "payload": "payload-2",
                    "tenant_name": "tenant-one",
                },
                {
                    "event_pk": "e3",
                    "tenant_pk": "t2",
                    "user_id": "u3",
                    "payload": "payload-3",
                    "tenant_name": "tenant-two",
                },
            ]
        ),
    )
    assert_datatable_equal(
        event_summaries,
        pd.DataFrame(
            [
                {"summary_event_id": "e1", "summary_tenant_id": "t1", "summary": "payload-1@tenant-one"},
                {"summary_event_id": "e2", "summary_tenant_id": "t1", "summary": "payload-2@tenant-one"},
                {"summary_event_id": "e3", "summary_tenant_id": "t2", "summary": "payload-3@tenant-two"},
            ]
        ),
    )

    time.sleep(0.01)
    tenants.store_chunk(pd.DataFrame([{"id": "t1", "tenant_name": "tenant-one-updated"}]), now=time.time())
    step.run_full(ds)

    assert_datatable_equal(
        enriched_events,
        pd.DataFrame(
            [
                {
                    "event_pk": "e1",
                    "tenant_pk": "t1",
                    "user_id": "u1",
                    "payload": "payload-1",
                    "tenant_name": "tenant-one-updated",
                },
                {
                    "event_pk": "e2",
                    "tenant_pk": "t1",
                    "user_id": "u2",
                    "payload": "payload-2",
                    "tenant_name": "tenant-one-updated",
                },
                {
                    "event_pk": "e3",
                    "tenant_pk": "t2",
                    "user_id": "u3",
                    "payload": "payload-3",
                    "tenant_name": "tenant-two",
                },
            ]
        ),
    )
    assert_datatable_equal(
        event_summaries,
        pd.DataFrame(
            [
                {"summary_event_id": "e1", "summary_tenant_id": "t1", "summary": "payload-1@tenant-one-updated"},
                {"summary_event_id": "e2", "summary_tenant_id": "t1", "summary": "payload-2@tenant-one-updated"},
                {"summary_event_id": "e3", "summary_tenant_id": "t2", "summary": "payload-3@tenant-two"},
            ]
        ),
    )

    time.sleep(0.01)
    tenants.delete_by_idx(pd.DataFrame([{"id": "t1"}]), now=time.time())
    step.run_full(ds)

    assert_datatable_equal(
        enriched_events,
        pd.DataFrame(
            [
                {
                    "event_pk": "e3",
                    "tenant_pk": "t2",
                    "user_id": "u3",
                    "payload": "payload-3",
                    "tenant_name": "tenant-two",
                },
            ]
        ),
    )
    assert_datatable_equal(
        event_summaries,
        pd.DataFrame(
            [
                {"summary_event_id": "e3", "summary_tenant_id": "t2", "summary": "payload-3@tenant-two"},
            ]
        ),
    )


def test_transform_keys_with_same_column_names_and_different_aliases(dbconn: DBConn):
    ds = DataStore(dbconn, create_meta_table=True)

    users = ds.create_table(
        "users",
        TableStoreDB(
            dbconn,
            "users",
            [
                Column("id", String, primary_key=True),
                Column("name", String),
                Column("team_id", String),
                Column("role_id", String),
            ],
            create_table=True,
        ),
    )
    teams = ds.create_table(
        "teams",
        TableStoreDB(
            dbconn,
            "teams",
            [
                Column("id", String, primary_key=True),
                Column("name", String),
            ],
            create_table=True,
        ),
    )
    roles = ds.create_table(
        "roles",
        TableStoreDB(
            dbconn,
            "roles",
            [
                Column("id", String, primary_key=True),
                Column("name", String),
            ],
            create_table=True,
        ),
    )
    memberships = ds.create_table(
        "memberships",
        TableStoreDB(
            dbconn,
            "memberships",
            [
                Column("member_id", String, primary_key=True),
                Column("member_team_id", String, primary_key=True),
                Column("member_role_id", String, primary_key=True),
                Column("user_name", String),
                Column("team_name", String),
                Column("role_name", String),
            ],
            create_table=True,
        ),
    )

    process_ts = time.time()
    users.store_chunk(
        pd.DataFrame(
            [
                {"id": "u1", "name": "Alice", "team_id": "t1", "role_id": "r1"},
                {"id": "u2", "name": "Bob", "team_id": "t1", "role_id": "r2"},
                {"id": "u3", "name": "Eve", "team_id": "t2", "role_id": "r1"},
            ]
        ),
        now=process_ts,
    )
    teams.store_chunk(
        pd.DataFrame(
            [
                {"id": "t1", "name": "Core"},
                {"id": "t2", "name": "ML"},
            ]
        ),
        now=process_ts,
    )
    roles.store_chunk(
        pd.DataFrame(
            [
                {"id": "r1", "name": "Admin"},
                {"id": "r2", "name": "Reviewer"},
            ]
        ),
        now=process_ts,
    )

    def transform_func(users_df, teams_df, roles_df):
        df = users_df.merge(teams_df, left_on="team_id", right_on="id", suffixes=("_user", "_team"))
        df = df.merge(roles_df, left_on="role_id", right_on="id")
        return pd.DataFrame(
            {
                "member_id": df["id_user"],
                "member_team_id": df["team_id"],
                "member_role_id": df["role_id"],
                "user_name": df["name_user"],
                "team_name": df["name_team"],
                "role_name": df["name"],
            }
        )

    step = BatchTransformStep(
        ds=ds,
        name="test_same_column_names_aliases",
        func=transform_func,
        input_dts=[
            ComputeInput(
                dt=users,
                join_type="full",
                keys={
                    "user_id": "id",
                    "team_id": DataField("team_id"),
                    "role_id": DataField("role_id"),
                },
            ),
            ComputeInput(
                dt=teams,
                join_type="inner",
                keys={
                    "team_id": "id",
                },
            ),
            ComputeInput(
                dt=roles,
                join_type="inner",
                keys={
                    "role_id": "id",
                },
            ),
        ],
        output_dts=[
            ComputeOutput(
                dt=memberships,
                keys={
                    "user_id": "member_id",
                    "team_id": "member_team_id",
                    "role_id": "member_role_id",
                },
            ),
        ],
        transform_keys=["user_id", "team_id", "role_id"],
    )

    step.run_full(ds)

    assert_datatable_equal(
        memberships,
        pd.DataFrame(
            [
                {
                    "member_id": "u1",
                    "member_team_id": "t1",
                    "member_role_id": "r1",
                    "user_name": "Alice",
                    "team_name": "Core",
                    "role_name": "Admin",
                },
                {
                    "member_id": "u2",
                    "member_team_id": "t1",
                    "member_role_id": "r2",
                    "user_name": "Bob",
                    "team_name": "Core",
                    "role_name": "Reviewer",
                },
                {
                    "member_id": "u3",
                    "member_team_id": "t2",
                    "member_role_id": "r1",
                    "user_name": "Eve",
                    "team_name": "ML",
                    "role_name": "Admin",
                },
            ]
        ),
    )

    time.sleep(0.01)
    teams.store_chunk(pd.DataFrame([{"id": "t1", "name": "Core Platform"}]), now=time.time())
    step.run_full(ds)

    assert_datatable_equal(
        memberships,
        pd.DataFrame(
            [
                {
                    "member_id": "u1",
                    "member_team_id": "t1",
                    "member_role_id": "r1",
                    "user_name": "Alice",
                    "team_name": "Core Platform",
                    "role_name": "Admin",
                },
                {
                    "member_id": "u2",
                    "member_team_id": "t1",
                    "member_role_id": "r2",
                    "user_name": "Bob",
                    "team_name": "Core Platform",
                    "role_name": "Reviewer",
                },
                {
                    "member_id": "u3",
                    "member_team_id": "t2",
                    "member_role_id": "r1",
                    "user_name": "Eve",
                    "team_name": "ML",
                    "role_name": "Admin",
                },
            ]
        ),
    )

    time.sleep(0.01)
    roles.delete_by_idx(pd.DataFrame([{"id": "r1"}]), now=time.time())
    step.run_full(ds)

    assert_datatable_equal(
        memberships,
        pd.DataFrame(
            [
                {
                    "member_id": "u2",
                    "member_team_id": "t1",
                    "member_role_id": "r2",
                    "user_name": "Bob",
                    "team_name": "Core Platform",
                    "role_name": "Reviewer",
                },
            ]
        ),
    )


def test_transform_keys_with_same_output_column_names_and_different_aliases(dbconn: DBConn):
    ds = DataStore(dbconn, create_meta_table=True)

    users = ds.create_table(
        "users",
        TableStoreDB(
            dbconn,
            "users",
            [
                Column("id", String, primary_key=True),
                Column("name", String),
                Column("role_id", String),
            ],
            create_table=True,
        ),
    )
    roles = ds.create_table(
        "roles",
        TableStoreDB(
            dbconn,
            "roles",
            [
                Column("id", String, primary_key=True),
                Column("name", String),
            ],
            create_table=True,
        ),
    )
    user_cards = ds.create_table(
        "user_cards",
        TableStoreDB(
            dbconn,
            "user_cards",
            [
                Column("id", String, primary_key=True),
                Column("user_name", String),
                Column("role_name", String),
            ],
            create_table=True,
        ),
    )
    role_cards = ds.create_table(
        "role_cards",
        TableStoreDB(
            dbconn,
            "role_cards",
            [
                Column("id", String, primary_key=True),
                Column("role_name", String),
                Column("users_count", String),
            ],
            create_table=True,
        ),
    )

    process_ts = time.time()
    users.store_chunk(
        pd.DataFrame(
            [
                {"id": "u1", "name": "Alice", "role_id": "r1"},
                {"id": "u2", "name": "Bob", "role_id": "r2"},
                {"id": "u3", "name": "Eve", "role_id": "r1"},
            ]
        ),
        now=process_ts,
    )
    roles.store_chunk(
        pd.DataFrame(
            [
                {"id": "r1", "name": "Admin"},
                {"id": "r2", "name": "Reviewer"},
            ]
        ),
        now=process_ts,
    )

    def transform_func(users_df, roles_df):
        df = users_df.merge(roles_df, left_on="role_id", right_on="id", suffixes=("_user", "_role"))
        user_cards_df = pd.DataFrame(
            {
                "id": df["id_user"],
                "user_name": df["name_user"],
                "role_name": df["name_role"],
            }
        )
        role_cards_df = (
            df.groupby(["id_role", "name_role"], as_index=False)
            .agg(users_count=("id_user", "count"))
            .rename(columns={"id_role": "id", "name_role": "role_name"})
        )
        role_cards_df["users_count"] = role_cards_df["users_count"].astype(str)
        return user_cards_df, role_cards_df

    step = BatchTransformStep(
        ds=ds,
        name="test_same_output_columns_aliases",
        func=transform_func,
        input_dts=[
            ComputeInput(
                dt=users,
                join_type="full",
                keys={
                    "user_id": "id",
                    "role_id": DataField("role_id"),
                },
            ),
            ComputeInput(
                dt=roles,
                join_type="inner",
                keys={
                    "role_id": "id",
                },
            ),
        ],
        output_dts=[
            ComputeOutput(
                dt=user_cards,
                keys={
                    "user_id": "id",
                },
            ),
            ComputeOutput(
                dt=role_cards,
                keys={
                    "role_id": "id",
                },
            ),
        ],
        transform_keys=["user_id", "role_id"],
    )

    step.run_full(ds)

    assert_datatable_equal(
        user_cards,
        pd.DataFrame(
            [
                {"id": "u1", "user_name": "Alice", "role_name": "Admin"},
                {"id": "u2", "user_name": "Bob", "role_name": "Reviewer"},
                {"id": "u3", "user_name": "Eve", "role_name": "Admin"},
            ]
        ),
    )
    assert_datatable_equal(
        role_cards,
        pd.DataFrame(
            [
                {"id": "r1", "role_name": "Admin", "users_count": "2"},
                {"id": "r2", "role_name": "Reviewer", "users_count": "1"},
            ]
        ),
    )

    time.sleep(0.01)
    roles.store_chunk(pd.DataFrame([{"id": "r1", "name": "Administrator"}]), now=time.time())
    step.run_full(ds)

    assert_datatable_equal(
        user_cards,
        pd.DataFrame(
            [
                {"id": "u1", "user_name": "Alice", "role_name": "Administrator"},
                {"id": "u2", "user_name": "Bob", "role_name": "Reviewer"},
                {"id": "u3", "user_name": "Eve", "role_name": "Administrator"},
            ]
        ),
    )
    assert_datatable_equal(
        role_cards,
        pd.DataFrame(
            [
                {"id": "r1", "role_name": "Administrator", "users_count": "2"},
                {"id": "r2", "role_name": "Reviewer", "users_count": "1"},
            ]
        ),
    )

    time.sleep(0.01)
    roles.delete_by_idx(pd.DataFrame([{"id": "r1"}]), now=time.time())
    step.run_full(ds)

    assert_datatable_equal(
        user_cards,
        pd.DataFrame(
            [
                {"id": "u2", "user_name": "Bob", "role_name": "Reviewer"},
            ]
        ),
    )
    assert_datatable_equal(
        role_cards,
        pd.DataFrame(
            [
                {"id": "r2", "role_name": "Reviewer", "users_count": "1"},
            ]
        ),
    )

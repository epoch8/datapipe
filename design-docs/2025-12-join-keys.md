# Goal

Make it possible to join tables in transformation where key in one and another
table do not match by name.

# Use case

You have tables `User (user_id)` and `Subscription (user_id, sub_user_id)`
You need to enrich both sides of `Subscription` with information

You might write:

```
BatchTransform(
    process_func,
    transform_keys=["user_id", "sub_user_id"],
    inputs=[
        # matches tr.user_id = Subscription.user_id and tr.sub_user_id = Subscription.sub_user_id
        Subscription,

        # matches tr.user_id = User.user_id
        User,

        # matches tr.sub_user_id = User.user_id
        JoinSpec(User, join_keys={"user_id", "sub_user_id"}) 
    ],
    outputs=[...],
)
```

And `process_func` at each execution will receive three dataframes:

* `subscription_df` - chunk of `Subscription`
* `user_df` - chunk of `User` matched by `user_id`
* `sub_user_df` - chunk of `User` matched by `sub_user_id`

Both `user_df` and `sub_user_df` have columns aligned with `User` table, i.e.
without renamings, it is up to end user to interpret the data.

# Implementation

## DX

* `datapipe.types.JoinSpec` receives `join_keys` parameter

## Compute

* `datapipe.compute.ComputeInput` receives `join_keys` parameter
* `datapipe.meta.sql_meta.SQLTableMeta` receives `join_keys` parameter into
  `get_agg_cte` i.e. it starts producing subquery with renamed keys
* `DataTable.get_data` accepts `join_keys` and pass idx through TableMeta to compute ids to retrieve from DataStore

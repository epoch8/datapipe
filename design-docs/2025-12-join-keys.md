# Open questions

* Maybe something like `column_aliases` is a better name for internal use than
  `join_keys`?
* How to deal with ChangeList filter_idx? Before this change we assume that idx
  mean the same everywhere and just filter by given idx name and values, now we
  should understand how table keys are propagated to join
* How to deal with RunConfig filters? This feature in general is not very
  transparent to me, we have to understand it and do something, probably similar
  to filter_idx

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

* [v] `datapipe.types.JoinSpec` receives `join_keys` parameter

## Compute

* [v] `datapipe.compute.ComputeInput` receives `join_keys` parameter

`datapipe.meta.sql_meta.SQLTableMeta`:
* [ ] new method `transform_idx_to_table_idx` which should be used to convert
  transform keys to table keys
* [v] `get_agg_cte` receives `join_keys` parameter and starts producing subquery
  with renamed keys
* [ ] `get_agg_cte` correctly applies `join_keys` to `filter_idx` parameter
* [ ] `get_agg_cte` correctly applies `join_keys` to `RunConfig` filters

`BatchTransform`:
* [v] Correctly converts transform idx to table idx in `get_batch_input_dfs`

`DataTable`:
* [v] `DataTable.get_data` accepts `table_idx` which is acquired by applying
  `tranform_idx_to_table_idx`

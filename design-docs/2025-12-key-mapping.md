# Open questions

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

You have tables `User (id: PK)` and `Subscription (id: PK, user_id: DATA, sub_user_id: DATA)`
You need to enrich both sides of `Subscription` with information

You might write:

```
BatchTransform(
    process_func,

    # defines ["user_id", "sub_user_id"] as a keys that identify each transform task
    # every table should have a way to join to these keys
    transform_keys=["user_id", "sub_user_id"],
    inputs=[
        # Subscription has needed columns in data table, we fetch them from there
        InputSpec(Subscription, keys={"user_id": DataField("user_id"), "sub_user_id": DataField("sub_user_id")}),

        # matches tr.user_id = User.id
        InputSpec(User, keys={"user_id": "id"}),

        # matches tr.sub_user_id = User.id
        InputSpec(User, keys={"sub_user_id": "id"}),
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

# InputSpec

We introduce `InputSpec` qualifier for `BatchTransform` inputs.

`keys` parameter defines which columns to use for this input table and where to
get them from. `keys` is a dict in a form `{"{transform_key}": key_accessor}`,
where `key_accessor` might be:
* a string, then a column from meta-table is used with possible renaming
* `DataField("data_col")` then a `data_col` from data-table is used instead of
  meta-table

If table is provided as is without `InputSpec` wrapper, then it is equivalent to
`InputSpec(Table, join_type="outer", keys={"id1": "id1", ...})`, join type is
outer join and all keys are mapped to themselves.

## DataField limitations

`DataField` accessor serves as an ad-hoc solution for a situation when for some
reason a data field can not be promoted to a meta-field.

Data fields are not used when retreiving a chunk of data, so it is possible to
over-fetch data.

Data fields are not enforced to have indices in DB, so their usage might be very
heavy for database.


# Implementation

## DX

* [x] `datapipe.types.JoinSpec` is renamed to `InputSpec` and receives `keys`
  parameter

## Compute

* [x] `datapipe.compute.ComputeInput` receives `keys` parameter

`datapipe.meta.sql_meta.SQLTableMeta`:
* [x] new method `transform_idx_to_table_idx` which should be used to convert
  transform keys to table keys
* [x] `get_agg_cte` receives `keys` parameter and starts producing subquery with
  renamed keys
* [ ] `get_agg_cte` correctly applies `keys` to `filter_idx` parameter
* [ ] `get_agg_cte` correctly applies `keys` to `RunConfig` filters

`BatchTransform`:
* [x] correctly converts transform idx to table idx in `get_batch_input_dfs`
* [x] inputs and outputs are stored as `ComputeInput` lists, because we need
  data table for `DataField`

`DataTable`:
* [x] `DataTable.get_data` accepts `table_idx` which is acquired by applying
  `tranform_idx_to_table_idx`

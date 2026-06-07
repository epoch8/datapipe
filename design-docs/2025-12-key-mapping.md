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

You have tables `User (id: PK)` and `Subscription (id: PK, user_id: PK, sub_user_id: PK)`
You need to enrich both sides of `Subscription` with information

You might write:

```
BatchTransform(
    process_func,

    # defines ["user_id", "sub_user_id"] as a keys that identify each transform task
    # every table should have a way to join to these keys
    transform_keys=["user_id", "sub_user_id"],
    inputs=[
        # Subscription has user_id and sub_user_id as primary keys
        InputSpec(Subscription, keys={"user_id": "user_id", "sub_user_id": "sub_user_id"}),

        # matches tr.user_id = User.id
        InputSpec(User, keys={"user_id": "id"}),

        # matches tr.sub_user_id = User.id
        InputSpec(User, keys={"sub_user_id": "id"}),
    ],
    outputs=[
        # output table has user_id as PK; transform key sub_user_id is not relevant here
        OutputSpec(SubscriptionUserSummary, keys={"user_id": "user_id"}),
    ],
)
```

And `process_func` at each execution will receive three dataframes:

* `subscription_df` - chunk of `Subscription`
* `user_df` - chunk of `User` matched by `user_id`
* `sub_user_df` - chunk of `User` matched by `sub_user_id`

Both `user_df` and `sub_user_df` have columns aligned with `User` table, i.e.
without renamings, it is up to end user to interpret the data.

# InputSpec and OutputSpec

We introduce `InputSpec` qualifier for `BatchTransform` inputs and `OutputSpec`
qualifier for `BatchTransform` outputs.

`InputSpec.keys` defines which columns to use for this input table and where to
get them from. `keys` is a dict in a form `{"{transform_key}": "table_pk_col"}`,
where `table_pk_col` is a string referencing a primary key column of the table.

If a table is provided as is without `InputSpec` wrapper, then it is equivalent to
`InputSpec(Table, join_type="outer", keys={"id1": "id1", ...})`, join type is
outer join and all keys are mapped to themselves.

`OutputSpec.keys` defines how transform keys map to the output table's primary key
columns for the purpose of cleanup. `keys` is a dict in the form
`{"{transform_key}": "output_pk_col"}`. Only the transform keys that are relevant
to this output table need to be listed. If a table is provided without `OutputSpec`,
all transform keys are assumed to match the output primary keys by name.

Note: transform key columns must always be primary keys in the table schema.


# Implementation

## DX

* [x] `datapipe.types.JoinSpec` is renamed to `InputSpec` and receives `keys`
  parameter
* [x] `datapipe.types.OutputSpec` is added with `keys` parameter to map transform
  keys to output table primary key columns

## Compute

* [x] `datapipe.compute.ComputeInput` receives `keys` parameter
* [x] `datapipe.compute.ComputeOutput` is added with `keys` parameter (mirrors
  `ComputeInput`); `pipeline_output_to_compute_output` converts `PipelineOutput`
  to `ComputeOutput`

`datapipe.meta.base.TableMeta` (base class, not `SQLTableMeta`):
* [x] new method `transform_idx_to_table_idx` which should be used to convert
  transform keys to table keys by applying `keys` aliasing

`datapipe.meta.sql_meta.SQLTableMeta`:
* [x] `get_agg_cte` receives `keys` parameter and starts producing subquery with
  renamed keys
* [ ] `get_agg_cte` correctly applies `keys` to `filter_idx` parameter
* [ ] `get_agg_cte` correctly applies `keys` to `RunConfig` filters

`BatchTransformStep`:
* [x] correctly converts transform idx to table idx in `get_batch_input_dfs` via
  `transform_idx_to_table_idx`
* [x] new method `_transform_idx_to_output_idx` converts transform idx to output
  table idx using `ComputeOutput.keys` for cleanup/delete operations
* [x] inputs are stored as `ComputeInput` list; outputs are stored as `ComputeOutput`
  list

`DataTable`:
* [x] `DataTable.get_data` accepts `table_idx` which is acquired by applying
  `transform_idx_to_table_idx`

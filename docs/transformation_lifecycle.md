# Lifecycle of transformation step

As a computational graph node, transformation consists of:

* `input_dts` - Input data tables
* `output_dts` - Output data tables
* Transformation logic

In order to run transformation, runtime performs actions with the following
structure:

1. `transform`
    * `get_idx_to_process` - Compute idx-es that require computation
    * For each `idx` batch:
        * `process_batch_dt` - Process batch in terms of DataTable
            * `get_batch_df` - Retreive batch data in pd.DataFrame form
            * `process_batch_df` - Process batch in terms of pd.DataFrame
        * `store_outputs`

!! Note, lifecycle of generator is different

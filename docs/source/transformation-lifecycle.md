# Lifecycle of a ComputeStep execution

As a computational graph node, transformation consists of:

* `input_dts` - Input data tables
* `output_dts` - Output data tables
* Transformation logic

In order to run transformation, runtime performs actions with the following
structure:

* `run_full` / `run_changelist`
    * `get_full_process_ds` / `get_change_list_process_ids` - Compute idx-es
      that require computation
    * For each `idx` in batch:
        * `process_batch` - Process batch in terms of DataTable
            * `process_batch_dts` - Process batch with DataTables as input and
              `pd.DataFrame` as output
                * `get_batch_input_dfs` - Retreive batch data in `pd.DataFrame`
                  form
                * `process_batch_df` - Process batch in terms of `pd.DataFrame`
            * store results

* `store_batch_result` is called when batch was processed successfuly
* `store_batch_err` is called when there was an exception during batch
  processing

![lifecycle](transformation_lifecycle.png)

!! Note, lifecycle of generator is different

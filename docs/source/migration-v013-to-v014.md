# Migration from v0.13 to v0.14

## DatatableTansform can become BatchTransform

Previously, if you had to do whole table transformation, you had to use
`DatatableTransform`. Now you can substitute it with `BatchTransform` which has
zero outputs.

Before:

```python
# Updates global count of input lines

def count(
    ds: DataStore,
    input_dts: List[DataTable],
    output_dts: List[DataTable],
    kwargs: Dict,
    run_config: Optional[RunConfig] = None,
) -> None:
    assert len(input_dts) == 1
    assert len(output_dts) == 1

    input_dt = input_dts[0]
    output_dt = output_dts[0]

    output_dt.store_chunk(
        pd.DataFrame(
            {"result_id": [0], "count": [len(input_dt.meta_table.get_existing_idx())]}
        )
    )

# ...

DatatableTransform(
    count,
    inputs=["input"],
    outputs=["result"],
)
```

After:

```python
# Updates global count of input lines

def count(
    input_df: pd.DataFrame,
) -> pd.DataFrame:
    return pd.DataFrame({"result_id": [0], "count": [len(input_df)]})

# ...

BatchTransform(
    count,
    inputs=["input"],
    outputs=["result"],

    # Important, we have to specify empty set in order for transformation to operate on 
    # the whole input at once
    transform_keys=[],
)
```

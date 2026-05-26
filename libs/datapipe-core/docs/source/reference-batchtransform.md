# BatchTransform

```python
BatchTransoform(
    func: BatchTransformFunc,
    inputs: List[PipelineInput],
    outputs: List[TableOrName],
    chunk_size: int = 1000,
    kwargs: Optional[Dict[str, Any]] = None,
    transform_keys: Optional[List[str]] = None,
    labels: Optional[Labels] = None,
    executor_config: Optional[ExecutorConfig] = None,
    filters: Optional[LabelDict | Callable[[], LabelDict]] = None,
    order_by: Optional[List[str]] = None,
    order: Literal["asc", "desc"] = "asc",
)
```

## Arguments

### `func`

Function which is a body of transform, it receives the same number of
`pd.DataFrame`-s in the same order as specified in `inputs`

It should return a single `pd.DataFrame` if the `output` has one element or a
tuple of `pd.DataFrame` of the same length as `output` which will be interpreted
as corresponding to elements in `output`.

### `inputs`

A list of input tables for a given transformation. Each element might be either:

* a string, this string will be interpreted as a name of `Table` from `Catalog`
* an SQLAlchemy ORM table, this table will be added implicitly to `Catalog` and
  used as an input
* a qualifier `Required` with parameter either a string or an SQLAlchemy table,
  in this case same rules apply to the inner part and qualifier `Required` tells
  Datapipe that rows from this table must be present at calculation of
  transformations to compute

Example:

```python
# ...
BatchTransform(
    func=apply_detection_model,
    inputs=[
        # This is a table from Catalog.
        # keys: <image_id>
        "images",

        # This is an SQLAlchemy table defined with declarative ORM.
        # keys: <model_id>
        DectionModel,

        # This is a table from Catalog, which contains the identifier of current 
        # model, entries from DetectionModel will be filtered joining on `model_id`.
        # keys: <model_id>
        Required("current_model"),
    ],
    # ...
)
# ...
```

### `outputs`

### `chunk_size`

### `kwargs`

### `transform_keys`

### `labels`

### `executor_config`

### `filters`

### `order_by`

### `order`

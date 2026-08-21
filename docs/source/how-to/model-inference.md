# How to Run Model Inference (Multi-Input Transforms)

Run work over a **cross product** of inputs — for example every model × every image — while keeping incremental scheduling correct.

## Goal

Build a `BatchTransform` with multiple inputs whose transform grain is the product of several primary-key dimensions, not a simple 1-to-1 key match.

## Steps

### 1. Give each input its own primary key schema

Tables may share some keys (tenant, pipeline) and diverge on others (`model_id` vs `input_id`):

```python
input_tbl = Table(
    name="input",
    store=TableStoreJsonLine(
        filename="input.jsonline",
        primary_schema=[
            sa.Column("pipeline_id", sa.String, primary_key=True),
            sa.Column("input_id", sa.Integer, primary_key=True),
        ],
    ),
)

models_tbl = Table(
    name="models",
    store=TableStoreJsonLine(
        filename="models.jsonline",
        primary_schema=[
            sa.Column("pipeline_id", sa.String, primary_key=True),
            sa.Column("model_id", sa.String, primary_key=True),
        ],
    ),
)
```

### 2. Set `transform_keys` to the product grain

List every key that identifies one unit of inference work:

```python
BatchTransform(
    apply_model,
    inputs=[input_tbl, models_tbl],
    outputs=[output_tbl],
    transform_keys=["pipeline_id", "input_id", "model_id"],
)
```

Datapipe schedules one task per unique combination of those keys. Changing a model re-runs that model against matching inputs; changing an input re-runs it for matching models.

### 3. Implement the multi-input function

The function receives one DataFrame per input (same order as `inputs`). Join or iterate as needed; return rows keyed by the full transform / output primary key:

```python
def apply_model(input_df: pd.DataFrame, model_df: pd.DataFrame) -> pd.DataFrame:
    merge_df = input_df.merge(model_df, on="pipeline_id")
    # … run model(s), build predictions …
    return result_df[["pipeline_id", "input_id", "model_id", "text"]]
```

### 4. Sync sources and run

If models and inputs are written outside Datapipe, precede the transform with `UpdateExternalTable` for each source table, then:

```bash
datapipe db create-all
datapipe run
```

## Expected result

- Output primary keys match the product grain (`pipeline_id`, `input_id`, `model_id`, …).
- Only combinations affected by a source change recompute.
- Adding a new model dirties that model × existing inputs; adding an input dirties that input × existing models.

## Example

Full pipeline: [`examples/datapipe_core/model_inference/`](https://github.com/epoch8/datapipe/tree/master/examples/datapipe_core/model_inference).

## See also

- [Primary Keys and Transform Keys](../concepts/primary-keys.md)
- [Map Mismatched Primary Keys](./key-mapping.md) — when PK *names* differ across tables
- [BatchTransform](../reference/steps/batch-transform.md)

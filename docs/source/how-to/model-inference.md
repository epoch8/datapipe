# How to Run Model Inference (Multi-Input Transforms)

Run work over a **cross product** of inputs — for example every model × every image — while keeping incremental scheduling correct.

## Goal

Build a `BatchTransform` (or ML inference step) with multiple inputs whose transform grain is the product of several primary-key dimensions, not a simple 1-to-1 key match.

## Prerequisites

- Understanding of [Primary Keys and Transform Keys](../concepts/primary-keys.md)
- For ML examples: `datapipe-ml` with torch extra, optional GPU

## Example repo

| Example | Pattern |
|---------|---------|
| [`examples/datapipe_core/model_inference/`](https://github.com/epoch8/datapipe/tree/master/examples/datapipe_core/model_inference) | Generic multi-input `BatchTransform` |
| [`examples/e2e_template/image_detection/app.py`](https://github.com/epoch8/datapipe/tree/master/examples/e2e_template/image_detection/app.py) | `Inference_DetectionModel` — images × models |
| [`examples/detection_tags/detection/app.py`](https://github.com/epoch8/datapipe/tree/master/examples/detection_tags/detection/app.py) | Same inference step, batch inference on all images |

### E2E detection — annotation pre-labels

Pre-annotate only images **without** ground truth, using the best (or fallback) model:

```python
Inference_DetectionModel(
    input__image=["s3_images", "sec__image_without_ground_truth"],
    input__detection_model=["detection_model", "best_detection_model"],
    output__detection_prediction="ls_detection_prediction_raw",
    primary_keys=["image_name"],
    transform_keys=["image_name", "detection_model_id"],  # implicit via step
    labels=[("stage", "annotation")],
)
```

The secondary input `sec__image_without_ground_truth` restricts which image keys participate — a join filter, not a full cross product with every model row.

### E2E detection — post-training inference

Run every trained model against train/val subsets:

```python
Inference_DetectionModel(
    input__image=["s3_images", "image__subset"],
    input__detection_model="detection_model",
    output__detection_prediction="detection_prediction_raw",
    primary_keys=["image_name"],
    labels=[("stage", "train"), ("stage", "inference")],
)
```

Output primary keys: `(image_name, detection_model_id)`. Changing one model re-runs only that model's predictions.

### Detection tags — batch inference

```python
Inference_DetectionModel(
    input__image="s3_images",
    input__detection_model="detection_model_train",
    output__detection_prediction="detection_prediction_train",
    primary_keys=["image_name"],
    batch_size_default=64,
    chunk_size=1024,
    labels=[("stage", "train"), ("stage", "inference")],
)
```

Run inference only:

```bash
cd examples/detection_tags/detection
uv run datapipe --executor RayExecutor step --labels=stage=inference run
```

See [datapipe-ml](../integrations/datapipe-ml.md) and [Run Detection Tags Pipeline](./run-detection-tags-pipeline.md).

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

For ML steps like `Inference_DetectionModel`, `primary_keys` on the image side plus model keys on the model table define the same product grain.

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

For staged ML pipelines, run by label:

```bash
uv run datapipe --executor RayExecutor step --labels=stage=inference run
```

Configure GPU via `executor_config=gpu_executor()` in the step (see e2e `config.py`).

## Verify

After training produces a new model row:

```bash
uv run datapipe table detection_prediction list | head
```

Expect new rows for `(image_name, detection_model_id)` pairs. Re-run inference with unchanged inputs:

```bash
uv run datapipe --executor RayExecutor step --labels=stage=inference list
```

Should show 0 pending batches. Add a new model — only that model × existing images should dirty.

## Expected result

- Output primary keys match the product grain (`pipeline_id`, `input_id`, `model_id`, …).
- Only combinations affected by a source change recompute.
- Adding a new model dirties that model × existing inputs; adding an input dirties that input × existing models.

## See also

- [Primary Keys and Transform Keys](../concepts/primary-keys.md)
- [Map Mismatched Primary Keys](./key-mapping.md) — when PK *names* differ across tables
- [BatchTransform](../reference/steps/batch-transform.md)
- [Filter Steps by Labels](./filter-by-labels.md) — run `stage=inference` only
- [E2E Image Detection Walkthrough](../getting-started/e2e-image-detection-walkthrough.md)

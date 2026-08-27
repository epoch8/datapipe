# Freeze dataset

Module: `datapipe_ml.datasets.freeze`

Creates an immutable **frozen dataset** snapshot from labeled images and train/val/test splits. A freeze run writes:

1. A metadata row (`output__frozen_dataset`) with counts, timestamps, and on-disk folder path.
2. Per-image ground-truth rows (`output__frozen_dataset__has__image_gt`) scoped by frozen-dataset id and `subset_id`.

Freeze is gated: at least `min_delta` ground-truth rows must have changed since the last snapshot, and `min_within_time` must have elapsed since the latest freeze.

---

## `FreezeDatasetStep`

Generic freeze step. Task-specific wrappers (for example `DetectionFreezeDataset`) set `model_type` and output column prefixes.

### Inputs / outputs

| Field | Type | Description |
|---|---|---|
| `input__image` | `PipelineInput` | Image catalog table (DB or `TableStoreFiledir`). |
| `input__image__ground_truth` | `PipelineInput` | Labels / bboxes (schema depends on `model_type`). |
| `input__subset__has__image` | `PipelineInput` | Join table with `primary_keys` + `subset_id` (`train`, `val`, `test`). |
| `output__frozen_dataset` | `PipelineOutput` | Snapshot metadata table. |
| `output__frozen_dataset__has__image_gt` | `PipelineOutput` | Snapshot content (images + GT per split). |

### Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `working_dir` | `str` | *(required)* | Root directory for `{model_type}_frozen_dataset/{id}/` folders. |
| `primary_keys` | `list[str]` | *(required)* | Image entity keys (for example `["image_name"]`). |
| `min_delta` | `int` | `10` | Minimum count of changed GT rows since last freeze. |
| `min_within_time` | `str` | `"1w"` | Minimum interval since last freeze (for example `"15min"`, `"1w"`). |
| `create_table` | `bool` | `False` | DDL-create output tables when registering in catalog. |
| `image__image_path__name` | `str` | `"image__image_path"` | Column on `input__image` with image URI or path. |
| `bbox_id__name` | `str \| None` | `None` | Per-bbox id column; when set, GT uses flat bbox columns instead of JSON `bboxes`/`labels`. |
| `labels` | `Labels \| None` | `None` | CLI / run filter labels. |
| `frozen_dataset_primary_keys` | `list[str] \| None` | `None` | PKs for frozen-dataset tables; defaults to `[frozen_dataset_id__name]`. |
| `frozen_dataset_id__name` | `str` | `"frozen_dataset_id"` | Column name for snapshot id. |
| `model_type` | `str` | `"generic"` | Prefix for output columns (`{model_type}_frozen_dataset__created_at`, …). Use `detection`, `segmentation`, or `classification`. |
| `label_column_name` | `str` | `"label"` | Classification label column when `model_type="classification"`. |
| `extra_gt_columns` | `list[str]` | `[]` | Additional GT columns copied into the snapshot (stored as JSON). |

### Output schema (metadata table)

Columns are prefixed with `{model_type}_frozen_dataset`:

| Column pattern | Description |
|---|---|
| `{frozen_dataset_id__name}` | Unique snapshot id (`YYYYMMDD_HHMM_{uuid}`). |
| `{model_type}_frozen_dataset__created_at` | UTC timestamp. |
| `{model_type}_frozen_dataset__folder_filepath` | On-disk snapshot root. |
| `{model_type}_frozen_dataset__images_count` | Total images across splits. |
| `{model_type}_frozen_dataset__train_images_count` | Train split count. |
| `{model_type}_frozen_dataset__val_images_count` | Val split count. |
| `{model_type}_frozen_dataset__test_images_count` | Test split count. |
| `{model_type}_frozen_dataset__class_names` | JSON class list (populated by downstream train steps). |

### Runtime

Expands to a single `DatatableTransform` calling `freeze_dataset`. Raises `ValueError` when:

- Not enough time since last freeze.
- Fewer than `min_delta` changed GT rows.
- No ground truth, no train samples, or no val samples.

### Example

From `examples/e2e_template/image_detection/app.py` (via `DetectionFreezeDataset`):

```python
DetectionFreezeDataset(
    input__image="s3_images",
    input__image__ground_truth="image__ground_truth",
    input__subset__has__image="image__subset",
    output__detection_frozen_dataset="detection_frozen_dataset",
    output__detection_frozen_dataset__has__image_gt="detection_frozen_dataset__has__image_gt",
    working_dir=str(DATAPIPE_DIR),
    min_within_time="15min",
    min_delta=10,
    primary_keys=["image_name"],
    bbox_id__name=None,
    image__image_path__name="image_url",
    labels=[("stage", "train"), ("stage", "train-prepare")],
)
```

---

## `DetectionFreezeDataset`

Module: `datapipe_ml.tasks.detection.freeze`

Thin wrapper around `FreezeDatasetStep` with `model_type="detection"` and detection-prefixed I/O names.

| Field | Default | Notes |
|---|---|---|
| `detection_frozen_dataset_id__name` | `"detection_frozen_dataset_id"` | Maps to `frozen_dataset_id__name`. |
| `detection_frozen_dataset_primary_keys` | `None` | Maps to `frozen_dataset_primary_keys`. |
| `bbox_id__name` | `"bbox_id"` | Set to `None` for JSON bbox lists (common in e2e templates). |

All other fields match `FreezeDatasetStep` (see above).

Parallel wrappers: `SegmentationFreezeDataset`, `KeypointsFreezeDataset`, `ClassificationFreezeDataset`.

---

## `FindBestModel`

Module: `datapipe_ml.metrics.model_selection`

Selects the best model row per metric on a subset and writes a boolean flag table plus a filtered best-model table.

### Inputs / outputs

| Field | Description |
|---|---|
| `input__model` | Model registry table. |
| `input__model__metrics_on__subset` | Metrics table with `subset_id` and `metric__name`. |
| `output__attr__model__is_best` | Model PKs + boolean `is_best__name`. |
| `output__best_model` | Rows where the flag is true. |

### Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `subset_id` | `str` | *(required)* | Subset to read metrics from (for example `"val"`). |
| `is_best__name` | `str` | *(required)* | Boolean column name on the attr table. |
| `primary_keys` | `list[str]` | *(required)* | Model keys (for example `["detection_model_id"]`). |
| `metric__name` | `str` | *(required)* | Metric column to optimize (for example `"calc__weighted_f1_score"`). |
| `func` | `"min" \| "max"` | *(required)* | Whether lower or higher metric is better. |
| `group_by` | `list[str] \| None` | `None` | Pick best per group; `None` picks one global best. |
| `create_table` | `bool` | `False` | DDL-create output tables. |
| `labels` | `Labels \| None` | `None` | Run filter labels. |
| `threshold` | `float \| None` | `None` | Optional cutoff; best is not marked if metric fails the threshold. |

### Example

```python
FindBestModel(
    input__model="detection_model",
    input__model__metrics_on__subset="pipeline_model__metrics_on_subset",
    output__attr__model__is_best="attr__detection_model__is_best",
    output__best_model="best_detection_model",
    subset_id="val",
    is_best__name="detection_model__is_best",
    primary_keys=["detection_model_id"],
    metric__name="calc__weighted_f1_score",
    func="max",
)
```

### See also

- [ML steps index](./index.md)
- [Detection train](./detection/train-yolov8.md)
- [Ops specs](../../../ops/ops-specs.md) — wire `is_best_table` on `OpsModelSpec`

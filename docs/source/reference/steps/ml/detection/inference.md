# Detection inference

Module: `datapipe_ml.tasks.detection.inference`

Runs object-detection inference using **cv-pipeliner** model specs (YOLOv5 / YOLOv8). Each step expands to incremental `BatchTransform` compute with optional GPU executor and batch sizing.

Supported model types (via `detection_model__type` column): `yolov5`, `yolov8`.

Required model table columns: `detection_model__input_size`, `detection_model__score_threshold`, `detection_model__model_path`, `detection_model__type`, `detection_model__class_names`.

---

## `Inference_DetectionModel`

Standard full-image inference.

### Inputs / outputs

| Field | Type | Description |
|---|---|---|
| `input__image` | `PipelineInput \| Sequence[PipelineInput]` | Image table(s). A sequence joins multiple inputs (for example catalog + filter table). |
| `input__detection_model` | `PipelineInput \| Sequence[PipelineInput]` | Model table(s). A sequence can merge registry + best-model pointer. |
| `output__detection_prediction` | `PipelineOutput` | Predictions with bboxes, labels, scores. |

### Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `primary_keys` | `list[str]` | *(required)* | Image entity keys. |
| `chunk_size` | `int` | `64` | Indexes per batch transform chunk. |
| `create_table` | `bool` | `False` | DDL-create output table. |
| `labels` | `Labels \| None` | `None` | Run filter labels. |
| `image__image_path__name` | `str` | `"image__image_path"` | Column holding image URI or local path. |
| `bbox_id__name` | `str \| None` | `"bbox_id"` | Per-bbox id column on output; `None` for JSON `bboxes`/`labels`. |
| `batch_size_default` | `int` | env `DETECTION_BATCH_SIZE_DEFAULT` or `64` | Inference batch size inside cv-pipeliner. |
| `executor_config` | `ExecutorConfig \| None` | `None` | GPU / Ray executor (for example `gpu_executor()`). |
| `filters` | `LabelDict \| Callable \| None` | `None` | Restrict which indexes are processed. |
| `detection_model_primary_keys` | `list[str] \| None` | `None` | Model grain; default `["detection_model_id"]`. |
| `prediction_threshold` | `float \| None` | `None` | Override model score threshold for this step. |

### Example (annotation pre-labeling)

```python
Inference_DetectionModel(
    input__image=["s3_images", "sec__image_without_ground_truth"],
    input__detection_model=["detection_model", "best_detection_model"],
    output__detection_prediction="ls_detection_prediction_raw",
    primary_keys=["image_name"],
    bbox_id__name=None,
    image__image_path__name="image_url",
    batch_size_default=1,
    executor_config=gpu_executor(),
    labels=[("stage", "annotation")],
)
```

### Example (post-train eval)

```python
Inference_DetectionModel(
    input__image=["s3_images", "image__subset"],
    input__detection_model="detection_model",
    output__detection_prediction="detection_prediction_raw",
    primary_keys=["image_name"],
    bbox_id__name=None,
    image__image_path__name="image_url",
    batch_size_default=1,
    executor_config=gpu_executor(),
    labels=[("stage", "inference")],
)
```

---

## `InferenceBySplitOnCrops_DetectionModel`

Tile-based inference: splits each image into overlapping crops, runs detection per crop, merges results.

### Additional parameters (required)

| Parameter | Type | Description |
|---|---|---|
| `hCrossing` | `int` | Horizontal overlap crossings. |
| `vCrossing` | `int` | Vertical overlap crossings. |
| `thresholdSpace` | `int` | Merge threshold in pixel space. |
| `blockWidth` | `int` | Crop width. |
| `blockHeight` | `int` | Crop height. |

Other fields match `Inference_DetectionModel`. Default `batch_size_default` follows env with fallback `4`.

---

## `Inference_UsingThresholdsPerClasss_DetectionModel`

Per-class score thresholds from a separate thresholds table.

### Extra input

| Field | Description |
|---|---|
| `input__detection_model_thresholds` | Table with per-class thresholds. |

### Extra parameter

| Parameter | Type | Default | Description |
|---|---|---|---|
| `class_name_to_threshold__name` | `str` | `"class_name_to_threshold"` | Column mapping class name → threshold. |

`prediction_threshold` is not used; thresholds come from the thresholds input.

### Alias

`Inference_UsingThresholdsPerClass_DetectionModel` — same class (typo-compatible export).

---

## Environment

| Variable | Role |
|---|---|
| `DETECTION_BATCH_SIZE_DEFAULT` | Default `batch_size_default` when not set on the step. |

### See also

- [ML steps index](../index.md)
- [Train YOLOv8 detection](./train-yolov8.md)
- [Run model inference (how-to)](../../../../how-to/model-inference.md)

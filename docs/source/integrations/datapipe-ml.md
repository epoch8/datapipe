# datapipe-ml reference

`datapipe-ml` adds CV/ML **PipelineStep** implementations on top of `datapipe-core`. Class names below are taken from the package source (discover live with `datapipe step list` on your app).

## Install extras

```bash
uv pip install -e "libs/datapipe-ml[torch,fiftyone,tensorflow]"
```

| Extra | Provides |
|---|---|
| `torch` | `torch`, `torchvision`, Ultralytics YOLO, YOLOv5 pin, huggingface-hub |
| `tensorflow` | TensorFlow + `image-classifiers` (classification train) |
| `fiftyone` | FiftyOne (dataset stores / visualization helpers) |
| `sqlite` | SQLite helpers for local meta DBs |
| `sky-vast` | SkyPilot / Vast remote training helpers |

S3 listing/download often needs `datapipe-core[s3fs]`. Ray-backed steps need the `ray` extra on the example / app env (see example READMEs).

## PipelineStep classes

### Detection (`datapipe_ml.tasks.detection`)

| Class | Role |
|---|---|
| `Train_YoloV8_DetectionModel` | Train Ultralytics YOLOv8 detection |
| `Train_YoloV5_DetectionModel` | Train YOLOv5 detection |
| `Inference_DetectionModel` | Run detection inference |
| `InferenceBySplitOnCrops_DetectionModel` | Infer on cropped tiles |
| `Inference_UsingThresholdsPerClasss_DetectionModel` | Infer with per-class thresholds |
| `CountMetrics_Subset_DetectionModel` | Metrics on a labeled subset |

### Keypoints (`datapipe_ml.tasks.keypoints`)

| Class | Role |
|---|---|
| `Train_YoloV8_KeypointsModel` | Train pose / keypoints model |
| `Inference_KeypointsModel` | Keypoints inference |
| `InferenceBySplitOnCrops_KeypointsModel` | Infer on crops |
| `CountMetrics_Subset_KeypointsModel` | Metrics on a subset |
| `CountMetrics_FrozenDataset_KeypointsModel` | Metrics on a frozen dataset |

### Segmentation (`datapipe_ml.tasks.segmentation`)

| Class | Role |
|---|---|
| `Train_YoloV8_SegmentationModel` | Train YOLOv8 segmentation |
| `Inference_SegmentationModel` | Segmentation inference |
| `InferenceBySplitOnCrops_SegmentationModel` | Infer on crops |
| `Inference_UsingThresholdsPerClasss_SegmentationModel` | Per-class threshold inference |

### Classification (`datapipe_ml.tasks.classification`)

| Class | Role |
|---|---|
| `Train_Tensorflow_ClassificationModel` | TF classification training |
| `Inference_ClassificationModel` | Classification inference |
| `CountMetrics_Subset_ClassificationModel` | Classification metrics on a subset |

### Datasets / metrics / statistics

| Class | Module area | Role |
|---|---|---|
| `FreezeDatasetStep` | `datasets.freeze` | Freeze train/val (etc.) snapshot tables for reproducible training |
| `FindBestModel` | `metrics.model_selection` | Pick best model row from metrics tables |
| `CountTotalLabelOnSubset` | `statistics.total` | Label counts on a subset |
| `CountTotalLabel` | `statistics.total` | Global label counts |

### Detection↔classification workflow (`workflows.detection_classification`)

| Class | Role |
|---|---|
| `Define_PipelineModel` | Define a combined pipeline model |
| `Inference_PipelineModel` | Run the combined model |
| `CountMetrics_Subset_PipelineModel` | Combined metrics on a subset |
| `Inference_And_FindBestThresholdsPerClasssOnSubset_DetectionModel` | Threshold search (detection) |
| `Inference_And_FindBestThresholdsPerClasssOnSubset_SegmentationModel` | Threshold search (segmentation) |

## Observability plugin (not inside datapipe-ml alone)

ML Ops panels need:

- `datapipe-app-ml-ops` — ops specs, metrics/training APIs
- `datapipe-ui-ml` — SPA plugin (preferred static entry when `[ml]` is installed)

## Living examples

Prefer runnable apps over this index when learning:

- `examples/detection_tags/README.md` — tags metrics, FiftyOne, no Label Studio
- `examples/e2e_template/README.md` — LS → freeze → train → metrics → FiftyOne

## See also

- [ML mental model](./ml-overview.md)
- [Ops](../ops/index.md)

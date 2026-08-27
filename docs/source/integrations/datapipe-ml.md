# datapipe-ml reference

`datapipe-ml` adds CV/ML **PipelineStep** implementations on top of `datapipe-core`. For parameter tables and wiring examples, use the step reference docs below rather than duplicating class lists here.

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

## Step reference (parameter docs)

| Topic | Page |
|---|---|
| Overview, task matrix, all task families | [ML steps index](../reference/steps/ml/index.md) |
| Freeze snapshots, `FindBestModel` | [Freeze dataset](../reference/steps/ml/freeze-dataset.md) |
| Detection inference (+ crop / threshold variants) | [Detection inference](../reference/steps/ml/detection/inference.md) |
| YOLOv8 detection training + config types | [Train YOLOv8 detection](../reference/steps/ml/detection/train-yolov8.md) |

Discover live step names on your app:

```bash
datapipe --pipeline app:app step list
```

### Other step classes (see source / `step list`)

| Area | Module | Notable classes |
|---|---|---|
| Detection | `datapipe_ml.tasks.detection` | `Train_YoloV5_DetectionModel`, `CountMetrics_Subset_DetectionModel` |
| Keypoints | `datapipe_ml.tasks.keypoints` | `Train_YoloV8_KeypointsModel`, `Inference_KeypointsModel`, metrics steps |
| Segmentation | `datapipe_ml.tasks.segmentation` | `Train_YoloV8_SegmentationModel`, `Inference_SegmentationModel` |
| Classification | `datapipe_ml.tasks.classification` | `Train_Tensorflow_ClassificationModel`, `Inference_ClassificationModel` |
| Workflows | `datapipe_ml.workflows.detection_classification` | `Define_PipelineModel`, `Inference_PipelineModel`, threshold search |
| Statistics | `datapipe_ml.statistics.total` | `CountTotalLabel`, `CountTotalLabelOnSubset` |

Task-specific freeze wrappers (`DetectionFreezeDataset`, …) are documented under [Freeze dataset](../reference/steps/ml/freeze-dataset.md).

## Observability plugin (not inside datapipe-ml alone)

ML Ops panels need:

- `datapipe-app-ml-ops` — ops specs, metrics/training APIs — [Ops specs](../ops/ops-specs.md)
- `datapipe-ui-ml` — SPA plugin (preferred static entry when `[ml]` is installed)

## Living examples

Prefer runnable apps over this index when learning:

- `examples/detection_tags/README.md` — tags metrics, FiftyOne, no Label Studio
- `examples/e2e_template/README.md` — LS → freeze → train → metrics → FiftyOne

## See also

- [ML mental model](./ml-overview.md)
- [Label Studio integration](./label-studio.md)
- [Ops](../ops/index.md)

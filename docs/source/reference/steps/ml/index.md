# ML pipeline steps

`datapipe-ml` adds CV/ML **PipelineStep** types on top of `datapipe-core`. Steps are declarative dataclasses; at runtime each `build_compute()` expands into one or more core steps (`BatchTransform`, `DatatableTransform`, …).

Discover live step names on your app:

```bash
datapipe --pipeline app:app step list
```

## Install extras

From the monorepo root (or your project that vendors `libs/datapipe-ml`):

```bash
uv pip install -e "libs/datapipe-ml[torch,fiftyone,tensorflow]"
```

| Extra | Provides |
|---|---|
| `torch` | `torch`, `torchvision`, Ultralytics YOLO, YOLOv5 pin, `huggingface-hub` |
| `tensorflow` | TensorFlow + `image-classifiers` (classification training) |
| `fiftyone` | FiftyOne dataset stores / visualization helpers |
| `sqlite` | SQLite helpers for local meta DBs |
| `sky-vast` | SkyPilot / Vast remote training launcher (`SkyVastTrainingLauncherConfig`) |

Additional dependencies used by examples:

| Need | Install |
|---|---|
| S3 image listing / download | `datapipe-core[s3fs]` |
| Ray-backed executors | `ray` extra on the app / example env |
| Label Studio loop | `datapipe-label-studio` (separate package) |
| Ops ML panels | `datapipe-app-ml-ops`, `datapipe-ui-ml` |

## Task matrix

| Task | Freeze | Train | Inference | Metrics | Model selection |
|---|---|---|---|---|---|
| Detection | [`DetectionFreezeDataset`](./freeze-dataset.md#detectionfreezedataset) (wraps `FreezeDatasetStep`) | [`Train_YoloV8_DetectionModel`](./detection/train-yolov8.md), `Train_YoloV5_DetectionModel` | [`Inference_DetectionModel`](./detection/inference.md) (+ crop / threshold variants) | `CountMetrics_Subset_DetectionModel`, `CountMetrics_Subset_PipelineModel` | [`FindBestModel`](./freeze-dataset.md#findbestmodel) |
| Segmentation | `SegmentationFreezeDataset` | `Train_YoloV8_SegmentationModel` | `Inference_SegmentationModel`, crop / threshold variants | (subset metrics in package) | `FindBestModel` |
| Keypoints | `KeypointsFreezeDataset` | `Train_YoloV8_KeypointsModel` | `Inference_KeypointsModel`, crop variant | `CountMetrics_Subset_KeypointsModel`, `CountMetrics_FrozenDataset_KeypointsModel` | `FindBestModel` |
| Classification | `ClassificationFreezeDataset` | `Train_Tensorflow_ClassificationModel` | `Inference_ClassificationModel` | `CountMetrics_Subset_ClassificationModel` | `FindBestModel` |

Workflow helpers under `datapipe_ml.workflows.detection_classification` combine detection with downstream classification (`Define_PipelineModel`, `Inference_PipelineModel`, threshold search steps).

Statistics helpers: `CountTotalLabel`, `CountTotalLabelOnSubset` (`datapipe_ml.statistics.total`).

## Documented step groups

| Page | Contents |
|---|---|
| [Freeze dataset](./freeze-dataset.md) | `FreezeDatasetStep`, task-specific freeze wrappers, `FindBestModel` |
| [Detection inference](./detection/inference.md) | `Inference_DetectionModel` and variants |
| [Detection train (YOLOv8)](./detection/train-yolov8.md) | `Train_YoloV8_DetectionModel`, `YoloV8_TrainingConfig`, training DTOs |

Other task families (segmentation, keypoints, classification, YOLOv5) follow the same patterns; see module paths in the integration index.

## Wiring example

End-to-end detection pipeline (Label Studio → freeze → train → infer → metrics → best model):

- `examples/e2e_template/image_detection/app.py`
- `examples/detection_tags/detection/app.py` (tags metrics, no Label Studio)

Both register Ops specs via `app.add_specs([DatapipeOpsSpec(...)])` — see [Ops specs](../../ops/ops-specs.md).

## See also

- [datapipe-ml integration index](../../../integrations/datapipe-ml.md)
- [ML mental model](../../../integrations/ml-overview.md)
- [Label Studio steps](../../../integrations/label-studio.md)
- [Ops overview](../../../ops/index.md)

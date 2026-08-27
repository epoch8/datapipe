# Train YOLOv8 detection

Module: `datapipe_ml.tasks.detection.train.yolov8`

End-to-end YOLOv8 detection training on a frozen dataset: data prep (resize, YOLO label files), config registry, training requests, model artifacts, and training status rows.

Config types live in `datapipe_ml.frameworks.yolo.yolov8.runner` and `datapipe_ml.training.specs`.

---

## `Train_YoloV8_DetectionModel`

### Inputs

| Field | Description |
|---|---|
| `input__detection_frozen_dataset` | Frozen snapshot metadata table. |
| `input__detection_frozen_dataset__has__image_gt` | Per-image GT rows for the snapshot. |

### Outputs

| Field | Description |
|---|---|
| `output__yolov8_train_config` | Built-in training config presets (pipeline-owned). |
| `output__yolov8_custom_train_config` | User-defined experiments (Ops API writes here). |
| `output__detection_training_request` | Training request queue for Ops / auto-policy. |
| `output__model_detection_size_for_resize` | Model-side resize target sizes. |
| `output__detection_size_for_resize` | Dataset resize dimensions per snapshot. |
| `output__detection_frozen_dataset__class_names` | Class names JSON on the snapshot. |
| `output__detection_frozen_dataset__resized_image_file` | Resized image filedir table. |
| `output__detection_frozen_dataset__yolo_txt` | YOLO label file table. |
| `output__detection_model` | Trained model registry rows. |
| `output__detection_model_is_trained_on_detection_frozen_dataset` | Model ↔ snapshot link table. |
| `output__training_status` | Run status / manifest / timing. |

### Step parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `working_dir` | `str` | *(required)* | Artifact root (`models/`, frozen data mirrors). |
| `yolov8_train_configs` | `list[YoloV8TrainConfigItem]` | *(required)* | Built-in presets (`YoloV8_TrainingConfig` or `TrainingConfigPreset`). |
| `primary_keys` | `list[str]` | *(required)* | Image keys for data prep transforms. |
| `max_within_time` | `str` | `"1w"` | Skip re-training same snapshot+config inside this window. |
| `bbox_id__name` | `str \| None` | `None` | Flat bbox id column; `None` uses JSON bboxes. |
| `image__image_path__name` | `str` | `"image__image_path"` | Image path column on frozen GT rows. |
| `separator_to_split_attrnames` | `str` | `"__"` | Namespace separator for generated column names. |
| `create_table` | `bool` | `False` | DDL-create output tables. |
| `labels` | `Labels \| None` | `None` | Run filter labels. |
| `executor_config` | `ExecutorConfig \| None` | `None` | Executor for training orchestration steps. |
| `prepare_data_executor_config` | `ExecutorConfig \| None` | `None` | Executor for resize / YOLO txt preparation. |
| `resize_images` | `bool` | `True` | When false, skip resize pipeline (use native sizes). |
| `detection_model_primary_keys` | `list[str] \| None` | `None` | Extra model PK columns beyond `detection_model_id`. |
| `detection_model_id__name` | `str` | `"detection_model_id"` | Model id column. |
| `detection_frozen_dataset_id__name` | `str` | `"detection_frozen_dataset_id"` | Snapshot id column. |
| `tmp_folder` | `str` | `default_tmp_folder()` | Local scratch for cloud-backed images. |
| `allow_sample_size_mismatch` | `bool` | `False` | Allow train/val counts to differ from YOLO expectations. |
| `model_suffix` | `str` | `"_default"` | Suffix embedded in generated model ids. |
| `training_launcher_config` | `TrainingLauncherConfig \| None` | `None` | Local (default) or `SkyVastTrainingLauncherConfig`. |
| `sync_config` | `TrainingSyncConfig \| None` | `None` | Periodic artifact sync during remote training. |
| `resume_config` | `TrainingResumeConfig \| None` | `None` | Failed-run resume / checkpoint policy. |
| `filedir_fsspec_kwargs` | `dict \| None` | `None` | Extra kwargs for filedir cloud IO. |

### Example

From `examples/e2e_template/image_detection/app.py`:

```python
Train_YoloV8_DetectionModel(
    input__detection_frozen_dataset="detection_frozen_dataset",
    input__detection_frozen_dataset__has__image_gt="detection_frozen_dataset__has__image_gt",
    output__yolov8_train_config="yolov8_train_config",
    output__yolov8_custom_train_config="yolov8_custom_train_config",
    output__detection_training_request="detection_training_request",
    output__model_detection_size_for_resize="model_detection_size_for_resize",
    output__detection_size_for_resize="detection_size_for_resize",
    output__detection_frozen_dataset__resized_image_file="detection_frozen_dataset__resized_image_file",
    output__detection_frozen_dataset__yolo_txt="detection_frozen_dataset__yolo_txt",
    output__detection_model="detection_model",
    output__detection_model_is_trained_on_detection_frozen_dataset=(
        "detection_model_is_trained_on_detection_frozen_dataset"
    ),
    output__training_status="detection_training_status",
    output__detection_frozen_dataset__class_names="detection_frozen_dataset__class_names",
    max_within_time="1w",
    working_dir=str(DATAPIPE_DIR),
    tmp_folder=datapipe_tmp_folder(),
    yolov8_train_configs=[
        TrainingConfigPreset(
            name="Standard YOLOv8s 640",
            description="Default YOLOv8s detection preset",
            config=YoloV8_TrainingConfig(
                model="yolov8s.pt",
                imgsz=640,
                batch=10,
                epochs=30,
                exist_ok=True,
            ),
        )
    ],
    sync_config=TrainingSyncConfig(enabled=True, interval_s=30, retries=3, retry_sleep_s=30),
    resume_config=TrainingResumeConfig(
        continue_train_failed_models=True,
        min_completed_epochs=1,
        checkpoint="last",
        max_attempts=10,
        reset_attempts_after="10m",
        lease_ttl_s=60,
        heartbeat_interval_s=10,
    ),
    primary_keys=["image_name"],
    bbox_id__name=None,
    allow_sample_size_mismatch=True,
    model_suffix="_e2e",
    prepare_data_executor_config=parallel_io_executor(),
    labels=[("stage", "train"), ("stage", "train-yolo")],
)
```

---

## Config types overview

### `YoloV8TrainConfigItem`

Type alias: `YoloV8_TrainingConfig | TrainingConfigPreset`.

Pass either a bare config dataclass or a named preset wrapper.

### `TrainingConfigPreset`

Module: `datapipe_ml.training.train_config_id`

| Field | Type | Description |
|---|---|---|
| `name` | `str` | Display name in Ops / config registry. |
| `config` | `Any` | Dataclass instance (typically `YoloV8_TrainingConfig`). |
| `description` | `str \| None` | Optional longer description. |

### `YoloV8_TrainingConfig`

Module: `datapipe_ml.frameworks.yolo.yolov8.runner`

Dataclass passed to Ultralytics `model.train()`. Subclass `YoloV8DetectionTrainingConfig` defaults `model="yolov8n.pt"`.

#### datapipe-ml fields

| Field | Default | Description |
|---|---|---|
| `tmp_folder` | `default_tmp_folder()` | Local cache when images are remote. |
| `initial_weights_path` | `None` | Override starting weights path. |
| `persisted_project_dir` | `None` | Fixed Ultralytics project directory. |

#### Common Ultralytics fields

| Field | Default | Description |
|---|---|---|
| `model` | `"yolov8n.pt"` | Base checkpoint or architecture. |
| `data` | `"coco128.yaml"` | Dataset YAML path or `YoloDataYAMLConfig` (set by train step). |
| `epochs` | `300` | Training epochs. |
| `time` | `None` | Max training time (hours). |
| `patience` | `100` | EarlyStopping patience. |
| `batch` | `16` | Batch size. |
| `imgsz` | `640` | Train / val image size. |
| `save` | `True` | Save checkpoints. |
| `save_period` | `-1` | Checkpoint every N epochs (`<1` disables). |
| `cache` | `False` | Image cache mode (`True`/`"ram"`, `"disk"`, `False`). |
| `device` | `None` | GPU id(s) or `"cpu"`. |
| `workers` | `8` | Dataloader workers. |
| `project` | `default_train_project_dir()` | Ultralytics project root. |
| `name` | `"exp"` | Run subdirectory name. |
| `exist_ok` | `False` | Overwrite existing run dir. |
| `pretrained` | `True` | Use pretrained weights. |
| `optimizer` | `"auto"` | Optimizer name. |
| `verbose` | `False` | Verbose logging. |
| `seed` | `0` | Random seed. |
| `deterministic` | `True` | Deterministic mode. |
| `single_cls` | `False` | Single-class training flag. |
| `rect` | `False` | Rectangular training. |
| `cos_lr` | `False` | Cosine LR schedule. |
| `close_mosaic` | `10` | Disable mosaic last N epochs. |
| `resume` | `False` | Resume from checkpoint. |
| `amp` | `True` | Automatic mixed precision. |
| `fraction` | `1.0` | Dataset fraction to use. |
| `val` | `True` | Run validation. |
| `plots` | `True` | Save metric plots. |
| `lr0` | `0.01` | Initial learning rate. |
| `lrf` | `0.01` | Final LR fraction. |
| `momentum` | `0.937` | SGD momentum / Adam beta1. |
| `weight_decay` | `0.0005` | Weight decay. |
| `warmup_epochs` | `3.0` | LR warmup epochs. |
| `box` | `7.5` | Box loss gain. |
| `cls` | `0.5` | Class loss gain. |
| `dfl` | `1.5` | DFL loss gain. |
| `dropout` | `0.0` | Dropout (classification tasks). |
| `freeze` | `None` | Freeze first N layers. |

#### Augmentation fields (defaults disable augmentations)

`multi_scale`, `degrees`, `translate`, `scale`, `shear`, `perspective`, `fliplr`, `flipud`, `mosaic`, `mixup`, `copy_paste`, `copy_paste_mode`, `hsv_h`, `hsv_s`, `hsv_v`, `bgr`, `cutmix`, `erasing`, `auto_augment` — see Ultralytics train args docs for ranges.

Use `to_yolo_kwargs()` to serialize only Ultralytics-recognized keys.

### `TrainingSyncConfig`

| Field | Default | Description |
|---|---|---|
| `enabled` | `False` | Enable periodic artifact sync. |
| `interval_s` | `600` | Sync interval in seconds. |
| `retries` | `3` | Retries per sync attempt. |
| `retry_sleep_s` | `30` | Sleep between retries. |
| `max_consecutive_sync_failures` | `10` | Abort after this many consecutive failures. |

### `TrainingResumeConfig`

| Field | Default | Description |
|---|---|---|
| `continue_train_failed_models` | `False` | Retry failed training runs. |
| `min_completed_epochs` | `1` | Minimum epochs before treating a run as resumable. |
| `checkpoint` | `"last"` | `"last"` or `"best"` checkpoint selection. |
| `max_attempts` | `3` | Max resume attempts per model. |
| `reset_attempts_after` | `"1d"` | Reset attempt counter after this duration. |
| `lease_ttl_s` | `600` | Training lease TTL for distributed locking. |
| `heartbeat_interval_s` | `60` | Heartbeat interval during training. |

### `TrainingLauncherConfig`

`LocalTrainingLauncher()` (default when `None`) or `SkyVastTrainingLauncherConfig` for remote GPU (`datapipe-ml[sky-vast]`).

### See also

- [Freeze dataset](../freeze-dataset.md)
- [Detection inference](./inference.md)
- [Ops specs](../../../ops/ops-specs.md) — `OpsTrainingSpec`, `OpsTrainConfigRegistrySpec`
- [Ultralytics train args](https://docs.ultralytics.com/modes/train/)

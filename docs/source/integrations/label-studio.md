# Label Studio integration

Package: `datapipe-label-studio`. Pipeline steps that keep Label Studio projects, tasks, annotations, and predictions in sync with Datapipe tables.

## When to use

- Human-in-the-loop labeling inside an incremental ETL / ML graph
- Re-upload only tasks (or predictions) for dirty upstream keys

---

## `CreateLabelStudioProjects`

Module: `datapipe_label_studio.create_projects_step`

Ensures Label Studio projects exist from a settings table, and writes `(project_identifier, project_id)` to an output table.

### Inputs / outputs

| Field | Type | Description |
|---|---|---|
| `input__label_studio_project_setting` | `PipelineInput` | Rows with project creation settings (see required columns). |
| `output__label_studio_project` | `PipelineOutput` | Project registry: `project_identifier`, `project_id`. |

### Required input columns

| Column | Description |
|---|---|
| `project_identifier` | Project title (≤50 chars) or numeric LS project id. |
| `project_label_config_at_create` | Label Studio labeling config XML. |
| `project_description_at_create` | Project description passed to LS on create. |

### Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ls_url` | `str` | *(required)* | Label Studio base URL. |
| `api_key` | `str` | *(required)* | Access token. |
| `storages` | `list[GCSBucket \| S3Bucket] \| None` | `None` | Cloud storages attached to each created project. |
| `create_table` | `bool` | `False` | DDL-create the output table. |
| `labels` | `Labels \| None` | `None` | Run filter labels. |
| `executor_config` | `ExecutorConfig \| None` | `None` | Executor for the underlying `BatchTransform`. |

---

## `LabelStudioUploadTasks`

Module: `datapipe_label_studio.upload_tasks_pipeline`

Uploads tasks for a **single** project identifier, then syncs annotations back.

### Inputs / outputs

| Field | Type | Description |
|---|---|---|
| `input__item` | `PipelineInput` | Rows to upload; must contain `primary_keys` + `columns`. |
| `output__label_studio_project_task` | `PipelineOutput` | Task mapping: primary keys + `task_id`. |
| `output__label_studio_project_annotation` | `PipelineOutput` | Pulled annotations JSON per primary keys. |
| `output__label_studio_sync_table` | `PipelineOutput` | Sync watermark: `project_id`, `last_updated_at`. |

### Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ls_url` | `str` | *(required)* | Label Studio base URL. |
| `api_key` | `str` | *(required)* | Access token. |
| `project_identifier` | `str \| int` | *(required)* | Project title (≤50 chars) or numeric id. |
| `primary_keys` | `list[str]` | *(required)* | Join keys; must not include `task_id` or `annotations`. |
| `columns` | `list[str]` | *(required)* | Payload columns sent into LS task `data`. |
| `chunk_size` | `int` | `100` | Upload batch size. |
| `project_label_config_at_create` | `str` | `""` | Label config used if the project must be created. |
| `project_description_at_create` | `str` | `""` | Description used if the project must be created. |
| `storages` | `list[GCSBucket \| S3Bucket] \| None` | `None` | Cloud storages attached on project create. |
| `create_table` | `bool` | `False` | DDL-create output tables. |
| `delete_unannotated_tasks_only_on_update` | `bool` | `False` | Safer delete behaviour when upstream tasks change. |
| `labels` | `Labels \| None` | `None` | Run filter labels. |
| `executor_config` | `ExecutorConfig \| None` | `None` | Executor for upload transform. |

Internally builds a `BatchTransform` (upload) plus a `DatatableTransform` (annotation pull).

### Example

From `examples/e2e_template/image_detection/app.py`:

```python
LabelStudioUploadTasks(
    input__item="s3_images",
    output__label_studio_project_task="ls_task",
    output__label_studio_project_annotation="ls_annotations",
    output__label_studio_sync_table="ls_sync",
    ls_url=LABEL_STUDIO_URL,
    api_key=LABEL_STUDIO_API_KEY,
    project_identifier=PROJECT_NAME,
    project_label_config_at_create=LABEL_CONFIG,
    primary_keys=["image_name"],
    columns=["image_url"],
    storages=label_studio_storages(),
    chunk_size=100,
    labels=[("stage", "annotation")],
)
```

---

## `LabelStudioUploadPredictions`

Module: `datapipe_label_studio.upload_predictions_pipeline`

Pushes model predictions into an existing project's tasks.

### Inputs / outputs

| Field | Type | Description |
|---|---|---|
| `input__item__has__prediction` | `PipelineInput` | Rows with primary keys + `prediction` JSON column. |
| `input__label_studio_project_task` | `PipelineInput` | Task mapping from upload step (`task_id` join). |
| `input__best_model` | `PipelineInput` | Best-model row(s); supplies `model_keys` for version id. |
| `output__label_studio_project_prediction` | `PipelineOutput` | Stored prediction ids + JSON per row. |
| `output__label_studio_current_model_version` | `PipelineOutput` | Current model version watermark for the project. |

### Required input columns

| Table | Columns |
|---|---|
| `input__item__has__prediction` | `primary_keys` + `prediction` (`{"result": [...], "score": ...}`) |
| `input__label_studio_project_task` | task join keys + `task_id` |
| `input__best_model` | `model_keys` columns |

### Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ls_url` | `str` | *(required)* | Label Studio base URL. |
| `api_key` | `str` | *(required)* | Access token. |
| `project_identifier` | `str \| int` | *(required)* | Project title (≤50 chars) or numeric id. |
| `primary_keys` | `list[str]` | *(required)* | Entity keys; validated against `model_keys`. |
| `chunk_size` | `int` | `100` | Upload batch size. |
| `create_table` | `bool` | `False` | DDL-create output tables. |
| `labels` | `Labels \| None` | `None` | Run filter labels. |
| `model_keys` | `list[str]` | `["model_version"]` | Columns identifying model version (e2e uses `["detection_model_id"]`). |
| `executor_config` | `ExecutorConfig \| None` | `None` | Executor for upload transform. |

### Example

```python
LabelStudioUploadPredictions(
    input__item__has__prediction="images_with_predictions",
    input__label_studio_project_task="ls_task",
    input__best_model="best_detection_model",
    output__label_studio_project_prediction="ls_predictions",
    output__label_studio_current_model_version="ls_current_model_version",
    ls_url=LABEL_STUDIO_URL,
    api_key=LABEL_STUDIO_API_KEY,
    project_identifier=PROJECT_NAME,
    primary_keys=["image_name", "detection_model_id"],
    model_keys=["detection_model_id"],
    labels=[("stage", "annotation")],
)
```

---

## Multi-project variants

| Class | Module | Role |
|---|---|---|
| `LabelStudioUploadTasksToProjects` | `upload_tasks_pipeline` | Joins `input__label_studio_project`; requires `project_identifier` in `primary_keys`. |
| `LabelStudioUploadPredictionsToProjects` | `upload_predictions_pipeline` | Multi-project predictions variant. |

Parse annotations into domain tables with your own `BatchTransform` / generate steps — see `examples/e2e_template/*/steps.py`.

## Environment (e2e_template pattern)

From `examples/e2e_template/.env.example`:

| Variable | Role |
|---|---|
| `LABEL_STUDIO_URL` | Base URL (e.g. `http://localhost:8080`) |
| `LABEL_STUDIO_API_KEY` | Access token |
| `LABEL_STUDIO_EMAIL` / `LABEL_STUDIO_PASSWORD` | Used by `scripts/label_studio_token.py` to fetch a token |
| `LABEL_STUDIO_S3_ENDPOINT_URL` | S3 endpoint reachable from the Label Studio container (MinIO in compose) |

Pass URL + key into steps as `ls_url=` / `api_key=` (templates read them from `config.py`).

## Incremental behaviour

Upload steps are normal `PipelineStep`s. Dirty upstream items (new or changed keys in `input__item`) re-enter the upload transform; unchanged keys are skipped by Datapipe meta. Annotation sync uses a dedicated transform that can poll LS for updates independently of the upload dirty set.

## See also

- [ML mental model](./ml-overview.md)
- [ML steps reference](../reference/steps/ml/index.md)
- Package README: `libs/datapipe-label-studio/README.md`

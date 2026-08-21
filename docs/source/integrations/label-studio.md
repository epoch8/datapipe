# Label Studio integration

Package: `datapipe-label-studio`. Pipeline steps that keep Label Studio projects, tasks, annotations, and predictions in sync with Datapipe tables.

## When to use

- Human-in-the-loop labeling inside an incremental ETL / ML graph
- Re-upload only tasks (or predictions) for dirty upstream keys

## Main steps

### `CreateLabelStudioProjects`

Module: `datapipe_label_studio.create_projects_step`

Ensures Label Studio projects exist from a settings table, and writes `(project_identifier, project_id)` to an output table.

| Field | Role |
|---|---|
| `input__label_studio_project_setting` | Input with `project_identifier`, `project_label_config_at_create`, `project_description_at_create` |
| `output__label_studio_project` | Output project registry (`project_identifier`, `project_id`) |
| `ls_url` | Label Studio base URL |
| `api_key` | Access token |
| `storages` | Optional list of `GCSBucket` / `S3Bucket` cloud storages to attach |
| `create_table` | Whether to DDL-create the output table (default `False`) |
| `labels` / `executor_config` | Standard step labels and executor config |

### `LabelStudioUploadTasks`

Module: `datapipe_label_studio.upload_tasks_pipeline`

Uploads tasks for a **single** project identifier, then syncs annotations back.

| Field | Role |
|---|---|
| `input__item` | Rows to upload (must contain `primary_keys` + `columns`) |
| `output__label_studio_project_task` | Task mapping (`… primary keys + task_id`) |
| `output__label_studio_project_annotation` | Pulled annotations (`… primary keys + annotations` JSON) |
| `output__label_studio_sync_table` | Sync watermark (`project_id`, `last_updated_at`) |
| `ls_url` / `api_key` | Connection |
| `project_identifier` | Project title (≤50 chars) or numeric id |
| `primary_keys` | Join keys; reserved names `task_id`, `annotations` are forbidden |
| `columns` | Payload columns sent into the LS task data |
| `chunk_size` | Batch size (default `100`) |
| `project_label_config_at_create` / `project_description_at_create` | Used if the project must be created |
| `storages` | Optional cloud storages |
| `delete_unannotated_tasks_only_on_update` | Safer delete behavior on updates |
| `create_table` / `labels` / `executor_config` | As usual |

Internally builds a `BatchTransform` (upload) plus a `DatatableTransform` (annotation pull).

### Multi-project and prediction variants

| Class | Module | Role |
|---|---|---|
| `LabelStudioUploadTasksToProjects` | `upload_tasks_pipeline` | Same as upload-tasks, but joins `input__label_studio_project` and requires `project_identifier` in `primary_keys` |
| `LabelStudioUploadPredictions` | `upload_predictions_pipeline` | Pushes model predictions into an existing project’s tasks (`input__item__has__prediction` with a `prediction` column, plus task + best-model inputs) |
| `LabelStudioUploadPredictionsToProjects` | `upload_predictions_pipeline` | Multi-project predictions variant |

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

## Minimal usage sketch

```python
from datapipe.compute import Catalog, Pipeline, Table
from datapipe_label_studio.upload_tasks_pipeline import LabelStudioUploadTasks

pipeline = Pipeline(
    [
        LabelStudioUploadTasks(
            input__item="images",
            output__label_studio_project_task="ls_tasks",
            output__label_studio_project_annotation="ls_annotations",
            output__label_studio_sync_table="ls_sync",
            ls_url=LABEL_STUDIO_URL,
            api_key=LABEL_STUDIO_API_KEY,
            project_identifier="my-project",
            primary_keys=["image_id"],
            columns=["image_url"],
            project_label_config_at_create="<View>...</View>",
        ),
        # … parse annotations → train / infer …
    ]
)
```

Full loop (detection / keypoints / classification): `examples/e2e_template/README.md`.

## Incremental behaviour

Upload steps are normal `PipelineStep`s. Dirty upstream items (new or changed keys in `input__item`) re-enter the upload transform; unchanged keys are skipped by Datapipe meta. Annotation sync uses a dedicated transform that can poll LS for updates independently of the upload dirty set.

## See also

- [ML mental model](./ml-overview.md)
- Package README: `libs/datapipe-label-studio/README.md`

# CVAT integration

Package: `datapipe-cvat`. Drive CVAT projects from Datapipe tables: batch files into tasks, upload media, sync annotations back.

## When to use

- Teams standardized on CVAT (instead of or in addition to Label Studio)
- SAM / pre-annotation loops — see `examples/sam_cvat/`

## `CVATStep`

Module: `datapipe_cvat.cvat_step`

| Field | Role |
|---|---|
| `input` | Input table with media rows |
| `output__input_batches` | Batch assignment (`inner_task_id`, …) |
| `output__cvat_task` | CVAT task registry linked to Datapipe |
| `output__cvat_files` | Per-file CVAT mapping |
| `output__cvat_annotation` | Synced annotations |
| `task_sync_table` | Name of the table used to sync CVAT tasks with Datapipe |

### Connection

| Field | Role |
|---|---|
| `cvat_url` | CVAT base URL |
| `cvat_organization` | Organization slug |
| `cvat_credentials` | `(username, password)` tuple |
| `cvat_project_id` | Target CVAT project id (`int`) |

### Behaviour

| Field | Default | Role |
|---|---|---|
| `primary_keys` | (required) | Row identity; must include `task_queue_id__name` |
| `file_path_column` | (required) | Column with local/cloud path to upload |
| `cloud_storage_bucket` | `None` | Optional cloud storage bucket name |
| `delete_unannotated_tasks_only_on_update` | `False` | Safer deletes on update |
| `file_type` | `"image"` | `"image"` or `"video"` |
| `files_batch` | `100` | Max files per batch (int or per-queue `dict`) |
| `minimum_files_in_job` | `50` | Min files before opening a new batch (int or `dict`); must be ≤ `files_batch` |
| `task_queue_id__name` | `"task_queue_id"` | Column that scopes batching per queue |
| `task_name_format` | `"[{date:%Y-%m-%d}] {task_queue_id} batch={inner_task_id}"` | Must include `{task_queue_id__name}` and `{inner_task_id}` placeholders |
| `sampling_order` | `"default"` | `"default"` or `"random"` |
| `sampling_random_seed` | `None` | Seed when sampling randomly |
| `max_attempts` | `5` | Retry budget for CVAT ops |
| `attempt_poll_s` | `60` | Seconds between poll attempts |
| `create_table` | `False` | DDL-create output tables |
| `labels` | `None` | Step labels |

`image__path` must not appear in `primary_keys` (implicitly unique). `cvat__file_path` is reserved.

## Examples

| Example | Focus |
|---|---|
| `examples/datapipe_cvat/simple_project/` | Minimal local images → CVAT → annotation sync |
| `examples/sam_cvat/` | SAM-assisted annotation loop |

Follow each example README for env vars, CVAT URL/credentials, and `datapipe` commands. The simple project README documents starting CVAT via `libs/datapipe-cvat/tests/start-cvat.sh` (version-pinned to match `cvat-sdk`).

## Incremental note

Treat CVAT sync like any other step: upstream image/table changes dirty keys; unchanged media should not force a full re-upload if hashes / `update_ts` are stable. Batching (`files_batch` / `minimum_files_in_job`) only assigns **new** files into open or new jobs.

## See also

- [ML mental model](./ml-overview.md)
- Package README: `libs/datapipe-cvat/README.md`

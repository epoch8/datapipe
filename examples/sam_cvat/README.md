# SAM3 + CVAT Pipeline

**Claude Code skill:** `/setup-sam-cvat` — auto-loads when relevant, or invoke it directly.

Datapipe example for:

- loading images (local folder or Hugging Face dataset fallback)
- SAM3 text-prompt inference (boxes + mask polygons)
- uploading images and preannotations to CVAT via `CVATStep`
- syncing edited annotations back into datapipe tables

## Hugging Face auth (required for SAM3)

SAM3 weights are a **gated model** on Hugging Face. Before running inference:

1. Accept the model license on the [SAM3 model page](https://huggingface.co/facebook/sam3) (or the checkpoint your `sam3` install pulls).
2. Create a Hugging Face access token with read access to gated repos.
3. Set `HF_TOKEN` in `.env`.

`sam_inference` calls `ensure_hf_login()` before loading SAM3 weights. Without a valid token and accepted terms, model download/load will fail.

`SAM_TEXT_PROMPT` is written to the `sam_config` table on each run. Changing it in `.env` updates that table and retriggers SAM inference via datapipe transform cache.

**CVAT does not pick up new preannotations for existing tasks** — `CVATStep` only uploads when images are added, removed, or paths change, not when `annotations` XML changes. After a prompt change, inference reruns and the `image` table updates locally, but existing CVAT tasks stay as they were.

Treat `SAM_TEXT_PROMPT` as **fixed for a CVAT annotation run**: tune the prompt in a notebook or script first, then set one stable value here. To force new CVAT tasks after a prompt change, delete the old tasks and wipe datapipe CVAT tables (`cvat_task`, `cvat_files`, `image_batches`, `cvat_task_sync_table`, etc.) before re-running.

## Data sources

### Local mode (default when folder has images)

Set `INPUT_DIR` with supported images inside (`.jpg`, `.jpeg`, `.png`).

File stem becomes `image_id`.

### Hugging Face fallback

Used when `INPUT_DIR` is **unset**, missing, or **empty**.

Loads `HF_DATASET_NAME` (default `HZMD/cats-n-dogs`), filters by `HF_DATASET_LABEL` on split `HF_DATASET_SPLIT`.

Images are materialized into `HF_DATASET_CACHE_FOLDER` (default `.hf_dataset_cache`) with stable basenames (`hf_000000.jpg`, …). Required because HF decodes images into temporary files with random paths; CVAT upload expects consistent filenames.

Env vars: `HF_DATASET_NAME`, `HF_DATASET_SPLIT`, `HF_DATASET_LABEL`, `HF_DATASET_CACHE_FOLDER`.

(`HF_TOKEN` is required for SAM3 — see above — not for the dataset itself.)

## Prerequisites

- **GPU** with more than **8 GB VRAM** for SAM3 inference (`DEVICE` defaults to `cuda:0` when CUDA is available).
- **CVAT** deployed and reachable at `CVAT_URL` (see [datapipe_cvat simple_project](../datapipe_cvat/simple_project/README.md) for local Docker setup).
- **CVAT project** created; set `CVAT_PROJECT_ID` in `.env`.
- Project must include labels whose names match `CVAT_BOX_LABEL` and `CVAT_POLYGON_LABEL` from `.env` (defaults: `cat_box`, `cat_mask`). SAM preannotations use these label names in exported XML.
- `CVAT_USERNAME`, `CVAT_PASSWORD`, and optionally `CVAT_ORGANIZATION` set in `.env`.

## Run

1. Copy env file: `cp .env.example .env`
2. Edit `.env` (`DB_URL`, `HF_TOKEN`, `CVAT_*`, `SAM_TEXT_PROMPT`, etc.)
3. Dependencies live in pyproject.toml, install them as follows: `uv sync`
4. Run pipeline from this folder — `app.py` calls `load_dotenv()` before config import:

```bash
datapipe db create-all
datapipe run
```

Run by stage: `datapipe step --labels stage=sam run`, `datapipe step --labels stage=cvat run`.

## Annotate in CVAT

Open CVAT UI, find tasks named like `[YYYY-MM-DD] TaskQueue:queue1 batch:N`, review or fix SAM preannotations, save, mark task as completed. Re-run the pipeline to pull updated annotations into table `image__annotations`.

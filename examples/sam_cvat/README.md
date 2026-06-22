# SAM3 + CVAT Pipeline

Datapipe example for:

- loading images (local folder or Hugging Face dataset fallback)
- SAM3 text-prompt inference (boxes + mask polygons)
- uploading images and preannotations to CVAT via `CVATStep`
- syncing edited annotations back into datapipe tables

## Hugging Face auth (required for SAM3)

SAM3 weights are a **gated model** on Hugging Face. Before running inference:

1. Accept the model license on the [SAM3 model page](https://huggingface.co/facebook/sam3) (or the checkpoint your `sam3` install pulls).
2. Create a Hugging Face access token with read access.
3. Set `HF_TOKEN` in `.env`.

`sam_inference` calls `ensure_hf_login()` before loading SAM3 weights. Without a valid token and accepted terms, model download/load will fail.

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

## Run

1. Copy env file: `cp .env.example .env`
2. Edit `.env` (`DB_URL`, `HF_TOKEN`, `CVAT_PROJECT_ID`, `SAM_TEXT_PROMPT`, etc.)
3. Install deps: `uv sync`
4. Open CVAT and create a project with labels matching `CVAT_BOX_LABEL` / `CVAT_POLYGON_LABEL`
5. Run pipeline from this folder — `app.py` calls `load_dotenv()` before config import:

```bash
datapipe db create-all
datapipe run
```

Run by stage: `datapipe step --labels stage=sam run`, `datapipe step --labels stage=cvat run`.

## Annotate in CVAT

Open CVAT UI, find tasks named like `[YYYY-MM-DD] TaskQueue:queue1 batch:N`, review or fix SAM preannotations, save, mark task as completed. Re-run the pipeline to pull updated annotations into table `image__annotations`.

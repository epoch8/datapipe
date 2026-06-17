# Embedder + FiftyOne Pipeline

Datapipe example for:
- loading images + class labels (local folder or FiftyOne zoo fallback)
- running multiple embedders
- uploading samples to FiftyOne via `FiftyOneImagesDataTableStore`
- computing `compute_visualization` and `compute_similarity` per embedder
- visualizing embedding clusters and similarity search results in FiftyOne UI

## Data sources

### Local mode (default when folder has images)

Set `LOCAL_IMAGES_DIR` with supported images inside (`.jpg`, `.jpeg`, `.png`).

Optional labels JSON (`LABELS_JSON`):

```json
{
  "cat_001": "cat",
  "dog_042": "dog"
}
```

Keys must match `image_name` (file stem). Missing keys → unlabeled sample.

### Zoo fallback

Used when `LOCAL_IMAGES_DIR` is **unset**, missing, or **empty**.

Loads `ZOO_DATASET` (default `caltech101`) via FiftyOne zoo, keeps last `ZOO_NUM_CLASSES` classes from `ZOO_LABEL_FIELD` (default `ground_truth`), emits paths + labels into datapipe tables.

First run downloads dataset (network + disk).

Env vars: `ZOO_DATASET`, `ZOO_LABEL_FIELD`, `ZOO_NUM_CLASSES`.

## Run

1. Copy env file: `cp .env.example .env`
2. Edit `.env` (paths, `DB_URL`, `FIFTYONE_DATASET_NAME`, etc.)
3. Install deps: `uv sync`
4. Run pipeline from this folder — `app.py` calls `load_dotenv()` before config import.

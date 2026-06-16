# Embedder + FiftyOne Pipeline

Datapipe example for:
- loading local images
- loading optional image-level class labels from JSON
- running multiple embedders
- uploading samples to FiftyOne via `FiftyOneImagesDataTableStore`
- computing `compute_visualization` and `compute_similarity` per embedder
- visualizing embedding clusters and similarity search results in FiftyOne UI

## Data format

Images:
- place files under `LOCAL_IMAGES_DIR`
- supported suffixes: `.jpg`, `.jpeg`, `.png`
- `image_name` uses file stem

Optional labels JSON (`LABELS_JSON`):

```json
{
  "cat_001": "cat",
  "dog_042": "dog"
}
```

Keys must match `image_name` (file stem). Missing keys -> unlabeled sample.

## Run

1. Copy env file:
   - `cp .env.example .env`
2. Edit `.env` (paths, `DB_URL`, `FIFTYONE_DATASET_NAME`, etc.)
3. Install deps:
   - `uv sync`
4. Run pipeline from this folder — `app.py` calls `load_dotenv()` before config import, so `.env` loads automatically.

# Embedder + FiftyOne Pipeline

Datapipe example for:
- loading local images
- loading optional image-level class labels from JSON
- running multiple embedders
- uploading samples to FiftyOne via `FiftyOneImagesDataTableStore`
- computing `compute_visualization` and `compute_similarity` per embedder

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

## Pipeline tables

- `local_images`: image path inventory
- `image_labels`: optional labels
- `embedder`: embedder configs (`cls`, `init_kwargs`, batch params)
- `fiftyone_samples`: images + optional `ground_truth` classification
- `embeddings`: `.npy` vectors in filedir store
- `brain_status`: status marker for FiftyOne brain computations

## Run

1. Copy env file:
   - `cp .env.example .env`
2. Edit `.env` (paths, `DB_URL`, `FIFTYONE_DATASET_NAME`, etc.)
3. Install deps:
   - `uv sync`
4. Run pipeline from this folder — `app.py` calls `load_dotenv()` before config import, so `.env` loads automatically.

Manual `source .env` only needed if you run `steps.py` / `config.py` outside `app.py`.

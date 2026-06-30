# Embedder + FiftyOne Pipeline

> **Using Claude Code?** This repo ships a setup skill (`.claude/skills/setup-embedder-fiftyone`) —
> just describe your task and it's auto-loaded with the prerequisites and gotchas. See the repo root README.

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

## Prerequisites

### PostgreSQL

Obtain a connection to a PostgreSQL database and set `DB_URL` in `.env` using SQLAlchemy format:

```text
postgresql+psycopg2://user:password@host:port/dbname
```

Example:

```text
DB_URL=postgresql+psycopg2://postgres:postgres@localhost:5432/postgres
```

The database can be empty on first run — datapipe creates all required tables automatically (`datapipe db create-all`).

### FiftyOne App

FiftyOne must run on the **same machine** as the datapipe pipeline (samples and brain results are stored in the local FiftyOne/MongoDB stack).

On the server, launch the app bound to all interfaces:

```shell
fiftyone app launch --remote --address 0.0.0.0 --port 5151 --wait -1
```

If you work from another machine, either use SSH port forwarding to `localhost:5151` on the server, or keep `--address 0.0.0.0` and connect to `http://<server-host>:5151` directly.

## Run

1. Copy env file: `cp .env.example .env`
2. Edit `.env` (paths, `DB_URL`, `FIFTYONE_DATASET_NAME`, etc.)
3. Install deps: `uv sync`
4. From this folder, run the pipeline (`app.py` calls `load_dotenv()` before config import).

Create tables and metadata:

```shell
datapipe db create-all
```

Run the full pipeline:

```shell
datapipe run
```

Or run a single stage by label (stage names match `labels=[("stage", ...)]` in `app.py`):

```shell
datapipe step --labels=stage=source run
datapipe step --labels=stage=embedder run
datapipe step --labels=stage=fiftyone run
datapipe step --labels=stage=embeddings run
datapipe step --labels=stage=fiftyone-brain run
```

Plain `datapipe run` executes all steps; `datapipe step --labels=... run` runs only matching steps.

## Visualize in FiftyOne

After `fiftyone-brain` stage, open dataset `FIFTYONE_DATASET_NAME` in the FiftyOne App (see Prerequisites). Pipeline already ran `compute_visualization` (2D UMAP per embedder) and `compute_similarity` (sklearn cosine index per embedder).

### Embeddings panel

**Why:** see how images cluster in embedding space — check class separation, find outliers, compare embedders side by side.

**How:** open **Embeddings** panel (panels menu). Pick **Brain key** `{embedder_id}_umap` (e.g. `dinov2_base_umap`). Optionally **Color by** `ground_truth` to color points by label. Lasso regions to filter the sample grid to matching images.

Details: [Embeddings panel](https://docs.voxel51.com/user_guide/app.html#embeddings-panel)

### Similarity search

**Why:** find nearest neighbors in embedding space — explore visually similar images, build subsets, sanity-check embedder quality.

**How:** open **Similarity Search** panel (panels menu or similarity popover in grid toolbar). Pick index `{embedder_id}__sim` (e.g. `dinov2_base__sim`). Select one or more samples as query; results sort by cosine similarity. Reopen past runs from panel home page.

Details: [Similarity Search panel](https://docs.voxel51.com/user_guide/app.html#similarity-search-panel-sub-new)

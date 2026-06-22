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

When you enter FiftyOne UI on host:port, you can:
1. Visualize embeddings - instructions [here](https://docs.voxel51.com/user_guide/app.html#embeddings-panel)
2. Search similar images by each embedder - instructions [here](https://docs.voxel51.com/user_guide/app.html#similarity-search-panel-sub-new)

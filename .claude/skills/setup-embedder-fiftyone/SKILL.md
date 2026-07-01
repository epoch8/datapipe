---
name: setup-embedder-fiftyone
description: >
  Use when working in examples/embedder_fiftyone, or running / debugging the embedder→FiftyOne
  datapipe example (DINOv2/DINOv3 image embeddings, UMAP, similarity search).
---

# embedder_fiftyone (DINO → FiftyOne)

This skill = run the DINO→FiftyOne embedder on YOUR images. The FiftyOne-zoo dataset (caltech101) is just a smoke-test; the real goal is your own images — set the knobs below first.

**Ask first — don't assume (only the unresolved):** demo (zoo) or your own data? **which Postgres + which database** for `DB_URL` — never point it at an existing DB or drop in a `localhost` default without confirming; reuse an existing venv / `uv` env or create a fresh one? GPU? surface stage logs or run quiet?

**How to work:** read the setup, then propose a short plan and get a go-ahead before touching anything. Prepare `.env` and **pause for the user to verify it** before running. Run each stage with logs surfaced (e.g. `datapipe run 2>&1 | tail -60`) unless told otherwise, and after each stage say what you did and what changed — don't run the whole pipeline silently.

## Run on YOUR data
- **Align `LABELS_JSON` keys to the file stem** (optional) — a mismatch silently yields an unlabeled
  sample (`None`) / 0 useful results.
- **Your images:** `LOCAL_IMAGES_DIR=/path` (`.jpg/.jpeg/.png`; file stem → `image_name`).
- **Pick models:** `config.py` → `EMBEDDERS` (default dinov2_base + dinov2_large).

## Prerequisites
Stages (labels match `app.py`): `source` (local folder or zoo fallback) → `embedder` → `fiftyone`
(publish samples) → `embeddings` (per-image `.npy`) → `fiftyone-brain` (UMAP + cosine sim per embedder).
- **External PostgreSQL** at `DB_URL` (SQLAlchemy URL). Not started by the example; an empty DB is fine —
  `datapipe db create-all` auto-creates tables. `.env.example` default `...postgres@localhost:5432/postgres`.
- **FiftyOne must run on the SAME machine** — samples/brain live in local FiftyOne/MongoDB; remote →
  empty panels / `no_matching_samples`.
- **GPU** ~8 GB plenty for dinov2_base/large (CPU works, slower). **`uv` + Python 3.10–3.12** → `uv sync`
  (pins cu124 `torch==2.6.0`, `transformers`, `fiftyone`).
- **HF token only for DINOv3.** Defaults (dinov2_base/large) are public; the `dinov3_vitl16` entry is
  **commented out** — to use it, uncomment, accept the `facebook/dinov3-*` license, set `HF_TOKEN` with
  gated-repo read access (else the whole embeddings batch fails).

## Quick demo to verify setup
Skip if you have data. Leave `LOCAL_IMAGES_DIR` unset → zoo fallback (`ZOO_DATASET`=caltech101,
`ZOO_NUM_CLASSES`=10, keeps the **last N** classes); run §Run as-is to confirm install/DB/GPU. First run
downloads it, then auto-deletes the source zoo dataset; `image_name` rewritten `/`→`__` (`steps.py:83`).

## Run (from examples/embedder_fiftyone)
```bash
cp .env.example .env   # DB_URL, LOCAL_IMAGES_DIR or ZOO_*, FIFTYONE_DATASET_NAME (+ FIFTYONE_DATABASE_DIR, see traps)
uv sync && source .venv/bin/activate   # else prefix each command with `uv run`
datapipe db create-all && datapipe run
# one stage: datapipe step --labels=stage=source run  (source|embedder|fiftyone|embeddings|fiftyone-brain)
```

## Caching
Embeddings cache as `.npy` in `EMBEDDINGS_DIR/{embedder_id}/` (default `./data/embeddings`); delete to recompute.

## View in FiftyOne (same machine)
```bash
fiftyone app launch --remote --address 0.0.0.0 --port 5151 --wait -1   # venv active; SSH-forward 5151
```
Open `FIFTYONE_DATASET_NAME`. **Embeddings** panel → key `<embedder_id>_umap` (single `_`), color by
`ground_truth`. **Similarity** → `<embedder_id>__sim` (double `_`) — don't transpose.

## Success criteria
`brain_status.status = 'ok'` per embedder — **trust the table, not the exit code**: datapipe swallows
step errors and still exits 0. Plus: Embeddings panel shows clusters, similarity returns neighbors.

## Troubleshooting (may already be fixed — verify against current files)
- **`brain_status` empty / 0 images though run exited 0** → FiftyOne mongod failed to start (old
  `~/.fiftyone` datadir: FCV mismatch → `Wrong mongod version` / `failed to bind to port`). Use a fresh
  dir: `FIFTYONE_DATABASE_DIR=/tmp/fo_db`.
- **`no_matching_samples`** → no sample `image_name` matched an embedding key: check the `fiftyone`
  stage published samples, pipeline + FiftyOne on the same machine, and (zoo) the `__`-rewrite.
- **DINOv3 403 / `LocalEntryNotFoundError`** → needs a license-accepted gated `HF_TOKEN` (see Prerequisites).

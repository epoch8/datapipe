---
name: datapipe-examples
description: >
  Use when working in the epoch8 datapipe repo with the examples/* folders, when asked to run or
  debug any datapipe example pipeline on your own data, or when unsure which example or setup skill
  applies.
---

# datapipe examples — router

Every example here is meant to run on YOUR data; each bundled demo dataset is only a smoke-test. Pick the example, then its setup skill, then set that skill's "Run on YOUR data" knobs first.

Each `examples/*` pipeline has a dedicated setup skill — pick by what it does:

| Example | What it does | Skill |
|---|---|---|
| `embedder_fiftyone` | DINOv2/DINOv3 image embeddings → FiftyOne UMAP + similarity | **setup-embedder-fiftyone** |
| `e2e_template/image_detection` | YOLO detection + Label Studio human-in-the-loop → train → FiftyOne | **setup-e2e-template** |
| `e2e_template/image_keypoints` | YOLO-pose keypoints + Label Studio → train → FiftyOne | **setup-e2e-template** |
| `sam_cvat` | SAM3 text-prompt boxes+masks → CVAT pre-annotations | **setup-sam-cvat** |
| `detection_tags` | YOLO detection + **tags** (per-scenario metrics), injected GT, FiftyOne A/B view | **setup-detection-tags** |

## Ask first — don't assume (only the unresolved)
Clarify what's not obvious before acting — don't spin up services or pull data you don't need. Ask only the unresolved:
- **Demo or your own data?** · **Provision Postgres/services or reuse existing?** (e2e ships `docker compose`; embedder/sam need external Postgres)
- **Which Postgres + which database** for `DB_URL`? Never point it at an existing DB or drop in a `localhost` default without confirming.
- **Reuse an existing venv / `uv` env, or create a fresh one?** · **Which GPU?** (SAM3 >8 GB; DINOv2/YOLO ~8 GB) · **Annotation backend up?** (LS for e2e / external CVAT for sam)
- **Surface stage logs or run quiet?** · **Per-tag scenario metrics** (retrain new case, old vs new)? → the `detection_tags` example / **setup-detection-tags**

## How to work
Read the setup, then propose a short plan and get a go-ahead before touching anything. Prepare `.env` and **pause for the user to verify it** before running. Run each stage with its logs shown and, after each, say what you did and what changed — don't run the pipeline silently. Trust the status table (`*_training_status`/`brain_status`), not the exit code. If a stage fails and the cause isn't clear from the normal logs, re-run it with `datapipe --debug … run` (or `--debug-sql` for SQL errors) sent to a file and `grep`ped, rather than dumping the verbose output inline.

## Run on YOUR data (universal rule)
When you swap in your own subject, **align your class name across every place it appears** — text
prompt / label config, the label field, and any class filter. A mismatch runs fine but yields 0
useful results. The per-example skill lists the exact knobs.

## Universal prerequisites (every example)
1. **PostgreSQL** at `DB_URL`. embedder + sam_cvat need an EXTERNAL Postgres (they don't start one);
   e2e_template bundles Postgres in its `docker compose`. Empty DB is fine; tables auto-create via
   `datapipe db create-all`. `.env.example` default `...postgres:postgres@localhost:5432/postgres`.
   External quick start: `docker run -d -e POSTGRES_PASSWORD=postgres -e POSTGRES_USER=postgres -e POSTGRES_DB=postgres -p 5432:5432 postgres:16`
   (k8s pod / no docker → conda postgres, `initdb` as non-root with `--locale=C`.)
2. **A GPU big enough** (`nvidia-smi`): DINOv2/YOLO fit ~8 GB; SAM3 wants >8 GB (OOM'd on an 8 GB Pascal in our tests).
3. **Annotation backend, if used:** e2e_template ships Label Studio in its `docker compose`; sam_cvat
   needs an external CVAT you provide (URL + creds + a project whose labels match its config).
4. **`uv` + Python ≥3.10,<3.13.** Each example has a `pyproject.toml` → `uv sync` (cu124 torch pinned,
   CUDA OOTB; no manual venv/pip). Read-only/small `$HOME`: `export UV_CACHE_DIR=/tmp/uvcache HF_HOME=/tmp/hf`.
5. **Human-in-the-loop** (e2e_template, sam_cvat): a human reviews and marks tasks completed before
   the pipeline advances.

## HF auth (only for GATED models)
Needed for `dinov3_*` (embedder, off by default) and `sam3` (sam_cvat). Accept the model license on
its HF page, then `export HF_TOKEN=...` with **gated-repo read access**. A plain token → `403`, often
masked as `LocalEntryNotFoundError: check your connection`. Public models (dinov2, YOLO) need no token.

## Universal run shape
```bash
cp .env.example .env        # set DB_URL and the example's data + backend vars
uv sync
datapipe db create-all      # auto-creates all tables
datapipe run                # or: datapipe step --labels stage=<s> run
```
Datapipe caches by content: re-running does little work unless inputs or tracked config change.
Then follow the per-example skill — set its **Run on YOUR data** knobs first.

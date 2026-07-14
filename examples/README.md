# Datapipe examples

Every directory here is a **self-contained project** built on the Datapipe workspace libraries: its
own `pyproject.toml` (workspace path-deps), `.env.example`, usually a `docker-compose.yml` for the
services it needs, and a README with exact commands. Nothing is shared between examples at runtime —
you can `uv sync` and run any of them in isolation.

They fall into two groups: **small, single-concept snippets** that teach the core API, and **full
pipelines** that look like real projects (data loading → processing/training → metrics → a viewer).

## Map: what each example shows

| example | what it demonstrates | key pieces | look at results in |
|---|---|---|---|
| [`datapipe_core/`](datapipe_core) | a folder of minimal single-concept scripts: batch transforms, key mapping, one-to-many/many-to-zero relations, parquet processing, image resize, model inference, neo4j | `datapipe` core API only, sqlite | stdout / tables |
| [`datapipe_app/`](datapipe_app) | the smallest possible **service**: a 2-step pipeline (events → user profiles) served with REST API + debug UI | `datapipe_app.DatapipeAPI`, Dockerfile, alembic | REST / debug UI |
| [`datapipe_cvat/`](datapipe_cvat) | basic **CVAT** integration on a toy image project | `datapipe_cvat` | CVAT |
| [`sam_cvat/`](sam_cvat) | **SAM3 → CVAT pre-annotation**: segment images automatically, ship masks to CVAT for human correction | `datapipe_cvat`, SAM3 | CVAT |
| [`embedder_fiftyone/`](embedder_fiftyone) | image **embeddings → visualization**: DINOv2/DINOv3 vectors, UMAP projection, similarity search | `datapipe_ml`, FiftyOne | FiftyOne App |
| [`ocr/`](ocr) | **multi-LLM structured OCR**: run several engines (OpenAI / Gemini / Qwen) over id-doc images, get schema-validated JSON per engine, compare side by side | LLM APIs, pydantic schema, FiftyOne Caption Viewer | FiftyOne App |
| [`e2e_template/`](e2e_template) | the **full production loop** for detection and keypoints: Label Studio annotation → frozen datasets → YOLO training → inference → metrics → best-model selection | `datapipe_ml`, `datapipe_label_studio`, YOLOv8, FiftyOne | Label Studio, FiftyOne, datapipe UI |
| [`detection_tags/`](detection_tags) | **per-scenario (tag) metrics** — the reason to slice metrics by tag: train a baseline that is blind in the dark, add a *tagged* low-light batch, retrain, watch the tag metric rise; ground truth is injected (no annotation tool needed) | `datapipe_ml`, YOLOv8, frozen val split, FiftyOne | datapipe UI (metric tables), FiftyOne |

Rough learning path: `datapipe_core` → `datapipe_app` → `detection_tags` (self-contained ML loop,
no external annotation) → `e2e_template` (adds the Label Studio loop) → the specialised ones
(`ocr`, `embedder_fiftyone`, `sam_cvat`).

## Anatomy of a full example

The bigger examples share one layout and a set of conventions:

```
examples/<name>/
├── pyproject.toml        # own project; depends on ../../libs/* via workspace paths
├── uv.lock               # committed where reproducibility matters (see detection_tags)
├── .env.example          # DB_URL, S3/MinIO, API keys — copy to .env and edit
├── docker-compose.yml    # the services this example needs (Postgres, MinIO, Mongo, ...)
├── scripts/              # helper CLIs (enqueue data, build caches, diagnostics)
└── <package>/            # the pipeline itself
    ├── config.py         # env parsing, constants
    ├── data.py           # Catalog: table schemas
    ├── steps.py          # transform functions (pd.DataFrame -> pd.DataFrame)
    └── app.py            # Pipeline wiring + DatapipeApp (+ UI specs where present)
```

Conventions worth knowing before reading any code:

- **Stages via labels.** Steps are tagged like `labels=[("stage", "load")]`, so you run one phase at
  a time: `datapipe step --labels=stage=load run`, then `stage=train`, etc. `datapipe run` executes
  everything.
- **Tables first.** All state lives in DB tables declared in `data.py`; transforms are stateless
  functions. `datapipe db create-all` creates the tables.
- **Incremental by default.** Re-running a stage reprocesses only changed rows — loading the same
  batch twice is a no-op, adding rows processes just the delta.
- **Trust status tables, not exit codes.** Long steps (training) record their outcome in `*_status`
  tables; a zero exit code alone does not mean success.

## Running any example

```bash
cd examples/<name>
cp .env.example .env            # then edit: DB_URL, keys, data paths
docker compose up -d            # if the example ships one
uv sync                         # do NOT edit deps ad-hoc: re-locking drifts versions across machines
source .venv/bin/activate       # or prefix commands with the venv path
datapipe db create-all
datapipe run                    # or stage by stage: datapipe step --labels=stage=<stage> run
```

Each example's own README documents its stages, data knobs and viewers, plus a troubleshooting
section collected from real runs.

## Running with an AI agent

Every full example ships an **operational skill** in [`.claude/skills/`](../.claude/skills)
(`setup-detection-tags`, `setup-e2e-template`, `setup-ocr`, `setup-embedder-fiftyone`,
`setup-sam-cvat`, plus the umbrella `datapipe-examples`). Open this repository in
[Claude Code](https://claude.com/claude-code) and type `/setup-<example>` — the agent will check your
environment, prepare `.env`, run the stages with logs, and knows the example's failure modes. The
skills encode the same instructions as the READMEs, but in a form an agent can execute.

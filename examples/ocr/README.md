# OCR + FiftyOne Pipeline

**Claude Code skill:** `/setup-ocr` — auto-loads when relevant, or invoke directly.

Datapipe example for:
- ingesting passport / id-doc images (local folder or Hugging Face dataset)
- running OCR with multiple LLM engines (OpenAI, Gemini)
- publishing pydantic `OUTPUT_MODEL` JSON to FiftyOne StringFields
- viewing results with the Caption Viewer plugin

## Data sources

Supports **local folder** or **Hugging Face dataset** fallback.

- Local: `LOCAL_IMAGES_DIR` with `.jpg`/`.jpeg`/`.png`/`.webp`/`.heic` inside (recursive scan)
- HF fallback when local dir unset, missing, or empty: `HF_DATASET_NAME` (default `ud-synthetic/printed-usa-passports`)
- HF images cached under `DATA_DIR/<dataset>/`

Use `HF_LIMIT` to cap HF download size and LLM cost (ignored for local dirs).

## Engine switch

Single source of truth: `config.ENGINE_REGISTRY` + `.env` `OCR_ENGINES`.

```text
OCR_ENGINES=openai,gemini,qwen
```

FiftyOne tables and pipeline steps are generated from the same enabled engine list, so config and tables stay aligned.

Model names and inference kwargs (LLM `temperature`, etc.) live in `ENGINE_REGISTRY` inside `config.py`, not in `.env`.

## Prerequisites

### PostgreSQL

Set `DB_URL` in `.env`:

```text
postgresql+psycopg2://user:password@host:port/dbname
```

Create tables (use a fresh DB or drop old tables after schema changes):

```shell
datapipe db create-all
```

### API keys

- `OPENAI_API_KEY` for `openai`
- `GEMINI_API_KEY` for `gemini`
- `QWEN_API_KEY` for `qwen` (DashScope compatible OpenAI API)

### FiftyOne App

First - after creating virtual env install the community plugin for readable JSON in the FiftyOne modal:

```shell
fiftyone plugins download https://github.com/harpreetsahota204/caption_viewer
```

Launch on the same machine as the pipeline:

```shell
fiftyone app launch --remote --address 0.0.0.0 --port 5151 --wait -1
```

To visualize JSON structured output in the App: open a sample → add panel → **Caption Viewer** → select `openai_ocr`, `gemini_ocr`, or `qwen_ocr`.

## Run

1. Copy env: `cp .env.example .env`
2. Edit `.env` (DB, API keys, dataset, limits)
3. Install deps: `uv sync`
4. From this folder:

```shell
datapipe run
```

Or run stages by label:

```shell
datapipe step --labels=stage=ingest run
datapipe step --labels=stage=ocr run
datapipe step --labels=stage=fiftyone run
```

## Visualize in FiftyOne

Dataset name: `FIFTYONE_DATASET_NAME` (default `datapipe_ocr`).

| Engine | FiftyOne field | How to view |
|--------|----------------|-------------|
| `openai` | `openai_ocr` | Caption Viewer panel (pretty JSON) |
| `gemini` | `gemini_ocr` | Caption Viewer panel (pretty JSON) |
| `qwen` | `qwen_ocr` | Caption Viewer panel (pretty JSON) |

Open multiple Caption Viewer panels to compare engine fields side by side.

## Customize output schema

Edit `OUTPUT_MODEL` in `config.py` (default: passport fields `name`, `surname`, `birthdate`). All engines use it as output schema. OCR instruction text auto-generated via `ocr_prompt()`.

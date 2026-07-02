# OCR + FiftyOne Pipeline

Datapipe example for:
- ingesting two Hugging Face datasets (unstructured book scans + structured passport images)
- running OCR with a config-driven engine switch (PaddleOCR v6, OpenAI, Gemini)
- publishing results to FiftyOne (native boxes for unstructured, JSON StringFields for structured)
- optional side-by-side composite renders for unstructured OCR

## Data sources

Each branch supports **local folder** or **Hugging Face dataset** fallback.

### Unstructured (`paddle`)

- Local: `LOCAL_UNSTRUCTURED_DIR` with `.jpg`/`.jpeg`/`.png`/`.webp`/`.heic` inside (recursive scan)
- HF fallback when local dir unset, missing, or empty: `HF_DATASET_UNSTRUCTURED` (default `MLap/Book-Scan-OCR`)
- HF images cached under `DATA_DIR/<dataset>/`
- Routed to engines with `engine_type=unstructured`

### Structured (`openai`, `gemini`)

- Local: `LOCAL_STRUCTURED_DIR` with supported images inside (recursive scan)
- HF fallback when local dir unset, missing, or empty: `HF_DATASET_STRUCTURED` (default `ud-synthetic/printed-usa-passports`)
- HF images cached under `DATA_DIR/<dataset>/`
- Routed to engines with `engine_type=structured`
- Both LLMs share the same pydantic schema `IdDocument(name, surname, birthdate)` from `config.py`

Use `HF_LIMIT_UNSTRUCTURED` / `HF_LIMIT_STRUCTURED` to cap HF download size and LLM cost (ignored for local dirs).

## Engine switch

Single source of truth: `config.ENGINE_REGISTRY` + `.env` `OCR_ENGINES`.

```text
OCR_ENGINES=paddle,openai,gemini
```

FiftyOne tables and pipeline steps are generated from the same enabled engine list, so config and tables stay aligned.

Model names and inference kwargs (PaddleOCR init, LLM `temperature`, etc.) live in `ENGINE_REGISTRY` inside `config.py`, not in `.env`.

## Prerequisites

### PostgreSQL

Set `DB_URL` in `.env`:

```text
postgresql+psycopg2://user:password@host:port/dbname
```

Create tables:

```shell
datapipe db create-all
```

### API keys (structured engines)

- `OPENAI_API_KEY` for `openai`
- `GEMINI_API_KEY` (or `GOOGLE_API_KEY`) for `gemini`

### FiftyOne App

Launch on the same machine as the pipeline:

```shell
fiftyone app launch --remote --address 0.0.0.0 --port 5151 --wait -1
```

### Caption Viewer plugin (structured JSON)

Install the community plugin for readable JSON in the FiftyOne modal:

```shell
fiftyone plugins download https://github.com/harpreetsahota204/caption_viewer
```

In the App: open a sample → add panel → **Caption Viewer** → select `openai_ocr` or `gemini_ocr`.

## Run

1. Copy env: `cp .env.example .env`
2. Edit `.env` (DB, API keys, datasets, limits)
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
datapipe step --labels=stage=composite run
```

## Visualize in FiftyOne

Dataset name: `FIFTYONE_DATASET_NAME` (default `datapipe_ocr`).

| Engine type | FiftyOne fields | How to view |
|-------------|-----------------|-------------|
| unstructured (`paddle`) | `paddle_boxes` detections, `paddle_ocr` text | Toggle detections overlay; box label = recognized text |
| structured (`openai`, `gemini`) | `openai_ocr`, `gemini_ocr` StringFields | Caption Viewer panel (pretty JSON) |

Open multiple Caption Viewer panels to compare `openai_ocr` vs `gemini_ocr` side by side.

Composite PNGs (unstructured only) are written to `data/composites/{engine_id}/` when running `stage=composite`.

## Customize structured schema

Edit `IdDocument` in `config.py`. Both OpenAI and Gemini use it as structured output schema. The OCR instruction text is generated automatically from that model via `structured_ocr_prompt()`.

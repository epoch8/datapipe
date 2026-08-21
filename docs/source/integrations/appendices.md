# Appendices: FiftyOne and OCR

Optional specialized pipelines. Not required for core Datapipe.

## FiftyOne

**Problem it solves:** browse embeddings, ground truth, and model predictions in FiftyOne’s App, while Datapipe still owns incremental tables and training/inference steps.

### Where it lives

- FiftyOne-oriented table stores and helpers ship with `datapipe-ml[fiftyone]`
- E2E templates optionally publish predictions/annotations into FiftyOne (`examples/e2e_template/`, `examples/detection_tags/`)
- Dedicated embedder demo: `examples/embedder_fiftyone/`

### Install hints

```bash
# library extra
uv pip install -e "libs/datapipe-ml[fiftyone]"

# embedder example
cd examples/embedder_fiftyone
uv sync
cp .env.example .env   # set DB_URL, FIFTYONE_DATASET_NAME, paths
datapipe db create-all
datapipe run
```

FiftyOne stores dataset metadata in **MongoDB**. For package tests, `libs/datapipe-ml` docker compose exposes `mongo` on `27017` (see `libs/datapipe-ml/README.md`). Examples that use compose (`e2e_template`, `detection_tags`) start Mongo + a FiftyOne App service (often `:5151`).

`embedder_fiftyone` expects the FiftyOne App on the **same machine** as the pipeline:

```bash
fiftyone app launch --remote --address 0.0.0.0 --port 5151 --wait -1
```

That example loads local images or falls back to a FiftyOne zoo dataset, runs multiple embedders, uploads via `FiftyOneImagesDataTableStore`, then `compute_visualization` / `compute_similarity` per embedder.

## OCR

**Problem it solves:** run several LLM/OCR engines over the same document images, write structured JSON into Datapipe tables, and inspect side-by-side results in FiftyOne — without hand-rolling incremental bookkeeping.

### Example

`examples/ocr/`

- Ingest passport / ID images from a local folder or a Hugging Face dataset fallback
- Switch engines via `OCR_ENGINES` (registry in `config.py`: OpenAI, Gemini, Qwen, …)
- Publish pydantic `OUTPUT_MODEL` JSON to FiftyOne StringFields
- View with the Caption Viewer FiftyOne plugin

### Install hints

```bash
cd examples/ocr
cp .env.example .env
# set DB_URL, OPENAI_API_KEY / GEMINI_API_KEY / QWEN_API_KEY as needed
uv sync
datapipe db create-all
datapipe run
```

FiftyOne plugin (once per machine):

```bash
fiftyone plugins download https://github.com/harpreetsahota204/caption_viewer
fiftyone app launch --remote --address 0.0.0.0 --port 5151 --wait -1
```

Use `HF_LIMIT` to cap Hugging Face downloads and LLM cost when not using a local folder.

## See also

- [ML mental model](./ml-overview.md)
- [datapipe-ml index](./datapipe-ml.md)

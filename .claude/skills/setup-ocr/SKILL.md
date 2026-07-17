---
name: setup-ocr
description: >
  Use when working in examples/ocr, or running / debugging the OCR→FiftyOne
  datapipe example (multi-LLM structured OCR on passport/id-doc images).
---

# ocr (LLM OCR → FiftyOne)

This skill = run multi-engine LLM OCR on YOUR passport/id-doc images and publish structured JSON to FiftyOne. The HF dataset (`ud-synthetic/printed-usa-passports`) is just a smoke-test; the real goal is your own images — set the knobs below first.

**Ask first — don't assume (only the unresolved):** demo (HF passports) or your own images? **which Postgres + which database** for `DB_URL` — never point it at an existing DB or drop in a `localhost` default without confirming; reuse an existing venv / `uv` env or create a fresh one? **which OCR engines** (`OCR_ENGINES` → subset of `openai,gemini,qwen`; each needs its API key)? **which model names** (defaults below — keep or change in `config.py` `ENGINE_REGISTRY`)? **cost cap** (`HF_LIMIT` in demo mode; local mode = all images × all engines)? **custom output schema** (default `IdDocument` in `config.py` or user edits `OUTPUT_MODEL`)? surface stage logs or run quiet?

**How to work:** read the setup, then propose a short plan and get a go-ahead before touching anything. Prepare `.env` and **pause for the user to verify it** before running. Run each stage with its logs shown and, after each, say what you did and what changed — don't run the pipeline silently. If a stage fails and the cause isn't clear from the normal logs, re-run it with `datapipe --debug … run` (or `--debug-sql` for SQL errors); debug is very verbose, so send it to a file and `grep` it (e.g. `datapipe --debug run > /tmp/dp_debug.log 2>&1; grep -nEi "error|traceback" /tmp/dp_debug.log`) rather than dumping it inline.

## Run on YOUR data
- **Your images:** `LOCAL_IMAGES_DIR=/path` (recursive `.jpg/.jpeg/.png/.webp/.heic`; file stem → `image_id`).
  Local wins when the dir exists and has images (`use_local_images()` in `config.py`); otherwise HF fallback.
- **Or HF:** `HF_DATASET_NAME`, `HF_SPLIT`, `HF_LIMIT` (caps download **and** LLM calls in demo mode).
- **Engines:** `OCR_ENGINES=openai,gemini` (comma list). Model names + inference kwargs live in `config.py`
  `ENGINE_REGISTRY` (not `.env`) — **ask which models to use**, then edit `ENGINE_REGISTRY[..]["model"]` if needed.
  Current defaults (verify against `config.py`): `openai` → `gpt-5.4-nano`, `gemini` → `gemini-3.5-flash`,
  `qwen` → `qwen3.7-plus`.
- **FiftyOne dataset:** `FIFTYONE_DATASET_NAME` (default `datapipe_ocr`).
- **Optional:** edit `OUTPUT_MODEL` in `config.py` — `ocr_prompt()` auto-follows the pydantic schema.

## Prerequisites
Stages (labels match `app.py`): `ingest` (list images + engines) → `ocr` (LLM inference, images × engines
cross-join, `chunk_size=2`) → `fiftyone` (per-engine `{engine_id}_ocr` StringField via Caption Viewer).
- **External PostgreSQL** at `DB_URL` (SQLAlchemy URL). Not started by the example; an empty DB is fine —
  `datapipe db create-all` auto-creates tables. `.env.example` default `...postgres@localhost:5432/postgres`.
- **LLM API keys** for each enabled engine: `OPENAI_API_KEY`, `GEMINI_API_KEY` (or `GOOGLE_API_KEY`), `QWEN_API_KEY`.
  **Geoblock:** `openai` / `gemini` call the **native** OpenAI / Google APIs — blocked from Russian IPs. Workarounds
  (proxy/router) are possible outside this example; from RU, prefer `qwen` (DashScope) or run from a non-blocked network.
- **FiftyOne must run on the SAME machine** — samples live in local FiftyOne/MongoDB; remote → empty panels.
- **Caption Viewer plugin** (required for readable JSON in the App):
  `fiftyone plugins download https://github.com/harpreetsahota204/caption_viewer`
- **No GPU needed** — OCR calls LLM APIs, not local inference. **`uv` + Python 3.10–3.12** → `uv sync`
  (pins CPU `torch==2.6.0` for transitive cv-pipeliner; OCR itself doesn't use it).
- Default HF dataset (`ud-synthetic/printed-usa-passports`) is public — no `HF_TOKEN` required.

## Quick demo to verify setup
Skip if you have data. Leave `LOCAL_IMAGES_DIR` unset → HF fallback (`HF_DATASET_NAME`=ud-synthetic/printed-usa-passports,
`HF_LIMIT=10` in `.env.example` keeps LLM cost low); run §Run as-is to confirm install/DB/API keys/FiftyOne.

## Run (from examples/ocr)
```bash
cp .env.example .env   # DB_URL, API keys, OCR_ENGINES, LOCAL_IMAGES_DIR or HF_*, FIFTYONE_DATASET_NAME
uv sync && source .venv/bin/activate   # else prefix each command with `uv run`
fiftyone plugins download https://github.com/harpreetsahota204/caption_viewer   # once per venv
datapipe db create-all && datapipe run
# one stage: datapipe step --labels=stage=ingest run  (ingest|ocr|fiftyone)
```
Run from `examples/ocr/` (`load_dotenv()` in `app.py` — wrong cwd breaks `.env`).

## Cost note
OCR runs a **cross-product** of images × enabled engines. Demo: `HF_LIMIT` caps both. Local: every image hits
every engine — confirm image count and `OCR_ENGINES` before a full run.

## View in FiftyOne (same machine)
```bash
fiftyone app launch --remote --address 0.0.0.0 --port 5151 --wait -1   # venv active; SSH-forward 5151
```
Open `FIFTYONE_DATASET_NAME`. Per engine: open a sample → add panel → **Caption Viewer** → field
`openai_ocr` / `gemini_ocr` / `qwen_ocr`. Open multiple panels to compare engines side by side.

## Success criteria
- `ocr_results` table has a row per `(image_id, engine_id)` pair with populated `output_json`.
- FiftyOne dataset has samples with `{engine_id}_ocr` fields containing structured JSON.
- Caption Viewer panel renders the JSON readably.

## Troubleshooting (may already be fixed — verify against current files)
- **No images found** → empty/missing `LOCAL_IMAGES_DIR` and HF misconfigured; or local dir has no supported suffixes.
- **No OCR engines enabled** → `OCR_ENGINES` empty or unknown IDs (must match `ENGINE_REGISTRY` keys in `config.py`).
- **API key errors** → missing/wrong key for an enabled engine (`OPENAI_API_KEY`, `GEMINI_API_KEY`, `QWEN_API_KEY`).
- **OpenAI/Gemini timeout / region / connection refused from RU** → geoblock on native APIs (see Prerequisites); use `qwen` or a non-blocked network/proxy.
- **Runaway LLM cost** → forgot `HF_LIMIT` in demo mode, or large local folder with all three engines enabled.
- **FiftyOne empty / mongod bind errors** → old `~/.fiftyone` datadir (FCV mismatch / port bind). Use a fresh
  dir: `FIFTYONE_DATABASE_DIR=/tmp/fo_db`.
- **JSON not pretty in App** → Caption Viewer plugin not installed (see Run).
- **Schema mismatch after editing `OUTPUT_MODEL`** → re-run `datapipe step --labels=stage=ocr run` so engines pick up the new prompt.

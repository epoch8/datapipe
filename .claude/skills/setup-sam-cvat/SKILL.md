---
name: setup-sam-cvat
description: >
  Use when working in examples/sam_cvat, or when setting up / running / debugging the datapipe
  SAM3→CVAT pre-annotation example, on the cats-n-dogs demo data or on your own images.
---

# sam_cvat (SAM3 + CVAT)

This skill = run SAM3→CVAT pre-annotation on YOUR images. The HZMD/cats-n-dogs demo is just a smoke-test; the real goal is your own images — set the knobs below first.

**Before starting, if not already provided, ask the user:** demo (cats-n-dogs) or your own data? external Postgres + CVAT ready or provision/point at them? GPU >8 GB? — ask only what's unresolved, then do only what's needed.

## Run on YOUR data
- **Align across all three:** `SAM_TEXT_PROMPT` == the CVAT labels (`CVAT_BOX_LABEL` / `CVAT_POLYGON_LABEL`) == `HF_DATASET_LABEL` must all mean the same class — a mismatch runs clean but yields 0 useful results.
- **Point at your images:** `INPUT_DIR=/path` (`.jpg/.jpeg/.png`; stem → `image_id`). An existing-but-EMPTY `INPUT_DIR` yields 0 images (no HF fallback).
- **Levers:** `SAM_SCORE_THRESHOLD` (0.5), `SAM_MAX_DETECTIONS` (10).

## Prerequisites
- **GPU >8 GB with FlashAttention.** *Field note:* ~5.5 GB on a 16 GB RTX 4060 Ti, but OOM on an 8 GB
  GTX 1070 (Pascal: no FlashAttention → math-attention, O(n²) memory).
- **HF token — SAM3 is gated.** Accept the license at huggingface.co/facebook/sam3, set `HF_TOKEN`
  with gated-repo read access (a non-gated token → download/load failure).
- **External PostgreSQL** at `DB_URL` (not started here; tables auto-create via `datapipe db create-all`).
- **External CVAT** (not started here): set `CVAT_URL`, `CVAT_USERNAME`, `CVAT_PASSWORD`, optional
  `CVAT_ORGANIZATION`, and `CVAT_PROJECT_ID` for a project whose label names match `CVAT_BOX_LABEL` /
  `CVAT_POLYGON_LABEL`.
- **`uv` + Python ≥3.10,<3.13** → `uv sync`. `pyproject.toml` pins cu124 torch and editable local libs
  (`../../libs/datapipe-*`, resolves only inside the monorepo checkout), and builds `sam3` from a pinned
  git source (needs git, a compiler, `nvcc`/`CUDA_HOME`). If `uv sync` fails, check the `sam3` rev in `[tool.uv.sources]`.
- Stages: `ingest` (local folder or HF-dataset fallback) → `sam` (text-prompt inference → boxes + mask
  polygons) → `cvat` (upload images + pre-annotations; parse edits back into `image__annotations`).

## Quick demo to verify setup
Skip if you have data. Leave `INPUT_DIR` unset → HF fallback; shipped `.env.example` is self-consistent
(`a cat` / `cat_box`/`cat_mask` / `HF_DATASET_LABEL=0`=cats) → first run on `HZMD/cats-n-dogs` yields
detections, confirming install/DB/GPU/CVAT.

## Run
```bash
cp .env.example .env   # DB_URL, HF_TOKEN, CVAT_*, CVAT_PROJECT_ID, SAM_TEXT_PROMPT, labels
uv sync && source .venv/bin/activate   # else prefix each command with `uv run`
datapipe db create-all && datapipe run
# or by stage: datapipe step --labels stage=sam run
```
Run from `examples/sam_cvat/` (`app.py` `load_dotenv()`s before importing config — wrong cwd breaks `.env`).

## Caching gotcha
`SAM_TEXT_PROMPT` IS tracked (→ `sam_config` table) so changing it retriggers inference — **but
`CVATStep` does NOT push new pre-annotations to EXISTING tasks** (only on image add/remove/path change).
Treat the prompt as fixed per run; to change it, wipe the old CVAT tasks + datapipe CVAT tables, re-run.

## Annotate (human-in-the-loop)
In CVAT fix preannotations, **mark the task completed**, re-run `datapipe run` → edits land in
`image__annotations`. CVAT tables (for wipes): `image_batches`, `cvat_task`, `cvat_images` (not "cvat_files"), `cvat_task_sync_table`.

## Troubleshooting (may already be fixed — verify against current files)
- **0 detections** → `.env` class misaligned; re-align all three.
- **CVAT rejects label** → project needs labels named `CVAT_BOX_LABEL`/`CVAT_POLYGON_LABEL`.
- **Prompt change not in CVAT** → expected (see Caching).

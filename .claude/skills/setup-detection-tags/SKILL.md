---
name: setup-detection-tags
description: >
  Use when setting up or running examples/detection_tags — a self-contained datapipe detection
  demo built around tags (per-scenario metrics), split into two parts around a checkpoint (train a
  baseline, then add a tagged TRAIN batch and retrain, watching the tag metric rise), served under the
  datapipe-app UI front (graph + observability + the cat_dog ops-spec; training triggered
  from the UI) with a FiftyOne view and injected ground truth (no Label Studio). Also for "add a tagged
  batch, retrain, watch the tag metric rise" and for rehearsing that retraining demo from a saved checkpoint.
---

# detection_tags (tags demo — two-part, FiftyOne, no Label Studio)

Detection pipeline whose whole point is **tags**: train a baseline (model A), then add a **tagged
scenario TRAIN batch** and retrain (model B), and watch recall on that tag rise in a `tag_metrics`
table. Ground truth is **injected** (COCO labels, lowercase `cat`/`dog`) — no human annotation.

The two-part / checkpoint split below is a **presentation device for the live demo only** — it lets
you prep part 1 ahead of time, present part 2, and rehearse it. It is NOT required to use the pipeline:
for real work you just run the stages end to end (the frozen-val data layout still matters — that's
about metric correctness, not the demo). See "Real data" at the end.

The demo is deliberately **two parts around a checkpoint**, so you can prepare part 1 ahead of time
and present/rehearse part 2 (the retraining) as often as you like:

- **Part 1 (prep, before the audience):** deploy → load `base-train` + `base-val` + `night-val`
  (val is frozen up front) → train **model A** → compute all metrics → **take a checkpoint** → build
  the FiftyOne view. Stop here.
- **Part 2 (the live demo):** show model A's metrics + FiftyOne → *ask "ready to retrain?"* → load
  the tagged TRAIN batch `night-train` → retrain **model B** → show the metrics again: `night` recall
  rises from A to B. Rehearse by resetting to the checkpoint.

> **Training runs on the UI side (integrated datapipe-app setup).** This skill **sets everything up
> itself**: it deploys the stack, checks prerequisites (`uv`, `docker compose`, GPU), loads the data,
> and **verifies the data is in the pipe** — then **stops**. It does **not** run training. **You
> trigger each training run yourself from the datapipe-app front** (the pipeline graph's
> `stage=train` steps, port 8000): model A in part 1, then — after the skill tops up `night-train` —
> model B in part 2. The `datapipe step --labels=stage=train run` commands below are the
> standalone/CLI equivalent for a no-UI setup; in the UI setup you (the human) press run in the front
> instead.

## Ask first — don't assume (only the unresolved)

1. **Demo (test) or real data?** — the whole flow branches on this (see "Real data" at the end).
2. **Is an env/compose already up?** Check before deploying: `docker compose ls` /
   `docker ps` on the target host, and whether a `.venv` already has torch+fiftyone. **Reuse it**
   (skip `uv sync` / `compose up` / `db create-all` as appropriate) rather than redeploying — only
   deploy the pieces that are missing. Don't drop a `localhost` DB default or target an existing DB
   without confirming.
3. **GPU available?** Training is GPU. On a remote cluster (e.g. epoch8 gpu5) run the whole stack
   there over SSH; note the host may have a **read-only home** and **no AVX2** (see Troubleshooting).
4. **Surface stage logs or run quiet?** Default: show each stage and report what changed.

## Frozen val — why the data is loaded in this order (READ THIS)

A metric on `val` is an **aggregate over the set of images currently in val**. Change that set and
the number moves *even if the model is byte-identical*. If you load the tagged batch as one blob and
let the random split scatter it across train/val at retrain time, model A's val (and tag/val) numbers
shift between the two measurements — an apples-to-oranges comparison that confuses everyone.

Fix: **freeze val up front.** Load `base-val` and `night-val` (subset pinned to `val`) *before*
training model A, and only add `night-train` (pinned to `train`) later. Then val never changes; model
A is measured once on the full frozen val and its numbers are stable, and B is compared on the exact
same val. `add_request.py --subset train|val` pins a batch; the load step emits `image__subset_hint`
and the split step honors it (random split only fills images without a hint).

Batches (COCO cat/dog; the pre-staged cache holds **1000 images** — fetch it from the public bucket
or build it, see Troubleshooting; keep the batch total ≤ the cache size):

| batch | n | offset | subset | tag | darken | when |
|-------|---|--------|--------|-----|--------|------|
| `base-train`   | 400 | 0   | train | —     | —    | part 1 |
| `base-val`     | 150 | 400 | val   | —     | —    | part 1 |
| `night-val`    | 150 | 550 | val   | night | 0.40 | part 1 |
| `night-train-a`| 100 | 700 | train | night | 0.30 | part 2 |
| `night-train-b`| 100 | 800 | train | night | 0.40 | part 2 |
| `night-train-c`| 100 | 900 | train | night | 0.55 | part 2 |

**Why THREE night-train batches with different gammas (0.30/0.40/0.55 around val's 0.40):** a single
gamma makes model B memorize that exact darkness (night/train high, night/val flat); gamma DIVERSITY
forces it to generalize "low light", which is what lifts the held-out val. More epochs does NOT fix
this (deeper memorization, val drops) — the lever is data diversity, keep epochs=5.

**Why 1000 images / batch=32 / epochs=5 / freeze=10 / prediction_threshold=0.10** (validated by a
130+-run sweep + multi-seed full-pipe cycles; the headline metric is **weighted F1 on val**):
- `batch=10` gave noisy gradients — the B−A margin flipped sign depending on seed/machine; `batch=32`
  keeps it positive on every seed tested.
- Doubling data (500→1000) doubled val support (379/207 boxes) and stabilized absolutes; with
  `epochs=5` the fitness curve still rises at the last epoch, so best.pt == last.pt (no
  epoch-selection jitter).
- `freeze=10` (frozen backbone, only the head trains) is the biggest stability lever: far fewer
  degrees of freedom → per-seed/per-machine F1 spread collapses from 12-15pp to 2-5pp.
- `prediction_threshold=0.10` on the Inference step pins ONE operating point for all models and
  machines. Without it each model gets its own `best_threshold` from a noisy F1 curve (observed
  0.06-0.15), which both jitters the reported metrics AND flatters the weak baseline (best_threshold
  maximizes F1 by construction), shrinking the B−A gap to noise on unlucky seeds.

**Cross-machine numbers:** training is bit-reproducible on one machine (verified: two different
physical GPUs of the same model → identical metrics), but weights legitimately differ across GPU/CPU models —
float arithmetic on different chips, not a bug (verified down to byte-identical augmented batches).
With this config the weighted-F1 spread across seeds (proxy upper bound for machines) is ≤5pp for
both models on both val sets.

**Deterministic reference** (seed=42, batch=32, epochs=5, freeze=10, thr=0.10, 1000 images;
weighted F1 / recall on val): model A → overall 0.483/0.348, night 0.408/0.285;
model B → overall **0.610/0.536**, night **0.551/0.469**. **B beats A on BOTH val sets on every
seed tested** (F1 margin across seeds 2/7/42: overall +0.081..+0.127, night +0.082..+0.143; per-model
F1 spread ≤4.6pp). Exact numbers reproduce bit-for-bit on the same GPU/CUDA/torch stack; data/splits/
tags reproduce byte-identically on any machine (seeded canonical cache). Full cold-start cycle
(wipe → cache from bucket → load → A → metrics → load → B → metrics → FiftyOne) ≈ 10 min on a
mid-range GPU.

## Pre-flight — CHECK what's already there, then ASK (do this before deploying)

Never assume a clean host. Before `docker compose up` / loading / training, check each resource and,
if it's occupied, ASK the user what to do (reuse / different port / different schema or DB / wipe) —
don't silently claim it or clobber someone else's work.

```bash
# 1) ports free? (host)  — postgres 5432, minio 9000/9001, mongo 27017, fiftyone 5151, app 8000
ss -ltn | grep -E ':(5432|9000|9001|27017|5151|8000)\b' || echo "all free"
# 2) is a stack already up?
docker compose ls ; docker ps --format '{{.Names}}\t{{.Ports}}'
# 3) does the target DB/schema already hold this pipeline's tables?
psql "$DB_URL" -c "\dt $DB_SCHEMA.*" 2>/dev/null | grep -qi detection && echo "SCHEMA IN USE" || echo "schema clean"
# 4) is a venv already built (torch+fiftyone)?  reuse vs fresh uv sync
```

Findings → questions to ask: a **busy port** means another stack (reuse it, or remap ports?); a
**populated schema** means a prior run (resume it, wipe it, or use a different `DB_SCHEMA`/DB?); a
**running compose** (reuse or `down`?). The same check applies on your laptop when a **local tunnel
port is busy** — pick a different left-hand port (`-L 5433:localhost:5432`), don't kill blindly.

## Deploy from scratch (standard ports; skip pieces already up)

```bash
cp .env.example .env && set -a && source .env && set +a   # DB_URL, S3/MinIO, FIFTYONE_DATABASE_URI
HOST_UID=$(id -u) HOST_GID=$(id -g) docker compose up -d   # postgres + minio + mongo + fiftyone (:5151); HOST_UID/GID avoid sudo perms. No Label Studio.
uv sync                       # modern hosts: THIS, unmodified. Do NOT edit dependencies/re-lock —
                              # that drifts lib versions across machines (e.g. a different ultralytics)
                              # and changes training results. The lock pins everyone to the same stack.
# Legacy host (e.g. epoch8 gpu5: pre-AVX2 CPU) ONLY — extras + force the lts polars to win:
uv sync --extra old-cpu
uv pip uninstall polars polars-lts-cpu && uv pip install polars-lts-cpu==1.33.1
cd detection
# DB_SCHEMA defaults to `public` (already exists). Only if you set a dedicated schema (to share the
# Postgres with other pipelines) create it first:  psql "$DB_URL" -c "CREATE SCHEMA IF NOT EXISTS $DB_SCHEMA"
datapipe db create-all
```

Then bring up the **datapipe-app UI front** — it serves the pipeline graph, the observability panels
(training status/curves + the `cat_dog` ops-spec with the `model_metrics` and `tag_metrics`
tables), and the **run triggers** you use to launch training:

```bash
# from examples/detection_tags/detection, .env sourced; run it under tmux/nohup so it survives ssh drops
datapipe --pipeline app api --host 127.0.0.1 --port 8000
```

Bind to `127.0.0.1` (not `0.0.0.0`) and reach it over an SSH tunnel `-L 8000:localhost:8000`; open
`http://localhost:8000`. The `app.add_specs([...])` block in `detection/app.py` is what registers the
`cat_dog` spec the front renders.

## Part 1 — set up + load + verify, then STOP (you trigger model-A training from the UI)

The skill's job in part 1: load the baseline data (val frozen up front) and **verify it's in the
pipe** — then stop. Training is **yours to trigger from the datapipe-app front** (port 8000).

```bash
# SKILL DOES — from examples/detection_tags/detection, with .env sourced:
python ../scripts/add_request.py --id base-train --n 400 --offset 0   --subset train
python ../scripts/add_request.py --id base-val   --n 150 --offset 400 --subset val
python ../scripts/add_request.py --id night-val  --n 150 --offset 550 --subset val --tag night --darken 0.40
datapipe step --labels=stage=load run                 # 700 images; val frozen (150 base + 150 night)

# VERIFY the data is in the pipe before handing off:
#   SELECT subset_id, count(*) FROM image__subset_hint GROUP BY subset_id      -- expect train=400, val=300
#   SELECT count(*) FROM image__ground_truth                                   -- expect 700
```

**Stop here and hand off.** You now trigger **model-A training from the datapipe-app front** (port
8000) — the `stage=train` steps in the graph (split→freeze→train→inference). Watch it in the
observability panels (training status/curves). When it finishes, compute metrics + the FiftyOne view
(from the UI, or the CLI equivalents):

```bash
# CLI equivalent (no-UI) of what you trigger in the front:
datapipe step --labels=stage=train run                # split -> freeze -> train model A -> inference
datapipe step --labels=stage=count-metrics run        # metrics_on_image/subset + tag_metrics
#   count-metrics can print "Batches to process 0" right after training — RE-RUN it once. (Troubleshooting.)
datapipe step --labels=stage=fiftyone run             # download_images -> GT + model-A predictions
# (demo-only) snapshot the post-A state to rehearse part 2 later:
docker exec <pg> pg_dump -U postgres -n "$DB_SCHEMA" postgres > /tmp/checkpoint.sql
```

**After model A, hand the baseline over:**
- show the **full** metric tables (`model_metrics` / `metrics_on_subset` + `tag_metrics`) — in the
  front's `cat_dog` spec or via the RULE query below; `tag_metrics` for model A at
  `night/val` is **low** (baseline blind in the dark);
- point the user at the **datapipe-app front** (graph + observability + the two metric tables) and the
  **FiftyOne App** (GT vs model-A predictions) — give the addresses / tunnels (see "Let the user watch");
- then ask whether to proceed to part 2 (retrain). That's the problem part 2 fixes.

Surface every epoch from the training log (it streams to a file, e.g. `/tmp/train_A.log`):
```bash
grep -oE "[0-9]+/[0-9]+ +[0-9.]+G +[0-9.]+ +[0-9.]+ +[0-9.]+" /tmp/train_A.log   # per-epoch box/cls/dfl loss
grep -E "^ +all +[0-9]+ +[0-9]+" /tmp/train_A.log                                # per-epoch val P/R/mAP50/mAP50-95
```

## Part 2 — skill tops up the tagged batch, then you retrain model B from the UI

The skill **tops up** the tagged TRAIN batch and reloads; **you trigger the retrain from the front**.

```bash
# SKILL DOES — add the tagged TRAIN batch and load it (val stays frozen):
python ../scripts/add_request.py --id night-train-a --n 100 --offset 700 --subset train --tag night --darken 0.30
python ../scripts/add_request.py --id night-train-b --n 100 --offset 800 --subset train --tag night --darken 0.40
python ../scripts/add_request.py --id night-train-c --n 100 --offset 900 --subset train --tag night --darken 0.55
datapipe step --labels=stage=load run                 # 1000 images total
# VERIFY val stayed frozen (night went to TRAIN only):
#   SELECT h.subset_id, count(*) FROM image__tag t
#     JOIN image__subset_hint h USING(image_name) WHERE t.tag_id='night' GROUP BY h.subset_id
#   -- expect train=300, val=150
```

**Stop here and hand off — you trigger model-B training from the datapipe-app front** (`stage=train`;
night is now in TRAIN, val unchanged). Then metrics + FiftyOne (front or CLI equivalent):

```bash
datapipe step --labels=stage=train run                # model B (night in training)
datapipe step --labels=stage=count-metrics run        # re-run if "0 batches"
datapipe step --labels=stage=fiftyone run             # adds predictions_model_b
```

`night/val` recall rises from model A to model B — the payoff. Show the full tables again (in the
front's `cat_dog` metrics tables, or the RULE query below).

## Rehearse part 2 from the snapshot (demo-only)

Restore the post-A snapshot and wipe the FiftyOne db, then re-run Part 2 — no retraining of model A,
no image re-download:

```bash
docker exec <pg> psql -U postgres -c "DROP SCHEMA IF EXISTS $DB_SCHEMA CASCADE; CREATE SCHEMA $DB_SCHEMA"
docker exec -i <pg> psql -U postgres < /tmp/checkpoint.sql
docker exec <mongo> mongosh --quiet --eval "db.getSiblingDB('fiftyone').dropDatabase()"
```

## RULE: always print the FULL metrics table

When you show metrics, dump the **whole** table, not a truncated view. Both of these, every time:

```bash
docker exec <pg> psql -U postgres -x -c \
  "SELECT * FROM $DB_SCHEMA.pipeline_model__metrics_on_subset ORDER BY detection_model_id, subset_id"
docker exec <pg> psql -U postgres -x -c \
  "SELECT * FROM $DB_SCHEMA.pipeline_model__metrics_by_tag_on_subset ORDER BY detection_model_id, tag_id, subset_id"
```

Metrics live in `pipeline_model__metrics_on_subset` (overall, shown as **Model metrics** in the front)
and `pipeline_model__metrics_by_tag_on_subset` (per-tag, **Tag metrics**). `calc__support` is the GT
**box** count (≥ image count — an image can hold several cat/dog boxes); recall/precision have
`weighted`/`macro` variants. `tag_id` **is the tag name itself** (text, e.g. `night`) — no numeric
surrogate, no join. The `tag` dimension is two columns: `tag_id` (name) + `tag_description`.

The payoff: `pipeline_model__metrics_by_tag_on_subset` at `tag=night`, model A vs B. The **reliable
signal is night/train** (rises sharply once night is in training). **night/val is directional** — it
rises only if the model *generalizes* the low-light scenario (needs enough night-train + epochs);
with too few, model B memorizes night-train and night/val stays flat.

## Let the user watch (offer it — work out the specifics per setup)

The user watches (and drives) several things; make them available and hand over whatever access they
need — don't prescribe fixed commands here and don't leave a committed file behind (it would bake in
one machine's host/ports/paths and mislead on the next). Compute the specifics for the ACTUAL setup at
runtime (local vs remote host, which ports are free, tunnels if remote) and give the user the commands
directly in chat:

- **the datapipe-app front** (`datapipe --pipeline app api --host 127.0.0.1 --port 8000`, tunnel 8000)
  — the pipeline graph, observability panels (training status/curves), the `cat_dog`
  ops-spec with the `model_metrics` + `tag_metrics` tables, and the **run triggers** the user presses
  to launch model A / model B. This is the primary surface in the UI setup.
- **tables** in DBeaver (Postgres → the `$DB_SCHEMA` schema): `pipeline_model__metrics_on_subset`,
  `pipeline_model__metrics_by_tag_on_subset`, `detection_training_status`.
- **images** in the FiftyOne App: `ground_truth` vs `predictions_model_a` vs `predictions_model_b`,
  filterable by `tag`/`subset`.
- **training progress**: the run streams to a log file — tail it; trust
  `detection_training_status.status`, not exit codes.

**When you START a training run, immediately tell the user how to follow it** (before it finishes),
so they can watch the epochs live — give the log path and the follow command, e.g.
`tail -f /tmp/train_A.log` (or the per-epoch `grep` from Part 1). Do this the moment training kicks
off, for model A and again for model B — not only after it completes.

When the host is remote these are reached over SSH tunnels; if a local tunnel port is busy, pick a
different left-hand port rather than killing whatever holds it.

## FiftyOne

`stage=fiftyone` downloads images locally then publishes to one dataset (`FIFTYONE_DATASET_NAME`,
metadata in MongoDB): fields `ground_truth`, `predictions_model_a` (baseline), `predictions_model_b`
(retrained), plus sample fields `tag` and `subset`. Ported straight from
`examples/e2e_template/image_detection` (same `download_images` + `publish_to_fiftyone` +
`FiftyOneImagesDataTableStore`); the two model fields are assigned by sorted model-id (earliest =
`model_a`). The FiftyOne App is a **docker-compose service** (`fiftyone` on :5151) — no manual
`fiftyone app launch`; after `stage=fiftyone`, open `http://localhost:5151` (tunnel 5151; local port
MUST equal remote — the App's WebSocket calls the same port).

`tag` and `subset` are ordinary **sample fields** (set in-pipeline by `publish_gt_to_fiftyone`, no
scripts) — NOT FiftyOne's native "sample tags". Filter by the **fields** in the sidebar: pick
`tag = night` and `subset = val` — different fields AND together, so you get exactly the night-val
set. (Selecting multiple values inside FiftyOne's native TAGS panel ORs them, which is why we do NOT
mirror tag/subset into native tags.) That isolates where model A misses in the dark; after part 2,
`predictions_model_b` shows model B catching it. Fields stay `predictions_model_a` (baseline,
earliest model id) / `predictions_model_b` (retrained); the exact model ids are in the DB tables.

## How to work

Propose a short plan and get a go-ahead; show each stage's logs and report what changed. Trust the
`*_training_status` table, not the exit code. On an unclear failure re-run with `datapipe --debug … run`
to a file + `grep`, not inline.

## Troubleshooting (verify against current files)

- **Getting the cache (fresh host)** → easiest: fetch the pre-built 1000-image cache from the public
  bucket (no auth, works from RU) into `$DATAPIPE_TAGS_CACHE_DIR`:
  ```bash
  BASE=https://storage.yandexcloud.net/e8-demo/datasets/coco-cat-dog-1000
  mkdir -p "$DATAPIPE_TAGS_CACHE_DIR/images" && cd "$DATAPIPE_TAGS_CACHE_DIR"
  curl -sf "$BASE/gt.json" -o gt.json
  python -c "import json; print('\n'.join(json.load(open('gt.json'))))" \
    | xargs -P 16 -I{} curl -sf -o "images/{}" "$BASE/images/{}"
  ls images | wc -l   # expect 1000
  ```
  Also on the bucket: `datasets/coco-cat-dog-500/` (500-image variant, byte-identical prefix of the
  same canonical order) and `datasets/coco-annotations/annotations_trainval2017.zip` (raw COCO
  annotations). Alternative on a COCO-reachable host: `python ../scripts/build_cache.py 1000` —
  byte-identical result (canonical order; gt.json md5 must match the bucket's). The cache caps the
  pool at its size, so keep the batch total ≤ N. From RU `images.cocodataset.org` is unreachable —
  use the bucket.
- **`SIGILL` / `Illegal instruction` in training** → `polars` built for a newer CPU than the host
  (pre-AVX2). The `polars-lts-cpu` pin isn't enough alone; the regular `polars` comes in transitively.
  After `uv sync`: `uv pip uninstall polars polars-lts-cpu && uv pip install polars-lts-cpu==1.33.1`,
  then verify `python -c "import polars; print(polars.__version__)"` is the lts one.
- **`No labels found` / every image "corrupt: No module named 'pi_heif'`** → reinstall `pi-heif`.
- **count-metrics prints "Batches to process 0" right after training** → datapipe hasn't propagated
  the fresh predictions in the same pass. Re-run `count-metrics` once; then the metric tables fill.
- **Model B didn't retrain / trains on the OLD frozen dataset** → the freeze step has a time debounce:
  `DetectionFreezeDataset(min_within_time=...)` refuses to create a new frozen dataset if the last one is
  younger than that ("Not enough time passed since last frozen dataset" in the log). The demo app sets
  `min_within_time="1s"` so part 2 retrains immediately; if you see the error (e.g. a config with "5min"),
  either lower it or wait it out, then re-run `stage=train`. Verify via
  `detection_frozen_dataset__train_images_count` growing (a second `detection_frozen_dataset` row).
- **Two identical models get trained** → you ran `stage=train-prepare` and then `stage=train`
  separately; the split between them changes `image__subset`, which re-freezes the dataset and
  retrains. Run `stage=train` as one step (it includes prepare), and show the split with a query.
- **`kill -0 <PID>` waiters, not `pgrep -f "<cmd string>"`** → a `pgrep`/`until` loop whose own
  command line contains the pattern (e.g. `stage=load`, `uv sync`, `bin/datapipe step`) matches
  ITSELF and never exits. Wait on the captured PID instead.
- **Read-only home on the cluster** (e.g. gpu5 `ml`) → put the repo, `HOME`, uv cache, and FiftyOne
  local images under a writable path like `/var/tmp`; `/tmp` and `/var/tmp` are writable.
- **Flaky link to the cluster** (Moscow/RU hosts) → SSH may time out transiently; retry.
- **Metrics 0 on a trained model** → tiny/noisy val latches an early "best" checkpoint; use enough
  data (~450 total) and the shipped epoch config.
- **Training exits 0 but no model** → datapipe swallows step errors; check `detection_training_status`.

## Real data

**Skip the two-part / checkpoint / rehearse machinery — that's demo-only.** For real data just run the
stages end to end (`load` → `train` → `count-metrics` → optionally `fiftyone`); no artificial stop, no
snapshot/restore. `--darken` is also demo-only (it synthesizes the low-light scenario from normal COCO
images — real data has real tagged images, so drop it). Still keep **val frozen** (pin `--subset`) —
that's about metric correctness, not the demo.

Don't guess; gather up front and wire the loader to the real source instead of the COCO `load_batch`:
images + where boxes/labels come from (a labelled set to inject, or real annotation); real class
names (set `DETECTION_CLASSES` / GT labels to match exactly, casing!); the tag/scenario and which
images carry it; storage + DB (`DATAPIPE_TAGS_DIR`, `DB_URL`/`DB_SCHEMA`); GPU + enough data/epochs
that metrics are meaningful.

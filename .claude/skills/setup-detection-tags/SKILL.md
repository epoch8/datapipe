---
name: setup-detection-tags
description: >
  Use when setting up or running examples/detection_tags — a self-contained datapipe detection
  demo built around tags (per-scenario metrics), split into two parts around a checkpoint (train a
  baseline, then add a tagged TRAIN batch and retrain, watching the tag metric rise), with a
  FiftyOne view and injected ground truth (no Label Studio). Also for "add a tagged batch, retrain,
  watch the tag metric rise" and for rehearsing that retraining demo from a saved checkpoint.
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

Batches (COCO cat/dog; the pre-staged cache holds **500 images**, so keep the total ≤ 500 or expand
the cache — see Troubleshooting):

| batch | n | offset | subset | tag | darken | when |
|-------|---|--------|--------|-----|--------|------|
| `base-train` | 325 | 0   | train | —     | —    | part 1 |
| `base-val`   | 100 | 325 | val   | —     | —    | part 1 |
| `night-val`  | 25  | 425 | val   | night | 0.25 | part 1 |
| `night-train`| 50  | 450 | train | night | 0.25 | part 2 |

Validated on gpu5: model A → train recall ~0.95, night/val recall ~0.09 (blind in the dark); after
part 2, model B → night/train recall ~0.86. Keep night/val small but not tiny (25 imgs / ~45 GT) —
below ~20 the val payoff is pure ±1-box noise and can even go the wrong way.

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
docker compose up -d          # postgres + minio + mongo (mongo is for FiftyOne). No Label Studio.
uv sync                       # cu124 torch + datapipe-ml[torch,fiftyone] + fiftyone + pi-heif
# On a pre-AVX2 host, force the lts polars to win (else the training subprocess SIGILLs):
uv pip uninstall polars polars-lts-cpu && uv pip install polars-lts-cpu==1.33.1
cd detection
# DB_SCHEMA defaults to `public` (already exists). Only if you set a dedicated schema (to share the
# Postgres with other pipelines) create it first:  psql "$DB_URL" -c "CREATE SCHEMA IF NOT EXISTS $DB_SCHEMA"
datapipe db create-all
```

## Part 1 — baseline to checkpoint

Run `stage=train` as ONE step (it internally does split→freeze→train→inference); surface each stage
from its log and by querying the tables after — do NOT invoke `stage=train-prepare` separately before
`stage=train`, or the intermediate re-freeze retrains the model twice (see Troubleshooting).

```bash
# from examples/detection_tags/detection, with .env sourced
python ../scripts/add_request.py --id base-train --n 325 --offset 0   --subset train
python ../scripts/add_request.py --id base-val   --n 100 --offset 325 --subset val
python ../scripts/add_request.py --id night-val  --n 25  --offset 425 --subset val --tag night --darken 0.25
datapipe step --labels=stage=load run                 # 450 images; val frozen (100 base + 25 night)
# show the frozen split:  SELECT subset_id, count(*) FROM image__subset_hint GROUP BY subset_id

datapipe step --labels=stage=train run                # split -> freeze -> train model A -> inference (SHOW every epoch)
datapipe step --labels=stage=count-metrics run        # metrics_on_image/subset + tag_metrics
#   count-metrics can print "Batches to process 0" right after training (it hasn't seen the fresh
#   predictions yet) — RE-RUN it once; then the tables fill. (See Troubleshooting.)

# SHOW THE FULL TABLES (see the Rule below). To rehearse part 2 later, snapshot the post-A state
# BEFORE the fiftyone stage (demo-only convenience; skip for real use):
docker exec <pg> pg_dump -U postgres -n "$DB_SCHEMA" postgres > /tmp/checkpoint.sql

datapipe step --labels=stage=fiftyone run             # download_images -> GT + model-A predictions
```

**Stop here (end of stage 1) and hand the baseline over to the user:**
- show the full training log (all epochs) and the **full** metric tables (`metrics_on_subset` +
  `tag_metrics`) — `tag_metrics` for model A at `night/val` is **low** (baseline blind in the dark);
- bring up the **FiftyOne App** service so they can browse GT vs model-A predictions, and give them
  its address / how to reach it (see "Let the user watch" — specifics depend on the setup);
- then ask whether to proceed to part 2 (retrain). That's the problem part 2 fixes.

Surface every epoch from the training log (it streams to a file, e.g. `/tmp/train_A.log`):
```bash
grep -oE "[0-9]+/[0-9]+ +[0-9.]+G +[0-9.]+ +[0-9.]+ +[0-9.]+" /tmp/train_A.log   # per-epoch box/cls/dfl loss
grep -E "^ +all +[0-9]+ +[0-9]+" /tmp/train_A.log                                # per-epoch val P/R/mAP50/mAP50-95
```

## Part 2 — retrain and watch the tag metric rise

```bash
python ../scripts/add_request.py --id night-train --n 50 --offset 450 --subset train --tag night --darken 0.25
datapipe step --labels=stage=load run                 # 500 images total
datapipe step --labels=stage=train run                # split (night -> TRAIN, val UNCHANGED) + train model B (SHOW every epoch)
# verify val stayed frozen after the split:
#   SELECT subset_id, count(*) FILTER (WHERE image_name LIKE '%night%') night, count(*) total
#   FROM image__subset s JOIN image__ground_truth USING(image_name) GROUP BY subset_id
datapipe step --labels=stage=count-metrics run        # re-run if "0 batches"
datapipe step --labels=stage=fiftyone run             # adds predictions_model_b
```

`night/val` recall rises from model A to model B — the payoff. Show the full tables again.

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
  "SELECT * FROM $DB_SCHEMA.detection_model_train__metrics_on_subset ORDER BY detection_model_id, subset_id"
docker exec <pg> psql -U postgres -x -c \
  "SELECT tm.*, t.tag_name FROM $DB_SCHEMA.tag_metrics tm JOIN $DB_SCHEMA.tag t USING(tag_id) \
   ORDER BY tm.detection_model_id, tm.tag_id, tm.subset_id"
```

`tag_id` is a numeric surrogate key (deterministic map in `config.TAG_IDS`, e.g. `night`→1); the
`tag` dimension holds `tag_name`/`tag_description`, so join it to show the readable name.

The payoff comparison: `tag_metrics` at `tag=night, subset=val`, model A vs model B — recall rises.
The night/val set is small (25 images), so treat the rise as **directional**.

## Let the user watch (offer it — work out the specifics per setup)

The user can watch three things; make them available and hand over whatever access they need — don't
prescribe fixed commands here and don't leave a committed file behind (it would bake in one machine's
host/ports/paths and mislead on the next). Compute the specifics for the ACTUAL setup at runtime
(local vs remote host, which ports are free, tunnels if remote) and give the user the commands
directly in chat:

- **tables** in DBeaver (Postgres → the `$DB_SCHEMA` schema): `tag_metrics`,
  `detection_model_train__metrics_on_subset`, `detection_training_status`.
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
`model_a`). Launch: `fiftyone app launch "$FIFTYONE_DATASET_NAME" --port 5151 --address 0.0.0.0`,
tunnel 5151 (local port MUST equal remote port — the App's WebSocket calls the same port).

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

- **Pre-staged cache is exactly 500 images** → `gt.json` in the cache dir caps the pool. An `offset`
  past 500 yields an empty batch silently. Keep the batch total ≤ 500, or rebuild the cache larger
  (or point `DATAPIPE_TAGS_CACHE_DIR` at a fresh dir to force a full COCO download).
- **`SIGILL` / `Illegal instruction` in training** → `polars` built for a newer CPU than the host
  (pre-AVX2). The `polars-lts-cpu` pin isn't enough alone; the regular `polars` comes in transitively.
  After `uv sync`: `uv pip uninstall polars polars-lts-cpu && uv pip install polars-lts-cpu==1.33.1`,
  then verify `python -c "import polars; print(polars.__version__)"` is the lts one.
- **`No labels found` / every image "corrupt: No module named 'pi_heif'`** → reinstall `pi-heif`.
- **count-metrics prints "Batches to process 0" right after training** → datapipe hasn't propagated
  the fresh predictions in the same pass. Re-run `count-metrics` once; then `metrics_on_image/subset`
  and `tag_metrics` fill.
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

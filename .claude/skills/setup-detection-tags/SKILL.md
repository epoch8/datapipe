---
name: setup-detection-tags
description: >
  Use when setting up or running examples/detection_tags — a self-contained datapipe detection
  demo built around tags (per-scenario metrics), split into two parts around a checkpoint (train a
  baseline, then add a tagged TRAIN batch and retrain, watching the tag metric rise), served under the
  datapipe-app UI front (graph + observability + the detection_tags_yolo ops-spec; training triggered
  from the UI) with a FiftyOne view and injected ground truth (no Label Studio). Also for "add a tagged
  batch, retrain, watch the tag metric rise" and for rehearsing that retraining demo from a saved checkpoint.
---

# detection_tags (tags demo — two-part, FiftyOne, no Label Studio)

Detection pipeline whose whole point is **tags**: train a baseline (model A), then add a **tagged
scenario TRAIN batch** and retrain (model B), and watch recall on that tag rise in
`pipeline_model__metrics_by_tag_on_subset`. Ground truth is **injected** (COCO cat/dog via
`coco_demo`, lowercase labels) — no Label Studio. Metrics use two **`CountMetrics_Subset_PipelineModel`**
steps (subset + tag arc); ops-spec id **`detection_tags_yolo`**.

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
HOST_UID=$(id -u) HOST_GID=$(id -g) docker compose up -d   # postgres + minio + mongo + fiftyone (:5151). No Label Studio.
uv sync                       # cu124 torch + datapipe-ml[torch,fiftyone] (+ pi-heif if needed)
# On a pre-AVX2 host, force the lts polars to win (else the training subprocess SIGILLs):
uv pip uninstall polars polars-lts-cpu && uv pip install polars-lts-cpu==1.33.1
cd detection
# DB_SCHEMA defaults to `public` (already exists). Only if you set a dedicated schema (to share the
# Postgres with other pipelines) create it first:  psql "$DB_URL" -c "CREATE SCHEMA IF NOT EXISTS $DB_SCHEMA"
datapipe db create-all
```

Then bring up the **datapipe-app UI front** — it serves the pipeline graph, the observability panels
(training status/curves + the `detection_tags_yolo` ops-spec with `model_metrics`,
`tag_metrics_on_subset`, and the two **class_metrics** tables), and the **run triggers** you use to
launch training:

```bash
# from examples/detection_tags/detection, .env sourced; run it under tmux/nohup so it survives ssh drops
datapipe --pipeline app api --host 127.0.0.1 --port 8000
```

Bind to `127.0.0.1` (not `0.0.0.0`) and reach it over an SSH tunnel `-L 8000:localhost:8000`; open
`http://localhost:8000`. The `app.add_specs([...])` block in `detection/app.py` is what registers the
`detection_tags_yolo` spec the front renders.

## Part 1 — set up + load + verify, then STOP (you trigger model-A training from the UI)

The skill's job in part 1: load the baseline data (val frozen up front) and **verify it's in the
pipe** — then stop. Training is **yours to trigger from the datapipe-app front** (port 8000).

```bash
# SKILL DOES — from examples/detection_tags/detection, with .env sourced:
python ../scripts/add_request.py --id base-train --n 325 --offset 0   --subset train
python ../scripts/add_request.py --id base-val   --n 100 --offset 325 --subset val
python ../scripts/add_request.py --id night-val  --n 25  --offset 425 --subset val --tag night --darken 0.25
datapipe step --labels=stage=load run                 # 450 images; val frozen (100 base + 25 night)

# VERIFY the data is in the pipe before handing off:
#   SELECT subset_id, count(*) FROM image__subset_hint GROUP BY subset_id      -- expect train=325, val=125
#   SELECT count(*) FROM image__ground_truth                                   -- expect 450
```

**Stop here and hand off.** You now trigger **model-A training from the datapipe-app front** (port
8000) — the `stage=train` steps in the graph (split→freeze→train→inference). Watch it in the
observability panels (training status/curves). When it finishes, compute metrics + the FiftyOne view
(from the UI, or the CLI equivalents):

```bash
# CLI equivalent (no-UI) of what you trigger in the front:
datapipe step --labels=stage=train run                # split -> freeze -> train model A -> inference
datapipe step --labels=stage=count-metrics run        # subset metrics + tag metrics (two PipelineModel steps)
#   count-metrics can print "Batches to process 0" right after training — RE-RUN it once. (Troubleshooting.)
datapipe step --labels=stage=fiftyone run             # download → images → annotations → model_a → model_b
# (demo-only) snapshot the post-A state to rehearse part 2 later:
docker exec <pg> pg_dump -U postgres -n "$DB_SCHEMA" postgres > /tmp/checkpoint.sql
```

**After model A, hand the baseline over:**
- show the **full** metric tables (`model_metrics` → `pipeline_model__metrics_on_subset`;
  `tag_metrics_on_subset` → `pipeline_model__metrics_by_tag_on_subset`; plus class tables
  `pipeline_model__metrics_by_cls_on_subset` / `pipeline_model__metrics_by_tag_by_cls_on_subset`)
  — in the front's `detection_tags_yolo` spec or via the RULE query below; `tag_id=night, subset_id=val`
  recall for model A is **low** (baseline blind in the dark);
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
python ../scripts/add_request.py --id night-train --n 50 --offset 450 --subset train --tag night --darken 0.25
datapipe step --labels=stage=load run                 # 500 images total
# VERIFY val stayed frozen (night went to TRAIN only):
#   SELECT h.subset_id, count(*) FROM image__tag it
#     JOIN image__subset_hint h USING(image_name) WHERE it.tag_id='night' GROUP BY h.subset_id
#   -- expect train=50, val=25
```

**Stop here and hand off — you trigger model-B training from the datapipe-app front** (`stage=train`;
night is now in TRAIN, val unchanged). Then metrics + FiftyOne (front or CLI equivalent):

```bash
datapipe step --labels=stage=train run                # model B (night in training)
datapipe step --labels=stage=count-metrics run        # re-run if "0 batches"
datapipe step --labels=stage=fiftyone run             # adds predictions_model_b
```

`night/val` recall rises from model A to model B — the payoff. Show the full tables again (in the
front's `detection_tags_yolo` metrics tables, or the RULE query below).

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

`tag_id` **is the tag name itself** (text, e.g. `night`) — no numeric surrogate, no join needed. The
`tag` dimension is two columns: `tag_id` (name) + `tag_description` (readable, notes darkening).

The payoff comparison: `pipeline_model__metrics_by_tag_on_subset` at `tag_id=night, subset_id=val`,
model A vs model B — `calc__weighted_recall` / `calc__weighted_f1_score` rise. FindBestModel also uses
`calc__weighted_f1_score` on `pipeline_model__metrics_on_subset`.
The night/val set is small (25 images), so treat the rise as **directional**.

## Let the user watch (offer it — work out the specifics per setup)

The user watches (and drives) several things; make them available and hand over whatever access they
need — don't prescribe fixed commands here and don't leave a committed file behind (it would bake in
one machine's host/ports/paths and mislead on the next). Compute the specifics for the ACTUAL setup at
runtime (local vs remote host, which ports are free, tunnels if remote) and give the user the commands
directly in chat:

- **the datapipe-app front** (`datapipe --pipeline app api --host 127.0.0.1 --port 8000`, tunnel 8000)
  — the pipeline graph, observability panels (training status/curves), the `detection_tags_yolo`
  ops-spec with `model_metrics` + `tag_metrics_on_subset` + **class_metrics** (`subset_class_metrics`,
  `tag_class_metrics_on_subset`), and the **run triggers** the user presses
  to launch model A / model B. This is the primary surface in the UI setup.
- **tables** in DBeaver (Postgres → the `$DB_SCHEMA` schema): `pipeline_model__metrics_on_subset`,
  `pipeline_model__metrics_by_tag_on_subset`, `pipeline_model__metrics_by_cls_on_subset`,
  `detection_training_status`.
- **images** in the FiftyOne App: `annotations` vs `predictions_model_a` vs `predictions_model_b`,
  filterable by sample fields `tag_id` / `subset_id`.
- **training progress**: the run streams to a log file — tail it; trust
  `detection_training_status.status`, not exit codes.

**When you START a training run, immediately tell the user how to follow it** (before it finishes),
so they can watch the epochs live — give the log path and the follow command, e.g.
`tail -f /tmp/train_A.log` (or the per-epoch `grep` from Part 1). Do this the moment training kicks
off, for model A and again for model B — not only after it completes.

When the host is remote these are reached over SSH tunnels; if a local tunnel port is busy, pick a
different left-hand port rather than killing whatever holds it.

## FiftyOne

`stage=fiftyone` follows **`examples/e2e_template/image_detection`** (same step order), with an
A/B twist: baseline vs retrained predictions in one dataset (`FIFTYONE_DATASET_NAME`, metadata in
MongoDB).

Pipeline steps (`detection/steps.py` + `detection/app.py` wiring):

1. `download_images` → `local_images`
2. `publish_to_fiftyone` → `fiftyone_images` (field `images`)
3. `publish_to_fiftyone_ground_truth` → `fiftyone_annotations` (field `annotations`)
   - Inputs: `local_images` + `Required(image__ground_truth|subset|tag)` (inner join on annotated images).
   - Merges GT with `local_images` only; `subset_id` / `tag_id` come from dict lookups on
     `image__subset` / `image__tag` — missing → `"none"` (do not left-merge subset/tag onto GT).
4. `publish_to_fiftyone_predictions_baseline` → `fiftyone_predictions_model_a` (field
   `predictions_model_a`)
5. `publish_to_fiftyone_predictions_retrained` → `fiftyone_predictions_model_b` (field
   `predictions_model_b`)

**Prediction publish wiring (important):** both steps use
`inputs=[Required("local_images"), Required("detection_prediction_train")]` and
`transform_keys=["image_name", "detection_model_id"]`. That inner-joins images to rows that actually
have train predictions — a full join on all `local_images` puts `NaN` in `detection_model_id` for
images without predictions (e.g. some `*__night.jpg` in val-only) and Postgres errors with
`varchar = double precision`. Input order must stay `local_images` first, then
`detection_prediction_train` — it matches `(images_df, predictions_df)` in `steps.py`.

**A/B model assignment:** earliest sorted `detection_model_id` → baseline (A,
`detection_model_id_a`); **every later** id → retrained (B, `detection_model_id_b`) — `ids[1:]`, not
just the second model. With 3+ models, B publishes all non-baseline models (same as the old
`publish_predictions_to_fiftyone` slot behaviour).

Stores share one `fo_session` / dataset (`detection/data.py`). Separate `detection_model_id_a` /
`detection_model_id_b` sample fields avoid the two prediction stores clobbering each other.

**App UI:** `docker compose up` starts **`fiftyone`** (`voxel51/fiftyone:1.17.0` on **:5151**), wired
to the same MongoDB. Open **http://localhost:5151** (remote → SSH tunnel `-L 5151:localhost:5151`).
Like e2e: run `HOST_UID=$(id -u) HOST_GID=$(id -g) docker compose up -d` — compose includes
**minio-data-init** (root `chown` on `.docker-data/minio`) so MinIO can write as your uid even if
Docker created the bind mount as root. The service bind-mounts
`DATAPIPE_TAGS_TMP_DIR` (default `/tmp/datapipe-tags`) read-only; local images go to
`$DATAPIPE_TAGS_TMP_DIR/local_images` (override with `LOCAL_IMAGES_DIR` if needed).
The pipeline still uses the **`fiftyone` Python lib** from `datapipe-ml[fiftyone]` to publish samples;
only the App server runs in Docker (no host `fiftyone app launch` needed).

`tag_id` and `subset_id` are ordinary **sample fields** (set in GT publish via dict lookup on
`image__subset` / `image__tag`) — NOT FiftyOne's
native "sample tags". Filter in the sidebar: `tag_id = night` AND `subset_id = val` (field filters,
not native TAGS OR-semantics). After part 2, `predictions_model_b` shows model B catching boxes model A
missed in the dark.

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
  the fresh predictions in the same pass. Re-run `count-metrics` once; then `pipeline_model__metrics_*`
  tables fill (subset + tag + class metrics).
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
- **`stage=fiftyone` fails: `operator does not exist: character varying = double precision`** on
  `detection_prediction_train_meta` — batch index included `local_images` without matching
  predictions (`detection_model_id` = `NaN`). Fix: both prediction publish inputs must be
  `Required(...)` and `transform_keys` must include `detection_model_id` (see FiftyOne section).
  Re-run `datapipe step --labels=stage=fiftyone run` after fixing `app.py`.

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

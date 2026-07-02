# e2e_template — add-on: tags for per-scenario metrics

**An optional layer on top of the base pipeline** (not needed to run it — a bolt-on you add when you
want it).

**When you need it.** A new case shows up (e.g. "the model misses pallets in dark rooms"). You want
to: (1) label those images with a **tag**, (2) let part of them flow into training, (3) **measure the
model on that scenario separately** and compare old vs new — without touching the normal train/val.

This is a **datapipe-native recipe**. Below: what to add and how. It was run end-to-end on e2e
detection and checks out.

## Principle
- **Don't touch the split.** Scenario images get a tag and flow through the normal `image__subset` →
  part lands in `train` (the model learns them = "added to training"), part in the holdout.
  **"Add to training" = just tag the new images** — no extra logic.
- **A tag is just a label** (`image__tag`, many tags per image); it does NOT affect split/training.
- **Slice metrics by `(tag_id, subset_id)`** in a separate `tag_metrics` table; standard metrics untouched.
- **Old vs new model** — compare `tag_metrics` rows by eye (no automated gate).
- ⚠️ In the example the split yields only **`train`/`val`** (no `test`): holdout = `val` (or do a 3-way split).

## What to add (catalog + one step; example logic unchanged)
- `tag(tag_id PK, name)` — tag dictionary.
- `image__tag(image_name PK, tag_id PK)` — tags per image, **many per image**. Key = `image_name`
  (as in every e2e table). Both read via the standard `UpdateExternalTable`; who writes them is out of
  scope (UI / manual INSERT / script).
- `tag_metrics(detection_model_id PK, tag_id PK, subset_id PK, calc__precision, calc__recall,
  calc__f1_score, support, images_support)` — a new `BatchTransform` step.
  **Source — NOT the raw `detection_predictions`** (that's pre-annotation; the trained model's output is
  in `detection_prediction_train`). Cleanest: take the already-computed **per-image**
  `detection_model_train__metrics_on_image` (`image_name, detection_model_id, subset_id,
  calc__TP/FP/FN/support`), join `image__tag` on `image_name`, and **re-aggregate by
  `(detection_model_id, tag_id, subset_id)`** — same as `count_..._metrics_on_subset`, just with
  `tag_id` in the GROUP BY.
- **Leave alone:** `image__subset`/split, `..._metrics_on_subset`, `FindBestModel(subset_id="val")`, training.

## How to use
1. Label the scenario images → rows in `image__tag` (a tag, e.g. `dark_room`).
2. Run the pipeline **as usual** — tagged images go through the normal split, part to `train`
   (learned), part to `val` (holdout).
3. Read `tag_metrics`: the row `model=new, tag=dark_room, subset=val` against the same row for the old
   model (several versions live in `detection_model_train`, per-image metrics computed for each).

## Aggregation (core of the `tag_metrics` step)
The same can be checked one-off in SQL. ⚠️ Columns `calc__TP/FP/FN` were created in **mixed case** →
in Postgres quote them (`"calc__TP"`), otherwise `column ... does not exist`.
```sql
SELECT m.detection_model_id, t.tag_id, m.subset_id,
       count(*) AS images_support,
       sum(m."calc__TP")+sum(m."calc__FN")                                   AS support,
       sum(m."calc__TP")::numeric / NULLIF(sum(m."calc__TP")+sum(m."calc__FP"),0) AS precision,
       sum(m."calc__TP")::numeric / NULLIF(sum(m."calc__TP")+sum(m."calc__FN"),0) AS recall
FROM detection_model_train__metrics_on_image m
JOIN image__tag t USING (image_name)
WHERE t.tag_id = 'dark_room'
GROUP BY 1,2,3;
```

## Gotchas (datapipe-ml behavior)
- **Any custom script that drives datapipe-ml training must be guarded with `if __name__ == "__main__":`**
  — datapipe-ml launches YOLO training via multiprocessing **spawn**, so without the guard the child
  re-imports the module and crashes with `RuntimeError: An attempt has been made to start a new process
  before ... bootstrapping`. The stock `datapipe step ... stage=train run` is already guarded.
- **Post-training is not instant:** datapipe-ml syncs every epoch checkpoint to S3/MinIO and
  `collect_results`/select-best reads them back, so on a slow link this takes minutes — normal, not a
  hang (the process sits in `Sl`/`Dl`).
- **Don't carve up `image__subset`** to fake a "scenario" — the tag IS the slice; slice metrics by the
  tag and keep the split full, otherwise old and new models get measured on different holdouts.

## Demo scenario — how to reproduce (for an agent)

Goal: show that a model scores near-0 on a tagged scenario and, after the tagged images are added to
training, the metric on that scenario rises. Steps to recreate it end-to-end:

1. **Base data.** Seed a non-trivial cat/dog set (the 10-image smoke default is too small — the
   metric on a tiny val is noisy). `scripts/seed_sample_data.py` defaults to `--detection-limit 120`,
   which is enough for the pipeline's own metrics to work; `--keypoints-limit 0` for a detection-only
   demo. Images download from COCO — needs outbound internet; on a restricted node fetch them
   elsewhere and upload to MinIO. Read-only `/home` → set `DATAPIPE_CACHE_DIR` and `UV_*` under `/tmp`.
2. **Build the scenario.** Take ~40 of the already-labelled cat/dog images, make **darkened copies**
   (gamma ≈ 0.10 — 0.25 is too mild to actually stump the model), upload them, and **inherit the GT
   boxes/labels from each source image** (identical pixel size → identical boxes, so no annotation).
   Insert one `tag` row (e.g. `night`) and one `image__tag` row per darkened image; split them ~30
   train / ~10 val.
3. **Baseline A — scenario NOT in training.** Keep the scenario's val images in `image__subset` as
   `val`, but exclude its train images from `train`. Train through the repo's train flow → A scores
   near-0 on the tag.
4. **Model B — scenario IN training.** Add the scenario's train images to `image__subset` `train`, then
   retrain a fresh model. (Freeze is delta-gated: if it no-ops, bump those images' GT `update_ts` so it
   re-cuts a dataset.)
5. **Metrics.** Compute per-image TP/FP/FN on val (predict, match GT at IoU≥0.5 per class), join
   `image__tag`, and aggregate by `(detection_model_id, tag_id, subset_id)` into `tag_metrics`.
6. **Compare.** Read `tag_metrics` rows for `model=A` vs `model=B` at the **holdout** subset — B is
   higher. The e2e split yields only `train`/`val`, so the holdout is `val` (include `test` too if your
   split has one); ignore the `train` row (a model scores high on its own training data). For a visual,
   open FiftyOne filtered to the tag and show B detecting where A is empty.

**Note on checkpoint selection.** With enough data the pipeline's built-in metrics compute fine and the
tag arc shows up in `detection_model_train__metrics_on_subset` natively. On a *small* validation set the
"best epoch" pick can latch onto an early, noisy metric peak and publish an under-trained checkpoint — if
a model looks suspiciously empty, sanity-check against the final-epoch weights.

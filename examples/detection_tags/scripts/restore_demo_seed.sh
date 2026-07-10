#!/bin/bash
# Restore the reference demo state (models A/B + all metrics + run history) so the UI shows the
# EXACT reference numbers on ANY machine — no training, ~2 minutes. Live retraining stays possible
# afterwards, but live-training numbers vary per GPU (proven; see README).
#
# Prereqs: docker compose stack up, .env sourced, venv built, image cache present
# (scripts/build_cache.py — build off-RU and copy if the host can't reach COCO).
#
# Usage, from examples/detection_tags:   bash scripts/restore_demo_seed.sh [pg_container]
set -euo pipefail
PG=${1:-detection_tags-postgres-1}
HERE=$(cd "$(dirname "$0")/.." && pwd)

echo "== 1/3 restore Postgres schema from demo_seed/db.sql.gz (DROPS current public schema!) =="
docker exec "$PG" psql -U postgres -c "DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;"
gunzip -c "$HERE/demo_seed/db.sql.gz" | docker exec -i "$PG" psql -U postgres -q
echo "== 2/3 re-upload images to object storage (byte-identical, derived from the cache) =="
cd "$HERE/detection"
../.venv/bin/python ../scripts/seed_upload_images.py
echo "== 3/3 sanity =="
docker exec "$PG" psql -U postgres -tAc \
  "SELECT 'models='||count(*) FROM public.detection_model_train"
docker exec "$PG" psql -U postgres -tAc \
  "SELECT substring(detection_model_id,10,4)||' '||subset_id||' recall='||round(calc__weighted_recall::numeric,3) FROM public.pipeline_model__metrics_on_subset ORDER BY subset_id, detection_model_id"
echo "done — start/refresh the datapipe front; the cat_dog spec shows the reference metrics."
echo "(FiftyOne view: optionally run  ../.venv/bin/datapipe step --labels=stage=fiftyone run )"

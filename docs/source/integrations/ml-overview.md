# ML mental model

## Stages of a typical CV loop

```text
images (S3/files)
    → annotate (Label Studio / CVAT / seed COCO)
    → freeze dataset
    → train
    → infer
    → metrics
    → (optional) FiftyOne / Ops UI
```

Each arrow is usually one or more Datapipe steps. Incremental meta means:

- New images → annotation upload / inference wake up for those keys
- New model version → inference dirty for `model_id × image_id` grains
- Unchanged images + same model → skipped (same hash / `update_ts` story as core)

Re-read [Incremental Processing](../concepts/incremental-processing.md) if this feels magical — ML steps are still `PipelineStep` subclasses. Concrete class names: [datapipe-ml reference](./datapipe-ml.md).

## Which example to open

| Goal | Example |
|---|---|
| Metrics / tags without full LS | `examples/detection_tags/` |
| Full LS → train → metrics → FiftyOne | `examples/e2e_template/` |
| FiftyOne embeds | `examples/embedder_fiftyone/` |
| Multi-engine OCR | `examples/ocr/` |

`e2e_template` documents three templates: detection, keypoints, classification — same lifecycle, separate DB schemas.

## Concrete command sequence (`detection_tags`)

From the example README (do not invent flags — copy these paths):

```bash
cd examples/detection_tags
uv sync --extra ray
cp .env.example .env && set -a && source .env && set +a

# monorepo root — build Ops UI once
cd ../.. && yarn install && yarn workspace @datapipe/ui-ml build:package

cd examples/detection_tags
HOST_UID=$(id -u) HOST_GID=$(id -g) docker compose up -d
cd detection
datapipe db create-all

# Ops API
set -a && source ../.env && set +a
uv run datapipe --executor RayExecutor --pipeline app api --host 127.0.0.1 --port 8000

# Pipeline work (separate shell, same env)
python ../scripts/add_request.py --id base-train --n 400 --offset 0 --subset train
uv run datapipe --executor RayExecutor step --labels=stage=load run
uv run datapipe --executor RayExecutor step --labels=stage=train run
```

Full part-1 / part-2 flow (val freeze, night tags, FiftyOne): `examples/detection_tags/README.md`.

## Concrete command sequence (`e2e_template` detection)

```bash
cd examples/e2e_template
uv sync --extra ray
docker compose up
cp .env.example .env
# set LABEL_STUDIO_API_KEY (UI or scripts/label_studio_token.py)
set -a && source .env && set +a

cd ../.. && yarn install && yarn workspace @datapipe/ui-ml build:package

cd examples/e2e_template
uv run python scripts/seed_sample_data.py

cd image_detection
set -a && source ../.env && set +a
uv run datapipe db create-all
uv run datapipe --executor RayExecutor step --labels=stage=annotation run
# annotate in Label Studio, then re-run annotation stage; then:
uv run datapipe --executor RayExecutor step --labels=stage=train run
uv run datapipe --executor RayExecutor step --labels=stage=fiftyone run

# Ops agent
uv run datapipe --executor RayExecutor --pipeline app:app api --port 8001
```

Keypoints / classification use the same pattern with ports `8002` / `8003` and their own directories — see `examples/e2e_template/README.md`.

## Ops UI for ML

Install `datapipe-app[ml]`, build UI assets (`yarn workspace @datapipe/ui-ml build:package` or `make -C libs/datapipe-ui-ml build-package`), run `datapipe api`. See [Ops UI walkthrough](../ops/ui-walkthrough.md).

## Next

- [datapipe-ml module map](./datapipe-ml.md)
- [Label Studio](./label-studio.md)
- [CVAT](./cvat.md)
- [FiftyOne and OCR appendices](./appendices.md)

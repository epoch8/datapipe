# datapipe-ml

## Local test services

Some integration tests need MongoDB (FiftyOne) and MinIO (cloud storage). Config lives in [`tests/.env.test`](tests/.env.test); pytest loads it automatically via `conftest.py`.

From the monorepo root:

```bash
cd libs/datapipe-ml

docker compose --env-file tests/.env.test -f tests/docker-compose.yml up
```

This starts:

| Service | Port | Used by |
|---------|------|---------|
| `mongo` | 27017 | FiftyOne tests |
| `minio` | 9000 | cloud storage / S3 tests |
| `minio-setup` | — | creates bucket `datapipe-e2e` |

Stop services:

```bash
docker compose -f tests/docker-compose.yml down
```

## Run tests

Commands mirror [`.github/workflows/lib-datapipe-ml.yml`](../../.github/workflows/lib-datapipe-ml.yml). Run from `libs/datapipe-ml` unless noted.

Install dependencies (local, all extras):

```bash
uv pip install -e "../datapipe-core[s3fs,sqlite]"
uv pip install -e ".[tensorflow,torch,sqlite,fiftyone]" "pytest<8" pytest-cases boto3
```

### `test` — fast unit tests

```bash
pytest tests -m "not training and not slow and not e2e and not e2e_examples and not service_e2e and not cloud_storage and not tensorflow and not torch and not fiftyone"
```

### `test-fiftyone` — needs `mongo` from docker compose

```bash
pytest tests/test_utils_image_data_stores.py -m fiftyone
```

### `test-cloud-storage` — needs `minio` from docker compose

```bash
pytest tests -m "cloud_storage and service_e2e" -v
```

### `test-torch` — YOLO / Ultralytics (slow, downloads COCO)

Same marker as CI; run on host if `torch` extra is installed:

```bash
DATAPIPE_ML_DOWNLOAD_IMAGES=12 \
DATAPIPE_ML_DOWNLOAD_KEYPOINT_IMAGES=12 \
DATAPIPE_ML_DELETE_COCO_CACHE_AFTER_READ=1 \
pytest tests -m "torch and not sky_vast and not e2e_examples and not service_e2e and not cloud_storage"
```


### `test-tensorflow` — classification training (slow, downloads COCO)

```bash
DATAPIPE_ML_DOWNLOAD_IMAGES=12 \
DATAPIPE_ML_DOWNLOAD_KEYPOINT_IMAGES=12 \
DATAPIPE_ML_DELETE_COCO_CACHE_AFTER_READ=1 \
pytest tests -m "tensorflow and not sky_vast and not e2e_examples and not service_e2e and not cloud_storage"
```

### Sky/Vast integration tests

Needs `sky-vast` extra and a Vast API key. **Do not** put the key in tracked `tests/.env.test`.

```bash
cp tests/.env.test.local.example tests/.env.test.local
# edit tests/.env.test.local — this file is gitignored
pytest tests -m sky_vast -v -s --log-cli-level=INFO
```

One-off run without a local file:

```bash
DATAPIPE_ML_RUN_SKY_VAST=1 VAPI_API_KEY=... pytest tests -m sky_vast -v
```

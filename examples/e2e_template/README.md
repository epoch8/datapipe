# Datapipe E2E Template

This example contains end-to-end Datapipe pipeline templates:

- `image_detection` for bbox detection.
- `image_keypoints` for bbox + keypoints pose pipelines.

Both templates cover the same lifecycle:

1. Load images from S3-compatible storage.
2. Create/sync tasks and predictions in Label Studio.
3. Parse annotations back into Datapipe tables.
4. Freeze a tiny training dataset, train a model, run inference, and compute metrics.
5. Optionally publish predictions and annotations into FiftyOne.

The modules are import-safe: environment variables are read only when building an app from settings. This keeps
structural tests fast while allowing service-backed tests to run the full pipeline with local Docker services.

## Running

Detection:

```bash
cd examples/e2e_template/image_detection
datapipe db create-all
datapipe run
```

Keypoints:

```bash
cd examples/e2e_template/image_keypoints
datapipe db create-all
datapipe run
```

The service-backed test path expects local Postgres, Label Studio, and S3-compatible storage to be configured through
environment variables. See `.env.example`.


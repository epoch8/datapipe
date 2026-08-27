# Ops specs

Package: `datapipe-app-ml-ops` (spec types), `datapipe-app` (registry on `DatapipeAPI`).

Ops specs declare how the ML Ops UI maps pipeline tables to entities: data browser, frozen datasets, models, training runs, metrics, and cross-entity relations. Specs are **declarative** — they do not run pipeline steps by themselves; they tell the API/UI which tables and columns to expose and which run labels to use for actions like “Freeze new dataset” or “Launch training”.

---

## `app.add_specs`

Available on `DatapipeAPI` (via `OpsSpecsMixin` in `datapipe_app.app.ops_specs`).

```python
from datapipe_app import DatapipeAPI
from datapipe_app_ml_ops.ops.ops_specs import DatapipeOpsSpec

app = DatapipeAPI(ds, catalog, pipeline)
app.add_specs([DatapipeOpsSpec(id="cat_dog", title="Cat/Dog Detection", ...)])
```

### Behaviour

1. Lazily creates an `OpsSpecRegistry` on the app.
2. Registers each spec by `id` (`add_many`).
3. Validates specs against live `catalog` and `ds` (`strict=True`) — rolls back on failure.
4. Attaches the registry to `observability_registry` when present (ML plugin routes).

Retrieve registered specs: `app.get_specs()`.

Requirements:

- Install `datapipe-app-ml-ops` and serve with `datapipe-ui-ml` for ML panels.
- Table and column names in specs must match catalog tables populated by your pipeline steps.

---

## `DatapipeOpsSpec` overview

Module: `datapipe_app_ml_ops.ops.ops_specs`

Extends `OpsSpecBase` from `datapipe_app.ops.specs`.

### Top-level fields (`OpsSpecBase`)

| Field | Type | Default | Description |
|---|---|---|---|
| `id` | `str` | *(required)* | Unique spec id (URL segment, registry key). |
| `title` | `str` | *(required)* | Display title in Ops UI. |
| `description` | `str` | `""` | Longer description. |
| `icon` | `str` | `"box"` | Icon name for the spec card. |
| `color` | `str` | `"blue"` | Theme color. |
| `relations` | `Sequence[OpsRelationSpec]` | `[]` | Join tables between entities (for example model ↔ frozen dataset). |
| `metrics` | `Sequence[OpsMetricTableSpec]` | `[]` | Model-level metric tables. |
| `class_metrics` | `Sequence[OpsMetricTableSpec]` | `[]` | Per-class metric tables. |
| `tags` | `Sequence[str]` | `[]` | Free-form tags (`["yolo", "image"]`). |

### ML-specific fields (`DatapipeOpsSpec`)

| Field | Type | Description |
|---|---|---|
| `data` | `OpsDataSpec \| None` | Core data browser: tables, item/label/subset refs, optional `image_view`. |
| `frozen_dataset` | `OpsFrozenDatasetSpec \| None` | Snapshot list, split counts, “Freeze new dataset” action. |
| `model` | `OpsModelSpec \| None` | Model registry, best-model flag, prediction overlay view. |
| `training` | `OpsTrainingSpec \| None` | Training status, config registry, training requests. |

---

## Nested spec types

### `OpsDataSpec`

| Field | Description |
|---|---|
| `tables` | All tables exposed in the data section. |
| `item_table` | Primary entity table (for example images). |
| `label_table` | Ground-truth annotations. |
| `subset_table` | Train/val/test assignments. |
| `tag_table` | Optional tag dimension (`detection_tags` example). |
| `image_view` | `OpsImageDataSpec` — image grid, GT overlay, subset chips. |

### `OpsFrozenDatasetSpec`

| Field | Description |
|---|---|
| `table` | Frozen snapshot metadata table. |
| `id_column` | Snapshot id column. |
| `created_at_column` | Created timestamp. |
| `label_mode` | `"timestamp"`, `"id"`, or `"short_id"` for display. |
| `split_columns` | Map split name → count column (`train`, `val`, `test`). |
| `models_count_relation_id` | `OpsRelationSpec.id` for model count column. |
| `record_view` | `OpsImageRecordViewSpec` — images inside a snapshot. |
| `columns` | List / table column defs (`OpsColumn`). |
| `default_sort` | Default table sort. |
| `run_labels` | Pipeline labels for “Freeze new dataset” (for example `[("stage", "train-prepare")]`). Empty disables the action. |

### `OpsModelSpec`

| Field | Description |
|---|---|
| `table` | Model registry. |
| `id_column` | Model id. |
| `artifact_uri_column` | Weights path / URI. |
| `is_best_table` / `is_best_column` | Best-model flag (from `FindBestModel`). |
| `prediction_view` | `OpsImagePredictionViewSpec` — predictions vs GT, per-image metrics. |

### `OpsTrainingSpec`

| Field | Description |
|---|---|
| `status_table` | Training run status rows. |
| `artifact_columns` | Named artifact paths (`manifest`, `run_dir`, …). |
| `columns` | Training history table columns. |
| `experiments` | `OpsTrainConfigRegistrySpec` — built-in + custom YOLO configs. |
| `requests` | `OpsTrainingRequestSpec` — queue + UI launch labels. |

### `OpsTrainConfigRegistrySpec`

Maps `yolov8_train_config` (defaults) and `yolov8_custom_train_config` (API-owned) tables. Key fields: `table`, `default_table`, `id_column`, `params_column`, `config_type`, display/hash/active columns.

### `OpsTrainingRequestSpec`

Maps `detection_training_request` (or task equivalent). Key fields:

| Field | Description |
|---|---|
| `run_labels` | Labels passed to `datapipe run` when launching from UI (for example `[("stage", "train-without-freeze")]`). |
| `max_within_time` | UI freshness window matching the train step. |
| `status_table` | Join for run counts. |

### `OpsRelationSpec`

Links entities for navigation and counts:

```python
OpsRelationSpec(
    id="model_trained_on_frozen_dataset",
    table="detection_model_is_trained_on_detection_frozen_dataset",
    from_entity="model",
    from_column="detection_model_id",
    to_entity="frozen_dataset",
    to_column="detection_frozen_dataset_id",
)
```

### `OpsMetricTableSpec`

Declares a metrics table for the Ops UI: `primary_key_columns`, `entity_links`, `primary_columns`, `metric_columns`, `best_metric_column`, `default_filters`.

---

## Example: `cat_dog` detection spec

Both `examples/e2e_template/image_detection/app.py` and `examples/detection_tags/detection/app.py` register a spec with `id="cat_dog"`. Minimal skeleton:

```python
app.add_specs([
    DatapipeOpsSpec(
        id="cat_dog",
        title="Cat/Dog Detection",
        description="YOLO training pipeline over frozen image snapshots.",
        icon="shield",
        color="blue",
        data=OpsDataSpec(
            tables=["s3_images", "image__ground_truth", "image__subset", "detection_frozen_dataset", ...],
            item_table="s3_images",
            label_table="image__ground_truth",
            subset_table="image__subset",
            image_view=OpsImageDataSpec(
                image_table="s3_images",
                image_primary_key_columns=("image_name",),
                image_url_column="image_url",
                subset_table="image__subset",
                subset_join_columns={"image_name": "image_name"},
                ground_truth=OpsImageAnnotationSpec(
                    table="image__ground_truth",
                    primary_key_columns=("image_name",),
                    bboxes_column="bboxes",
                    labels_column="labels",
                    join_columns={"image_name": "image_name"},
                    role="gt",
                ),
                records_show_subset=True,
                records_show_ground_truth=True,
            ),
        ),
        frozen_dataset=OpsFrozenDatasetSpec(
            table="detection_frozen_dataset",
            id_column="detection_frozen_dataset_id",
            created_at_column="detection_frozen_dataset__created_at",
            split_columns={
                "train": "detection_frozen_dataset__train_images_count",
                "val": "detection_frozen_dataset__val_images_count",
                "test": "detection_frozen_dataset__test_images_count",
            },
            run_labels=[("stage", "train-prepare")],
        ),
        model=OpsModelSpec(
            table="detection_model",
            id_column="detection_model_id",
            artifact_uri_column="detection_model__model_path",
            is_best_table="attr__detection_model__is_best",
            is_best_column="detection_model__is_best",
        ),
        training=OpsTrainingSpec(
            status_table="detection_training_status",
            experiments=OpsTrainConfigRegistrySpec(
                table="yolov8_custom_train_config",
                default_table="yolov8_train_config",
                id_column="detection_train_config_id",
                params_column="detection_train_config__params",
                config_type="yolov8_detection",
            ),
            requests=OpsTrainingRequestSpec(
                table="detection_training_request",
                id_column="training_request_id",
                frozen_dataset_id_column="detection_frozen_dataset_id",
                train_config_id_column="detection_train_config_id",
                max_within_time="1w",
                run_labels=[("stage", "train-without-freeze")],
                status_table="detection_training_status",
            ),
        ),
        relations=[OpsRelationSpec(id="model_trained_on_frozen_dataset", ...)],
        metrics=[OpsMetricTableSpec(id="model_metrics", ...)],
        tags=["yolo", "image", "training"],
    )
])
```

### `detection_tags` differences

The tags example adds tag tables to `OpsDataSpec.tables`, extra metric tables (`pipeline_model__metrics_by_tag_on_subset`, …), and tag-aware filters — same spec id and entity wiring pattern.

---

## Checklist for a new ML Ops spec

1. Pipeline produces tables referenced in the spec (freeze, model, metrics, training status).
2. Column names match step output schemas (`detection_frozen_dataset__created_at`, …).
3. `run_labels` on `OpsFrozenDatasetSpec` / `OpsTrainingRequestSpec` match step `labels` in `app.py`.
4. `FindBestModel` output wired to `OpsModelSpec.is_best_table`.
5. Call `app.add_specs([...])` before `datapipe api`.

### See also

- [Ops overview](./index.md)
- [DatapipeApp and API](./datapipe-app.md)
- [ML steps reference](../reference/steps/ml/index.md)
- `examples/e2e_template/README.md` — Ops specs section

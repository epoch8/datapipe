from __future__ import annotations

from typing import Any, Optional

from datapipe.compute import Catalog
from datapipe.datatable import DataStore, DataTable
from datapipe.store.database import TableStoreDB


def table_schema_columns(dt: DataTable) -> list[str]:
    store = dt.table_store
    if isinstance(store, TableStoreDB):
        return [column.name for column in store.data_sql_schema]
    return []


def is_metrics_table(name: str, dt: DataTable) -> bool:
    if "training_status" in name:
        return False
    if "metrics_on_subset" in name or name.endswith("_metrics_on_subset"):
        return True
    if "metrics_on_" in name and "calc__" in "".join(table_schema_columns(dt)):
        return True
    columns = table_schema_columns(dt)
    return any(column.startswith("calc__") for column in columns) and "metrics" in name


def is_training_status_table(name: str) -> bool:
    return "training_status" in name


def infer_task_type(table_name: str) -> Optional[str]:
    lowered = table_name.lower()
    for task_type in ("detection", "keypoints", "segmentation", "classification"):
        if task_type in lowered:
            return task_type
    return None


def discover_metrics_tables(catalog: Catalog, ds: DataStore) -> list[tuple[str, DataTable]]:
    tables: list[tuple[str, DataTable]] = []
    for name in catalog.catalog:
        dt = ds.get_table(name)
        if is_metrics_table(name, dt):
            tables.append((name, dt))
    return tables


def discover_training_status_tables(catalog: Catalog, ds: DataStore) -> list[tuple[str, DataTable]]:
    tables: list[tuple[str, DataTable]] = []
    for name in catalog.catalog:
        if is_training_status_table(name):
            tables.append((name, ds.get_table(name)))
    return tables


def metric_columns(columns: list[str]) -> list[str]:
    return [column for column in columns if column.startswith("calc__")]


def model_id_column(columns: list[str]) -> Optional[str]:
    for column in columns:
        if column.endswith("_model_id"):
            return column
    if "model_id" in columns:
        return "model_id"
    return None


def row_model_id(row: Any, columns: list[str]) -> Optional[str]:
    model_col = model_id_column(columns)
    if model_col is None:
        return None
    value = row.get(model_col)
    if value is None or (hasattr(value, "__float__") and str(value) == "nan"):
        return None
    return str(value)


def is_frozen_dataset_table(name: str) -> bool:
    return name.endswith("_frozen_dataset") and "__" not in name


def is_model_frozen_dataset_link_table(name: str) -> bool:
    return "model_is_trained_on" in name and "frozen_dataset" in name


def frozen_dataset_id_column(columns: list[str]) -> Optional[str]:
    matches = [column for column in columns if column.endswith("_frozen_dataset_id")]
    return matches[0] if matches else None


def frozen_dataset_metadata_columns(columns: list[str]) -> dict[str, Optional[str]]:
    return {
        "created_at": next((c for c in columns if c.endswith("_frozen_dataset__created_at")), None),
        "train_count": next((c for c in columns if c.endswith("_frozen_dataset__train_images_count")), None),
        "val_count": next((c for c in columns if c.endswith("_frozen_dataset__val_images_count")), None),
        "test_count": next((c for c in columns if c.endswith("_frozen_dataset__test_images_count")), None),
    }

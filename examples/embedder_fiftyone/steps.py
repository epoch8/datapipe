from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Iterator

import numpy as np
import pandas as pd
from cv_pipeliner.core.data import ImageData
from cv_pipeliner.utils.fiftyone import FifyOneSession

from config import (
    BRAIN_METHODS,
    DEVICE,
    EMBEDDERS,
    FIFTYONE_DATASET_NAME,
    IMAGE_SUFFIXES,
    LABELS_JSON,
    LOCAL_IMAGES_DIR,
    ZOO_DATASET,
    ZOO_LABEL_FIELD,
    ZOO_NUM_CLASSES,
    use_local_images,
)
from models import MODELS_REGISTRY, get_embeddings_batch

logger = logging.getLogger(__name__)


def _delete_brain_run_if_exists(dataset, brain_key: str) -> None:
    if dataset.has_brain_run(brain_key):
        logger.info("Deleting existing brain run: %s", brain_key)
        dataset.delete_brain_run(brain_key)


def _delete_zoo_dataset_if_loaded(fo) -> None:
    if use_local_images() or ZOO_DATASET == FIFTYONE_DATASET_NAME:
        return
    if fo.dataset_exists(ZOO_DATASET):
        logger.info("Deleting zoo source dataset %s (copied to %s)", ZOO_DATASET, FIFTYONE_DATASET_NAME)
        fo.delete_dataset(ZOO_DATASET)


def _local_images_df() -> pd.DataFrame:
    assert LOCAL_IMAGES_DIR is not None
    files = [path for path in LOCAL_IMAGES_DIR.rglob("*") if path.is_file() and path.suffix.lower() in IMAGE_SUFFIXES]
    rows = [{"image_name": path.stem, "image_path": str(path.resolve())} for path in files]
    return pd.DataFrame(rows, columns=["image_name", "image_path"])


def _local_labels_df() -> pd.DataFrame:
    if not LABELS_JSON:
        return pd.DataFrame(columns=["image_name", "label"])
    labels_path = Path(LABELS_JSON)
    if not labels_path.exists():
        logger.warning("LABELS_JSON path does not exist: %s", labels_path)
        return pd.DataFrame(columns=["image_name", "label"])

    with labels_path.open("r", encoding="utf-8") as f:
        labels_data = json.load(f)
    rows = [{"image_name": str(image_name), "label": str(label)} for image_name, label in labels_data.items()]
    return pd.DataFrame(rows, columns=["image_name", "label"])


def _zoo_images_and_labels_df() -> tuple[pd.DataFrame, pd.DataFrame]:
    import fiftyone.zoo as foz

    dataset = foz.load_zoo_dataset(ZOO_DATASET)
    classes_to_leave = set(dataset.get_classes(ZOO_LABEL_FIELD)[-ZOO_NUM_CLASSES:])
    bad_sample_ids = [
        sample.id
        for sample in dataset
        if sample[ZOO_LABEL_FIELD] is None or sample[ZOO_LABEL_FIELD].label not in classes_to_leave
    ]
    if bad_sample_ids:
        dataset.delete_samples(bad_sample_ids)

    image_rows: list[dict] = []
    label_rows: list[dict] = []
    for sample in dataset:
        filepath = str(sample.filepath)
        image_name = Path(filepath).as_posix().replace("/", "__")
        label = str(sample[ZOO_LABEL_FIELD].label)
        image_rows.append({"image_name": image_name, "image_path": filepath})
        label_rows.append({"image_name": image_name, "label": label})

    logger.info(
        "Using zoo dataset %s: %d images, %d classes",
        ZOO_DATASET,
        len(image_rows),
        len(classes_to_leave),
    )
    return (
        pd.DataFrame(image_rows, columns=["image_name", "image_path"]),
        pd.DataFrame(label_rows, columns=["image_name", "label"]),
    )


def list_images_and_labels() -> Iterator[tuple[pd.DataFrame, pd.DataFrame]]:
    """Load local images+labels or fall back to FiftyOne zoo dataset."""
    if use_local_images():
        images_df = _local_images_df()
        labels_df = _local_labels_df()
        logger.info("Using local images from %s: %d images", LOCAL_IMAGES_DIR, len(images_df))
        if not labels_df.empty:
            logger.info("Loaded %d labels from %s", len(labels_df), LABELS_JSON)
        yield images_df, labels_df
    else:
        yield _zoo_images_and_labels_df()


def list_embedders() -> Iterator[pd.DataFrame]:
    """Emit embedder configs from static configuration."""
    yield pd.DataFrame(EMBEDDERS)


def build_image_data(local_images_df: pd.DataFrame, image_labels_df: pd.DataFrame) -> pd.DataFrame:
    """Build ImageData objects with optional classification label."""
    merged = local_images_df.merge(image_labels_df, on="image_name", how="left")
    merged["image_data"] = merged.apply(
        lambda row: ImageData(
            image_path=str(row["image_path"]),
            label=(None if pd.isna(row["label"]) else str(row["label"])),
        ),
        axis=1,
    )
    return merged[["image_name", "image_data"]]


def infer_embeddings(local_images_df: pd.DataFrame, embedder_df: pd.DataFrame) -> pd.DataFrame:
    """Run embedder(s) over all images and return vectors per (image, embedder)."""
    if local_images_df.empty or embedder_df.empty:
        return pd.DataFrame(columns=["image_name", "embedder_id", "ndarray"])

    ordered = local_images_df.sort_values("image_name").reset_index(drop=True)
    paths = ordered["image_path"].astype(str).tolist()
    image_names = ordered["image_name"].astype(str).tolist()

    records: list[dict] = []
    for _, embedder in embedder_df.iterrows():
        embedder_id = str(embedder["embedder_id"])
        model_cls_name = str(embedder["cls"])
        model_cls = MODELS_REGISTRY.get(model_cls_name)
        if model_cls is None:
            raise ValueError(f"Unknown model class: {model_cls_name}")
        init_kwargs = embedder["init_kwargs"] if isinstance(embedder["init_kwargs"], dict) else {}
        model = model_cls(**init_kwargs).to(DEVICE).eval()

        logger.info(
            "Computing embeddings for embedder=%s (%s), images=%d, device=%s",
            embedder_id,
            model_cls_name,
            len(paths),
            DEVICE,
        )
        vectors = get_embeddings_batch(
            model=model,
            paths=paths,
            batch_size=int(embedder["batch_size"]),
            max_size=int(embedder["max_size"]),
            embedder_id=embedder_id,
        )
        logger.info("Finished embeddings for embedder=%s, vectors=%d", embedder_id, len(vectors))
        records.extend(
            {
                "image_name": image_names[i],
                "embedder_id": embedder_id,
                "ndarray": vectors[i],
            }
            for i in range(len(vectors))
        )

    return pd.DataFrame(records, columns=["image_name", "embedder_id", "ndarray"])


def run_brain(
    embeddings_df: pd.DataFrame,
    fiftyone_samples_df: pd.DataFrame,  # noqa: ARG001
) -> pd.DataFrame:
    """Compute FiftyOne visualization and similarity for each embedder in batch."""
    fo_session = FifyOneSession()
    fo = fo_session.fiftyone

    try:
        if embeddings_df.empty:
            return pd.DataFrame(columns=["embedder_id", "n_samples", "vis_brain_key", "sim_brain_key", "status"])

        import fiftyone.brain as fob

        dataset = fo.load_dataset(FIFTYONE_DATASET_NAME)

        records: list[dict] = []
        for embedder_id, embedder_embeddings in embeddings_df.groupby("embedder_id", sort=False):
            embedder_id = str(embedder_id)

            image_to_vector = {
                str(row["image_name"]): np.asarray(row["ndarray"], dtype=np.float32)
                for _, row in embedder_embeddings.iterrows()
            }

            aligned_vectors = []
            sample_ids = []
            for sample in dataset:
                if sample.has_field("image_name"):
                    image_name = str(sample["image_name"])
                    if image_name in image_to_vector:
                        sample_ids.append(sample.id)
                        aligned_vectors.append(image_to_vector[image_name])

            if not sample_ids:
                logger.warning("No matching FiftyOne samples for embedder=%s", embedder_id)
                records.append(
                    {
                        "embedder_id": embedder_id,
                        "n_samples": 0,
                        "vis_brain_key": "",
                        "sim_brain_key": f"{embedder_id}__sim",
                        "status": "no_matching_samples",
                    }
                )
                continue

            embeddings = np.stack(aligned_vectors, axis=0)
            view = dataset.select(sample_ids)

            logger.info("Running brain for embedder=%s, samples=%d", embedder_id, len(sample_ids))

            vis_keys = []
            for method in BRAIN_METHODS:
                vis_key = f"{embedder_id}_{method}"
                _delete_brain_run_if_exists(dataset, vis_key)
                fob.compute_visualization(
                    view,
                    embeddings=embeddings,
                    num_dims=2,
                    method=method,
                    brain_key=vis_key,
                    verbose=True,
                    seed=42,
                )
                vis_keys.append(vis_key)

            sim_key = f"{embedder_id}__sim"
            _delete_brain_run_if_exists(dataset, sim_key)
            fob.compute_similarity(
                view,
                embeddings=embeddings,
                brain_key=sim_key,
                backend="sklearn",
                metric="cosine",
            )
            records.append(
                {
                    "embedder_id": embedder_id,
                    "n_samples": len(sample_ids),
                    "vis_brain_key": ",".join(vis_keys),
                    "sim_brain_key": sim_key,
                    "status": "ok",
                }
            )

        dataset.save()
        return pd.DataFrame(records, columns=["embedder_id", "n_samples", "vis_brain_key", "sim_brain_key", "status"])
    finally:
        _delete_zoo_dataset_if_loaded(fo)

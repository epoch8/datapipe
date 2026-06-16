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
    BRAIN_METHODS, DEVICE, EMBEDDERS, FIFTYONE_DATASET_NAME, LABELS_JSON, LOCAL_IMAGES_DIR,
    IMAGE_SUFFIXES,
)
from models import MODELS_REGISTRY, get_embeddings_batch

logger = logging.getLogger(__name__)


def _delete_brain_run_if_exists(dataset, brain_key: str) -> None:
    if dataset.has_brain_run(brain_key):
        logger.info("Deleting existing brain run: %s", brain_key)
        dataset.delete_brain_run(brain_key)


def list_local_images() -> Iterator[pd.DataFrame]:
    """List supported local images into source table rows."""
    files = [path for path in LOCAL_IMAGES_DIR.rglob("*") if path.is_file() and path.suffix.lower() in IMAGE_SUFFIXES]
    rows = [{"image_name": path.stem, "image_path": str(path.resolve())} for path in files]
    logger.info("Found %d images in %s", len(rows), LOCAL_IMAGES_DIR)
    yield pd.DataFrame(rows, columns=["image_name", "image_path"])


def list_image_labels() -> Iterator[pd.DataFrame]:
    """Load optional image labels from JSON mapping {image_name: label}."""
    if not LABELS_JSON:
        logger.info("No LABELS_JSON set, skipping labels")
        yield pd.DataFrame(columns=["image_name", "label"])
        return
    labels_path = Path(LABELS_JSON)
    if not labels_path.exists():
        logger.warning("LABELS_JSON path does not exist: %s", labels_path)
        yield pd.DataFrame(columns=["image_name", "label"])
        return

    with labels_path.open("r", encoding="utf-8") as f:
        labels_data = json.load(f)
    rows = [{"image_name": str(image_name), "label": str(label)} for image_name, label in labels_data.items()]
    logger.info("Loaded %d labels from %s", len(rows), labels_path)
    yield pd.DataFrame(rows, columns=["image_name", "label"])


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
    if embeddings_df.empty:
        return pd.DataFrame(columns=["embedder_id", "n_samples", "vis_brain_key", "sim_brain_key", "status"])

    import fiftyone.brain as fob

    fo_session = FifyOneSession()
    dataset = fo_session.fiftyone.load_dataset(FIFTYONE_DATASET_NAME)

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

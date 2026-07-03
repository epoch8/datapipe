from __future__ import annotations

import json
import logging
from collections.abc import Callable, Iterator
from pathlib import Path

import pandas as pd
from cv_pipeliner import ImageData
from PIL import Image

from config import (
    DATA_DIR,
    ENGINE_REGISTRY,
    ENABLED_ENGINES,
    HF_DATASET_NAME,
    HF_LIMIT,
    HF_SPLIT,
    IMAGE_SUFFIXES,
    LOCAL_IMAGES_DIR,
    fo_text_field,
    use_local_images,
)
from engines import run_engine

logger = logging.getLogger(__name__)


def _dataset_cache_dir(dataset_name: str) -> Path:
    return DATA_DIR / dataset_name.replace("/", "__")


def _save_pil_image(image: Image.Image, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    image.convert("RGB").save(path, format="JPEG", quality=95)


def _load_hf_split(dataset_name: str, split: str):
    from datasets import load_dataset

    return load_dataset(dataset_name, split=split)


def _list_images_from_local_dir(local_dir: Path) -> list[dict[str, str]]:
    files = sorted(
        path
        for path in local_dir.rglob("*")
        if path.is_file() and path.suffix.lower() in IMAGE_SUFFIXES
    )
    return [
        {
            "image_id": path.stem,
            "image_path": str(path.resolve()),
        }
        for path in files
    ]


def _hf_sample_stem(item: dict, idx: int) -> str:
    for key in ("sample_id", "Sample ID", "passport_id", "Passport ID", "id", "filename"):
        if key in item and item[key] is not None:
            return str(item[key]).replace("/", "_").replace(" ", "_")
    return f"item_{idx:06d}"


def _materialize_images() -> list[dict[str, str]]:
    if use_local_images():
        assert LOCAL_IMAGES_DIR is not None
        records = _list_images_from_local_dir(LOCAL_IMAGES_DIR)
        logger.info("Using local images from %s: %d images", LOCAL_IMAGES_DIR, len(records))
        return records

    ds = _load_hf_split(HF_DATASET_NAME, HF_SPLIT)
    save_dir = _dataset_cache_dir(HF_DATASET_NAME)
    records: list[dict[str, str]] = []

    for idx, item in enumerate(ds):
        if idx >= HF_LIMIT:
            break

        filename = item.get("filename")
        if filename:
            stem = Path(str(filename)).stem
            out_path = save_dir / str(filename)
        else:
            stem = _hf_sample_stem(item, idx)
            out_path = save_dir / f"{stem}.jpg"

        if not out_path.exists():
            image = item.get("image")
            if image is None:
                logger.warning("Skipping sample %s: no image field", idx)
                continue
            _save_pil_image(image, out_path)

        records.append(
            {
                "image_id": stem,
                "image_path": str(out_path.resolve()),
            }
        )

    logger.info("Using HF dataset %s: %d images", HF_DATASET_NAME, len(records))
    return records


def list_images() -> Iterator[pd.DataFrame]:
    records = _materialize_images()
    if not records:
        raise ValueError(
            "No images found. Set LOCAL_IMAGES_DIR with images or configure HF_DATASET_NAME in .env"
        )
    yield pd.DataFrame(records, columns=["image_id", "image_path"])


def list_engines() -> Iterator[pd.DataFrame]:
    rows = [
        {
            "engine_id": engine_id,
            "provider": ENGINE_REGISTRY[engine_id]["provider"],
            "model": ENGINE_REGISTRY[engine_id]["model"],
        }
        for engine_id in ENABLED_ENGINES
    ]
    if not rows:
        raise ValueError("No OCR engines enabled. Set OCR_ENGINES in .env")
    yield pd.DataFrame(rows)


def ocr_inference(images_df: pd.DataFrame, engines_df: pd.DataFrame) -> pd.DataFrame:
    if images_df.empty or engines_df.empty:
        return pd.DataFrame(columns=["image_id", "engine_id", "output_json", "full_text"])

    merged = images_df.merge(engines_df, how="cross")
    if merged.empty:
        return pd.DataFrame(columns=["image_id", "engine_id", "output_json", "full_text"])

    records = []
    for _, row in merged.iterrows():
        result = run_engine(engine_id=row["engine_id"], image_path=row["image_path"])
        records.append(
            {
                "image_id": row["image_id"],
                "engine_id": row["engine_id"],
                "output_json": result.output_json,
                "full_text": result.full_text,
            }
        )

    return pd.DataFrame(records)


def _build_image_data(image_path: str, engine_id: str, ocr_row: pd.Series) -> ImageData:
    text_field = fo_text_field(engine_id)
    output_json = ocr_row.get("output_json") or ocr_row.get("full_text") or ""
    if isinstance(output_json, dict):
        output_json = json.dumps(output_json, indent=2)
    return ImageData(
        image_path=image_path,
        bboxes_data=[],
        additional_info={text_field: str(output_json)},
    )


def make_build_image_data(engine_id: str) -> Callable[[pd.DataFrame, pd.DataFrame], pd.DataFrame]:
    def build_image_data(images_df: pd.DataFrame, ocr_results_df: pd.DataFrame) -> pd.DataFrame:
        if images_df.empty or ocr_results_df.empty:
            return pd.DataFrame(columns=["image_id", "image_data"])

        engine_results = ocr_results_df[ocr_results_df["engine_id"] == engine_id]
        if engine_results.empty:
            return pd.DataFrame(columns=["image_id", "image_data"])

        merged = images_df.merge(engine_results, on="image_id", how="inner")
        records = []
        for _, row in merged.iterrows():
            image_data = _build_image_data(str(row["image_path"]), engine_id, row)
            records.append({"image_id": row["image_id"], "image_data": image_data})

        return pd.DataFrame(records, columns=["image_id", "image_data"])

    build_image_data.__name__ = f"build_image_data_{engine_id}"
    return build_image_data

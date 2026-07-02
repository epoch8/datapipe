from __future__ import annotations

import json
import logging
from collections.abc import Callable, Iterator
from pathlib import Path

import pandas as pd
from cv_pipeliner import BboxData, ImageData
from PIL import Image

from config import (
    DATA_DIR,
    ENGINE_REGISTRY,
    ENGINE_TYPE_STRUCTURED,
    ENGINE_TYPE_UNSTRUCTURED,
    ENABLED_ENGINES,
    HF_DATASET_STRUCTURED,
    HF_DATASET_UNSTRUCTURED,
    HF_LIMIT_STRUCTURED,
    HF_LIMIT_UNSTRUCTURED,
    HF_SPLIT_STRUCTURED,
    HF_SPLIT_UNSTRUCTURED,
    IMAGE_SUFFIXES,
    LOCAL_STRUCTURED_DIR,
    LOCAL_UNSTRUCTURED_DIR,
    fo_text_field,
    is_unstructured_engine,
    use_local_structured_images,
    use_local_unstructured_images,
)
from engines import run_engine
from viz import visualize_ocr_side_by_side

logger = logging.getLogger(__name__)


def _dataset_cache_dir(dataset_name: str) -> Path:
    return DATA_DIR / dataset_name.replace("/", "__")


def _save_pil_image(image: Image.Image, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    image.convert("RGB").save(path, format="JPEG", quality=95)


def _image_id(kind: str, stem: str) -> str:
    return f"{kind}__{stem}"


def _load_hf_split(dataset_name: str, split: str):
    from datasets import load_dataset

    return load_dataset(dataset_name, split=split)


def _list_images_from_local_dir(kind: str, local_dir: Path) -> list[dict[str, str]]:
    files = sorted(
        path
        for path in local_dir.rglob("*")
        if path.is_file() and path.suffix.lower() in IMAGE_SUFFIXES
    )
    return [
        {
            "image_id": _image_id(kind, path.stem),
            "kind": kind,
            "image_path": str(path.resolve()),
        }
        for path in files
    ]


def _materialize_unstructured_dataset() -> list[dict[str, str]]:
    if use_local_unstructured_images():
        assert LOCAL_UNSTRUCTURED_DIR is not None
        records = _list_images_from_local_dir(ENGINE_TYPE_UNSTRUCTURED, LOCAL_UNSTRUCTURED_DIR)
        logger.info("Using local unstructured images from %s: %d images", LOCAL_UNSTRUCTURED_DIR, len(records))
        return records

    ds = _load_hf_split(HF_DATASET_UNSTRUCTURED, HF_SPLIT_UNSTRUCTURED)
    save_dir = _dataset_cache_dir(HF_DATASET_UNSTRUCTURED)
    records: list[dict[str, str]] = []

    for idx, item in enumerate(ds):
        if idx >= HF_LIMIT_UNSTRUCTURED:
            break

        filename = item.get("filename")
        if filename:
            stem = Path(str(filename)).stem
            out_path = save_dir / str(filename)
        else:
            stem = f"item_{idx:06d}"
            out_path = save_dir / f"{stem}.jpg"

        if not out_path.exists():
            image = item.get("image")
            if image is None:
                logger.warning("Skipping unstructured sample %s: no image field", idx)
                continue
            _save_pil_image(image, out_path)

        records.append(
            {
                "image_id": _image_id(ENGINE_TYPE_UNSTRUCTURED, stem),
                "kind": ENGINE_TYPE_UNSTRUCTURED,
                "image_path": str(out_path.resolve()),
            }
        )

    logger.info(
        "Using HF unstructured dataset %s: %d images",
        HF_DATASET_UNSTRUCTURED,
        len(records),
    )
    return records


def _structured_sample_stem(item: dict, idx: int) -> str:
    for key in ("sample_id", "Sample ID", "passport_id", "Passport ID", "id", "filename"):
        if key in item and item[key] is not None:
            return str(item[key]).replace("/", "_").replace(" ", "_")
    return f"item_{idx:06d}"


def _materialize_structured_dataset() -> list[dict[str, str]]:
    if use_local_structured_images():
        assert LOCAL_STRUCTURED_DIR is not None
        records = _list_images_from_local_dir(ENGINE_TYPE_STRUCTURED, LOCAL_STRUCTURED_DIR)
        logger.info("Using local structured images from %s: %d images", LOCAL_STRUCTURED_DIR, len(records))
        return records

    ds = _load_hf_split(HF_DATASET_STRUCTURED, HF_SPLIT_STRUCTURED)
    save_dir = _dataset_cache_dir(HF_DATASET_STRUCTURED)
    records: list[dict[str, str]] = []

    for idx, item in enumerate(ds):
        if idx >= HF_LIMIT_STRUCTURED:
            break

        stem = _structured_sample_stem(item, idx)
        out_path = save_dir / f"{stem}.jpg"

        if not out_path.exists():
            image = item.get("image")
            if image is None:
                logger.warning("Skipping structured sample %s: no image field", idx)
                continue
            _save_pil_image(image, out_path)

        records.append(
            {
                "image_id": _image_id(ENGINE_TYPE_STRUCTURED, stem),
                "kind": ENGINE_TYPE_STRUCTURED,
                "image_path": str(out_path.resolve()),
            }
        )

    logger.info(
        "Using HF structured dataset %s: %d images",
        HF_DATASET_STRUCTURED,
        len(records),
    )
    return records


def list_images() -> Iterator[pd.DataFrame]:
    records = _materialize_unstructured_dataset() + _materialize_structured_dataset()
    if not records:
        raise ValueError(
            "No images found. Set LOCAL_UNSTRUCTURED_DIR / LOCAL_STRUCTURED_DIR with images "
            "or configure HF datasets in .env"
        )
    yield pd.DataFrame(records, columns=["image_id", "kind", "image_path"])


def list_engines() -> Iterator[pd.DataFrame]:
    rows = [
        {
            "engine_id": engine_id,
            "engine_type": ENGINE_REGISTRY[engine_id]["type"],
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
        return pd.DataFrame(
            columns=[
                "image_id",
                "engine_id",
                "engine_type",
                "boxes",
                "texts",
                "scores",
                "structured_json",
                "full_text",
            ]
        )

    merged = images_df.merge(engines_df, how="cross")
    merged = merged[merged["kind"] == merged["engine_type"]]
    if merged.empty:
        return pd.DataFrame(
            columns=[
                "image_id",
                "engine_id",
                "engine_type",
                "boxes",
                "texts",
                "scores",
                "structured_json",
                "full_text",
            ]
        )

    records = []
    for _, row in merged.iterrows():
        result = run_engine(engine_id=row["engine_id"], image_path=row["image_path"])
        records.append(
            {
                "image_id": row["image_id"],
                "engine_id": row["engine_id"],
                "engine_type": row["engine_type"],
                "boxes": result.boxes,
                "texts": result.texts,
                "scores": result.scores,
                "structured_json": result.structured_json,
                "full_text": result.full_text,
            }
        )

    return pd.DataFrame(records)


def _build_unstructured_image_data(image_path: str, engine_id: str, ocr_row: pd.Series) -> ImageData:
    boxes = ocr_row.get("boxes") or []
    texts = ocr_row.get("texts") or []
    scores = ocr_row.get("scores") or []
    text_field = fo_text_field(engine_id)

    bboxes_data = []
    for box, text, score in zip(boxes, texts, scores):
        if len(box) == 4:
            xmin, ymin, xmax, ymax = [float(v) for v in box]
        else:
            xs = [float(p[0]) for p in box]
            ys = [float(p[1]) for p in box]
            xmin, ymin, xmax, ymax = min(xs), min(ys), max(xs), max(ys)

        bboxes_data.append(
            BboxData(
                xmin=xmin,
                ymin=ymin,
                xmax=xmax,
                ymax=ymax,
                label=str(text),
                detection_score=float(score),
                additional_info={"engine_id": engine_id},
            )
        )

    full_text = ocr_row.get("full_text")
    if full_text is None and texts:
        full_text = "\n".join(str(text) for text in texts)

    return ImageData(
        image_path=image_path,
        bboxes_data=bboxes_data,
        additional_info={text_field: full_text or ""},
    )


def _build_structured_image_data(image_path: str, engine_id: str, ocr_row: pd.Series) -> ImageData:
    text_field = fo_text_field(engine_id)
    structured_json = ocr_row.get("structured_json") or ocr_row.get("full_text") or ""
    if isinstance(structured_json, dict):
        structured_json = json.dumps(structured_json, indent=2)
    return ImageData(
        image_path=image_path,
        bboxes_data=[],
        additional_info={text_field: str(structured_json)},
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
            image_path = str(row["image_path"])
            if is_unstructured_engine(engine_id):
                image_data = _build_unstructured_image_data(image_path, engine_id, row)
            else:
                image_data = _build_structured_image_data(image_path, engine_id, row)
            records.append({"image_id": row["image_id"], "image_data": image_data})

        return pd.DataFrame(records, columns=["image_id", "image_data"])

    build_image_data.__name__ = f"build_image_data_{engine_id}"
    return build_image_data


def render_composite(images_df: pd.DataFrame, ocr_results_df: pd.DataFrame) -> pd.DataFrame:
    if images_df.empty or ocr_results_df.empty:
        return pd.DataFrame(columns=["engine_id", "image_id", "image"])

    unstructured_results = ocr_results_df[ocr_results_df["engine_type"] == ENGINE_TYPE_UNSTRUCTURED]
    if unstructured_results.empty:
        return pd.DataFrame(columns=["engine_id", "image_id", "image"])

    merged = images_df.merge(unstructured_results, on="image_id", how="inner")
    records = []
    for _, row in merged.iterrows():
        image = Image.open(row["image_path"]).convert("RGB")
        composite = visualize_ocr_side_by_side(
            image=image,
            boxes=row.get("boxes") or [],
            texts=row.get("texts") or [],
            scores=row.get("scores") or [],
        )
        records.append(
            {
                "engine_id": row["engine_id"],
                "image_id": row["image_id"],
                "image": composite,
            }
        )

    return pd.DataFrame(records, columns=["engine_id", "image_id", "image"])

from __future__ import annotations

import os
from datetime import date
from pathlib import Path
from typing import Any

from datapipe.store.database import DBConn
from pydantic import BaseModel, Field

DATA_DIR = Path(os.environ.get("DATA_DIR", "./data")).resolve()
COMPOSITES_DIR = DATA_DIR / "composites"

_local_unstructured_dir_raw = os.environ.get("LOCAL_UNSTRUCTURED_DIR")
LOCAL_UNSTRUCTURED_DIR = (
    Path(_local_unstructured_dir_raw).resolve() if _local_unstructured_dir_raw else None
)
_local_structured_dir_raw = os.environ.get("LOCAL_STRUCTURED_DIR")
LOCAL_STRUCTURED_DIR = Path(_local_structured_dir_raw).resolve() if _local_structured_dir_raw else None

IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png", ".webp", ".heic"}

HF_DATASET_UNSTRUCTURED = os.environ.get("HF_DATASET_UNSTRUCTURED", "MLap/Book-Scan-OCR")
HF_SPLIT_UNSTRUCTURED = os.environ.get("HF_SPLIT_UNSTRUCTURED", "train")
HF_LIMIT_UNSTRUCTURED = int(os.environ.get("HF_LIMIT_UNSTRUCTURED", "50"))

HF_DATASET_STRUCTURED = os.environ.get("HF_DATASET_STRUCTURED", "ud-synthetic/printed-usa-passports")
HF_SPLIT_STRUCTURED = os.environ.get("HF_SPLIT_STRUCTURED", "train")
HF_LIMIT_STRUCTURED = int(os.environ.get("HF_LIMIT_STRUCTURED", "50"))

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", os.environ.get("GOOGLE_API_KEY", ""))

DB_URL = os.environ.get("DB_URL")
DBCONN = DBConn(DB_URL, "public")

FIFTYONE_DATASET_NAME = os.environ.get("FIFTYONE_DATASET_NAME", "datapipe_ocr")

ENGINE_TYPE_UNSTRUCTURED = "unstructured"
ENGINE_TYPE_STRUCTURED = "structured"


class IdDocument(BaseModel):
    """Default structured OCR output for passport / id-doc images."""

    name: str = Field(description="Given name(s) as printed on the document")
    surname: str = Field(description="Family name / surname as printed on the document")
    birthdate: date | None = Field(default=None, description="Date of birth in ISO format YYYY-MM-DD")


STRUCTURED_OUTPUT_MODEL = IdDocument


def structured_ocr_prompt(model: type[BaseModel] = STRUCTURED_OUTPUT_MODEL) -> str:
    """Build OCR instruction text from the pydantic structured-output schema."""
    header = (model.__doc__ or "Extract structured fields from the document image.").strip()
    field_lines = [
        f"- {field_name}: {field_info.description or field_name}"
        for field_name, field_info in model.model_fields.items()
    ]
    return f"{header}\n\nReturn JSON matching these fields:\n" + "\n".join(field_lines)


def _parse_ocr_engines() -> list[str]:
    raw = os.environ.get("OCR_ENGINES", "paddle,openai,gemini")
    return [engine_id.strip() for engine_id in raw.split(",") if engine_id.strip()]


ENGINE_REGISTRY: dict[str, dict[str, Any]] = {
    "paddle": {
        "type": ENGINE_TYPE_UNSTRUCTURED,
        "provider": "paddle",
        "model": "PP-OCRv6",
        "init_kwargs": {
            "text_detection_model_name": "PP-OCRv6_tiny_det",
            "text_recognition_model_name": "PP-OCRv6_tiny_rec",
            "use_doc_orientation_classify": False,
            "use_doc_unwarping": False,
            "use_textline_orientation": False,
            "engine": "transformers",
        },
    },
    "openai": {
        "type": ENGINE_TYPE_STRUCTURED,
        "provider": "openai",
        "model": "gpt-5.4-nano",
        "init_kwargs": {
            "temperature": 0,
        },
    },
    "gemini": {
        "type": ENGINE_TYPE_STRUCTURED,
        "provider": "gemini",
        "model": "gemini-3.5-flash",
        "init_kwargs": {
            "temperature": 0,
        },
    },
}

ENABLED_ENGINES = [engine_id for engine_id in _parse_ocr_engines() if engine_id in ENGINE_REGISTRY]


def engines_of_type(engine_type: str) -> list[str]:
    return [engine_id for engine_id in ENABLED_ENGINES if ENGINE_REGISTRY[engine_id]["type"] == engine_type]


def fo_text_field(engine_id: str) -> str:
    return f"{engine_id}_ocr"


def fo_det_field(engine_id: str) -> str:
    return f"{engine_id}_boxes"


def is_unstructured_engine(engine_id: str) -> bool:
    return ENGINE_REGISTRY[engine_id]["type"] == ENGINE_TYPE_UNSTRUCTURED


def _local_dir_has_images(local_dir: Path | None) -> bool:
    if local_dir is None or not local_dir.exists():
        return False
    return any(
        path.is_file() and path.suffix.lower() in IMAGE_SUFFIXES for path in local_dir.rglob("*")
    )


def use_local_unstructured_images() -> bool:
    return _local_dir_has_images(LOCAL_UNSTRUCTURED_DIR)


def use_local_structured_images() -> bool:
    return _local_dir_has_images(LOCAL_STRUCTURED_DIR)

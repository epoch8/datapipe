from __future__ import annotations

import os
from datetime import date
from pathlib import Path
from typing import Any

from datapipe.store.database import DBConn
from pydantic import BaseModel, Field

DATA_DIR = Path(os.environ.get("DATA_DIR", "./data")).resolve()

_local_images_dir_raw = os.environ.get("LOCAL_IMAGES_DIR")
LOCAL_IMAGES_DIR = Path(_local_images_dir_raw).resolve() if _local_images_dir_raw else None

IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png", ".webp", ".heic"}

HF_DATASET_NAME = os.environ.get("HF_DATASET_NAME", "ud-synthetic/printed-usa-passports")
HF_SPLIT = os.environ.get("HF_SPLIT", "train")
HF_LIMIT = int(os.environ.get("HF_LIMIT", "50"))

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", os.environ.get("GOOGLE_API_KEY", ""))
QWEN_API_KEY = os.environ.get("QWEN_API_KEY", "")

DB_URL = os.environ.get("DB_URL")
DBCONN = DBConn(DB_URL, "public")

FIFTYONE_DATASET_NAME = os.environ.get("FIFTYONE_DATASET_NAME", "datapipe_ocr")


class IdDocument(BaseModel):
    """Default OCR output for passport / id-doc images."""

    name: str = Field(description="Given name(s) as printed on the document")
    surname: str = Field(description="Family name / surname as printed on the document")
    birthdate: date | None = Field(default=None, description="Date of birth in ISO format YYYY-MM-DD")


OUTPUT_MODEL = IdDocument


def ocr_prompt(model: type[BaseModel] = OUTPUT_MODEL) -> str:
    """Build OCR instruction text from the pydantic output schema."""
    header = (model.__doc__ or "Extract fields from the document image.").strip()
    field_lines = [
        f"- {field_name}: {field_info.description or field_name}"
        for field_name, field_info in model.model_fields.items()
    ]
    return f"{header}\n\nReturn JSON matching these fields:\n" + "\n".join(field_lines)


def _parse_ocr_engines() -> list[str]:
    raw = os.environ.get("OCR_ENGINES", "openai,gemini")
    return [engine_id.strip() for engine_id in raw.split(",") if engine_id.strip()]


ENGINE_REGISTRY: dict[str, dict[str, Any]] = {
    "openai": {
        "provider": "openai",
        "model": "gpt-5.4-nano",
        "init_kwargs": {
            "temperature": 0,
        },
    },
    "gemini": {
        "provider": "gemini",
        "model": "gemini-3.5-flash",
        "init_kwargs": {
            "temperature": 0,
        },
    },
    "qwen": {
        "provider": "qwen",
        "model": "qwen3.7-plus",
        "client_kwargs": {
            "base_url": "https://dashscope-intl.aliyuncs.com/compatible-mode/v1",
        },
        "init_kwargs": {
            "extra_body": {"enable_thinking": False},
            "response_format": {"type": "json_object"},
            "temperature": 0,
        },
    },
}

ENABLED_ENGINES = [engine_id for engine_id in _parse_ocr_engines() if engine_id in ENGINE_REGISTRY]


def fo_text_field(engine_id: str) -> str:
    return f"{engine_id}_ocr"


def _local_dir_has_images(local_dir: Path | None) -> bool:
    if local_dir is None or not local_dir.exists():
        return False
    return any(
        path.is_file() and path.suffix.lower() in IMAGE_SUFFIXES for path in local_dir.rglob("*")
    )


def use_local_images() -> bool:
    return _local_dir_has_images(LOCAL_IMAGES_DIR)

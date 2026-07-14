from __future__ import annotations

import base64
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from config import (
    ENGINE_REGISTRY,
    GEMINI_API_KEY,
    OPENAI_API_KEY,
    OUTPUT_MODEL,
    QWEN_API_KEY,
    ocr_prompt,
)

logger = logging.getLogger(__name__)


@dataclass
class OcrResult:
    output_json: str
    full_text: str


def _encode_image_base64(image_path: str) -> str:
    return base64.b64encode(Path(image_path).read_bytes()).decode("utf-8")


def _guess_mime_type(image_path: str) -> str:
    suffix = Path(image_path).suffix.lower()
    if suffix in {".jpg", ".jpeg"}:
        return "image/jpeg"
    if suffix == ".png":
        return "image/png"
    if suffix == ".webp":
        return "image/webp"
    if suffix == ".heic":
        return "image/heic"
    return "image/jpeg"


def _engine_config(engine_id: str) -> dict[str, Any]:
    return ENGINE_REGISTRY[engine_id]


def _document_from_output(document: Any) -> Any:
    if isinstance(document, OUTPUT_MODEL):
        return document
    return OUTPUT_MODEL.model_validate(document)


def _ocr_result_from_document(document: Any) -> OcrResult:
    document = _document_from_output(document)
    output_json = document.model_dump_json(indent=2)
    return OcrResult(output_json=output_json, full_text=output_json)


def run_openai(image_path: str, engine_id: str = "openai", **_kwargs: Any) -> OcrResult:
    if not OPENAI_API_KEY:
        raise ValueError("OPENAI_API_KEY is required for openai engine")

    from openai import OpenAI

    engine_cfg = _engine_config(engine_id)
    init_kwargs = engine_cfg.get("init_kwargs", {})
    prompt = ocr_prompt(OUTPUT_MODEL)

    client = OpenAI(api_key=OPENAI_API_KEY)
    mime = _guess_mime_type(image_path)
    b64 = _encode_image_base64(image_path)
    response = client.responses.parse(
        model=engine_cfg["model"],
        input=[
            {
                "role": "user",
                "content": [
                    {"type": "input_text", "text": prompt},
                    {
                        "type": "input_image",
                        "image_url": f"data:{mime};base64,{b64}",
                    },
                ],
            }
        ],
        text_format=OUTPUT_MODEL,
        **init_kwargs,
    )
    document = response.output_parsed
    if document is None:
        raise ValueError("OpenAI structured parse returned empty output")
    return _ocr_result_from_document(document)


def run_gemini(image_path: str, engine_id: str = "gemini", **_kwargs: Any) -> OcrResult:
    if not GEMINI_API_KEY:
        raise ValueError("GEMINI_API_KEY (or GOOGLE_API_KEY) is required for gemini engine")

    from google import genai
    from google.genai import types

    engine_cfg = _engine_config(engine_id)
    init_kwargs = engine_cfg.get("init_kwargs", {})
    prompt = ocr_prompt(OUTPUT_MODEL)

    client = genai.Client(api_key=GEMINI_API_KEY)
    image_bytes = Path(image_path).read_bytes()
    mime = _guess_mime_type(image_path)
    response = client.models.generate_content(
        model=engine_cfg["model"],
        contents=[
            types.Part.from_bytes(data=image_bytes, mime_type=mime),
            prompt,
        ],
        config=types.GenerateContentConfig(
            response_mime_type="application/json",
            response_schema=OUTPUT_MODEL,
            **init_kwargs,
        ),
    )
    document = getattr(response, "parsed", None)
    if document is None:
        document = OUTPUT_MODEL.model_validate_json(response.text or "{}")
    return _ocr_result_from_document(document)


def run_qwen(image_path: str, engine_id: str = "qwen", **_kwargs: Any) -> OcrResult:
    if not QWEN_API_KEY:
        raise ValueError("QWEN_API_KEY is required for qwen engine")

    from openai import OpenAI

    engine_cfg = _engine_config(engine_id)
    client_kwargs = engine_cfg.get("client_kwargs", {})
    init_kwargs = engine_cfg.get("init_kwargs", {})
    prompt = ocr_prompt(OUTPUT_MODEL)

    client = OpenAI(api_key=QWEN_API_KEY, **client_kwargs)
    mime = _guess_mime_type(image_path)
    b64 = _encode_image_base64(image_path)
    response = client.chat.completions.create(
        model=engine_cfg["model"],
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:{mime};base64,{b64}"},
                    },
                    {"type": "text", "text": prompt},
                ],
            }
        ],
        **init_kwargs,
    )
    content = response.choices[0].message.content
    if not content:
        raise ValueError("Qwen completion returned empty content")
    document = OUTPUT_MODEL.model_validate_json(content)
    return _ocr_result_from_document(document)


ENGINE_RUNNERS = {
    "openai": run_openai,
    "gemini": run_gemini,
    "qwen": run_qwen,
}


def run_engine(engine_id: str, image_path: str) -> OcrResult:
    runner = ENGINE_RUNNERS.get(engine_id)
    if runner is None:
        raise KeyError(f"Unknown OCR engine: {engine_id}")
    return runner(image_path=image_path, engine_id=engine_id)

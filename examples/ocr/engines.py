from __future__ import annotations

import base64
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from config import (
    ENGINE_REGISTRY,
    GEMINI_API_KEY,
    IdDocument,
    OPENAI_API_KEY,
    STRUCTURED_OUTPUT_MODEL,
    structured_ocr_prompt,
)

logger = logging.getLogger(__name__)

_paddle_ocr_instance = None


@dataclass
class OcrResult:
    has_boxes: bool
    boxes: list[list[float]]
    texts: list[str]
    scores: list[float]
    structured_json: str | None
    full_text: str | None


def _get_paddle_ocr():
    global _paddle_ocr_instance
    if _paddle_ocr_instance is None:
        from paddleocr import PaddleOCR

        init_kwargs = ENGINE_REGISTRY["paddle"]["init_kwargs"]
        _paddle_ocr_instance = PaddleOCR(**init_kwargs)
    return _paddle_ocr_instance


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


def run_paddle(image_path: str, engine_id: str = "paddle", **_kwargs: Any) -> OcrResult:
    del engine_id
    output = _get_paddle_ocr().predict(input=image_path)[0]
    boxes = [list(map(float, box)) for box in output.get("rec_boxes", [])]
    texts = [str(text) for text in output.get("rec_texts", [])]
    scores = [float(score) for score in output.get("rec_scores", [])]
    full_text = "\n".join(texts)
    return OcrResult(
        has_boxes=len(boxes) > 0,
        boxes=boxes,
        texts=texts,
        scores=scores,
        structured_json=None,
        full_text=full_text,
    )


def run_openai(image_path: str, engine_id: str = "openai", **_kwargs: Any) -> OcrResult:
    if not OPENAI_API_KEY:
        raise ValueError("OPENAI_API_KEY is required for openai engine")

    from openai import OpenAI

    engine_cfg = _engine_config(engine_id)
    init_kwargs = engine_cfg.get("init_kwargs", {})
    prompt = structured_ocr_prompt(STRUCTURED_OUTPUT_MODEL)

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
        text_format=STRUCTURED_OUTPUT_MODEL,
        **init_kwargs,
    )
    document = response.output_parsed
    if document is None:
        raise ValueError("OpenAI structured parse returned empty output")
    if not isinstance(document, IdDocument):
        document = IdDocument.model_validate(document)

    structured_json = document.model_dump_json(indent=2)
    return OcrResult(
        has_boxes=False,
        boxes=[],
        texts=[],
        scores=[],
        structured_json=structured_json,
        full_text=structured_json,
    )


def run_gemini(image_path: str, engine_id: str = "gemini", **_kwargs: Any) -> OcrResult:
    if not GEMINI_API_KEY:
        raise ValueError("GEMINI_API_KEY (or GOOGLE_API_KEY) is required for gemini engine")

    from google import genai
    from google.genai import types

    engine_cfg = _engine_config(engine_id)
    init_kwargs = engine_cfg.get("init_kwargs", {})
    prompt = structured_ocr_prompt(STRUCTURED_OUTPUT_MODEL)

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
            response_schema=STRUCTURED_OUTPUT_MODEL,
            **init_kwargs,
        ),
    )
    document = getattr(response, "parsed", None)
    if document is None:
        document = IdDocument.model_validate_json(response.text or "{}")
    elif not isinstance(document, IdDocument):
        document = IdDocument.model_validate(document)

    structured_json = document.model_dump_json(indent=2)
    return OcrResult(
        has_boxes=False,
        boxes=[],
        texts=[],
        scores=[],
        structured_json=structured_json,
        full_text=structured_json,
    )


ENGINE_RUNNERS = {
    "paddle": run_paddle,
    "openai": run_openai,
    "gemini": run_gemini,
}


def run_engine(engine_id: str, image_path: str) -> OcrResult:
    runner = ENGINE_RUNNERS.get(engine_id)
    if runner is None:
        raise KeyError(f"Unknown OCR engine: {engine_id}")
    return runner(image_path=image_path, engine_id=engine_id)


def serialize_boxes(boxes: list[list[float]]) -> str:
    return json.dumps(boxes)


def serialize_texts(texts: list[str]) -> str:
    return json.dumps(texts)


def serialize_scores(scores: list[float]) -> str:
    return json.dumps(scores)

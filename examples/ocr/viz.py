from __future__ import annotations

from typing import Iterable, Sequence

from PIL import Image, ImageDraw, ImageFont


def _fit_font_to_box(
    draw: ImageDraw.ImageDraw,
    text: str,
    box_w: float,
    box_h: float,
    max_size: int = 64,
    min_size: int = 8,
) -> ImageFont.ImageFont:
    """Return largest font that fits text into target box."""
    box_w = max(1.0, box_w)
    box_h = max(1.0, box_h)
    for size in range(max_size, min_size - 1, -1):
        font = ImageFont.load_default(size)
        left, top, right, bottom = draw.textbbox((0, 0), text, font=font)
        text_w = right - left
        text_h = bottom - top
        if text_w <= box_w and text_h <= box_h:
            return font
    return ImageFont.load_default(min_size)


def visualize_ocr_side_by_side(
    image: Image.Image,
    boxes: Iterable[Sequence[Sequence[float] | float]],
    texts: Iterable[str],
    scores: Iterable[float],
) -> Image.Image:
    """Return image with right-side OCR visualization panel.

    Output image size: (2 * width, height).
    Left side: original image.
    Right side: white canvas with OCR boxes and labels.
    """
    width, height = image.size
    result = Image.new("RGB", (width * 2, height), "white")
    result.paste(image.convert("RGB"), (0, 0))

    draw = ImageDraw.Draw(result)
    x_offset = width

    for box, text, score in zip(boxes, texts, scores):
        if len(box) == 4 and isinstance(box[0], (int, float)):
            x1, y1, x2, y2 = [float(v) for v in box]  # type: ignore[arg-type]
            polygon = [(x1, y1), (x2, y1), (x2, y2), (x1, y2)]
        else:
            polygon = [(float(p[0]), float(p[1])) for p in box]  # type: ignore[index]

        draw.polygon(polygon, outline="red", width=2)
        shifted = [(x + x_offset, y) for x, y in polygon]
        draw.polygon(shifted, outline="red", width=2)

        min_x = min(x for x, _ in shifted)
        max_x = max(x for x, _ in shifted)
        min_y = min(y for _, y in shifted)
        max_y = max(y for _, y in shifted)
        box_w = max_x - min_x
        box_h = max_y - min_y

        main_font = _fit_font_to_box(draw, text, box_w * 0.9, box_h * 0.55)
        main_left, main_top, main_right, main_bottom = draw.textbbox((0, 0), text, font=main_font)
        main_w = main_right - main_left
        main_h = main_bottom - main_top
        text_x = min_x + (box_w - main_w) / 2
        text_y = min_y + (box_h - main_h) / 2
        draw.text((text_x, text_y), text, fill="blue", font=main_font)

        conf_text = f"{float(score):.3f}"
        conf_font = _fit_font_to_box(draw, conf_text, box_w * 0.45, box_h * 0.25, max_size=28, min_size=6)
        draw.text((min_x + 2, min_y + 2), conf_text, fill="green", font=conf_font)

    return result

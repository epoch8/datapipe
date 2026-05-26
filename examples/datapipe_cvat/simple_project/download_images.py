from __future__ import annotations

from pathlib import Path

import requests
from PIL import Image

IMAGE_URLS = [
    "https://images.unsplash.com/photo-1518791841217-8f162f1e1131?w=640",
    "https://images.unsplash.com/photo-1517849845537-4d257902454a?w=640",
    "https://images.unsplash.com/photo-1519681393784-d120267933ba?w=640",
    "https://images.unsplash.com/photo-1500530855697-b586d89ba3ee?w=640",
    "https://images.unsplash.com/photo-1495567720989-cebdbdd97913?w=640",
    "https://images.unsplash.com/photo-1500534314209-a25ddb2bd429?w=640",
    "https://images.unsplash.com/photo-1507149833265-60c372daea22?w=640",
    "https://images.unsplash.com/photo-1472214103451-9374bd1c798e?w=640",
    "https://images.unsplash.com/photo-1493246507139-91e8fad9978e?w=640",
    "https://images.unsplash.com/photo-1501854140801-50d01698950b?w=640",
]


def download_images(output_dir: Path = Path("input"), limit: int = 10) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for idx, url in enumerate(IMAGE_URLS[:limit], start=1):
        output_path = output_dir / f"{idx:02d}.jpeg"
        if output_path.exists():
            continue

        response = requests.get(url, timeout=60)
        response.raise_for_status()

        tmp_path = output_path.with_suffix(".tmp")
        tmp_path.write_bytes(response.content)
        with Image.open(tmp_path) as image:
            image.convert("RGB").save(output_path, format="JPEG")
        tmp_path.unlink(missing_ok=True)


if __name__ == "__main__":
    download_images()


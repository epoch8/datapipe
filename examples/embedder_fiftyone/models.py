from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
from PIL import Image
from tqdm import tqdm
from transformers import AutoImageProcessor, AutoModel


def resize_down_keep_aspect(
    img: Image.Image,
    max_size: int | Sequence[int],
    resample: int = Image.BICUBIC,
) -> Image.Image:
    """Resize image only when bigger than max_size, keep aspect ratio."""
    w, h = img.size
    if isinstance(max_size, int):
        max_w, max_h = max_size, max_size
    else:
        max_w, max_h = max_size

    scale = min(max_w / w, max_h / h, 1.0)
    if scale == 1.0:
        return img

    new_size = (int(w * scale), int(h * scale))
    return img.resize(new_size, resample=resample)


class EmbedderModel(Protocol):
    def __call__(self, images: list[Image.Image]) -> torch.Tensor: ...


class HFDinoModel(nn.Module):
    """HuggingFace DINO-style embedder returning CLS token embeddings."""

    def __init__(self, ckpt: str = "facebook/dinov2-large") -> None:
        super().__init__()
        self.backbone = AutoModel.from_pretrained(ckpt)
        self.processor = AutoImageProcessor.from_pretrained(ckpt, use_fast=True)

    def forward(self, images: list[Image.Image]) -> torch.Tensor:
        dev = next(self.parameters()).device
        inputs = self.processor(images=images, return_tensors="pt").to(dev)
        out = self.backbone(**inputs)
        feats = out.last_hidden_state[:, 0, :]
        return feats


def get_embeddings_batch(
    model: EmbedderModel,
    paths: list[str],
    batch_size: int = 8,
    max_size: int = 512,
    embedder_id: str | None = None,
) -> np.ndarray:
    """Compute normalized embeddings in batches for file paths."""
    if not paths:
        return np.array([])

    prefix = f"embedder={embedder_id}" if embedder_id else "embeddings"
    total = len(paths)
    all_embs: list[np.ndarray] = []
    for i in tqdm(range(0, total, batch_size), desc=prefix, unit="batch"):
        paths_batch = paths[i : i + batch_size]
        images = [resize_down_keep_aspect(Image.open(path).convert("RGB"), max_size) for path in paths_batch]
        with torch.no_grad():
            embeddings = model(images)
            embeddings = F.normalize(embeddings, dim=1)
        all_embs.extend(embeddings.cpu().numpy())
    return np.array(all_embs)


MODELS_REGISTRY: dict[str, type[nn.Module]] = {
    "HFDinoModel": HFDinoModel,
}

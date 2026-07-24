"""Reproducibility fingerprint — run on EACH machine and diff the outputs.

Prints md5 fingerprints at every data-layer boundary plus the environment, so a cross-machine
metric divergence can be pinned to a specific layer:

  [cache]   gt.json + raw image bytes        -> differ => different cache/build
  [darken]  darken(raw, 0.25) bytes          -> differ => PIL/numpy stack differs (not using uv.lock?)
  [env]     lib versions, GPU, CUDA, cuDNN   -> differ => not the same software stack / hardware
  [model]   best.pt of each trained model    -> differ with all above equal => GPU-arch training divergence

Run from examples/detection_tags/detection:   ../.venv/bin/python ../scripts/repro_fingerprint.py
"""
from __future__ import annotations

import hashlib
import json
import os
import platform
import sys
from pathlib import Path

sys.path.insert(0, ".")
import coco_demo  # noqa: E402


def md5(b: bytes) -> str:
    return hashlib.md5(b).hexdigest()


def main() -> int:
    print("== env ==")
    print("python", platform.python_version(), platform.machine(), platform.system())
    import importlib.metadata as m
    for p in ["torch", "torchvision", "ultralytics", "pillow", "numpy", "opencv-python-headless"]:
        try:
            print(p, m.version(p))
        except Exception:
            print(p, "not-installed")
    try:
        import torch
        print("cuda", torch.version.cuda, "cudnn", torch.backends.cudnn.version(),
              "gpu", torch.cuda.get_device_name(0) if torch.cuda.is_available() else "none")
    except Exception as e:
        print("torch probe failed:", e)

    print("== cache ==")
    gt_path = coco_demo.CACHE / "gt.json"
    gt = json.loads(gt_path.read_text())
    print("gt.json md5", md5(gt_path.read_bytes()), "keys", len(gt))
    keys = list(gt.keys())
    print("keys-order md5", md5("\n".join(keys).encode()))
    probe = keys[0]
    raw = (coco_demo.CACHE / "images" / probe).read_bytes()
    print(f"raw[{probe}] md5", md5(raw))

    print("== darken ==")
    dark = coco_demo.darken(raw, 0.25)
    print(f"darken(raw,0.25) md5", md5(dark))

    print("== trained models: WEIGHT hashes (state_dict tensors, not file bytes) ==")
    # file md5 is misleading — checkpoints embed dates/paths; hash the tensors instead.
    # Artifacts may sit under DATAPIPE_TAGS_TMP_DIR (training output) or DATAPIPE_TAGS_DIR
    # (persisted models) — scan both when they are LOCAL paths (skip s3:// etc.).
    dirs = []
    for var, default in (("DATAPIPE_TAGS_TMP_DIR", "/tmp/datapipe-tags"), ("DATAPIPE_TAGS_DIR", "")):
        v = os.environ.get(var, default)
        if v and "://" not in v:
            dirs.append(Path(v))
    found = sorted({p for d in dirs if d.exists() for p in d.rglob("best.pt")})
    if found:
        import torch

        def whash(p: Path) -> str:
            ckpt = torch.load(p, map_location="cpu", weights_only=False)
            sd = ckpt["model"].state_dict() if isinstance(ckpt, dict) and "model" in ckpt else ckpt
            h = hashlib.md5()
            for k in sorted(sd):
                h.update(k.encode())
                h.update(sd[k].cpu().numpy().tobytes())
            return h.hexdigest()

        for p in found[-6:]:
            print(p.parent.parent.name, whash(p))
    else:
        print("(no best.pt under", tmp, ")")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

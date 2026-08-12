"""Build the pre-staged demo cache once (download from COCO), so later loads are fast + offline and
the download and cache selections agree byte-for-byte.

Writes ``$DATAPIPE_TAGS_CACHE_DIR/gt.json`` + ``images/<fn>`` for the first N images of the CANONICAL
pool order (filenames sorted, then the fixed seeded shuffle in coco_demo). Because the cache persists
that exact order and loads read it AS-IS, an offset/n slice picks the same images whether it came from
a fresh download or from the cache — on any machine.

Run from ``examples/detection_tags/detection`` (so ``import coco_demo`` / ``config`` resolve):

    python ../scripts/build_cache.py [N]        # default N=500

Forces DOWNLOAD mode (temporarily hides any existing gt.json), rebuilds gt.json + images from scratch,
and restores the previous cache if the build fails partway.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, ".")  # run from examples/detection_tags/detection so `import coco_demo` resolves
import coco_demo  # noqa: E402


def main() -> int:
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 500
    cache: Path = coco_demo.CACHE
    images_dir = cache / "images"
    images_dir.mkdir(parents=True, exist_ok=True)
    gt_path = cache / "gt.json"

    # Force download mode so we build from the canonical COCO-derived order, not an existing cache.
    bak = cache / "gt.json.bak"
    if gt_path.exists():
        gt_path.rename(bak)

    try:
        src = coco_demo.CocoDemoSource()
        assert not src._use_cache, "expected download mode — remove/rename the cache gt.json first"
        gt: dict = {}
        for i, fn in enumerate(src.pool[:n], 1):
            raw, boxes, labels = src.fetch(fn)          # original (undarkened) bytes
            (images_dir / fn).write_bytes(raw)
            gt[fn] = {"bboxes": boxes, "labels": labels}
            if i % 50 == 0 or i == n:
                print(f"  {i}/{n} cached", flush=True)
        gt_path.write_text(json.dumps(gt))               # insertion order == canonical first-N
    except BaseException:
        if bak.exists() and not gt_path.exists():
            bak.rename(gt_path)                          # restore prior cache on failure
        raise

    bak.unlink(missing_ok=True)
    print(f"cache built: {len(gt)} images -> {cache}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

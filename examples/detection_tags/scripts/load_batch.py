"""Load a batch of cat/dog images into the demo (no Label Studio).

Downloads COCO cat/dog images, uploads them to object storage, and injects ground truth
directly (labels lowercase, as in COCO). Optionally darkens the images and attaches a tag —
this is how you add a "scenario" batch that the baseline model hasn't seen.

Two-step demo:
    # batch 1 — base data
    python scripts/load_batch.py --n 120
    # batch 2 — a tagged low-light scenario
    python scripts/load_batch.py --n 40 --tag night --darken 0.1

Then run the pipeline (ingest -> split -> freeze -> train -> metrics -> tag_metrics).
Run from examples/detection_tags/detection (so `import config` resolves).
"""
from __future__ import annotations

import argparse
import io
import os
import random
import sys
import time
import zipfile
from pathlib import Path

import fsspec
import numpy as np
import pandas as pd
import requests
from PIL import Image
from sqlalchemy import Column, JSON, String

sys.path.insert(0, ".")
import config  # noqa: E402
from datapipe.datatable import DataStore  # noqa: E402
from datapipe.store.database import TableStoreDB  # noqa: E402

COCO_IMG_BASE = "http://images.cocodataset.org/train2017/"
COCO_ANN_URL = "http://images.cocodataset.org/annotations/annotations_trainval2017.zip"
CACHE = Path(os.environ.get("DATAPIPE_TAGS_CACHE_DIR", "/tmp/datapipe-tags-cache"))
RNG_SEED = 1234


def _get(url: str, attempts: int = 4) -> requests.Response:
    last = None
    for a in range(attempts):
        try:
            r = requests.get(url, timeout=60, stream=True)
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            last = e
            time.sleep(min(2 ** a, 20))
    raise last


def coco_instances() -> dict:
    import json
    CACHE.mkdir(parents=True, exist_ok=True)
    zip_path = CACHE / "annotations_trainval2017.zip"
    if not zip_path.exists() or zip_path.stat().st_size < 1_000_000:
        print(f"downloading COCO annotations (~241MB) to {zip_path} ...", flush=True)
        with _get(COCO_ANN_URL) as r, open(zip_path, "wb") as f:
            for chunk in r.iter_content(1 << 20):
                f.write(chunk)
    with zipfile.ZipFile(zip_path) as zf, zf.open("annotations/instances_train2017.json") as h:
        return json.load(h)


def darken(im: Image.Image, gamma: float) -> Image.Image:
    a = np.asarray(im.convert("RGB")).astype(np.float32) / 255.0
    return Image.fromarray((np.power(a, 1.0 / gamma) * 255).clip(0, 255).astype(np.uint8))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=120, help="number of cat/dog images")
    ap.add_argument("--tag", type=str, default=None, help="tag id to attach (e.g. night)")
    ap.add_argument("--darken", type=float, default=None, help="gamma < 1 darkens (e.g. 0.1)")
    ap.add_argument("--offset", type=int, default=0, help="skip the first OFFSET picked images")
    args = ap.parse_args()

    inst = coco_instances()
    id_to_img = {im["id"]: im for im in inst["images"]}
    anns: dict[int, list] = {}
    for a in inst["annotations"]:
        if a.get("category_id") in config.COCO_CAT_IDS and not a.get("iscrowd", 0) and a.get("bbox"):
            anns.setdefault(a["image_id"], []).append(a)
    cat_dog_ids = sorted(anns.keys())
    random.seed(RNG_SEED)
    random.shuffle(cat_dog_ids)
    picked = cat_dog_ids[args.offset: args.offset + args.n]

    bucket = config.input_bucket()
    fs = fsspec.filesystem("s3", **{k: v for k, v in config.input_storage_options().items()})
    ds = DataStore(config.DBCONN, create_meta_table=True)

    s3_rows, gt_rows, tag_rows = [], [], []
    for i, iid in enumerate(picked):
        fn = id_to_img[iid]["file_name"]  # e.g. 000000123456.jpg
        stem, ext = os.path.splitext(fn)
        raw = _get(COCO_IMG_BASE + fn).content
        if args.darken is not None:
            name = f"{stem}__{args.tag or 'dark'}{ext}"
            buf = io.BytesIO()
            darken(Image.open(io.BytesIO(raw)), args.darken).save(buf, format="JPEG", quality=92)
            data = buf.getvalue()
        else:
            name = fn
            data = raw
        fs.pipe(f"{bucket}/images/{name}", data)
        boxes, labels = [], []
        for a in anns[iid]:
            x, y, w, h = a["bbox"]
            boxes.append([int(x), int(y), int(x + w), int(y + h)])
            labels.append(config.COCO_CAT_IDS[a["category_id"]])
        s3_rows.append({"image_name": name, "image_url": config.input_image_url(name)})
        gt_rows.append({"image_name": name, "bboxes": boxes, "labels": labels})
        if args.tag:
            tag_rows.append({"image_name": name, "tag_id": args.tag})
        if (i + 1) % 20 == 0:
            print(f"  {i + 1}/{len(picked)}", flush=True)

    def tbl(nm, sch):
        return ds.get_or_create_table(nm, TableStoreDB(dbconn=config.DBCONN, name=nm, data_sql_schema=sch, create_table=True))

    # s3_images gets refreshed by the ingest stage too, but write it here so the batch is
    # usable immediately; image_name matches what list_s3_images emits (basename).
    tbl("s3_images", [Column("image_name", String, primary_key=True), Column("image_url", String)]).store_chunk(pd.DataFrame(s3_rows))
    tbl("image__ground_truth", [Column("image_name", String, primary_key=True), Column("bboxes", JSON), Column("labels", JSON)]).store_chunk(pd.DataFrame(gt_rows))
    if args.tag:
        tbl("tag", [Column("tag_id", String, primary_key=True), Column("name", String)]).store_chunk(
            pd.DataFrame([{"tag_id": args.tag, "name": args.tag}]))
        tbl("image__tag", [Column("image_name", String, primary_key=True), Column("tag_id", String, primary_key=True)]).store_chunk(pd.DataFrame(tag_rows))
    print(f"loaded {len(s3_rows)} images"
          + (f", tag={args.tag}" if args.tag else "")
          + (f", darken={args.darken}" if args.darken is not None else ""), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Re-upload demo images to object storage after restoring demo_seed/db.sql.gz.

The seed dump restores all TABLES (models, metrics, run history) but not the MinIO objects the UI
renders. Re-deriving them beats shipping a 70MB tar: the cache bytes and darken() output are
byte-identical on every machine (md5-proven), so this reproduces exactly the objects the dump's
`s3_images` rows point at. Replays load_batch's upload loop over the restored `load_request` rows.

Run from examples/detection_tags/detection with .env sourced:
    ../.venv/bin/python ../scripts/seed_upload_images.py
"""
from __future__ import annotations

import os
import sys

import fsspec
import pandas as pd
import sqlalchemy as sa

sys.path.insert(0, ".")
import coco_demo  # noqa: E402
from coco_demo import CocoDemoSource, darken  # noqa: E402
from config import DB_URL, input_bucket, input_storage_options  # noqa: E402


def main() -> int:
    eng = sa.create_engine(DB_URL)
    schema = os.environ.get("DB_SCHEMA", "public")
    reqs = pd.read_sql(f'SELECT * FROM {schema}.load_request ORDER BY "offset"', eng)
    if reqs.empty:
        sys.exit("load_request is empty — restore demo_seed/db.sql.gz first")

    src = CocoDemoSource()
    fs = fsspec.filesystem("s3", **input_storage_options())
    bucket = input_bucket()
    n_up = 0
    for _, req in reqs.iterrows():
        n, offset = int(req["n"]), int(req.get("offset") or 0)
        tag = req.get("tag")
        tag = None if pd.isna(tag) or str(tag) == "" else str(tag)
        gamma = req.get("darken")
        gamma = None if pd.isna(gamma) or str(gamma) == "" else float(gamma)
        for fn in src.pool[offset : offset + n]:
            stem, ext = os.path.splitext(fn)
            raw, _, _ = src.fetch(fn)
            if gamma is not None:
                name, data = f"{stem}__{tag or 'dark'}{ext}", darken(raw, gamma)
            else:
                name, data = fn, raw
            fs.pipe(f"{bucket}/images/{name}", data)
            n_up += 1
        print(f"  {req['request_id']}: {n} images uploaded", flush=True)
    print(f"done: {n_up} objects -> {bucket}/images (byte-identical to the seed state)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Add a load request, then run the load step:  datapipe step --labels=stage=load run

Fixed-val demo (freeze val up front, add the tagged train batch later):

    # loaded BEFORE training model A — val is frozen from the start
    python ../scripts/add_request.py --id base-train --n 400 --offset 0   --subset train
    python ../scripts/add_request.py --id base-val   --n 100 --offset 400 --subset val
    python ../scripts/add_request.py --id night-val  --n 50  --offset 500 --subset val   --tag night --darken 0.25
    # loaded BEFORE retraining model B (the tagged TRAIN batch — the fix)
    python ../scripts/add_request.py --id night-train --n 50 --offset 550 --subset train --tag night --darken 0.25

--subset train|val pins every image of the batch to that subset so the random split never moves it.
Run from examples/detection_tags/detection (so `import config` resolves)."""
from __future__ import annotations

import argparse
import sys

import pandas as pd
from sqlalchemy import Column, Float, Integer, String

sys.path.insert(0, ".")
import config  # noqa: E402
from datapipe.datatable import DataStore  # noqa: E402
from datapipe.store.database import TableStoreDB  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", required=True, help="request_id (unique per batch)")
    ap.add_argument("--n", type=int, default=450)
    ap.add_argument("--offset", type=int, default=0, help="skip the first OFFSET picked COCO images")
    ap.add_argument("--tag", default=None)
    ap.add_argument("--darken", type=float, default=None, help="gamma < 1 darkens (e.g. 0.1)")
    ap.add_argument("--subset", default=None, choices=["train", "val"],
                    help="pin every image of this batch to a subset (freezes val); omit to random-split")
    a = ap.parse_args()

    ds = DataStore(config.DBCONN, create_meta_table=True)
    dt = ds.get_or_create_table("load_request", TableStoreDB(
        dbconn=config.DBCONN, name="load_request",
        data_sql_schema=[
            Column("request_id", String, primary_key=True),
            Column("n", Integer), Column("offset", Integer),
            Column("tag", String), Column("darken", Float),
            Column("subset", String),
        ], create_table=True))
    dt.store_chunk(pd.DataFrame([{
        "request_id": a.id, "n": a.n, "offset": a.offset, "tag": a.tag,
        "darken": a.darken, "subset": a.subset,
    }]))
    print(f"added request {a.id}: n={a.n} offset={a.offset} tag={a.tag} "
          f"darken={a.darken} subset={a.subset}")
    print("now run:  datapipe step --labels=stage=load run")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

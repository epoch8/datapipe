"""Add a load request, then run the load step:  datapipe step --labels=stage=load run

    # batch 1 — base cat/dog
    python ../scripts/add_request.py --id base --n 120
    # batch 2 — tagged low-light scenario
    python ../scripts/add_request.py --id night --n 40 --offset 120 --tag night --darken 0.1

Run from examples/detection_tags/detection after `datapipe db create-all`."""
from __future__ import annotations

import argparse
import sys

import pandas as pd

sys.path.insert(0, ".")
import config  # noqa: E402
from data import catalog  # noqa: E402
from datapipe.datatable import DataStore  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", required=True, help="request_id (unique per batch)")
    ap.add_argument("--n", type=int, default=450)
    ap.add_argument("--offset", type=int, default=0, help="skip the first OFFSET picked COCO images")
    ap.add_argument("--tag", default=None)
    ap.add_argument("--darken", type=float, default=None, help="gamma < 1 darkens (e.g. 0.1)")
    a = ap.parse_args()

    ds = DataStore(config.DBCONN)
    catalog.get_datatable(ds, "load_request").store_chunk(pd.DataFrame([{
        "request_id": a.id, "n": a.n, "offset": a.offset, "tag": a.tag, "darken": a.darken,
    }]))
    print(f"added request {a.id}: n={a.n} offset={a.offset} tag={a.tag} darken={a.darken}")
    print("now run:  datapipe step --labels=stage=load run")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

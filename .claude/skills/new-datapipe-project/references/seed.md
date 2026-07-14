# Deterministic seed skeleton

The universal starting point of every project. Scaffold it, run the gate, THEN grow. Layout follows
the datapipe examples convention so pattern cards apply verbatim later:

```
<project>/
├── pyproject.toml
├── uv.lock                  # committed after first sync — the cross-machine contract
├── .env.example             # + .env (gitignored, chmod 600)
├── docker-compose.yml       # Postgres minimum; add services per SPEC
├── docs/SPEC.md
├── .gitignore               # .env, .venv, __pycache__, data/
└── <package>/
    ├── config.py            # env parsing (fail loudly if DB_URL missing)
    ├── data.py              # Catalog: table schemas
    ├── steps.py             # transform functions
    └── app.py               # Pipeline wiring + stage labels
```

## Dependencies (decide once, record in SPEC)

Git-subdirectory installs from the monorepo are VERIFIED to work outside the workspace (core and
ml both resolve and import; tested on linux-x86 and mac). Pin as git deps to a monorepo rev:

```toml
[project]
name = "<project>"
requires-python = ">=3.10,<3.13"
dependencies = [
    "datapipe-core @ git+https://github.com/epoch8/datapipe@<REV>#subdirectory=libs/datapipe-core",
    # add per blocks: datapipe-ml (+#subdirectory=libs/datapipe-ml), datapipe-app, ...
    # "stringzilla==4.4.0",  # REQUIRED whenever datapipe-ml is used (transitive albumentations dep breaks unpinned)
    "pandas", "sqlalchemy", "psycopg2-binary", "python-dotenv",
]
```

Platform note: `datapipe-ml` does not install on macOS ARM (`ray` has no arm wheel) — ML-block
projects run on linux-x86; plain-ETL projects (core only) work anywhere.

Pin `<REV>` to a commit sha, not a branch. After `uv sync`, COMMIT `uv.lock`. Never edit deps
ad-hoc later — re-locking drifts versions across machines and changes results.

## Minimal code (adapt names; this is the proven shape, not a fill-in template)

`config.py` — loads `.env` (`load_dotenv()`), exports `DBCONN = DBConn(DB_URL, DB_SCHEMA)`; raise
with a helpful message if `DB_URL` unset. Set determinism env at the TOP of `app.py` (before any
torch import), e.g. `os.environ.setdefault("CUBLAS_WORKSPACE_CONFIG", ":4096:8")` when training is
planned.

`data.py`:
```python
from datapipe.compute import Catalog, Table
from datapipe.store.database import TableStoreDB
from sqlalchemy import Column, String, JSON
from config import DBCONN

catalog = Catalog({
    "items": Table(store=TableStoreDB(name="items", dbconn=DBCONN, create_table=True,
        data_sql_schema=[Column("item_id", String, primary_key=True), Column("payload", JSON)])),
    "items_out": Table(store=TableStoreDB(name="items_out", dbconn=DBCONN, create_table=True,
        data_sql_schema=[Column("item_id", String, primary_key=True), Column("payload", JSON)])),
})
```

`steps.py`:
```python
import pandas as pd

def passthrough(df: pd.DataFrame) -> pd.DataFrame:
    return df
```

`app.py`:
```python
from datapipe.compute import DatapipeApp, Pipeline
from datapipe.datatable import DataStore
from datapipe.step.batch_transform import BatchTransform
import steps
from config import DBCONN
from data import catalog

pipeline = Pipeline([
    BatchTransform(steps.passthrough, inputs=["items"], outputs=["items_out"],
                   transform_keys=["item_id"], labels=[("stage", "seed")]),
])
ds = DataStore(DBCONN, create_meta_table=True)
app = DatapipeApp(ds, catalog, pipeline)
```

## The bring-up gate (nothing is added until this passes)

```bash
cp .env.example .env            # user fills/verifies — pause here
docker compose up -d
uv sync                         # then commit uv.lock
datapipe db create-all
# put one real sample row into `items` (tiny script or psql insert), then:
datapipe run
# GATE: items_out contains that row. Show the SELECT to the user.
```

Verify by querying the table, not by exit code. From here, grow block by block per the pattern
cards, replacing the passthrough with the first real transform.

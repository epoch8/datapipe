# Datapipe App example (SQLite + Alembic)

Minimal pipeline with `datapipe-app`: three SQL tables, one batch transform, and
`/api/v1alpha3` (graph / table data / capabilities / settings / reset-metadata).

This branch does **not** expose runs or run-logs APIs. Capabilities are addon-based
(`datapipe.capabilities` entry points) and do not hard-code ML features.

## Setup

From this directory:

```bash
uv sync
export DB_CONN_URI="${DB_CONN_URI:-sqlite+pysqlite3:///store.sqlite}"
uv run alembic upgrade head
# or for a throwaway local DB:
# uv run datapipe db create-all
```

## Run

```bash
uv run datapipe run
uv run datapipe api --port 8000
```

Open http://localhost:8000 — UI plus `/api/v1alpha3/...`.

Useful endpoints:

- `GET /api/v1alpha3/capabilities`
- `GET /api/v1alpha3/settings`
- `GET /api/v1alpha3/graph`
- `POST /api/v1alpha3/get-table-data`
- `POST /api/v1alpha3/transforms/{name}/reset-metadata`

## Tests

Library coverage lives in `libs/datapipe-app/tests/test_v1alpha3.py`. Example smoke:

```bash
uv run pytest tests
```

# Installation

## Requirements

- Python 3.10 or later
- A SQL database for the metadata store (SQLite for local development, PostgreSQL for production)

## Install from PyPI

The core package is published as `datapipe-core`:

```bash
pip install datapipe-core
```

For local development with SQLite, add the `sqlite` extra. Python ships with an older SQLite version that datapipe cannot use — the extra installs a compatible binary:

```bash
pip install "datapipe-core[sqlite]"
```

## Install from the monorepo (`uv`)

This repository is a `uv` workspace (`libs/*`). From the repo root:

```bash
uv sync --all-packages
# or a single package:
uv sync --package datapipe-core
uv pip install -e "libs/datapipe-core[sqlite,ray]"
```

Editable installs of sibling packages work the same way (`libs/datapipe-app`, `libs/datapipe-ml`, …).

## Ops and ML are separate packages

`datapipe-core` is the ETL library and CLI. Ops UI/API and ML helpers are **not** included:

| Package | Role |
|---|---|
| `datapipe-core` | Pipeline, meta store, steps, CLI (`datapipe`) |
| `datapipe-app` | REST API, observability, Ops CLI extensions |
| `datapipe-ml` | ML training / inference helpers on top of core |
| `datapipe-ui` / `datapipe-ui-ml` | Front-end assets pulled in via app extras |

### Ops (`datapipe-app`)

```bash
pip install "datapipe-app[ui]"     # API + core Ops UI
pip install "datapipe-app[ml]"     # UI + ML ops backend + ML UI plugin
```

From the monorepo:

```bash
uv pip install -e "libs/datapipe-app[ui]"
uv pip install -e "libs/datapipe-app[ml]"
```

UI static assets still need a build step when developing from source — see [Install and run Ops](../ops/install-and-run.md).

### ML (`datapipe-ml`)

```bash
pip install datapipe-ml
# optional: pip install "datapipe-ml[torch]" / [tensorflow] / …
```

See [Integrations](../integrations/index.md).

## Optional extras (`datapipe-core`)

| Extra | Installs |
|---|---|
| `sqlite` | `pysqlite3-binary` — required for SQLite support |
| `alembic` | Alembic for schema migrations |
| `redis` | `redis` client |
| `elastic` | `elasticsearch` client |
| `qdrant` | `qdrant-client` |
| `milvus` | `pymilvus` |
| `ray` | `ray[default]` — for parallel execution across steps |
| `gcsfs` | `gcsfs` — for Google Cloud Storage file backends |
| `s3fs` | `s3fs` — for S3 file backends |
| `excel` | `xlrd`, `openpyxl` — for Excel file backends |
| `gcp` | OpenTelemetry GCP trace exporter |
| `pyarrow` | Parquet file backend support |
| `neo4j` | Neo4j graph store backend |

Multiple extras can be combined:

```bash
pip install "datapipe-core[sqlite,redis,ray]"
```

## Verify

```bash
datapipe --help
```

This should print the datapipe CLI help. If the command is not found, check that the Python environment where you installed the package is active.

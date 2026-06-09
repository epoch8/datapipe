# Installation

## Requirements

- Python 3.10 or later
- A SQL database for the metadata store (SQLite for local development, PostgreSQL for production)

## Install

The package is published as `datapipe-core` on PyPI:

```bash
pip install datapipe-core
```

For local development with SQLite, add the `sqlite` extra. Python ships with an older SQLite version that datapipe cannot use — the extra installs a compatible binary:

```bash
pip install "datapipe-core[sqlite]"
```

## Optional extras

| Extra | Installs |
|---|---|
| `sqlite` | `pysqlite3-binary` — required for SQLite support |
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
pip install "datapipe-core[sqlite,redis]"
```

## Verify

```bash
datapipe --help
```

This should print the datapipe CLI help. If the command is not found, check that the Python environment where you installed the package is active.

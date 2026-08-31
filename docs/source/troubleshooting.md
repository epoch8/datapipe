# Troubleshooting

Short answers to common “why isn’t it doing what I expect?” questions. Most of these come back to [Incremental Processing](./concepts/incremental-processing.md).

## Nothing runs on the second pass

**Expected.** If inputs were rewritten with the **same content**, CityHash matches, `update_ts` does not move, and the step is not scheduled. Your `func` never runs — that is the incremental win.

See the “Unchanged” panels and scheduling rule in [Incremental Processing](./concepts/incremental-processing.md).

To force work: change content, delete keys, reset transform meta (`datapipe step reset-metadata`), or fix a failed run (`is_success=False` keeps keys dirty).

## Empty dirty set / run finishes instantly

Same story: no keys where `max(input.update_ts) > step.process_ts` (and no missing / failed step rows). Check table and step `*_meta`, or study the insert/update/delete panels on the incremental page.

Ops “runs” can still be recorded even when almost no `func` work happens.

## Soft delete vs hard delete

| Layer | What happens on delete |
|---|---|
| Data store | **Hard delete** — row removed from the store |
| Table meta | **Soft delete** — key kept with `delete_ts` set; `update_ts` bumps so downstream steps still schedule |

Downstream cleanup is driven by meta, not by “row still in the store.” Details: [Soft Delete](./concepts/soft-delete.md) and the delete panels in [Incremental Processing](./concepts/incremental-processing.md). Re-inserting the same key after delete is **resurrection** — see [Soft Delete § Resurrection](./concepts/soft-delete.md#resurrection).

## Stale or missing output rows after a transform

Often **`processed_idx`** cleanup: the batch index defines which output keys this run owns; rows under that index but absent from your returned DataFrame are deleted. In 1-to-N steps, returning only part of the child set **silently removes** the rest.

→ [Output Cleanup and `processed_idx`](./concepts/processed-idx.md) · figure: [05-processed-idx](./concepts/incremental-processing.md#output-cleanup-processed_idx)

Wrong **`transform_keys`** or multi-input join grain can also schedule the wrong units of work or skip pairs you expected.

→ [Transform Grain](./concepts/transform-grain.md)

## Where are the incremental figures / docs?

Six **Before / During / After** table panels (insert, update, delete, unchanged, processed_idx, resurrection) live on:

→ **[Incremental Processing](./concepts/incremental-processing.md)**

Source assets: `docs/source/assets/incremental/*.png` (Before / During / After table panels from `docs/scripts/render_incremental_panels.py`).

## SQLite errors / old SQLite version

Do not use the stdlib `sqlite://` driver for meta. Install the extra and use `pysqlite3`:

```bash
pip install "datapipe-core[sqlite]"
```

```python
DBConn("sqlite+pysqlite3:///db.sqlite")
```

Prefer `pysqlite3-binary` (via the extra) over a plain `pysqlite3` package that may ship an outdated engine. See [Use SQLite as Metadata Store](./how-to/using-sqlite.md).

## Alembic vs `db create-all` conflict

If the DB has a stamped `alembic_version` table, `datapipe db create-all` **refuses** to create/drop/alter. Use `alembic upgrade` instead.

`create-all` is for empty local scratch DBs only. Production: [Alembic](./how-to/alembic-migrations.md), [Postgres](./how-to/production-postgres.md).

## Duplicate step names

After `build_compute`, two compute steps with the same `name` raise `Duplicate step name: …`. Set explicit unique `name=` on steps, or rely on distinct auto-munged names from func + I/O.

## Labels filter too strict

CLI `--labels=k=v,k2=v2` requires the step to have **all** listed pairs. A step tagged only `stage=enrich` will not match `--labels=stage=enrich,team=search`.

Use fewer labels, or combine with `--name` prefix matching. See [Filter Steps by Labels](./how-to/filter-by-labels.md).

## `fail_fast` if it exists

`RunConfig.fail_fast` (default `False`, overridable with env `DATAPIPE_FAIL_FAST`) stops the whole run when a batch errors. Otherwise the error is logged, transform meta is marked unsuccessful, and other keys can continue — failed keys stay dirty for a later run.

```python
from datapipe.run_config import RunConfig

run_config = RunConfig(fail_fast=True)
```

## See also

- [Incremental Processing](./concepts/incremental-processing.md) — start here
- [Soft Delete](./concepts/soft-delete.md) · [Transform Grain](./concepts/transform-grain.md) · [Output Cleanup and `processed_idx`](./concepts/processed-idx.md)
- [Change Detection and Merging](./explanation/change-detection.md)
- [CLI reference](./reference/cli.md)

# `fail_fast` flag in `RunConfig`

## Problem

By default, `BatchTransformStep` swallows per-batch exceptions: when processing
fails for a chunk, the error is stored in the meta table and processing
continues with the next chunk. This is the right behavior in production, but
during development it makes debugging painful — errors are silently swallowed
and you have to go looking for them in error tables.

## Solution

Add a `fail_fast: bool` field to `RunConfig`. When `True`, any exception raised
during batch processing is re-raised immediately instead of being stored and
swallowed. This lets developers get a full traceback at the point of failure
rather than discovering errors after the full pipeline run.

```python
# Production default — keep processing, store errors
step.run_full(ds)

# Development — stop on first error
step.run_full(ds, run_config=RunConfig(fail_fast=True))
```

## Default value

The default is controlled by the `DATAPIPE_FAIL_FAST` environment variable,
parsed by `datapipe/settings.py` via `pydantic-settings`. Pydantic accepts any
of `1`, `true`, `yes`, `on` (case-insensitive), so you can enable fail-fast
globally for a dev environment without changing any code:

```sh
DATAPIPE_FAIL_FAST=true python my_pipeline.py
```

All datapipe env vars live in `DatapipeSettings` (prefix `DATAPIPE_`), making
it the single place to look for runtime configuration.

Tests override the default to `True` via `conftest.py`:

```python
from datapipe.settings import settings
settings.fail_fast = True
```

Tests that specifically exercise error-swallowing behavior opt out explicitly
with `RunConfig(fail_fast=False)`.

## Affected code

- `datapipe/settings.py` — new `DatapipeSettings` (pydantic-settings, prefix `DATAPIPE_`); single home for all runtime env config
- `datapipe/run_config.py` — adds the field, default sourced from `settings.fail_fast`
- `datapipe/step/batch_transform.py` — re-raises instead of storing when `fail_fast=True`
- `datapipe/executor/__init__.py` / `ray.py` — wrap the `idx_gen` loop in
  `try/finally` so the generator is always closed, even when `fail_fast` causes
  an early exit
- `datapipe/meta/sql_meta.py` — `get_stale_idx` changed from loading all rows
  into a list to a proper generator (`yield`), so the `finally: idx_gen.close()`
  pattern works correctly

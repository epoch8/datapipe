# Documentation writer guide

Templates and conventions for Datapipe **reference** pages under `docs/source/reference/`. Reference pages answer: *What exactly does this symbol do?* They do not teach concepts or walk through tasks — link out for those.

Audience: contributors adding or updating API docs. This file lives outside mdBook (`docs/writer-guide.md`); link it from [docs/README.md](./README.md) if helpful.

---

## Page structure

Every reference page follows this skeleton:

```markdown
# Title (symbol or group name)

One sentence: what this is and when a reader looks it up.

Module: `datapipe.module`

---

## `ClassName` or `function_name`

When to use: …

```python
# minimal signature
```

### Parameters

| Name | Type | Default | Description |
|---|---|---|---|
| … | … | … | … |

### Returns

…

### Raises

…

### Behavior notes

- …

### Example

```python
…
```

### See also

- [Related page](./path.md)
```

Rules:

- **English only** on reference pages (translations live elsewhere if needed).
- **Diátaxis reference style**: complete, precise, lookup-oriented. No motivation essays.
- **Accurate to code**: read `libs/datapipe-core/datapipe/` before writing; do not infer behavior.
- **No WIP banners** (`**Work in progress.**` / `**Needs review.**`) on finished pages.
- Prefer documenting **public** surfaces: classes and methods users call from pipelines, CLI, or custom steps.
- Skip private helpers (`_foo`), unless they explain non-obvious runtime behavior referenced from a public method.

---

## Class page template

Use for types like `ComputeStep`, `DataTable`, `BatchTransform`.

```markdown
# ComputeStep

Runtime unit in the compute graph: binds input/output `DataTable`s and defines how to run full, changelist, or index-scoped execution.

Module: `datapipe.compute`

---

## Overview

| Attribute / field | Type | Description |
|---|---|---|
| `name` | `str` | Unique step name in the pipeline. |
| … | … | … |

---

## `ComputeStep.__init__`

When to use: Construct a custom runtime step (subclass) or inspect fields on a built step.

```python
def __init__(
    self,
    name: str,
    input_dts: Sequence[ComputeInput],
    output_dts: Sequence[ComputeOutput],
    labels: Labels | None = None,
    executor_config: ExecutorConfig | None = None,
) -> None: ...
```

### Parameters

| Name | Type | Default | Description |
|---|---|---|---|
| `name` | `str` | — | Unique identifier; duplicate names fail at `build_compute`. |
| `input_dts` | `Sequence[ComputeInput]` | — | Input tables and join/key options. |
| `output_dts` | `Sequence[ComputeOutput]` | — | Output tables and optional key maps. |
| `labels` | `Labels \| None` | `None` | `[(key, value), …]` for CLI filtering. |
| `executor_config` | `ExecutorConfig \| None` | `None` | Parallelism/resources for batch executors. |

### Returns

`None`

### Raises

None from the constructor itself.

### Behavior notes

- Subclasses must implement abstract run/status methods.
- `input_dts` / `output_dts` are stored as lists (copies of the sequence).

### Example

```python
# Usually obtained via PipelineStep.build_compute, not constructed directly.
steps = my_step.build_compute(ds, catalog)
assert isinstance(steps[0], ComputeStep)
```

---

## `ComputeStep.run_full`

… (repeat method template for each public method)
```

Guidelines:

- Put **Overview** (fields/properties) once at the top for dataclasses and stateful classes.
- Document **each public method** with the method template below.
- For `@property`, use `` `ClassName.property_name` `` as the heading and omit `self` from the signature block if useful.

---

## Method template

Copy this block per method.

```markdown
## `ClassName.method_name`

When to use: One line — the caller intent (e.g. "Run all pending indexes for this step").

```python
def method_name(
    self,
    ds: DataStore,
    run_config: RunConfig | None = None,
) -> ReturnType: ...
```

### Parameters

| Name | Type | Default | Description |
|---|---|---|---|
| `ds` | `DataStore` | — | Active datastore (meta + table registry). |
| `run_config` | `RunConfig \| None` | `None` | Filters, labels, callbacks, `fail_fast`. |

### Returns

`ReturnType` — what the caller receives (shape, empty cases).

### Raises

| Exception | When |
|---|---|
| `NotImplementedError` | Base class; subclass must override. |
| `ValueError` | … |

Omit the **Raises** section if the method does not raise.

### Behavior notes

- Side effects (DB writes, meta updates).
- Interaction with `RunConfig.filters`, callbacks, tracing.
- Edge cases: empty index, zero batches, no-op paths.

### Example

```python
step.run_full(ds, run_config=RunConfig(fail_fast=True))
```
```

For **classmethod** / **staticmethod**, adjust the signature and heading accordingly.

---

## Function page template

For module-level functions (`build_compute`, `run_steps`, `get_cancel_token`):

```markdown
## `build_compute`

When to use: Turn a declarative `Pipeline` into a list of runtime `ComputeStep`s.

```python
def build_compute(
    ds: DataStore,
    catalog: Catalog,
    pipeline: Pipeline,
) -> list[ComputeStep]: ...
```

### Parameters

…

### Returns

…

### Raises

| Exception | When |
|---|---|
| `Exception` | Duplicate step name. |
| `ValueError` | Join-key type mismatch from `validate()`. |

### Behavior notes

…

### Example

…
```

---

## Dataclass / Protocol template

**Dataclass** (e.g. `RunConfig`, `ComputeInput`):

- Document fields in an **Overview** table.
- Document `@property` methods with the method template.
- Constructor = generated from fields; one combined **Parameters** table is enough unless custom `__init__` exists.

**Protocol** (e.g. `RunCallback`):

- List required methods in a table with brief contracts.
- Note: structural subtyping — any object with matching methods works.
- Document bundled implementations (`CompositeRunCallback`) as separate classes.

---

## How-to cross-link template

Reference pages should link out, not duplicate guides:

```markdown
### See also

- [Incremental processing](../concepts/incremental-processing.md) — why changelist mode exists
- [Filter by labels](../how-to/filter-by-labels.md) — applying `RunConfig.filters`
- [Compute step lifecycle](../explanation/compute-step-lifecycle.md) — internal sequencing
```

Use relative paths from the current file under `docs/source/`.

---

## Inventory checklist

When adding a new public symbol:

1. Add or extend a page under `docs/source/reference/`.
2. Add a row to `docs/inventory-map.yaml` (`SymbolName: reference/….md`).
3. Add a link in `docs/source/SUMMARY.md` if the page is new.
4. Run `python docs/scripts/inventory_core.py --strict` from the repo root.

---

## Minimal example (full method entry)

```markdown
## `DataTable.store_chunk`

When to use: Upsert a batch of rows and optionally delete indexes that were processed but missing from the chunk.

```python
def store_chunk(
    self,
    data_df: DataDF,
    processed_idx: IndexDF | None = None,
    now: float | None = None,
    run_config: RunConfig | None = None,
) -> IndexDF: ...
```

### Parameters

| Name | Type | Default | Description |
|---|---|---|---|
| `data_df` | `DataDF` | — | Rows to insert or update. Primary-key values must be unique. |
| `processed_idx` | `IndexDF \| None` | `None` | If set, rows in this index that exist in meta but not in `data_df` are deleted. Ignored when it shares no columns with table primary keys. |
| `now` | `float \| None` | `None` | Timestamp for meta; defaults inside meta helpers. |
| `run_config` | `RunConfig \| None` | `None` | Passed through to delete path. |

### Returns

`IndexDF` — concatenation of new, changed, and deleted index rows from this call.

### Raises

| Exception | When |
|---|---|
| `ValueError` | Duplicate primary-key values in `data_df`. |

### Behavior notes

- Hashes rows via `table_store.hash_rows`, splits new/changed via `meta.get_changes_for_store_chunk`.
- Empty `data_df` still runs cleanup when `processed_idx` is set.

### Example

```python
changed = dt.store_chunk(out_df, processed_idx=batch_idx, now=time.time())
```

### See also

- [Change detection](../explanation/change-detection.md)
```

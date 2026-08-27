# BatchGenerate vs BatchTransform

Both steps move data through the pipeline, but they use **different incremental machinery**. Mixing up their semantics leads to "why doesn't my generator skip unchanged rows?" or "why does my transform re-list S3 every time?"

## Side-by-side

| | **BatchGenerate** | **BatchTransform** |
|---|---|---|
| **Inputs** | None (seeds outputs) | One or more tables |
| **Function shape** | Generator (`yield` chunks) | Plain callable (DataFrame → DataFrame) |
| **Scheduling** | Runs as a **whole step** each time the pipeline executes it | **Per transform key** via `{step}_meta` + input `update_ts` |
| **Step meta** | No transform-meta dirty SQL | `{step}_meta`: `process_ts`, `is_success` per key |
| **Write path** | `store_chunk` **without** `processed_idx` | `store_chunk` **with** `processed_idx` for outputs |
| **Stale rows** | Optional `delete_stale_by_process_ts` after generator finishes | Per-key cleanup via `processed_idx` or `None` delete path |
| **Unchanged content** | Table **hash** unchanged → `update_ts` does not move → downstream skips | Same hash rule on inputs; step itself skips unchanged **keys** |

```text
BatchGenerate                    BatchTransform
─────────────                    ──────────────
yield chunks  ──► store_chunk    dirty keys SQL ──► batches ──► func ──► store_chunk(+ processed_idx)
       │                                                              │
       └── delete_stale (optional)                                    └── step_meta updated per key
```

## BatchGenerate: hash and stale deletion

Each yielded chunk is written with ordinary `store_chunk` — no `processed_idx`. Row-level delete inside a chunk still works if you pass `processed_idx` manually from app code, but the generator step does not do transform-style batch cleanup.

After the generator exhausts:

- With **`delete_stale=True`** (default), rows whose meta `process_ts` is older than the run start are soft-deleted — useful for "full refresh" feeds where absent keys should disappear.
- Identical re-yielded content matches existing **CityHash** → data not rewritten, **`update_ts` unchanged** → downstream `BatchTransform` steps do not re-run for those keys.

Generators do **not** maintain `{step}_meta` like transforms. Incremental benefit is mostly at the **table meta** layer and downstream.

## BatchTransform: step meta and keys

Transforms select dirty keys with SQL (`max(input.update_ts)` vs `step.process_ts`). Each batch:

1. Loads input DataFrames for the index.
2. Calls `func`.
3. Writes outputs with `processed_idx` or deletes on `None`.
4. Updates `{step}_meta` for those transform keys.

Failed batches leave `is_success=False` so keys stay dirty. Crashes mid-run resume on still-dirty keys.

## When to use which

| Use **BatchGenerate** when… | Use **BatchTransform** when… |
|---|---|
| Pulling from API, listing files, bootstrapping catalog tables | Deriving one table from another |
| Source of truth is outside Datapipe | Work is keyed and should rerun only on change |
| Full pass with optional stale purge is OK | You need per-key changelist propagation |

For data written entirely outside Datapipe but stored in catalog tables, consider **`UpdateExternalTable`** instead — it syncs meta without re-implementing a generator.

## Tips & pitfalls

| Pitfall | Context | Guidance |
|---|---|---|
| **Expecting transform-style skip inside Generate** | Generator always runs its loop when the step runs | Push incremental work to downstream transforms, or externalize with `UpdateExternalTable` |
| **`delete_stale=False` on a full feed** | Old keys never removed | Default `True` unless you manage deletes explicitly |
| **Using Generate for pairwise work** | No input join semantics | Use multi-input `BatchTransform` with `transform_keys` |
| **Assuming step_meta exists for Generate** | Monitoring / reset-metadata confusion | Only transform steps have `{step}_meta` |
| **Large generator without chunking** | Memory spikes | Yield reasonably sized DataFrames |

## See also

- [Pipeline Steps](./pipeline-steps.md) — step catalog and patterns
- [Incremental Processing](./incremental-processing.md) — transform scheduling
- [Compute Step Lifecycle](../explanation/compute-step-lifecycle.md) — batch call tree
- [BatchGenerate](../reference/steps/batch-generate.md) / [BatchTransform](../reference/steps/batch-transform.md) — API reference

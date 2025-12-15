# Offset Optimization

Offset optimization is a feature that improves performance of incremental processing by tracking the last processed timestamp (offset) for each input table in a transformation. This allows Datapipe to skip already-processed records without scanning the entire transformation metadata table.

## How It Works

### Without Offset Optimization (v1)

The traditional approach (v1) uses a FULL OUTER JOIN between input tables and transformation metadata:

```sql
SELECT transform_keys
FROM input_table
FULL OUTER JOIN transform_meta ON transform_keys
WHERE input.update_ts > transform_meta.process_ts
   OR transform_meta.is_success != True
```

This approach:
- ✅ Always correct - finds all records that need processing
- ❌ Scans entire transformation metadata table on every run
- ❌ Performance degrades as metadata grows

### With Offset Optimization (v2)

The optimized approach (v2) uses per-input-table offsets to filter data early:

```sql
-- For each input table, filter by offset first
WITH input_changes AS (
    SELECT transform_keys, update_ts
    FROM input_table
    WHERE update_ts >= :offset  -- Early filtering by offset
),
error_records AS (
    SELECT transform_keys
    FROM transform_meta
    WHERE is_success != True
)
-- Union all changes
SELECT transform_keys, update_ts
FROM input_changes
UNION ALL
SELECT transform_keys, NULL as update_ts
FROM error_records
-- Then check process_ts to avoid reprocessing
LEFT JOIN transform_meta ON transform_keys
WHERE update_ts IS NULL  -- Error records
   OR process_ts IS NULL  -- Never processed
   OR (is_success = True AND update_ts > process_ts)  -- Updated after processing
ORDER BY update_ts, transform_keys
```

This approach:
- ✅ Filters most records early using index on `update_ts`
- ✅ Only scans records with `update_ts >= offset`
- ✅ Performance stays constant regardless of metadata size
- ⚠️ Requires careful implementation to avoid data loss

## Key Implementation Details

### 1. Inclusive Inequality (`>=` not `>`)

The offset filter must use `>=` instead of `>`:

```python
# Correct
WHERE update_ts >= offset

# Wrong - loses records with update_ts == offset
WHERE update_ts > offset
```

### 2. Process Timestamp Check

After filtering by offset, we must check `process_ts` to avoid reprocessing:

```python
WHERE (
    update_ts IS NULL  # Error records (always process)
    OR process_ts IS NULL  # Never processed
    OR (is_success = True AND update_ts > process_ts)  # Updated after last processing
)
```

This prevents infinite loops when using `>=` offset.

### 3. Ordering

Results are ordered by `update_ts` first, then `transform_keys` for determinism:

```sql
ORDER BY update_ts, transform_keys
```

This ensures that:
- Records are processed in chronological order
- The offset accurately represents the last processed timestamp
- No records with earlier timestamps are skipped

### 4. Error Records

Records that failed processing (`is_success != True`) are always included via a separate CTE, regardless of offset:

```sql
error_records AS (
    SELECT transform_keys, NULL as update_ts
    FROM transform_meta
    WHERE is_success != True
)
```

Error records have `update_ts = NULL` to distinguish them from changed records.

## Enabling Offset Optimization

Offset optimization is controlled by the `use_offset_optimization` field in transform configuration:

```python
BatchTransform(
    func=my_transform,
    inputs=[input_table],
    outputs=[output_table],
    # Add this field to enable offset optimization
    use_offset_optimization=True,
)
```

When enabled, Datapipe tracks offsets in the `offset_table` and uses them to optimize changelist queries.

## Important Considerations

### Timestamp Accuracy

The offset optimization relies on accurate timestamps. If you manually call `store_chunk()` with a `now` parameter that is in the past:

```python
# Warning: This may cause data loss with offset optimization!
dt.store_chunk(data, now=old_timestamp)
```

Datapipe will log a warning:

```
WARNING - store_chunk called with now=X which is Ys in the past.
This may cause data loss with offset optimization if offset > now.
```

In normal operation, `store_chunk()` uses the current time automatically, so this is not a concern unless you explicitly provide the `now` parameter.

### When to Use

Offset optimization is most beneficial when:
- ✅ Transformations have large metadata tables (many processed records)
- ✅ Incremental updates are small compared to total data
- ✅ Input tables have an index on `update_ts`

It may not help when:
- ❌ Processing all data on every run (full refresh)
- ❌ Metadata table is small (< 10k records)
- ❌ Most records are updated on every run

## See Also

- [How Merging Works](./how-merging-works.md) - Understanding the changelist query strategy
- [BatchTransform](./reference-batchtransform.md) - Transform configuration reference
- [Lifecycle of a ComputeStep](./transformation-lifecycle.md) - Transformation execution flow

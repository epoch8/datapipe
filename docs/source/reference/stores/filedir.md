# Filedir store

One file per row, path pattern encodes primary keys. Uses fsspec.

Module: `datapipe.store.filedir`

---

## `TableStoreFiledir`

When to use: Images, JSON docs, blobs, or parquet payloads addressed by path.

```python
class TableStoreFiledir(TableStore):
    def __init__(
        self,
        filename_pattern: str | Path,
        adapter: ItemStoreFileAdapter,
        add_filepath_column: bool = False,
        primary_schema: DataSchema | None = None,
        read_data: bool = True,
        readonly: bool | None = None,
        enable_rm: bool = False,
        fsspec_kwargs: dict[str, Any] | None = None,
    ): ...
```

### Arguments

| Arg | Description |
|---|---|
| `filename_pattern` | fsspec path (no chaining). `{id_field}` → primary key; `*` / `**` → not indexed (forces read-only unless `readonly=False` which errors). Optional suffixes `(jpg\|png)`. |
| `adapter` | Load/dump/hash adapter (see below). |
| `add_filepath_column` | Add `filepath` column on read. |
| `primary_schema` | Explicit PK types (`String`/`Integer` only); must match `{...}` names. Default: all `String`. |
| `read_data` | If `False`, skip parsing file bodies. |
| `readonly` | `None` → auto (`True` if pattern has `*`). |
| `enable_rm` | Allow `delete_rows` to remove files (all OR-suffixes). |
| `fsspec_kwargs` | Passed to `fsspec.filesystem`; sets `auto_mkdir` for local `file`. |

### Caps

Delete yes (only if `enable_rm`); get_schema no; read_all yes; read_nonexistent no; meta_pseudo_df yes.

### Notes

- Multiple OR suffixes: writes use the first pattern; deletes remove each existing variant.
- Custom `hash_rows` delegates to the adapter.

### See also

- [Transform files how-to](../../how-to/transform-files.md)
- [UpdateExternalTable](../steps/update-external-table.md)

---

## Adapters (`ItemStoreFileAdapter`)

```python
class ItemStoreFileAdapter(ABC):
    mode: str  # "t" text or "b" binary
    def load(self, f: IO) -> dict[str, Any]: ...
    def dump(self, obj: dict[str, Any], f: IO) -> None: ...
    def hash_rows(self, df: DataDF, keys: list[str]) -> HashDF: ...
```

### `JSONFile`

When to use: One JSON object per file → record dict.

```python
JSONFile(**dump_params)  # forwarded to json.dump
```

### `BytesFile`

When to use: Raw bytes column (default name `bytes`).

```python
BytesFile(bytes_columns: str = "bytes")
```

### `PILFile`

When to use: Image column as PIL / ndarray / base64 string.

```python
PILFile(format: str, image_column: str = "image", **dump_params)
```

### `PandasParquetFile`

When to use: Nested `pd.DataFrame` in a parquet file under one column (default `data`).

```python
PandasParquetFile(
    pandas_column: str = "data",
    engine: Literal["auto", "pyarrow", "fastparquet"] = "auto",
    compression: Literal["snappy", "gzip", "brotli", "lz4", "zstd"] = "snappy",
)
```

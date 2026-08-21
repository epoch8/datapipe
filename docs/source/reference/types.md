# Types

Shared typing aliases and step I/O helpers.

Module: `datapipe.types`

---

## Aliases (quick)

| Name | Meaning |
|---|---|
| `IndexDF` | DataFrame of primary-key columns only |
| `DataDF` | DataFrame of index + data columns |
| `MetadataDF` | Index + hash + timestamps (`create_ts`, `update_ts`, `process_ts`, `delete_ts`) |
| `HashDF` | Index + `hash` |
| `Labels` | `list[tuple[str, str]]` |
| `TableOrName` | `str \| OrmTable \| Table` |
| `PipelineInput` | `TableOrName \| InputSpec` |
| `PipelineOutput` | `TableOrName \| OutputSpec` |
| `TransformResult` | `DataDF \| list[DataDF] \| tuple[DataDF, ...]` |

---

## `InputSpec`

When to use: Pass a table as a step input with optional key remapping from transform index → table columns.

```python
@dataclass
class InputSpec:
    table: TableOrName
    keys: dict[str, str] | None = None
```

### Arguments

| Arg | Description |
|---|---|
| `table` | Catalog name, ORM class, or `Table`. |
| `keys` | Map `{transform_col: table_col}`. Example: `{"transform_col": "table_col"}` reads `table_col` from meta to fill `transform_col`. |

### Notes

- Plain `str` / ORM / `Table` as an input is equivalent to `InputSpec(table=...)` with full join and no key map.
- Join type for a bare input is `full`; wrap with `Required` for `inner`.

### See also

- [Key mapping how-to](../how-to/key-mapping.md)
- [Primary keys](../concepts/primary-keys.md)

---

## `Required`

When to use: Mark an input as mandatory for a transform batch (inner join on that table).

```python
@dataclass
class Required(InputSpec):
    pass
```

### Notes

- Subclass of `InputSpec`; same `table` / `keys`.
- In compute wiring: `join_type="inner"` (vs `"full"` for other inputs).

---

## `OutputSpec`

When to use: Pass a table as a step output with optional key remapping for cleanup / `processed_idx`.

```python
@dataclass
class OutputSpec:
    table: TableOrName
    keys: dict[str, str] | None = None
```

### Arguments

| Arg | Description |
|---|---|
| `table` | Catalog name, ORM class, or `Table`. |
| `keys` | Map `{transform_key: output_pk}`. Example: `{"post_id": "id"}` applies cleanup for transform key `post_id` onto output PK `id`. |

### Notes

- Used by batch transforms when mapping batch indexes onto output primary keys for delete-of-missing rows.

---

## `ChangeList`

When to use: Carry changed indexes per table name through changelist runs.

```python
@dataclass
class ChangeList:
    changes: dict[str, IndexDF] = field(default_factory=dict)

    def append(self, table_name: str, idx: IndexDF) -> None: ...
    def extend(self, other: ChangeList) -> None: ...
    def empty(self) -> bool: ...

    @classmethod
    def create(cls, name: str, idx: IndexDF) -> ChangeList: ...
```

### Notes

- `append` concatenates indexes for the same table; column sets must match.
- `empty()` is true when there are no table keys (even if values were empty frames, keys may still be present after appends — callers typically check after building from step results).
- Used by `run_steps_changelist` and `ComputeStep.run_changelist`.

### See also

- [run_changelist / run_steps_changelist](./pipeline-catalog.md#run_changelist)
- [Incremental processing](../concepts/incremental-processing.md)

# How to Expand One Row Into Many (1-to-N)

Turn one source row into several output rows (or collapse many rows into one) with a normal `BatchTransform` — Datapipe still tracks work by transform keys.

## Goal

Unpack nested structures (for example a product’s attribute map) into a child table whose primary key is a **superset** of the parent key.

## Steps

### 1. Define parent and child schemas

Parent key: `(pipeline_id, offer_id)`. Child key adds a dimension such as `name`:

```python
class TestProducts(Base):
    __tablename__ = "test_products"
    pipeline_id: Mapped[int] = mapped_column(primary_key=True)
    offer_id: Mapped[int] = mapped_column(primary_key=True)
    attributes: Mapped[dict] = mapped_column(type_=JSON)

class TestAttrProducts(Base):
    __tablename__ = "test_attr_products"
    pipeline_id: Mapped[int] = mapped_column(primary_key=True)
    offer_id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(primary_key=True)
    value: Mapped[int]
```

### 2. Expand in the transform function

Return one row per child entity. Empty input should still return a DataFrame with the right columns so cleanup stays correct:

```python
def unpack_attr(df: pd.DataFrame) -> pd.DataFrame:
    dfs = []
    for _, row in df.iterrows():
        data = [
            {
                "pipeline_id": row["pipeline_id"],
                "offer_id": row["offer_id"],
                "name": key,
                "value": value,
            }
            for key, value in row["attributes"].items()
        ]
        dfs.append(pd.DataFrame(data=data))

    if not dfs:
        return pd.DataFrame(columns=["pipeline_id", "offer_id", "name"])
    return pd.concat(dfs, ignore_index=True)
```

### 3. Register the step

```python
BatchTransform(
    unpack_attr,
    inputs=[TestProducts],
    outputs=[TestAttrProducts],
    chunk_size=2,
)
```

Default transform keys are the shared primary keys (`pipeline_id`, `offer_id`). When a parent row changes, Datapipe re-runs that key and replaces the child rows for that batch’s `processed_idx`.

### 4. (Optional) Collapse N-to-1 the same way

A second transform can pack attributes or offers back into one row per parent key. Same step type; only the DataFrame shape changes.

## Expected result

- One dirty parent key produces many child rows (or one packed row).
- Deleting or changing a parent invalidates and rebuilds its children for that key.
- Downstream steps that join on the shared keys stay incremental.

## Example

Full pipeline (unpack, pack, filter): [`examples/datapipe_core/one_to_many_pipeline/`](https://github.com/epoch8/datapipe/tree/master/examples/datapipe_core/one_to_many_pipeline).

## See also

- [Primary Keys and Transform Keys](../concepts/primary-keys.md)
- [Pipeline Steps](../concepts/pipeline-steps.md)

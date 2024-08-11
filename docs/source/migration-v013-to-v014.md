# Migration from v0.13 to v0.14

## DatatableTansform can become BatchTransform

Previously, if you had to do whole table transformation, you had to use
`DatatableTransform`. Now you can substitute it with `BatchTransform` which has
zero outputs.

Before:

```python
# Updates global count of input lines

def count(
    ds: DataStore,
    input_dts: List[DataTable],
    output_dts: List[DataTable],
    kwargs: Dict,
    run_config: Optional[RunConfig] = None,
) -> None:
    assert len(input_dts) == 1
    assert len(output_dts) == 1

    input_dt = input_dts[0]
    output_dt = output_dts[0]

    output_dt.store_chunk(
        pd.DataFrame(
            {"result_id": [0], "count": [len(input_dt.meta_table.get_existing_idx())]}
        )
    )

# ...

DatatableTransform(
    count,
    inputs=["input"],
    outputs=["result"],
)
```

After:

```python
# Updates global count of input lines

def count(
    input_df: pd.DataFrame,
) -> pd.DataFrame:
    return pd.DataFrame({"result_id": [0], "count": [len(input_df)]})

# ...

BatchTransform(
    count,
    inputs=["input"],
    outputs=["result"],

    # Important, we have to specify empty set in order for transformation to operate on 
    # the whole input at once
    transform_keys=[],
)
```

## SQLAlchemy tables can be used directly without duplication in Catalog

Starting `v0.14` SQLA table can be provided directly into `inputs=` or
`outputs=` parameters without duplicating entry in `Catalog`.

Note, that in order for `datapipe db create-all` to work, we should use the same
SQLA for declarative base and in datapipe.

Example:

```python
class Base(DeclarativeBase):
    pass


class Input(Base):
    __tablename__ = "input"

    group_id: Mapped[int] = mapped_column(primary_key=True)
    item_id: Mapped[int] = mapped_column(primary_key=True)


class Output(Base):
    __tablename__ = "output"

    group_id: Mapped[int] = mapped_column(primary_key=True)
    count: Mapped[int]

# ...

pipeline = Pipeline(
    [
        BatchGenerate(
            generate_data,
            outputs=[Input],
        ),
        DatatableBatchTransform(
            count_tbl,
            inputs=[Input],
            outputs=[Output],
        ),
    ]
)

# Note! `sqla_metadata` is used from SQLAlchemy DeclarativeBase
dbconn = DBConn("sqlite+pysqlite3:///db.sqlite", sqla_metadata=Base.metadata)
ds = DataStore(dbconn)

app = DatapipeApp(ds=ds, catalog=Catalog({}), pipeline=pipeline)
```

## Table can be provided directly without Catalog

Similar to usage pattern of SQLA tables, it is also possible to pass
`datapipe.compute.Table` instance directly without registering in catalog.

```python

from datapipe.compute import Table
from datapipe.store.filedir import PILFile, TableStoreFiledir
from datapipe.step.batch_transform import BatchTransform
from datapipe.step.update_external_table import UpdateExternalTable

input_images_tbl = Table(
    name="input_images",
    store=TableStoreFiledir("input/{id}.jpeg", PILFile("jpg")),
)

preprocessed_images_tbl = Table(
    name="preprocessed_images",
    store=TableStoreFiledir("output/{id}.png", PILFile("png")),
)

# ...

pipeline = Pipeline(
    [
        UpdateExternalTable(output=input_images_tbl),
        BatchTransform(
            batch_preprocess_images,
            inputs=[input_images_tbl],
            outputs=[preprocessed_images_tbl],
            chunk_size=100,
        ),
    ]
)
```
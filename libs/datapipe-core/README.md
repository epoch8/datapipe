# datapipe-core

[Datapipe](https://datapipe.dev/) is a Python framework for **durable, incremental batch processing**.

Define a pipeline as a graph of tables connected by transform functions. Datapipe tracks dependencies at the record level: when a row in an input table changes, only the downstream steps that depend on that row are re-run. Processing state is persisted, so a pipeline interrupted mid-run resumes from where it left off.

```python
pipeline = Pipeline([
    UpdateExternalTable(output=images_tbl),
    BatchTransform(
        resize_images,
        inputs=[images_tbl],
        outputs=[thumbnails_tbl],
        chunk_size=100,
    ),
])
```

Your transform functions stay simple and stateless — they receive a `pd.DataFrame` and return a `pd.DataFrame`. Datapipe figures out which rows need processing.

**Documentation:** https://epoch8.github.io/datapipe/
**Website:** https://datapipe.dev/
**Repository:** https://github.com/epoch8/datapipe

Install:

```bash
pip install datapipe-core
```

Import package: `datapipe`.

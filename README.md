# Datapipe

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

## Development

Active branches:

* `master` — current development state, will become the `0.15.x` release
* `v0.14` — current stable version

## Version compatibility

The library is under active development at `v0.*.*`. Each minor version should be considered incompatible with the previous one (`v0.7.0` is not compatible with `v0.6.1`). Pin dependencies to the exact minor version.

Compatibility guarantees following the standard semver rules (`v1.*.*` and beyond) will apply once the library stabilises.

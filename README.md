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

This repository contains the Datapipe Python packages as a `uv` monorepo workspace.

## Packages

- `libs/datapipe-core` - core incremental ETL library, import package `datapipe`.
- `libs/datapipe-ml` - ML addon for Datapipe, import package `datapipe_ml`.
- `libs/datapipe-label-studio` - Label Studio integration, import package `datapipe_label_studio`.
- `libs/datapipe-cvat` - CVAT integration, import package `datapipe_cvat`.
- `libs/datapipe-app` - REST API, debug UI, and CLI extensions, import package `datapipe_app`.

## Development

The workspace is configured in the root `pyproject.toml`. Package code and
package-local tests stay inside each `libs/*` directory. Shared docs and
examples live at the repository root.

Common commands:

```bash
uv sync --all-packages
uv run pytest libs/datapipe-core/tests
uv run pytest libs/datapipe-ml/tests -m "not training and not slow and not e2e and not tensorflow and not torch"
uv run pytest -vv -x libs/datapipe-label-studio/tests
uv run pytest libs/datapipe-app/tests
uv run python -c "import datapipe_cvat.cvat_step, datapipe_cvat.utils"
```

## Documentation

Documentation lives in `libs/datapipe-core/docs`. Design notes live in
`libs/datapipe-core/design-docs`.

## Claude Code skill: e2e_template setup

`.claude/skills/setup-e2e-template/` is a [Claude Code skill](https://code.claude.com/docs/en/skills)
for setting up and running `examples/e2e_template` (YOLO detection / keypoints + Label Studio →
train → FiftyOne) on your own data — external prerequisites, env knobs, and the data-alignment
gotchas are baked in.

**Adding it:** nothing to install. It's a project skill committed to the repo, so anyone who clones
this repo and opens it in Claude Code gets it auto-discovered.

**Using it:** just describe the task — e.g. *"set up the e2e_template detection example on my images"*
or *"the model misses pallets in dark rooms — add a tag and measure it separately"*. Claude loads the
skill when the request matches its triggers. The optional `setup-e2e-template/tags-addon.md` covers
per-scenario tag metrics (tag images → part flows into training → a separate `tag_metrics` table).

## Version Compatibility

* `master` — current development state, will become the `0.15.x` release
* `v0.14` — current stable version

## Version compatibility

The library is under active development at `v0.*.*`. Each minor version should be considered incompatible with the previous one (`v0.7.0` is not compatible with `v0.6.1`). Pin dependencies to the exact minor version.

Compatibility guarantees following the standard semver rules (`v1.*.*` and beyond) will apply once the library stabilises.

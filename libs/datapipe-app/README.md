# datapipe-app

REST API and CLI extensions for Datapipe pipelines. The Ops UI lives in a separate
package: [`datapipe-ui`](../datapipe-ui).

From the monorepo root:

```bash
uv sync --package datapipe-app --package datapipe-core --package datapipe-ui --extra sqlite
uv run pytest libs/datapipe-app/tests
```

## Common usage

```python
from datapipe_app import DatapipeApp

from pipeline import ds, catalog, pipeline

app = DatapipeApp(ds, catalog, pipeline)
```

## REST API + UI

```bash
# API only
uv sync --package datapipe-app --package datapipe-core

# API + Ops UI static assets
uv sync --package datapipe-app --package datapipe-ui --extra ui
# or: pip install 'datapipe-app[ui]'
```

```bash
datapipe --pipeline app:app api
```

Ops API docs: `/api/v1alpha3/docs` (also `/api/v1alpha1/docs`, `/api/v1alpha2/docs`).

Build the UI package before packaging wheels:

```bash
make -C libs/datapipe-ui build-package
```

# 0.5.4

* Improve granularity of prometheus metrics latency buckets
* Fix `datapipe-core >= 0.14.1` compatibility

# 0.5.3

* Add python 3.12 support

# 0.5.2

* Update static files

# 0.5.1

* Fixed an interface issue when opening the `DatatableTransformStep`

# 0.5.0

## Major changes
* Added `/api/v1alpha2` API
* Add transform view to UI
* Add ability to run transform step from UI with and without filters

## Improvements
* Add setting `DATAPIPE_APP_SHOW_STEP_STATUS` to control prometheus metrics, it
  can be slow on large graphs

# 0.4.6

* Add setting `DATAPIPE_APP_SHOW_STEP_STATUS` to control whether UI shows
  counters, it can be slow on large graphs

# 0.4.5

* Add total/changed counts to transform node in UI
* (post.1) Enable Python 3.11
* (post.2) Fix Python 3.9 incompatibility

# 0.4.4

* Fix bug with `total_count` connection in `get_data_get_pd`.

# 0.4.3

* Fix bug in get_data when page*page_size > total_count

# 0.4.2

* Fix bug when focus did not work in some cases

# 0.4.1

* Added Python 3.11 support. 

# 0.4.0

## Major changes
* Move cli to `datapipe-core` module
* Move `DatapipeApp` to `datapipe-core`

## Improvements
* Add `step status` command that shows status (number of idx's to process) for a
  specific step
* Add `DatapipeAPI` wrapper for `DatapipeApp`
* Add `background` argument to `/update-data` and `/labelstudio-webhook`
* Add `filters`, `order_by` and `order` in `GET` data.
* Add `labels` to `UPDATE` data request
* Fix bug with incorrect counter `total_count` when grabbing `GET` data from api

# 0.3.2

* Add `DatapipeApp.api` subapp to mount extra APIs
* Add support for filtering by labels in `step` commands

# 0.3.1

* Add input/output tables to `datapipe step list` output
* Add lint `LintDataWOMeta` to check and fix data without meta

# 0.3.0

* Add `/api/v1alpha1/labelstudio-webhook` endpoint for integration with Label Studio

# 0.2.1

* Add `api` command that runs `uvicorn` with ability to setup tracing etc
* Add otel instrumentation for API

# 0.2.0

* UI for table filters
* New command `db create-all` to initialize all the tables in database
* New commands `step list` and `step run STEP_NAME`
* Fix display on `None` values
* Try to fix debug logging
* Add `lint` command
  * Add `DTP001: "delete_ts is newer than update_ts or process_ts"` check
  * Add `--tables` filter for `lint`
  * Add `--fix` flag and fix functionality for `DTP001`

# 0.1.11

* Add `GET /api/v1alpha1/get-file` to get `gs://` or `s3://` files from frontend

# 0.1.10

* Add `POST /api/v1alpha1/get-data` with support for filtering

# 0.1.9

* Use `DataTable.get_size()`, fixes
  [#14](https://github.com/epoch8/datapipe-app/issues/14)

# 0.1.7, 0.1.8

* Relax FastAPI requirements for compatibility with Fiftyone

# 0.1.6

* Fix [#13](https://github.com/epoch8/datapipe-app/issues/13) - boolean values
  visualization.

# 0.1.5

* Fix [#12](https://github.com/epoch8/datapipe-app/issues/12) - transform nodes
  with the same name are incorrectly merged into one.

# 0.1.4

* Relax `uvicorn` requirements (>= 0.17)

# 0.1.3

* Relax `fastapi` requirements (>= 0.75)

# 0.1.2

* New CLI commands: `table list`, `table reset-metadata`

# 0.1.1

* Fixes [#2](https://github.com/epoch8/datapipe-app/issues/2),
  [#7](https://github.com/epoch8/datapipe-app/issues/7) - data table
  visualization issues

# 0.1.0

* Initial `datapipe-app` release

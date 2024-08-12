# Datapipe CLI

Datapipe provides `datapipe` CLI tool which can be useful for inspecting
pipeline, tables, and running steps.

`datapipe` CLI is build using `click` and provides several levels of commands
and subcommands each of which can have parameters. `click` parameters are
level-specific, i.e. global-level arguments should be specified at global level
only: 

`datapipe --debug run`, but NOT `datapipe run --debug`

## Global arguments

### `--pipeline`

By default `datapipe` looks for a file `app.py` in working directory and looks
for `app` object of type `DatapipeApp` inside this file. `--pipeline` argument
allows user to provide location for `DatapipeApp` object.

Format: `<module.import.path>:<symbol>`

Format is similar to other systems, like uvicorn.

Example: `datapipe --pipeline my_project.pipeline:app` will try to import module
`my_project.pipeline` and will look for object `app`, it will expect this object
to be of type `DatapipeApp`.

### `--executor`

Possible values:

* `SingleThreadExecutor`
* `RayExecutor`

TODO add separate section which describes Executor

### `--debug`, `--debug-sql`

`--debug` turns on debug logging in most places and shows internals of
datapipe processing.

`--debug-sql` additionally turns on logging for all SQL queries which might be
quite verbose, but provides insight on how datapipe interacts with database.

### `--trace-*`

* `--trace-stdout`
* `--trace-jaeger`
* `--trace-jaeger-host HOST`
* `--trace-jaeger-port PORT`
* `--trace-gcp`

This set of flags turns on different exporters for OpenTelemetry

## `db`

### `create-all`

`datapipe db create-all` is a handy shortcut for local development. It makes
datapipe to create all known SQL tables in a configured database.

## `lint`

Runs checks on current state of database. Can detect and fix commong issues.

## `run`

## `step`

## `table`

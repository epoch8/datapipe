# Consolidate DatapipeApp-related functionality into `DatapipeApp`

## Problem

Pipeline construction and execution live as free-standing functions in
`datapipe/compute.py` — `build_compute`, `run_steps`, `run_steps_changelist`,
`run_pipeline`, `run_changelist` — each taking `ds`, `catalog`, `pipeline`/`steps`
as separate arguments. `DatapipeApp` itself is a thin container with no methods
beyond `__init__`, which calls `build_compute` once to populate `self.steps`:

```python
class DatapipeApp:
    def __init__(self, ds: DataStore, catalog: Catalog, pipeline: Pipeline):
        self.ds = ds
        self.catalog = catalog
        self.pipeline = pipeline
        self.steps = build_compute(ds, catalog, pipeline)
```

Every real caller — the CLI (`datapipe/cli.py`), the FastAPI layer
(`datapipe-app`), and every example `app.py` — already holds a `DatapipeApp`
instance in scope, but has to manually destructure it before calling the free
functions:

```python
# datapipe/cli.py
run_steps(app.ds, steps, executor=executor, run_config=run_config)
run_steps_changelist(app.ds, steps_to_run, changes, executor=executor)

# examples/datapipe_core/neo4j_pipeline/app.py
run_steps(app.ds, app.steps)
```

`DatapipeAPI` (`libs/datapipe-app/datapipe_app/datapipe_api.py`) goes a step
further: it wraps or copies a `DatapipeApp`'s fields, then immediately unpacks
`self.ds, self.catalog, self.pipeline, self.steps` again just to pass them
individually into `api_v1alpha1.make_app(...)` / `api_v1alpha2.make_app(...)`,
whose route handlers call `run_steps` / `run_steps_changelist` with the
unpacked values directly.

Concrete costs of this shape:

- **Repeated boilerplate.** The "unpack app → call free function" pattern is
  copy-pasted identically across `cli.py` (5+ commands), the `datapipe-app`
  routes, `migrations_v013.migrate_transform_tables(app, ...)`,
  `PipelineStatusCollector` (reads `self.datapipe_app.steps` /
  `.ds` directly), and every example script.
- **No obvious entry point.** A user reading `DatapipeApp` sees four
  attributes and no methods; discovering that `run_steps(app.ds, app.steps)`
  is "how you run the app" requires reading `compute.py` separately.
- **Drift risk.** Behavior like "steps default to `app.steps`, ds defaults to
  `app.ds`" is re-derived by hand at each call site instead of being defined
  once, so call sites can silently diverge (e.g. some pass `executor=`, some
  don't; some pass `run_config=`, some don't).
- **Inconsistent partial consolidation.** Several modules already accept a
  full `app: DatapipeApp` argument and reach into its fields
  (`filter_steps_by_labels_and_name(app, ...)` in `cli.py`,
  `migrations_v013.migrate_transform_tables(app, ...)`,
  `PipelineStatusCollector(datapipe_app=app)`), but none of them call a
  method on `app` — there's no shared convention for "operations that take an
  app" versus "operations that take raw `ds`/`catalog`/`steps`".
- **Naming collisions add confusion on top of this.** `cli.py` defines a click
  command named `run_changelist` (`step run-changelist`) that internally
  calls `run_steps_changelist`, distinct from module-level
  `compute.run_changelist` (`build_compute` + `run_steps_changelist`).
  Separately, `datapipe-ml`'s test helper
  `tests/helpers/training_smoke.py:run_pipeline(runtime, steps)` has an
  unrelated signature from `compute.run_pipeline(ds, catalog, pipeline,
  run_config)` — same name, different thing.

Note: `build_compute` also has a second, unrelated use as an internal
implementation detail — dozens of `PipelineStep` subclasses (ML tasks, CVAT,
Label Studio steps) build a sub-`Pipeline` inside their own
`build_compute(self, ds, catalog)` method and flatten it via the module-level
`build_compute(ds, catalog, sub_pipeline)` call. Structurally this is the same
two-step shape as `DatapipeApp.__init__` (init catalog tables, then expand
`pipeline.steps` into `ComputeStep`s) — the difference is that these call
sites only want the flattened `list[ComputeStep]` back, to splice into their
own return value, not "the app". Any consolidation design has to decide how
these sites get that flattened list once the free function is gone — see the
Proposal section for how this plays out. For example:

- `libs/datapipe-cvat/datapipe_cvat/cvat_step.py:1163` — `CvatStep.build_compute`
  assembles a local `Pipeline` of `BatchTransform`s (fetch tasks, sync status,
  fetch annotations from CVAT) and returns `build_compute(ds, catalog,
  pipeline)` at line 1376.
- `libs/datapipe-label-studio/datapipe_label_studio/create_projects_step.py:85` —
  `CreateLabelStudioProjects.build_compute` builds a one-step `Pipeline`
  wrapping `create_projects` and likewise returns `build_compute(ds, catalog,
  pipeline)` at line 146.
- `libs/datapipe-ml/datapipe_ml/workflows/detection_classification/metrics.py:1075`
  and `:1174` compose steps by calling `inference_pipeline.build_compute(ds,
  catalog)` and `raw_count_metrics_pipeline.build_compute(ds, catalog)`
  directly (no `DatapipeApp` in sight) and concatenating the resulting
  `ComputeStep` lists.

None of these have a `DatapipeApp` in scope at all — `ds`/`catalog` come from
whatever step is currently being expanded — so any consolidation design has
to account for this usage, not just the CLI/API call sites that already hold
a full app.

## Context

- `DatapipeApp(ds, catalog, pipeline)` (`libs/datapipe-core/datapipe/compute.py:335`)
  is the object every pipeline module is expected to expose as `app`. The CLI
  loads it via `import_module` + `getattr(mod, "app")` and asserts
  `isinstance(app, DatapipeApp)` (`cli.py:load_pipeline`).
- Five free functions in `compute.py` currently do all the real work:
  - `build_compute(ds, catalog, pipeline) -> list[ComputeStep]` — expands each
    `PipelineStep` into `ComputeStep`s, checks for duplicate step names.
  - `run_steps(ds, steps, run_config=None, executor=None)` — runs
    `ComputeStep.run_full` for each step in order, with `run_config.callback`
    lifecycle hooks. This is the CLI's and the FastAPI `/run` route's actual
    execution primitive.
  - `run_steps_changelist(ds, steps, changelist, run_config=None,
    executor=None)` — change-driven propagation loop (up to 100 iterations)
    across `BaseBatchTransformStep`s.
  - `run_pipeline(ds, catalog, pipeline, run_config=None)` —
    `build_compute` + `run_steps` in one call; used only in tests today, not
    by the CLI, API, or examples.
  - `run_changelist(ds, catalog, pipeline, changelist, run_config=None)` —
    `build_compute` + `run_steps_changelist`; likewise test-only today.
- `Pipeline` (`compute.py:330-332`) is a single-field `@dataclass` —
  `steps: Sequence[PipelineStep]` — with no methods, never subclassed, never
  `isinstance`-checked or compared. It's constructed ~78 times across the
  repo, always as `Pipeline([...])` wrapping a list literal, and its `.steps`
  field is read only by `build_compute`.
- `DatapipeAPI(FastAPI, DatapipeApp)` (`libs/datapipe-app/datapipe_app/datapipe_api.py`)
  is the closest existing thing to "`DatapipeApp` + behavior": it can be built
  from `ds/catalog/pipeline` or by copying an existing `DatapipeApp`'s fields,
  then mounts FastAPI routes that call the free functions with the unpacked
  fields. It adds no methods to `DatapipeApp` itself.
- There is no dedicated test suite for `DatapipeApp`. It's exercised only
  indirectly, through CLI tests, `datapipe-app`'s API tests (via an `app`
  fixture), and one smoke-test construction in
  `libs/datapipe-ml/tests/test_app_smoke.py`.

This doc proposes moving `build_compute` / `run_steps` / `run_steps_changelist`
(and the `run_pipeline` / `run_changelist` convenience wrappers) into
`DatapipeApp` as real instance logic, with no free-function equivalents left
behind — including for the `PipelineStep.build_compute` recursion case that
never has a full app in scope, and for `Pipeline`, which turns out to have no
reason to exist as a separate class either. See Proposal.

## Proposal

The implementation bodies of `build_compute`, `run_steps`, and
`run_steps_changelist` move *into* `DatapipeApp` as plain instance logic —
`build_compute`'s loop runs inside `__init__`, and `run` / `run_changelist`
contain the full step-execution loops themselves. No `@staticmethod`s.
`datapipe/compute.py` stops defining `build_compute` / `run_steps` /
`run_steps_changelist` / `run_pipeline` / `run_changelist` at module scope
entirely. Any code that needs what these used to provide — including code
that only has a `ds`/`catalog`/`pipeline` fragment, not "the" app — gets
there by constructing a `DatapipeApp`.

```python
class DatapipeApp:
    def __init__(self, ds: DataStore, catalog: Catalog, pipeline: Sequence[PipelineStep]):
        self.ds = ds
        self.catalog = catalog
        self.pipeline = pipeline

        # moved verbatim from the old module-level build_compute
        with tracer.start_as_current_span("build_compute"):
            catalog.init_all_tables(ds)
            compute_pipeline: list[ComputeStep] = []
            seen_steps = []
            for step in pipeline:
                compute_steps = step.build_compute(ds, catalog)
                compute_pipeline.extend(compute_steps)
                for compute_step in compute_steps:
                    if compute_step.name in seen_steps:
                        raise Exception(f"Duplicate step name: {compute_step.name}")
                    seen_steps.append(compute_step.name)
                    compute_step.validate()
            self.steps = compute_pipeline

    def run(
        self,
        steps: Sequence[ComputeStep] | None = None,
        run_config: RunConfig | None = None,
        executor: Executor | None = None,
    ) -> None:
        # moved verbatim from the old module-level run_steps, operating on self.ds
        steps = steps if steps is not None else self.steps
        callback = run_config.callback if run_config is not None else None
        ...

    def run_changelist(
        self,
        changelist: ChangeList,
        steps: Sequence[ComputeStep] | None = None,
        run_config: RunConfig | None = None,
        executor: Executor | None = None,
    ) -> None:
        # moved verbatim from the old module-level run_steps_changelist, operating on self.ds
        steps = steps if steps is not None else self.steps
        ...
```

`steps` stays an optional override on `run`/`run_changelist` for two
distinct reasons, both grounded in real callers:

1. **Filtered subsets.** `cli.py`'s `step run` / `step run-changelist`
   commands operate on `filter_steps_by_labels_and_name(app, labels=...,
   name_prefix=...)`, not the whole app — `app.run(steps=filtered)`.
2. **Steps with no backing pipeline definition at all.** See below.

### Every call site constructs a `DatapipeApp` — including fragments and tests

Two categories of existing caller don't have "the" top-level app in scope,
only pieces of it. Under this design both go through `DatapipeApp`
construction anyway, rather than a static/free entry point:

**Nested `PipelineStep.build_compute` implementations** (`CvatStep`,
`CreateLabelStudioProjects`, ~20 more across
`datapipe-ml`/`datapipe-cvat`/`datapipe-label-studio` — see the Context
section above) build a local list of `PipelineStep`s and need back a flat
`list[ComputeStep]`. They construct a throwaway `DatapipeApp` and read
`.steps`:

```python
# before (libs/datapipe-cvat/datapipe_cvat/cvat_step.py:1376)
pipeline = Pipeline([BatchTransform(...), ...])
return build_compute(ds, catalog, pipeline)
# after
return DatapipeApp(ds, catalog, [BatchTransform(...), ...]).steps
```

**`libs/datapipe-core/tests/test_run_callbacks.py`** builds `ds =
MagicMock()` and hand-rolled `FakeStep(ComputeStep)` instances with no
`PipelineStep`s behind them at all — it's testing the callback-lifecycle
machinery of the run loop, not step construction. It gets there by
constructing a `DatapipeApp` with an empty `Catalog`/pipeline (so
`__init__`'s `build_compute` loop is a no-op, `self.steps == []`) and passing
the fake steps in through `run`'s `steps=` override:

```python
# before
run_steps(ds, [FakeStep("a"), FakeStep("b")], run_config=run_config)
# after
DatapipeApp(ds, Catalog({}), []).run(
    steps=[FakeStep("a"), FakeStep("b")], run_config=run_config
)
```

This is the real cost of "no static methods, always construct a
`DatapipeApp`": `DatapipeApp` stops being only "the one top-level app
object" and becomes a small, cheaply-constructible value type — bundling
`(ds, catalog, pipeline) → steps` plus the two run methods — that gets built
transiently wherever this shape is needed, including for a pipeline
fragment or a test fixture. The `Catalog({})`/`Pipeline([])` boilerplate in
the test above is the visible price of that; it was previously avoided by
calling `run_steps(ds, steps, ...)` directly with nothing else in scope.

### Retiring `run_pipeline` / `run_changelist` (module-level convenience wrappers)

`run_pipeline(ds, catalog, pipeline, run_config)` and
`run_changelist(ds, catalog, pipeline, changelist, run_config)` were only used
in tests (never CLI/API/examples), and both were just `build_compute` +
one of the run functions — they're now exactly `DatapipeApp(...)` + a method
call, so they're deleted rather than kept as aliases:

```python
# before
run_pipeline(ds, catalog, pipeline, run_config=run_config)
# after
DatapipeApp(ds, catalog, pipeline).run(run_config=run_config)

# before
run_changelist(ds, catalog, pipeline, changelist, run_config=run_config)
# after
DatapipeApp(ds, catalog, pipeline).run_changelist(changelist, run_config=run_config)
```

This also removes the naming collision between module-level
`compute.run_changelist` and the CLI's `step run-changelist` click command —
afterwards only the click command and `app.run_changelist` (method) remain,
disambiguated by call syntax.

Out of scope: `datapipe-ml`'s `tests/helpers/training_smoke.py:run_pipeline`
has an unrelated signature (`runtime`, not `ds/catalog/pipeline`) and isn't
touched by this change — pre-existing name collision worth a rename later,
not part of this consolidation.

### Dropping `Pipeline`

`Pipeline` (`compute.py:330`) carries no behavior beyond its one field:

```python
@dataclass
class Pipeline:
    steps: Sequence[PipelineStep]
```

It's never subclassed, never `isinstance`-checked, never compared — the
audit above found it constructed ~78 times, always as `Pipeline([...])`
immediately wrapping a list literal, and consumed only by the
`build_compute` loop reading `.steps` once. Even where it's threaded through
as a parameter beyond `DatapipeApp` — `api_v1alpha1.make_app(ds, catalog,
pipeline: Pipeline, steps)` / `api_v1alpha2.make_app(...)` — the `pipeline`
argument is dead: neither function body references it at all; the `/graph`
route's `pipeline=[...]` response field is built from `steps`, not from the
`pipeline` parameter.

Given that, drop the class and take `Sequence[PipelineStep]` directly
wherever `Pipeline` appeared — this is already reflected in the `__init__`
signature above (`pipeline: Sequence[PipelineStep]`, `self.pipeline` now
holds the raw list). Concretely:

- Every `pipeline = Pipeline([...])` construction becomes just `pipeline =
  [...]` (or the list is passed inline), collapsing a line at each of the
  ~78 sites.
- `api_v1alpha1.make_app` / `api_v1alpha2.make_app` drop the (already-dead)
  `pipeline` parameter entirely — a small, independently-justified cleanup
  that lands for free while those two signatures are already being touched
  for the `app: DatapipeApp` change in Migration scope below.
- `app.pipeline`/`self.pipeline` access is unaffected in the 3 places that
  read it today (`test_app_smoke.py: assert app.pipeline is not None`,
  `datapipe_api.py: self.pipeline = app.pipeline`) — same attribute name,
  simpler type.
- `PipelineStep` (the ABC with the abstract `build_compute` method) is
  unaffected and keeps its name — it's the actually-extensible piece.
  Whether "PipelineStep" is still the best name for it once there's no
  `Pipeline` type is a naming bikeshed, left out of scope here.

### Migration scope

This is a real breaking change to `datapipe-core`'s public surface, not a
purely additive one — `from datapipe.compute import build_compute` (etc.)
stops working. The package is `0.15.1-alpha.1` (pre-1.0, no semver
commitment yet per `CHANGELOG.md`), which is what makes a clean break a
reasonable default here rather than carrying deprecated aliases.

Every call site updates `from datapipe.compute import build_compute` to
`from datapipe.compute import Catalog, DatapipeApp` (as needed) and
`build_compute(ds, catalog, pipeline)` to `DatapipeApp(ds, catalog,
pipeline).steps`; `run_steps(ds, steps, ...)` becomes `DatapipeApp(ds,
catalog, pipeline).run(steps=steps, ...)` — or, where an `app` is already in
scope, just `app.run(steps=steps, ...)` without constructing anything new.
Every `Pipeline([...])` construction drops the wrapper, becoming a plain
list literal. Rough scope, from the current call-site audit:

- **`build_compute`**: ~70+ call sites — every `PipelineStep.build_compute`
  implementation across `datapipe-core`, `datapipe-cvat`,
  `datapipe-label-studio`, and (the bulk of it) `datapipe-ml`'s
  `tasks/`/`workflows/`/`datasets/`/`metrics/` modules, plus ~30 test files.
- **`Pipeline([...])`**: ~78 construction sites (heavily overlapping with the
  `build_compute` sites above, since almost every `build_compute` call is
  immediately preceded by building the `Pipeline` it's passed) — each drops
  to a plain list literal.
- **`run_steps`**: ~15+ call sites — `cli.py`, `datapipe-app`'s
  `api_v1alpha1.py`/`api_v1alpha2.py`, a handful of examples, and test files
  across `datapipe-core`/`datapipe-app`.
- **`run_steps_changelist`**: ~5 call sites — `cli.py`, `api_v1alpha1.py`,
  `test_chunked_processing_pipeline.py`, `test_cross_merge.py`.

Given the size, this is one mechanical, repo-wide codemod-style PR rather
than several independent PRs — the workspace is one repo (per this project's
`uv` workspace setup), so there's no cross-repo version-skew risk in doing it
atomically.

`datapipe-app`'s route handlers deserve a closer look rather than a
mechanical swap: `api_v1alpha1.make_app(ds, catalog, pipeline, steps)` /
`api_v1alpha2.make_app(...)` currently receive `ds`/`catalog`/`pipeline`/
`steps` unpacked, and call `run_steps`/`run_steps_changelist` directly inside
route closures. Naively replacing that with
`DatapipeApp(ds, catalog, pipeline).run(steps=steps, ...)` would reconstruct
a `DatapipeApp` (re-running the `build_compute` loop, including
`catalog.init_all_tables`) on *every request* — wasteful, and redundant with
the fact that `DatapipeAPI(FastAPI, DatapipeApp)` already built one such app
once at startup. The correct fix is for `make_app` to take the existing
`app: DatapipeApp` instance instead of its unpacked fields, so route handlers
call `app.run(steps=steps, ...)` on the object that already exists. That's a
real (if small) signature change to two API modules, so it's called out as a
**required part of this change**, not a follow-up — a mechanical import swap
alone isn't correct there.

### Open questions (not resolved by this proposal)

- **`executor` defaulting.** Every call site threads `executor` through by
  hand today (CLI picks one in `ctx.obj["executor"]` per invocation).
  `DatapipeApp` could grow a default `executor` field so `app.run()` needs no
  argument at all — left out of this proposal's scope since no current call
  site would benefit without further CLI changes.
- **Rebuilding `steps` after construction.** `self.steps` is computed once in
  `__init__`. No code today mutates `app.catalog`/`app.pipeline` after
  construction, so an explicit `app.rebuild()` is speculative — not proposed
  here without a concrete need.
- **Should `print_compute` move too?** Same free-floating shape as the
  others, but it's a trivial pretty-printer with no external callers found —
  low stakes either way; move it for consistency or leave it, doesn't affect
  the rest of this design.
- Confirmed no name collisions with `FastAPI`/`Starlette` for `run` or
  `run_changelist` — `DatapipeAPI(FastAPI, DatapipeApp)`'s MRO is safe to add
  both to `DatapipeApp`.

---
status: DRAFT
---

# Private monorepo: merge datapipe-cloud into datapipe

## Context

Datapipe code lives in two repositories today.

**`epoch8/datapipe`** (public, this repo) is a uv workspace. The root
`pyproject.toml` has `members = ["libs/*"]`, and there is a single `uv.lock`.
The workspace members are `datapipe-core`, `datapipe-app`, `datapipe-ml`,
`datapipe-label-studio` and `datapipe-cvat`. The repo also holds `examples/`,
`docs/`, `tests/` and `tools/`. CI is split per lib
(`.github/workflows/lib-*.yml`), and `publish.yml` publishes the libs to PyPI.

**`epoch8/datapipe-cloud`** (private) is a separate uv workspace with its own
`uv.lock`. Its `pyproject.toml` has `members = ["backend", "router"]`:

- `backend/` is the Django service (project `datapipe-cloud`,
  `package = false`). It includes the cloud agent UI in `backend/agent_ui/`,
  which is built with pnpm.
- `router/` is a thin gRPC process around `datapipe_router.DatapipeServer`
  (`router/app.py`), with its own alembic schema.
- `deploy/` is a Helm umbrella chart. The root `Dockerfile` builds one image
  for both services, and `.github/workflows/deploy.yml` builds that image and
  deploys it to GKE.

The two repos are joined through a long-lived **`cloud` branch of the public
repo**. That branch is 32 commits ahead of `master` and adds:

- `libs/datapipe-router` and `libs/datapipe-agent`
- the new React UI: `libs/datapipe-ui`, `packages/api-client` and
  `packages/ui-core`, with a yarn workspace root `package.json`
- the `/api/v1alpha3` changes in `datapipe-app`

`datapipe-cloud` consumes this branch by name:

- `backend/pyproject.toml` and `router/pyproject.toml` both declare
  `datapipe-router = { git = "https://github.com/epoch8/datapipe.git", branch = "cloud", subdirectory = "libs/datapipe-router" }`.
- `backend/agent_ui/package.json` pulls `@datapipe/api-client`, `@datapipe/ui`
  and `@datapipe/ui-core` from `github:epoch8/datapipe#cloud&path:...`.

This setup causes the following problems:

- **Every cross-cutting change is two PRs in two repos.** They have to land in
  order, followed by a lock bump in `datapipe-cloud`. The Python side is
  pinned to whatever commit `uv.lock` last resolved, and the JS side follows
  the branch head. Nothing checks that the two repos agree.
- **`cloud` cannot merge into `master`.** It contains code that is meant to be
  private, so it has to be kept in sync with `master` by hand. Any core change
  the cloud needs is made on one branch and ported to the other.
- **Private code has nowhere to live next to core.** Router and agent belong
  with the cloud, but they have to sit in the core repo to be developed
  against core. As a result they are already on a public branch, and so are
  the cloud design docs (branch `docs/cloud-architecture`).
- **Two lockfiles.** Dependencies shared by both sides (`grpcio`, `protobuf`,
  `sqlalchemy`, `pyarrow`) are resolved independently on each side. The
  lint/type-check setup is also duplicated (`mypy` in cloud, `ty` here).

One fact makes a clean split possible. On `cloud`, no public lib imports or
depends on `datapipe_router` or `datapipe_agent`. Dependencies only point from
private code to public code.

## Goal

Done when:

- One private repository, `epoch8/datapipe-private`, contains all Datapipe
  code: the public libs, router, agent, the backend and router services,
  deployment, and design docs. The history of both source repos is preserved.
- A top-level uv workspace covers everything, and
  `uv sync --all-packages --all-extras` at the root installs it all. Private
  components depend on core and router via `{ workspace = true }`. No git or
  branch sources point at `epoch8/datapipe`. The same holds for JS: the agent
  UI consumes `@datapipe/*` packages from the repo, not from GitHub.
- Everything public lives under `core/`, which is exported as the root of
  `epoch8/datapipe`. Everything outside `core/` is private. CI fails if code
  under `core/` depends on anything outside it.
- An export job updates `epoch8/datapipe` from the private repo. The export is
  incremental, contains public paths only, fast-forwards the public `master`,
  and is reproducible. The exported tree builds and passes the public CI on
  its own.
- External PRs to `epoch8/datapipe` are imported into the private repo by
  Copybara and reach the public repo through the next export. Nobody re-applies
  them by hand.
- The cloud image is built and deployed from the private repo.
  `epoch8/datapipe-cloud` is archived, and the `cloud` branch has been landed
  and deleted from the public repo.

Non-goals:

- **Changes to the architecture or APIs of router, agent or backend.** See the
  cloud connectivity architecture doc.
- **Renaming packages or changing what is published to PyPI.**
- **Recalling what is already public.** Deleting the `cloud` and
  `docs/cloud-architecture` branches does not remove existing clones and
  forks.
- **Unifying lint and type-check tooling** beyond what the merged workspace
  needs.

## Approach

**The private repo is a continuation of this repo; the public repo becomes an
export target.**

- `epoch8/datapipe-private` starts from the full history of `epoch8/datapipe`.
  All development moves there.
- `epoch8/datapipe` keeps its issues, stars, forks and PyPI links. From then
  on it receives code only through export.

**The boundary between public and private code is a directory.** `core/` is
the public repo, and everything outside it is private. `cloud/` holds
everything from `datapipe-cloud` together with the cloud libs, and keeps the
old repo's internal layout.

```
datapipe-private/
├── core/                 public: exported as the root of epoch8/datapipe
│   ├── libs/             datapipe-core, -app, -ml, -label-studio, -cvat, -ui
│   ├── packages/         JS packages (api-client, ui-core)
│   ├── examples/ docs/ tests/ tools/ design-docs/
│   ├── pyproject.toml    public uv workspace root (nested in the top-level one)
│   ├── uv.lock           public lock
│   ├── pnpm-workspace.yaml, pnpm-lock.yaml, package.json    public JS workspace
│   └── .github/          public CI; runs in epoch8/datapipe only
├── cloud/                private: all of epoch8/datapipe-cloud, plus the cloud libs
│   ├── libs/             datapipe-router, datapipe-agent
│   ├── backend/          Django service + agent UI
│   ├── router/           gRPC process
│   ├── deploy/           Helm chart
│   ├── design-docs/      cloud design docs
│   └── Dockerfile, Makefile, CHANGELOG.md, CLAUDE.md, ...
├── design-docs/          repo-wide private docs, including this one
├── copybara/             sync config
├── pyproject.toml        top-level uv workspace: core/libs/*, cloud/libs/*, cloud/backend, cloud/router
├── uv.lock               top-level lock
├── pnpm-workspace.yaml, pnpm-lock.yaml, package.json    top-level JS workspace
└── .github/workflows/    private CI and deploy, including CI for core/
```

A directory makes the public side an allowlist. Anything outside `core/` is
private by default, and publishing something takes an explicit move into
`core/`, which shows up in review. Every path shows its classification, and
there are no per-file markers or exception lists to maintain.

**Classification follows the dependency direction.** Core and everything a
user needs to run a pipeline locally is public:

- `datapipe-core` and the integration libs
- `datapipe-app` with its v1alpha3 API
- the new `datapipe-ui` and its JS packages

Everything that exists only for the cloud is private: router, agent, the
backend and its agent UI, the router service, and deployment. Private code
depends on public code. Public code never depends on private code.

**There are two workspaces: the top-level one and `core/`, nested inside it.**
The top-level workspace covers `core/` and `cloud/` alike, for both
development and the cloud build. `core/` stays a workspace of its own, with
its own lock, exactly as the public repo has it. `core/` therefore exports
unchanged, and running inside `core/` reproduces the public repo. The same
applies to the JS workspaces.

**Copybara syncs in both directions using one mapping, `core/` ↔ the public
root.** Export replays new private commits into `epoch8/datapipe`. Import
brings external PRs into the private repo for review. Because both directions
use the same mapping, they cannot disagree about what is public.

**Migration runs in dependency order, with the public `master` frozen from
seeding until the first export.**

## Implementation Steps

### 1. Create the private repo

- Create `epoch8/datapipe-private` (private) and push the full history of
  `epoch8/datapipe` to it, including `master` and the `cloud` branch.
- Freeze the public `master` from this point until the first export (step 9).

### 2. Land `cloud`

- Merge `cloud` into private `master` in the current layout, since that is
  the layout the branch was written against.
- This brings in `libs/datapipe-router`, `libs/datapipe-agent`,
  `libs/datapipe-ui`, `packages/api-client`, `packages/ui-core`, the root
  `package.json`/`yarn.lock`, and the v1alpha3 changes in `datapipe-app`.

### 3. Restructure into `core/` and `cloud/`

In a single commit:

- Move everything that came from `epoch8/datapipe` into `core/`:
  `libs/`, `packages/`, `examples/`, `docs/`, `tests/`, `tools/`,
  `design-docs/`, `.github/`, `pyproject.toml`, `uv.lock`, `package.json`,
  `yarn.lock`, `README.md`, `LICENSE`, `CHANGELOG.md`, `CLAUDE.md`, `TODO`,
  `.gitignore`, `.gitattributes`, `.dockerignore`, `.python-version`,
  `.readthedocs.yml` and `.vscode/`. Step 5 converts the JS files from yarn
  to pnpm.
- Move `libs/datapipe-router` and `libs/datapipe-agent` to `cloud/libs/`
  instead.
- Add a private root `CLAUDE.md`. `core/CLAUDE.md` stays publishable.

This commit is Copybara's export baseline. History before it is already
public and is not replayed.

### 4. Import `datapipe-cloud` into `cloud/`

- Rewrite the `datapipe-cloud` history into the `cloud/` subdirectory, then
  merge it into private `master`. Because this is a merge rather than a copy,
  `jj log` and `git blame` keep working across it.
- Move the cloud connectivity architecture doc (branch
  `docs/cloud-architecture`, `design-docs/2026-09-cloud-connectivity-architecture/`)
  into `cloud/design-docs/`.

### 5. Set up the workspaces

- **Top-level workspace.** Write a new root `pyproject.toml` with
  `members = ["core/libs/*", "cloud/libs/*", "cloud/backend", "cloud/router"]`
  and the shared ruff, mypy and ty config, then generate the root `uv.lock`.
  `uv sync --all-packages --all-extras` at the root installs everything.
- **`core/` workspace.** `core/pyproject.toml` stays a workspace root, with
  `members = ["libs/*"]` (router and agent have moved out) and its own
  `core/uv.lock`. uv 0.12.11 supports this nesting: each root locks
  independently.
- **`cloud/`.** Remove `[tool.uv.workspace]` from `cloud/pyproject.toml` and
  delete `cloud/uv.lock`. Its members join the top-level workspace directly,
  so there is no third workspace. Nothing needs to build the cloud in
  isolation.
- **Workspace sources.** In `cloud/backend/pyproject.toml` and
  `cloud/router/pyproject.toml`, replace
  `datapipe-router = { git = ..., branch = "cloud", subdirectory = ... }` with
  `{ workspace = true }`.
- **Which workspace uv uses.** uv uses the nearest workspace root:
  - Commands run inside `core/` use `core/.venv` and `core/uv.lock`. So
    `cd core && uv sync --all-packages --all-extras` reproduces what a public
    user gets.
  - Commands run from the root or from `cloud/` use the root `.venv`.
  - Work that spans `core/` and `cloud/` runs from the root, or passes
    `uv --project <root>`. Editors point at the root `.venv`.

  Document this in both the root `CLAUDE.md` and `core/CLAUDE.md`.
- **Why two locks.** One lock resolves all members together, so private
  constraints would narrow resolution for everyone. For example, the router
  service requires Python `>=3.12` and the backend requires `<3.13`, so the
  top-level lock resolves only for 3.12. `core/uv.lock` sees only public
  constraints.
- **JS: switch to pnpm.** `agent_ui` already uses pnpm. The `cloud` branch
  uses yarn 1 workspaces, and those move to pnpm.
  - Add a top-level `pnpm-workspace.yaml` listing `core/packages/*`,
    `core/libs/datapipe-ui` and `cloud/backend/agent_ui`, with a root
    `pnpm-lock.yaml`. Delete `cloud/backend/agent_ui/pnpm-lock.yaml`.
  - `core/` gets its own `pnpm-workspace.yaml` (listing `packages/*` and
    `libs/datapipe-ui`), `package.json` and `pnpm-lock.yaml`, nested the same
    way as the uv workspace. pnpm also resolves the nearest
    `pnpm-workspace.yaml`. Verify that pnpm handles this nesting the way uv
    does before relying on it.
  - In `cloud/backend/agent_ui/package.json`, the
    `github:epoch8/datapipe#cloud&path:...` dependencies become `workspace:*`.
    So do the `@datapipe/api-client` and `@datapipe/ui-core` dependencies of
    `core/libs/datapipe-ui`.
  - Set `packageManager` to pnpm in every `package.json`. Replace the `yarn`
    calls in `core/libs/datapipe-ui/Makefile`, the `build:package` script,
    `core/.github/workflows/lib-datapipe-ui.yml` and `lib-datapipe-app.yml`,
    and `core/examples/datapipe_app/Dockerfile` with their
    `pnpm --filter @datapipe/ui ...` equivalents.

### 6. Move the cloud build and deploy

- Move `cloud/.github/workflows/deploy.yml` and `lint.yml` to the root
  `.github/workflows/`, because GitHub only runs workflows from the repo root.
  Prefix their `paths:` filters with `cloud/`.
- Build the image from the repo root:
  `docker build -f cloud/Dockerfile .`. The manifest `COPY` layer copies the
  top-level `pyproject.toml` and `uv.lock` plus the `pyproject.toml` of every
  member the image installs. Move `.dockerignore` to match, and update the
  `IMAGE` and `deploy` paths in `cloud/Makefile`.

### 7. Set up private CI for `core/`

- `core/.github/workflows/` runs only in the public repo, after export. PRs in
  the private repo need their own root workflows for `core/`. To keep the
  duplication thin, these call the same entry points as the public workflows,
  with `working-directory: core`.
- **Dependency direction.** On every PR, export `core/` to a local folder with
  Copybara, then run `uv sync --frozen` and the public test suite there.
  Because the export is a pure move, this tests exactly the tree that will be
  published. Anything under `core/` that reaches outside it fails this build,
  including relative path sources that happen to resolve inside the private
  repo.
- **Lock freshness.** Run `uv lock --check` and
  `pnpm install --frozen-lockfile` both at the repo root and in `core/`. The
  top-level locks and the `core/` locks can pin shared dependencies to
  different versions, so both need checking.

### 8. Configure Copybara

**Export workflow** (private → public):

- Config lives in `copybara/copy.bara.sky`.
- Export `core/**`, moved to the public root. That move is the only
  transformation: there are no path filters, no file rewrites, and no
  commands run during export. The output is therefore deterministic, and
  re-running an export never rewrites public history.
- Run in iterative mode from the baseline set in step 3.
- Each public commit carries an origin-revision trailer naming its private
  commit, so the next run replays only what came after.
- Commits that don't touch `core/` are dropped. Commits that touch both
  `core/` and private code are exported with their `core/` part and **their
  message**. Convention: keep public and private changes in separate commits,
  and treat the message of any commit that touches `core/` as public.
- Run it from a root private workflow, on a schedule and on demand.
- Protect the public `master` so that only the Copybara bot can push.

**Import workflow** (public PR → private):

- Apply the export move in reverse (public root → `core/`) and open the result
  as a private PR.
- After that PR merges, the export publishes the change, and Copybara closes
  the public PR against the exported commit.

### 9. Run the first export

- Run the first export and unfreeze the public repo.
- The public `master` moves to the state of `core/`. The public parts of
  `cloud` (the new UI, the JS packages and v1alpha3) arrive in one commit.
  Router and agent stay out.
- Verify that the public CI passes on the exported tree.

### 10. Clean up

- Archive `epoch8/datapipe-cloud`.
- Delete the `cloud` and `docs/cloud-architecture` branches from
  `epoch8/datapipe`.

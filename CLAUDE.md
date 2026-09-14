# datapipe

This is a `uv` workspace (`pyproject.toml` + `uv.lock`) with member libs under `libs/*`.

## Commands

- Use `uv sync --all-packages --all-extras` to install/sync the environment — this is a multi-package workspace, so `--all-packages` is what installs every workspace member together.

## Code style

- Prefer modern `X | None` union syntax over `Optional[X]` in new or touched code.

## Design docs

Non-trivial changes get a design doc under `design-docs/`, named
`YYYY-MM-<slug>.md` (month the work started).

Structure:

- YAML frontmatter with a `status` field, then `# Title`, then the body:

  ```
  ---
  status: DRAFT
  ---

  # Title
  ```
- `## Context` — current state, with concrete file / symbol references; why the
  change is needed.
- `## Goal` — what "done" means, as a `Done when:` bullet list, followed by a
  `Non-goals:` list. State the outcome, not the mechanism, when the
  implementation is still open.
- `## Approach` — high level: the strategy and the reasoning behind it, in
  prose, no file-by-file detail.
- Then whatever else the change needs: `## Design`, `## Implementation Steps`,
  and after the work lands `## Implementation notes (as built)` and/or
  `## Verification`.
  - `## Implementation Steps` is specific — concrete files, symbols, and
    ordered edits, enough to execute from.

Cross-reference sibling docs by path (`design-docs/2026-09-tracing.md`).

`status` values in use:

- `DRAFT` — proposed, not agreed or not started.
- `IMPLEMENTED` — shipped; the doc reflects what was built.

Keep `status` current as a doc moves between these.

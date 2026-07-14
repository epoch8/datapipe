# `new-datapipe-project` — master pipeline-assembly skill (design)

**Date:** 2026-07-14 · **Status:** approved-in-chat, pending review · **Owner:** Renat

## 1. Goal

A skill that lets an agent **assemble a working Datapipe pipeline for a new task** — from a small
ETL (a couple of transforms) to a full project (ingest → training → metrics → viewers/UI) — as a
**standalone project outside the datapipe repo**, brought up **end-to-end on the user's real data**.

The existing `datapipe-examples` skill routes to *existing* examples; this skill *creates new*
pipelines. Deliverable of a run: a running project + its docs + its own operational skill.

## 2. Non-goals

- No rigid cookiecutter templates per archetype (explicitly rejected: the skill must cover "anything
  the pipe can do", composing from primitives when no ready block fits).
- No in-repo `examples/` scaffolding mode (v1 targets standalone projects only).
- The skill does not implement new reusable datapipe blocks itself — it *identifies and specs* them
  (gap analysis) and either builds a project-local custom step or files an upstream proposal.

## 3. UX flow

```
/new-datapipe-project (in an empty project dir)
  ① deep interview about the TASK  →  ② capability map + gap analysis (verdicts)
  → ③ project SPEC written to docs/SPEC.md (user reviews)  →  ④ assembly plan
  → ⑤ deterministic seed scaffold, brought up immediately (db create-all → run → table filled)
  → ⑥ blocks added ONE at a time, each run on real data with logs + "what changed"
  → ⑦ finalization: README + project-local skill
```

## 4. Deep interview (one question at a time; dig to specifics)

Not "which blocks do you want" but **what the task is**:

- **Task in plain words** — what goes in, what must come out; push past vagueness ("recognize
  documents" → which documents, what output schema, what accuracy is acceptable).
- **Data** — type, source (files/S3/DB/API/HF), volume now, growth/cadence (one-off vs incremental
  vs cron), sample availability.
- **Humans in the loop** — annotation (which tool), review gates, who presses "train".
- **Outputs** — tables / model / metrics (sliced how — subsets, tags?) / viewer / API / UI.
- **Where it runs** — host(s), GPU (model & VRAM), which services exist vs must be provisioned
  (Postgres, MinIO, Mongo, annotation tools), network constraints (geoblocks → proxies).
- **Secrets** — which API keys/tokens the chosen blocks imply (HF, OpenAI/Gemini/Qwen, …). The skill
  **asks for them AND offers help obtaining them**: where to create the token, which scopes/rights
  (e.g. HF gated-repo read access + accepting the model license on its page), known pitfalls
  (plain HF token → 403 masked as `LocalEntryNotFoundError`; OpenAI/Gemini geoblocked from RU —
  offer the CONNECT-proxy pattern).
- **Success criterion** — "done when …" in the user's words; becomes the acceptance check.

## 5. Capability map & gap analysis (the core feature)

The skill maintains a requirement table with three verdicts:

| verdict | meaning | action |
|---|---|---|
| ✅ ready block | a pattern card + reference example covers it | use it |
| 🔧 composable | no ready block, but primitives (BatchTransform over any source) express it; official docs confirm the API | compose, cite docs |
| ❌ gap | neither covers it | STOP and present options |

For every ❌ gap the skill must propose, with a recommendation: **(a)** custom step inside the
project now, **(b)** a new reusable block for `datapipe-ml`/libs — written up as a short upstream
proposal (what, interface, which projects need it), **(c)** a workaround — and how each choice
changes the plan. The user decides before assembly starts.

Knowledge sources, in order: pattern cards (below) → reference examples (shallow clone of
`epoch8/datapipe`) → **official docs https://epoch8.github.io/datapipe/** (authoritative for core
API when composing beyond the examples).

## 6. Project SPEC artifact

The interview + gap decisions are written to the project as **`docs/SPEC.md`** before any code:
task, data, deployment target(s), services, blocks chosen, gap resolutions, secrets list (names
only), determinism requirements, success criteria. The user reviews it; assembly follows the spec.
This file stays in the project as its source of truth (future sessions read it first).

## 7. Deterministic seed (always the first step)

A minimal skeleton that must run before any real block is added:

- `pyproject.toml` — datapipe deps (PyPI where published; otherwise git-pinned to a monorepo rev —
  resolve exact strategy at implementation time), `requires-python`, **committed `uv.lock`**
  (lesson learned: the lock IS the cross-machine software contract; never edit deps ad-hoc).
- `.env.example` (DB_URL + whatever the interview implied), compose file for required services
  (Postgres minimum), `.gitignore`.
- `<package>/` with `config.py`, `data.py` (one table), `steps.py` (one passthrough transform),
  `app.py` (Pipeline + stage labels convention).
- Bring-up gate: `uv sync → docker compose up → datapipe db create-all → datapipe run` → verify the
  table filled. Only then grow.

## 8. Determinism by default (baked into everything the skill generates)

- Every stochastic block pins its seeds: training → `seed=…, workers=0, deterministic=True` +
  `CUBLAS_WORKSPACE_CONFIG=:4096:8` set at the entrypoint; data sampling/shuffles → seeded with the
  seed recorded in SPEC.md; LLM steps → `temperature=0`/fixed decoding where the API allows.
- Data selection must be order-canonical (sorted + seeded shuffle; document the order contract).
- SPEC.md and the project README carry the honest statement (verified experimentally in this repo):
  **same machine → bit-identical reruns; different GPU/CPU models → legitimately different weights
  and metrics** — quote reference numbers per hardware; a reference DB dump is the lever if
  identical numbers across machines are ever required.

## 9. Pattern cards (`references/` inside the skill)

One card per block, each with: when to use, the Step classes / conventions involved, a pointer to
the reference implementation (example + file in the datapipe repo), the official-docs link, and
field-tested pitfalls. Initial set:

1. **Ingest** — external sources → tables (`UpdateExternalTable`, custom loaders; any
   source is a BatchTransform away).
2. **Transform chains** — BatchTransform/DatatableBatchTransform, chunking, transform_keys,
   incrementality rules ("re-run reprocesses only changed rows").
3. **ML loop** — frozen datasets (incl. `min_within_time` debounce pitfall), YOLO train/inference,
   metrics on subsets/**tags** (frozen-val discipline!), best-model selection; ref: `detection_tags`,
   `e2e_template`.
4. **LLM steps** — engine registry, pydantic output schema, data×engines cross-join, cost controls;
   ref: `ocr`.
5. **Annotation loops** — Label Studio (`e2e_template`), CVAT (`sam_cvat`); human gates.
6. **Viewers/UI** — FiftyOne publishing; `datapipe_app` serving + ops-specs (`add_specs`,
   metric tables) ref: `detection_tags` on the UI branch.
7. **Ops hygiene** — stage labels, status tables over exit codes, `--debug` to file + grep,
   pre-flight port/schema checks, read-only-HOME/geoblock workarounds.

## 10. Finalization (self-replicating operations)

After the pipeline runs end-to-end: generate the project README (stages, knobs, troubleshooting
observed during bring-up) and a **project-local skill** (`.claude/skills/<project>-ops/SKILL.md`)
in the style of the repo's `setup-*` skills, so any future agent session can operate the project
(including its SPEC.md pointer and secrets list).

## 11. Distribution

Source of truth: `.claude/skills/new-datapipe-project/` in the datapipe repo. Global install (the
skill must be available in empty dirs): `cp -r` into `~/.claude/skills/` — documented in the skill
itself and in `examples/README.md`. The skill shallow-clones the datapipe repo at run time for
reference reading (or uses an existing local clone).

## 12. Acceptance criteria for the skill itself

- In a clean dir, the skill takes a small ETL task from interview to a running incremental pipeline
  on user data with SPEC.md + README + project skill, without manual code edits by the user.
- The gap-analysis path triggers correctly on a request outside current blocks (produces options
  a/b/c and an upstream proposal draft for (b)).
- A generated training project reruns bit-identically on the same machine.
- Keys flow: on a blocks-set implying HF/LLM keys, the skill asks for them and provides the
  how-to-obtain guidance before any stage needs them.

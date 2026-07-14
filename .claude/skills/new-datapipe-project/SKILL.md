---
name: new-datapipe-project
description: >
  Use when asked to create, scaffold, or build a NEW datapipe pipeline or project (any size — plain
  ETL/ingest, ML training with metrics, LLM-processing steps, annotation loops, viewers/UI), usually
  in an empty directory outside the datapipe repo. Also when a user describes a recurring
  data-processing task ("files keep arriving and I need to...") and no pipeline exists yet.
---

# new-datapipe-project — assemble a working datapipe pipeline for a new task

Turn a user's task into a **standalone, running datapipe project on their real data** — not a code
dump that "should work". The run is done when the user's success criterion passes, and the project
carries its own SPEC, README and operational skill for future sessions.

**The deliverable contract (all six, in this order):**
1. **Interview** → the task pinned down to specifics (see below — this is the fast path, not overhead).
2. **Capability map** → every requirement gets a verdict: ready block / composable / gap.
3. **`docs/SPEC.md`** in the project → user reviews it BEFORE any code (template: `references/spec-template.md`).
4. **Deterministic seed** scaffold, brought up immediately (create tables → run → verify a row landed).
5. **Blocks added one at a time**, each run on real data with logs shown and "what changed".
6. **Finalization**: project README + a project-local skill in `.claude/skills/` so any future agent
   session can operate the pipeline.

## Interview first — the decisions are the user's

Under time pressure the tempting move is to guess the schema, the dedup semantics, the model, the
aggregation slicing — and deliver fast. A wrong guess costs a rebuild; the interview costs minutes.
**"Времени мало" means MORE reason to interview: it's the only way the first build is the right one.**

Ask one question at a time; dig until answers are concrete (never accept "recognize documents" —
which documents, what output schema, what accuracy is acceptable?). Full checklist with the
keys/secrets flow: `references/interview.md`. Minimum to cover: task in plain words · data
(type/source/volume/cadence/sample) · humans in the loop · outputs and how metrics are sliced ·
where it runs (host, GPU, existing services, network constraints) · secrets · success criterion
("done when …").

Every decision you were forced to make yourself is a defect: it belongs in SPEC.md as a question,
answered by the user.

## Capability map & gaps

Map each requirement to a verdict before writing code:

| verdict | meaning | action |
|---|---|---|
| ✅ ready block | a pattern card covers it | use it, follow its reference example |
| 🔧 composable | primitives express it (any source is a BatchTransform away) | compose; verify API against the official docs, not memory |
| ❌ gap | neither | STOP: present options (a) custom step in-project, (b) reusable block upstream (draft a short proposal), (c) workaround — with a recommendation; user picks before assembly |

Knowledge sources, in order:
1. **Pattern cards**: `references/patterns.md` (ingest, transforms, ML loop, LLM steps, annotation,
   viewers/UI, ops hygiene) — each points at a reference example and the docs section.
2. **Reference examples**: shallow-clone `https://github.com/epoch8/datapipe` (or reuse an existing
   local clone) and READ the named example files — do not reimplement a pattern an example already
   ships (the LLM-step machinery lives in `examples/ocr`; the tag-metrics ML loop in
   `examples/detection_tags`).
3. **Official docs**: https://epoch8.github.io/datapipe/ — authoritative for core API when composing
   beyond the examples. On a machine without a datapipe checkout this is your API source; **never
   write datapipe API from memory.**

## SPEC.md before code

Write `docs/SPEC.md` from `references/spec-template.md` (task, data, deployment target, services,
blocks with verdicts, gap resolutions, secrets list — names only, determinism plan, success
criteria) and **pause for the user to review it**. Assembly follows the spec; future sessions read
it first.

## Seed, then grow (bring-up gate)

Scaffold the minimal skeleton from `references/seed.md` — pyproject (datapipe deps pinned, see the
card for the PyPI-vs-git decision), committed `uv.lock`, `.env.example`, compose with Postgres,
package with one table + one passthrough transform — and **run it before adding anything**:

```bash
uv sync && docker compose up -d
datapipe db create-all && datapipe run   # gate: the table must contain rows
```

A seed that doesn't run is the most common scaffold death; nothing is added on top of an unproven
foundation. Then add blocks one at a time, running each stage on the user's data, showing logs and
stating what changed — the conventions (stage labels, status tables over exit codes, `--debug` to a
file) are in the ops-hygiene card.

## Determinism by default

Everything generated pins its randomness, recorded in SPEC.md:

| block | pin |
|---|---|
| any sampling/shuffle/split | seeded RNG + order-canonical selection (sort, then seeded shuffle) |
| training | `seed=…, workers=0, deterministic=True` + `CUBLAS_WORKSPACE_CONFIG=:4096:8` set at the entrypoint (before torch import) |
| LLM steps | `temperature=0` / fixed decoding where the API allows; engine+model recorded |
| dependencies | `uv.lock` committed; editing deps re-locks and drifts versions across machines — treat the lock as the contract |

State the honest limit in the project README: reruns are bit-identical **on the same machine**;
different GPU/CPU models legitimately produce different weights/metrics (float arithmetic per chip —
verified experimentally in the datapipe repo). Quote reference numbers per hardware.

## Secrets: ask AND help obtain

When a chosen block implies keys (HF token, OpenAI/Gemini/Qwen, annotation-tool creds), ask for them
at SPEC time — and **offer the how-to-get path**, don't silently fall back to a stub that fakes the
feature. Per-provider guidance (gated-HF 403 pitfall, RU geoblocks + proxy pattern, cost caps):
`references/interview.md#secrets`.

## Red flags — you are about to repeat a baseline failure

- Creating files before the user answered the interview → stop, ask.
- "I'll assume the CSV has columns …" → that's a SPEC question, not an assumption.
- Writing a datapipe import you haven't seen in the docs/examples this session → open the docs.
- A silent fallback that substitutes the real feature (heuristic instead of LLM) → surface the
  missing key instead.
- Declaring "ready to run" without the seed gate having passed → run it.
- Custom code for something a pattern card names an example for → read the example first.

## Quick reference

| artifact | where | template |
|---|---|---|
| project spec | `docs/SPEC.md` | `references/spec-template.md` |
| seed skeleton | project root | `references/seed.md` |
| pattern cards | — | `references/patterns.md` |
| interview + secrets | — | `references/interview.md` |
| project ops skill | `.claude/skills/<name>-ops/SKILL.md` | write like datapipe's `setup-*` skills |

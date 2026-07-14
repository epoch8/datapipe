# docs/SPEC.md template

Copy into the project as `docs/SPEC.md`, fill every REQUIRED slot from the interview, get the
user's review BEFORE scaffolding. Future agent sessions read this file first.

```markdown
# <project name> — pipeline spec

**Date:** <YYYY-MM-DD> · **Status:** draft | approved · **Owner:** <user>

## Task (REQUIRED)
<What goes in, what must come out, in the user's words — made concrete: schemas, formats, quality bar.>

## Data (REQUIRED)
- source: <dir/S3/DB/API/HF + path/URI>
- item identity / dedup rule: <what makes two items the same>
- volume & cadence: <now / growth; one-off | incremental | cron>
- sample for bring-up: <path>

## Runs on (REQUIRED)
- host(s): <where>; GPU: <model, VRAM | none>
- services: <Postgres (which instance/db!), MinIO, Mongo, annotation tool — existing vs provisioned here>
- network constraints: <geoblocks/proxies/no-internet | none>

## Blocks & verdicts (REQUIRED)
| requirement | verdict (✅ block / 🔧 composable / ❌ gap) | how |
|---|---|---|
| <…> | <…> | <pattern card / docs section / gap resolution> |

## Gap resolutions (if any ❌)
<For each gap: chosen option (custom step | upstream block proposal | workaround) and why.
Upstream proposals: 3-5 lines — what, interface, who else needs it.>

## Secrets (REQUIRED — names only, values live in .env)
<HF_TOKEN, OPENAI_API_KEY, … — or "none">

## Determinism (REQUIRED)
- seed: <N> (sampling/splits/training) · training: workers=0, deterministic, CUBLAS env
- LLM: temperature=0, engine+model pinned: <…>
- uv.lock committed; NOTE: bit-identical reruns hold on the same machine only.

## Stages
<stage=… list with one line each: what it does, what proves it worked (table/count)>

## Success criterion (REQUIRED)
<"Done when …" — the acceptance check to run at the end.>

## Decisions log
<Every choice made during assembly that the user signed off on, dated.>
```

# Interview checklist

One question at a time. Multiple-choice where possible. Dig until concrete — an answer you could
not write a table schema or an acceptance test from is not an answer yet. Everything lands in
SPEC.md; anything you decided yourself is a defect.

## The task
- In plain words: what goes in, what must come out? Push past vagueness: "process documents" →
  which documents, what fields, what output schema (draft the pydantic model WITH the user),
  what accuracy/quality is acceptable?
- One-off, incremental (new data keeps arriving), or scheduled (cron)?

## Data
- Type (images/CSV/text/…), source (local dir / S3 / DB / API / HF dataset), access path.
- Volume now; growth rate; size of one item.
- Is a sample available RIGHT NOW? (You need it for the bring-up; ask for a path or 10 rows.)
- What is the primary key / identity of an item? What counts as a duplicate?

## Humans in the loop
- Annotation needed? Which tool do they already use (Label Studio / CVAT / none)?
- Review gates: does a human approve before the next stage? Who triggers training/retraining?

## Outputs
- Tables? A trained model? Metrics — sliced how (subsets, per-tag/scenario slices)? A viewer
  (FiftyOne)? The datapipe UI with ops-specs? An API?

## Where it runs
- Host(s): local / server / pod. GPU model + VRAM if training. Read-only home? (set
  `UV_CACHE_DIR`/`HF_HOME` to a writable path).
- Which services exist vs must be provisioned: Postgres (required), MinIO/S3, Mongo (FiftyOne),
  annotation tool. Never point DB_URL at an existing database without explicit confirmation.
- Network constraints: geoblocked APIs (OpenAI/Gemini are blocked from RU IPs), no-internet hosts
  (pre-stage caches, build them off-site and copy).

## Success criterion
- "Done when …" in the user's words → becomes the acceptance check you run at the end.

## Secrets

Ask at SPEC time, not when a stage crashes. For each enabled block list the keys it needs, then for
any the user doesn't have — **offer the how-to-get path**:

| key | where to get it | pitfalls to warn about |
|---|---|---|
| `HF_TOKEN` | huggingface.co → Settings → Access Tokens; for gated models ALSO accept the license on the model page and grant the token gated-repo READ | plain token on a gated model → 403 masked as `LocalEntryNotFoundError: check your connection` |
| `OPENAI_API_KEY` | platform.openai.com → API keys | geoblocked from RU IPs (403); offer a CONNECT proxy on a non-blocked machine + `HTTPS_PROXY` for the calling stage |
| `GEMINI_API_KEY` / `GOOGLE_API_KEY` | aistudio.google.com | same geoblock story as OpenAI |
| `QWEN_API_KEY` | DashScope console | works from RU — suggest it as the RU-friendly engine |
| annotation tools | Label Studio: token in user settings; CVAT: URL + user/pass + an existing project whose labels match the config | label-name mismatch runs fine but yields 0 results |

Rules: keys go into `.env` (chmod 600, gitignored), **names only** into SPEC.md. Never mask a
missing key with a stub that fakes the feature — surface it and pause. For LLM blocks, agree a cost
cap (items × engines = calls) before the first paid run.

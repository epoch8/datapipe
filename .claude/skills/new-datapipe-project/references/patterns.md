# Pattern cards

One card per block. Each names the reference implementation (read it in a shallow clone of
`epoch8/datapipe` before writing your version) and the docs anchor
(https://epoch8.github.io/datapipe/). Do not reimplement what a card's example already ships.

## 1. Ingest — external source → tables
- **Use for:** dirs of files, S3/MinIO, DBs, APIs, HF datasets; one-off or watching for new items.
- **How:** `UpdateExternalTable` for stores datapipe can watch; otherwise a loader `BatchTransform`
  keyed by a request/registry table (pattern: enqueue a request row → load step materializes items) —
  see `examples/detection_tags` (`load_request` → `load_batch`) for the request-driven shape, incl.
  pre-staged local cache for offline/blocked hosts and `--subset` pinning.
- **Determinism:** selection must be order-canonical (sorted then seeded shuffle) if you sample.
- **Pitfall:** id = file stem/business key, decided in SPEC; uploads to object storage need the
  anonymous-download policy if anything reads by public URL.

## 2. Transform chains — the core
- **Use for:** everything between ingest and outputs.
- **First look for an existing entity:** the libs ship many ready Step classes and stores beyond
  `BatchTransform` — `UpdateExternalTable`, `DatatableBatchTransform`, `BatchGenerate`, table/filedir
  stores, and the whole `datapipe-ml` / label-studio / cvat step families. Docs + examples before
  custom code.
- **How:** `BatchTransform(func, inputs, outputs, transform_keys, chunk_size)`; funcs are stateless
  `pd.DataFrame → pd.DataFrame`. Aggregations = transform_keys on the GROUP key (many-to-few).
  Docs: core concepts + CLI; examples: `examples/datapipe_core/*` (one concept per folder).
- **Incrementality contract:** rerunning reprocesses only changed rows; loading the same data twice
  is a no-op. Deleting an input request row does NOT cascade — clean state = wipe schema + rerun.
- **Stage labels:** every step gets `labels=[("stage", "<name>")]`; run one phase via
  `datapipe step --labels=stage=<name> run`.

## 3. ML loop — freeze → train → inference → metrics → best model
- **Reference:** `examples/detection_tags` (self-contained, injected GT) and
  `examples/e2e_template/image_detection` (with annotation loop). Lib: `datapipe-ml`.
- **Shape:** split (honor pinned subsets!) → `DetectionFreezeDataset` → `Train_YoloV8_*` →
  inference → metrics on subsets (and per-tag slices — the point of `detection_tags`) →
  `FindBestModel`.
- **Frozen-val discipline:** pin val membership BEFORE training the baseline; add later data to
  TRAIN only — otherwise A-vs-B metric comparisons are apples-to-oranges.
- **Pitfalls (field-tested):** freeze has a time debounce (`min_within_time` — set `"1s"` for fast
  iteration; "Not enough time passed" in logs otherwise); trust `*_training_status` tables, not exit
  codes; CPU training requires `CUDA_VISIBLE_DEVICES=""` explicitly; pre-AVX2 hosts need
  `polars-lts-cpu` force-reinstalled after sync; metric gains must exceed cross-hardware noise
  (~±0.05) if the story must hold on other machines — data diversity (e.g. multiple augmentation
  intensities) generalizes, more epochs just memorizes.

## 4. LLM steps — structured model calls as transforms
- **Reference:** `examples/ocr` — engine registry (`config.ENGINE_REGISTRY`), pydantic
  `OUTPUT_MODEL` driving the prompt, items × engines cross-join, per-engine output columns.
- **Determinism:** temperature=0/fixed decoding; pin engine+model in SPEC.
- **Cost:** items × engines = calls; agree a cap before the first paid run (`HF_LIMIT`-style knob).
- **Pitfalls:** geoblocks (OpenAI/Gemini from RU → proxy or Qwen); never stub the LLM silently when
  the key is missing.

## 5. Annotation loops — humans in the pipeline
- **Reference:** Label Studio → `examples/e2e_template` (`datapipe-label-studio`: upload tasks,
  pull annotations, current-model predictions as pre-annotations); CVAT → `examples/sam_cvat`
  (SAM3 pre-annotation) and `examples/datapipe_cvat`.
- **Shape:** pipeline pauses at human gates — a stage publishes tasks, a later stage ingests
  completed work; the human marks completion in the tool.
- **Pitfall:** label names must match EXACTLY across prompt/config/label-field/class filters —
  mismatch runs clean and yields zero results.

## 6. Viewers & UI
- **FiftyOne:** publish images + per-model/per-field overlays; reference
  `examples/detection_tags` (`stage=fiftyone`, `publish_gt/predictions`, sample fields for
  filtering) and `examples/ocr` (Caption Viewer for JSON). Mongo required; App must run where the
  local image files are.
- **datapipe UI (ops-specs):** serve with `datapipe --pipeline app api --host 127.0.0.1 --port 8000`
  and register `app.add_specs([DatapipeOpsSpec(...)])` — data/frozen/training/metrics tables wired
  by column names; reference `examples/detection_tags/detection/app.py` on the UI branch. Bind
  localhost + SSH tunnel; UI can trigger stage runs.

## 7. Ops hygiene (applies to every project)
- Pre-flight before provisioning: ports (`ss -ltn`), existing schemas, running compose — ask, don't
  clobber.
- `db create-all` after any schema change; wipe = `DROP SCHEMA … CASCADE` + recreate + create-all.
- Debugging: rerun with `datapipe --debug … run > /tmp/dp.log 2>&1` then grep — never dump debug
  inline. Metrics steps may print "Batches to process 0" right after producing inputs — rerun once.
- Remote hosts: writable HOME (`UV_CACHE_DIR`, `HF_HOME`), flaky links → retry loops, long jobs in
  tmux/nohup with logs to files; local machine configs (fsspec/aws yandex leftovers!) can leak into
  clients — neutralize with `FSSPEC_CONFIG_DIR`/`AWS_CONFIG_FILE` if storage calls behave oddly.
- Finalize every project with its own ops skill: interview answers → `.claude/skills/<name>-ops/`
  written like datapipe's `setup-*` skills (stages, knobs, troubleshooting observed during bring-up).

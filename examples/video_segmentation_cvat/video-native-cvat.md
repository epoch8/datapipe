# Video-native annotation in CVAT — research notes

**Question:** can we annotate long videos *as video* in CVAT (tracks with keyframes/interpolation)
and combine that with an automated pre-annotation pipeline, instead of this example's current
"extract + dedup frames → SAM3 per-frame → upload images" approach?

**Bottom line:** Yes. CVAT supports true video-native annotation, and a `video → SAM → CVAT tracks`
workflow is feasible and, for video, better than per-frame image tasks. CVAT ingests a video file
directly (decodes frame chunks on demand), gives annotators **Track Mode** with keyframe
interpolation, exports interpolated tracks natively, and already ships a **native SAM2 tracker** that
propagates a mask/polygon across frames. Two caveats dominate the integration: the built-in
auto-annotation `detect` protocol is **per-frame only** (tracks need a separate import type), and
uploading annotations **replaces** rather than merges existing ones.

> Sourced from CVAT official docs + GitHub issues (see [Sources](#sources)). Confidence flags reflect
> adversarial verification; time-sensitive because CVAT docs URLs move and SAM2/SAM3 features are new.
> This deployment is self-hosted CVAT ~v2.65 — verify Enterprise-only features against it.

---

## 1. Native video vs pre-extracted frames

- CVAT ingests a **video file as a video task** using **"data on the fly"**: at task creation it
  collects only minimal meta/manifest info, then decodes & caches frame chunks on demand — it does
  *not* pre-extract every frame upfront (unlike our extract→upload-images flow). *(high)*
  — https://docs.cvat.ai/docs/dataset_management/data-on-fly/
- The **dataset manifest** is JSONL, one entry per keyframe `{number, pts, checksum(md5)}`, enabling
  random-access seeking (seek to keyframe PTS → decode forward). *(medium — seeking framing is
  inferred from code, not verbatim in docs)*
  — https://docs.cvat.ai/docs/dataset_management/dataset_manifest/
- **Track Mode** is the video-native workflow: annotators set keyframes (`K` / star) where a shape
  changes; CVAT **linearly interpolates** the shape (box, polygon, …) across intermediate frames. *(high)*
  — https://docs.cvat.ai/docs/annotation/manual-annotation/modes/track-mode-basics/
  — https://www.cvat.ai/academy/track-mode

## 2. Video-task limits & gotchas

- **Data-on-fly is not universal:** if a video has too few keyframes for smooth decoding, CVAT falls
  back to full pre-extraction at task-creation time (slow) — a real risk for long/egocentric /
  action-cam footage. First access is also slower. *(high)*
  — https://docs.cvat.ai/docs/dataset_management/data-on-fly/
  — issues: https://github.com/cvat-ai/cvat/issues/1507 · https://github.com/cvat-ai/cvat/issues/7425
    · https://github.com/cvat-ai/cvat/issues/9519 · https://github.com/cvat-ai/cvat/issues/8913
    · https://github.com/openvinotoolkit/cvat/issues/2694
- Reported in issues (**not independently verified**): a stall every ~36 frames at chunk boundaries
  (default chunk size); heavy buffering on 4K / remote 1080p; non-zip-chunk mode is faster (`D`/`F`
  hotkeys) but slightly degrades quality; **OpenH264 caps resolution at ~9.4 MP (~4K)**.
- ⚠️ **No source firmly quantified hard limits** (max length/size, codec allow-list, multi-hour
  performance). Treat these as open — measure on our build.

## 3. Tracks vs shapes, and export

- Data model cleanly separates **`LabeledShape`** (per-frame) from **`Track`/`TrackedShape`**; the
  `keyframe` and `outside` flags on a tracked shape mark keyframes and interpolation/absence
  boundaries. *(high)* — https://docs.cvat.ai/docs/contributing/new-annotation-format/
- **CVAT for video 1.1 (.xml)** represents each object as a `<track>` whose shapes carry
  `frame`/`keyframe`/`outside` — the structural basis of interpolation. *(high)*
  — https://docs.cvat.ai/docs/dataset_management/formats/format-cvat/
- Formats that **support tracks**: CVAT-for-video 1.1, CVAT-for-images 1.1, COCO, **MOT** (bbox tracks
  only), **MOTS** (mask tracks), **Datumaro** (via `track_id`), Ultralytics YOLO variants. *(high)*
  — https://docs.cvat.ai/docs/dataset_management/formats/
  — https://docs.cvat.ai/docs/dataset_management/formats/format-mot/
  — https://docs.cvat.ai/docs/dataset_management/formats/format-datumaro/
- ⚠️ **Per-frame export (`group_by_frame()`) flattens tracks into shapes** — object identity is lost.
  Track-aware formats iterate tracks directly. *(high)*
  — https://docs.cvat.ai/docs/contributing/new-annotation-format/

## 4. Auto-annotation / API (most relevant to integration)

- The SDK's built-in **`detect` protocol is per-image**: it returns per-frame shapes/tags, **not
  interpolated tracks** — exactly this example's per-frame SAM3 model. *(high)*
  — https://docs.cvat.ai/docs/api_sdk/sdk/auto-annotation/
- A **separate tracking protocol** (`init_tracking_state` + `track`) propagates shapes onto subsequent
  frames. Tracks are uploaded via a direct annotation-import type (`LabeledTrackRequest`), not `detect`. *(high)*
  — https://docs.cvat.ai/docs/api_sdk/sdk/auto-annotation/
- ⚠️ **Uploading annotations is destructive** — "CVAT removes the existing ones"; import is
  replace, not merge, per job/task. Merge locally before upload. *(high)*
  — https://docs.cvat.ai/docs/dataset_management/import-datasets/

## 5. SAM for video

- CVAT ships a **native SAM2 tracker**: it propagates an **existing** polygon/mask forward across
  frames (tracking, not detection), works with **polygons/masks only** (not boxes/skeletons), and has
  an optional **"convert polygon shapes to tracks"**. *(high)*
  — https://docs.cvat.ai/docs/annotation/auto-annotation/segment-anything-2-tracker/
  — https://www.cvat.ai/resources/changelog/video-annotation-sam-2
- Two deployment forms: a **Nuclio serverless function (self-hosted Enterprise)** and an **"AI Agent"
  worker** run on your own hardware (SAM2 tracking for CVAT Online, no server GPU/Nuclio). *(high)*
  — https://www.cvat.ai/resources/blog/sam2-ai-agent-tracking
- **SAM3:** two claims that CVAT's SAM3 is visual-prompt / image-only with no text prompt were
  **refuted (0-3)** — text prompts appear to be supported — but the exact scope (native video /
  temporal in the UI) was **not positively confirmed**. Open question. *(low)*
  — https://www.cvat.ai/resources/changelog/sam-3-image-segmentation

## 6. Options for this pipeline

| Approach | How | Pros | Cons |
|---|---|---|---|
| **Current** (this example) | extract+dedup frames → SAM3 per-frame → image task | Simple, already works, per-frame masks | No temporal tracks; object identity lost; more manual review |
| **Video-native, tracks from us** | upload video as video task; push SAM masks as `LabeledTrackRequest` | True tracks + interpolation for reviewers | SAM3 is per-frame with **no track_id** → needs an object-association step we don't have; destructive upload |
| **Video-native, CVAT tracks** | upload video; seed shapes; CVAT **SAM2 tracker** propagates | Least code on our side; temporal logic in CVAT | SAM2 Nuclio is **Enterprise** (check our ~v2.65 license) or run an AI-Agent worker |

**Migration checklist:** (1) tracks must be uploaded via direct import, not `detect`; (2) upload
replaces — structure as one-shot pre-annotation before review, or merge locally; (3) confirm our
videos don't fall back to slow pre-extraction (keyframe density); (4) confirm SAM3's real scope on our
build.

**Open questions:** concrete CVAT video limits; real SAM3 scope in v2.65; whether track import
round-trips cleanly.

---

## Sources

Primary (CVAT docs):
- data-on-fly — https://docs.cvat.ai/docs/dataset_management/data-on-fly/
- dataset manifest — https://docs.cvat.ai/docs/dataset_management/dataset_manifest/
- track mode — https://docs.cvat.ai/docs/annotation/manual-annotation/modes/track-mode-basics/
- annotation formats (overview) — https://docs.cvat.ai/docs/dataset_management/formats/
- CVAT format — https://docs.cvat.ai/docs/dataset_management/formats/format-cvat/
- MOT format — https://docs.cvat.ai/docs/dataset_management/formats/format-mot/
- Datumaro format — https://docs.cvat.ai/docs/dataset_management/formats/format-datumaro/
- annotation data model — https://docs.cvat.ai/docs/contributing/new-annotation-format/
- auto-annotation SDK — https://docs.cvat.ai/docs/api_sdk/sdk/auto-annotation/
- import datasets — https://docs.cvat.ai/docs/dataset_management/import-datasets/
- SAM2 tracker — https://docs.cvat.ai/docs/annotation/auto-annotation/segment-anything-2-tracker/

CVAT blog / academy / changelog:
- Track Mode academy — https://www.cvat.ai/academy/track-mode
- SAM2 video annotation changelog — https://www.cvat.ai/resources/changelog/video-annotation-sam-2
- SAM2 AI-Agent tracking — https://www.cvat.ai/resources/blog/sam2-ai-agent-tracking
- SAM3 image segmentation changelog — https://www.cvat.ai/resources/changelog/sam-3-image-segmentation

GitHub issues (video limits/perf, forum-quality):
- https://github.com/cvat-ai/cvat/issues/1507
- https://github.com/cvat-ai/cvat/issues/7425
- https://github.com/cvat-ai/cvat/issues/9519
- https://github.com/cvat-ai/cvat/issues/8913
- https://github.com/openvinotoolkit/cvat/issues/2694

_Method: multi-agent web research — 5 search angles, 22 sources fetched, 93 claims extracted, top 25
adversarially verified (23 confirmed / 2 refuted). Confidence flags above reflect the vote._

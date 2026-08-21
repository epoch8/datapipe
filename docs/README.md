# Datapipe Documentation

## Framework: Diátaxis

This documentation follows the [Diátaxis](https://diataxis.fr/) framework, which organises content into four distinct quadrants based on the reader's intent:

| Section | Reader's question | Nature |
|---|---|---|
| **Getting Started** | "How do I get up and running?" | Tutorial — learning-oriented |
| **Concepts** | "Why does it work this way?" | Explanation — understanding-oriented |
| **How-to Guides** | "How do I accomplish X?" | Task-oriented, assumes prior knowledge |
| **Reference** | "What exactly does this parameter do?" | Lookup — complete, precise, terse |

A fifth section, **Explanation**, holds deeper internals (lifecycle, SQL schema) that are not needed day-to-day. **Migration** guides live separately.

The key discipline of Diátaxis: each page belongs to **exactly one quadrant**. Tutorials do not double as reference; reference does not explain motivation. When you add a page, decide which quadrant it belongs to before writing the first word.

## Source layout

```
docs/
├── README.md                  ← you are here
├── book.toml                  ← mdBook configuration
├── Makefile                   ← build / inventory / gifs
├── inventory-map.yaml         ← symbol → page map
├── inventory-core.yaml        ← generated coverage snapshot
├── scripts/
│   ├── inventory_core.py
│   └── render_incremental_gifs.py
└── source/
    ├── SUMMARY.md             ← TOC
    ├── troubleshooting.md
    ├── getting-started/
    ├── concepts/              ← includes Incremental Processing + GIFs
    ├── how-to/
    ├── reference/
    ├── ops/                   ← App / UI / observability
    ├── integrations/          ← ML / Label Studio / CVAT
    ├── explanation/
    ├── migration/
    └── assets/incremental/    ← regenerable GIFs
```

## Tooling

### Core API inventory (coverage gate)

`docs/scripts/inventory_core.py` walks `libs/datapipe-core` (AST, no optional imports),
lists public pipe surfaces (`PipelineStep` / `TableStore` / `Executor` subclasses, catalog types, CLI),
and checks each symbol against `docs/inventory-map.yaml` → an existing page under `docs/source/`.

```bash
cd docs
make inventory          # print report + write inventory-core.yaml
make inventory-strict   # same, exit 1 if any symbol has no page
```

Edit `inventory-map.yaml` when you add a new public step/store/CLI command. Generated
`inventory-core.yaml` is a snapshot for PR review; regenerate before committing docs changes.

### Incremental GIFs

Four explainers for insert / update / delete / unchanged:

```bash
cd docs && make gifs
```

Writes `source/assets/incremental/*.gif`. Commit the GIFs so mdBook/RTD need no renderer. Embedded from [Incremental Processing](./source/concepts/incremental-processing.md).

### mdBook (primary)

The docs are built with [mdBook](https://rust-lang.github.io/mdBook/).

```bash
# Install
cargo install mdbook

# Serve locally with live reload
cd docs && mdbook serve

# Build static site to docs/book/
cd docs && mdbook build
```

`SUMMARY.md` is the single source of truth for navigation. Every page that should appear in the sidebar must have an entry there. Unlisted files are ignored by mdBook.

### ReadTheDocs / Sphinx (CI publishing)

`.readthedocs.yml` in the repo root configures automated builds on ReadTheDocs using Sphinx with the `myst_parser` extension, which parses the same Markdown files. The Sphinx configuration lives in `docs/source/conf.py`.

Sphinx does not use `SUMMARY.md` for navigation — ReadTheDocs builds its own TOC from the file tree. The two tools render the same source files independently.

## Writing guidelines

- **Stub pages** — new pages start with a single `> This page is a work in progress.` blockquote. This is intentional: an honest stub is better than an absent entry in the TOC.
- **Language** — all documentation is in English.
- **Code examples** — prefer complete, runnable snippets. Link to `examples/` in the repo when a full working app exists.
- **No mixed quadrants** — do not add conceptual prose to a reference page or step-by-step instructions to a concept page.

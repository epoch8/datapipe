# Prefer local binary docs/.bin/mdbook (no system install needed).
MDBOOK ?= $(shell if [ -x .bin/mdbook ]; then echo .bin/mdbook; \
	elif command -v mdbook >/dev/null 2>&1; then command -v mdbook; \
	else echo mdbook; fi)

.PHONY: build serve inventory inventory-strict gifs

build:
	$(MDBOOK) build

serve:
	$(MDBOOK) serve

# Discover datapipe-core pipe surfaces and write docs/inventory-core.yaml
inventory:
	python3 scripts/inventory_core.py --write

# Fail if any inventoried symbol lacks a docs page (stubs still OK)
inventory-strict:
	python3 scripts/inventory_core.py --write --strict

# Regenerable incremental-processing explainers (Pillow)
gifs:
	python3 scripts/render_incremental_gifs.py

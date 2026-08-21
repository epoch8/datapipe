# brew install mdbook

.PHONY: build serve inventory inventory-strict gifs

build:
	mdbook build

serve:
	mdbook serve

# Discover datapipe-core pipe surfaces and write docs/inventory-core.yaml
inventory:
	python3 scripts/inventory_core.py --write

# Fail if any inventoried symbol lacks a docs page (stubs still OK)
inventory-strict:
	python3 scripts/inventory_core.py --write --strict

# Regenerable incremental-processing explainers (Pillow)
gifs:
	python3 scripts/render_incremental_gifs.py
MONOREPO_ROOT := $(abspath ../..)

.PHONY: test build package

# Compiled into @datapipe/ui — no standalone SPA build here.
test:
	cd $(MONOREPO_ROOT) && yarn workspace @datapipe/ui test

build:
	cd $(MONOREPO_ROOT) && yarn workspace @datapipe/ui build

package:
	uv build --package datapipe-ui-ml

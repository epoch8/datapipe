MONOREPO_ROOT := $(abspath ../..)
HOST_UID := $(shell id -u)
HOST_GID := $(shell id -g)
DOCKER_RUN_NODE = docker run --rm --user $(HOST_UID):$(HOST_GID) -e HOME=/tmp \
	-e COREPACK_ENABLE_DOWNLOAD_PROMPT=0 \
	-v $(MONOREPO_ROOT):/monorepo -w /monorepo/libs/datapipe-ui node:20-bookworm-slim

.PHONY: install start build build-package test package build-docker

install:
	cd $(MONOREPO_ROOT) && pnpm install

start:
	cd $(MONOREPO_ROOT) && pnpm --filter @datapipe/ui start

build:
	cd $(MONOREPO_ROOT) && pnpm --filter @datapipe/ui build

build-package:
	cd $(MONOREPO_ROOT) && pnpm --filter @datapipe/ui build:package

test:
	cd $(MONOREPO_ROOT) && pnpm --filter @datapipe/ui test

package: build-package
	uv build --package datapipe-ui

build-docker:
	$(DOCKER_RUN_NODE) bash -c "mkdir -p /tmp/bin && corepack enable --install-directory /tmp/bin && export PATH=/tmp/bin:\$$PATH && cd /monorepo && pnpm install --frozen-lockfile && pnpm --filter @datapipe/ui build:package"

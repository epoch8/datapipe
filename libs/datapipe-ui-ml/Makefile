MONOREPO_ROOT := $(abspath ../..)
HOST_UID := $(shell id -u)
HOST_GID := $(shell id -g)
DOCKER_RUN_NODE = docker run --rm --user $(HOST_UID):$(HOST_GID) -e HOME=/tmp \
	-v $(MONOREPO_ROOT):/monorepo -w /monorepo/libs/datapipe-ui-ml node:18.7.0-slim

.PHONY: install start build build-package test test-build package build-docker

install:
	cd $(MONOREPO_ROOT) && yarn install

start:
	cd $(MONOREPO_ROOT) && yarn workspace @datapipe/ui-ml start

build:
	cd $(MONOREPO_ROOT) && yarn workspace @datapipe/ui-ml build

build-package:
	cd $(MONOREPO_ROOT) && yarn workspace @datapipe/ui-ml build:package

test:
	cd $(MONOREPO_ROOT) && yarn workspace @datapipe/ui-ml test

test-build:
	cd $(MONOREPO_ROOT) && yarn workspace @datapipe/ui-ml test:build

package: build-package
	uv build --package datapipe-ui-ml

build-docker:
	$(DOCKER_RUN_NODE) bash -c "cd /monorepo && yarn install && yarn workspace @datapipe/ui-ml build:package"

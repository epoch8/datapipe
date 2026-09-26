MONOREPO_ROOT := $(abspath ../..)
HOST_UID := $(shell id -u)
HOST_GID := $(shell id -g)
DOCKER_RUN_NODE = docker run --rm --user $(HOST_UID):$(HOST_GID) -e HOME=/tmp \
	-v $(MONOREPO_ROOT):/monorepo -w /monorepo/libs/datapipe-ui node:18.7.0-slim

.PHONY: install start build build-package test package build-docker

install:
	cd $(MONOREPO_ROOT) && yarn install

start:
	cd $(MONOREPO_ROOT) && yarn workspace @datapipe/ui start

build:
	cd $(MONOREPO_ROOT) && yarn workspace @datapipe/ui build

build-package:
	cd $(MONOREPO_ROOT) && yarn workspace @datapipe/ui build:package

test:
	cd $(MONOREPO_ROOT) && yarn workspace @datapipe/ui test

package: build-package
	uv build --package datapipe-ui

build-docker:
	$(DOCKER_RUN_NODE) bash -c "cd /monorepo && yarn install && yarn workspace @datapipe/ui build:package"

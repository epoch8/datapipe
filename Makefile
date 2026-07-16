# Frontend builds for Datapipe Ops UI (monorepo root).
#
#   make build-datapipe-ui       # core-only SPA → libs/datapipe-ui/datapipe_ui/static
#   make build-datapipe-ui-ml    # core + ML plugin SPA → libs/datapipe-ui-ml/datapipe_ui_ml/static
#   make build-datapipe-ui-all   # both packages in parallel (after one yarn install)

.PHONY: install-frontend build-datapipe-ui build-datapipe-ui-ml build-datapipe-ui-all \
	_build-datapipe-ui-package _build-datapipe-ui-ml-package

install-frontend:
	yarn install

_build-datapipe-ui-package:
	$(MAKE) -C libs/datapipe-ui build-package

_build-datapipe-ui-ml-package:
	$(MAKE) -C libs/datapipe-ui-ml build-package

build-datapipe-ui: install-frontend _build-datapipe-ui-package

build-datapipe-ui-ml: install-frontend _build-datapipe-ui-ml-package

# One install, then both CRA builds concurrently.
build-datapipe-ui-all: install-frontend
	$(MAKE) -j2 _build-datapipe-ui-package _build-datapipe-ui-ml-package

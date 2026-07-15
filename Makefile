# Frontend builds for Datapipe Ops UI (monorepo root).
#
#   make build-datapipe-ui       # core-only SPA → libs/datapipe-ui/datapipe_ui/static
#   make build-datapipe-ui-ml    # core + ML plugin SPA → libs/datapipe-ui-ml/datapipe_ui_ml/static
#   make build-datapipe-ui-all   # both packages

.PHONY: install-frontend build-datapipe-ui build-datapipe-ui-ml build-datapipe-ui-all

install-frontend:
	yarn install

build-datapipe-ui: install-frontend
	$(MAKE) -C libs/datapipe-ui build-package

build-datapipe-ui-ml: install-frontend
	$(MAKE) -C libs/datapipe-ui-ml build-package

build-datapipe-ui-all: build-datapipe-ui build-datapipe-ui-ml

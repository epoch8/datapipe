HOST_UID := $(shell id -u)
HOST_GID := $(shell id -g)
FRONTEND_VOLUMES = -v ${CURDIR}/frontend:/app -v ${CURDIR}/datapipe_app/frontend:/app/build -w /app
DOCKER_RUN_NODE = docker run --rm --user $(HOST_UID):$(HOST_GID) -e HOME=/tmp $(FRONTEND_VOLUMES)

build-example:
	docker build -f examples/datapipe_app/Dockerfile .

build-frontend:
	$(DOCKER_RUN_NODE) node:18.7.0-slim yarn
	$(DOCKER_RUN_NODE) node:18.7.0-slim yarn build

build: build-frontend
	uv build --package datapipe-app

lint:
	black --line-length=120 datapipe_app
	flake8 --max-line-length=120 datapipe_app
	mypy -p datapipe_app --ignore-missing-imports --follow-imports=silent --namespace-packages --check-untyped-defs

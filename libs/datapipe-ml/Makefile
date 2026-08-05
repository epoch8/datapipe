PYTHON ?= $(if $(wildcard ../.venv/bin/python),../.venv/bin/python,python)
HOST_UID := $(shell id -u)
HOST_GID := $(shell id -g)
COMPOSE = HOST_UID=$(HOST_UID) HOST_GID=$(HOST_GID) docker compose

.PHONY: black mypy compose-up compose-down

compose-up:
	mkdir -p tests/.docker-data/minio
	$(COMPOSE) --env-file tests/.env.test -f tests/docker-compose.yml up -d

compose-down:
	$(COMPOSE) -f tests/docker-compose.yml down

black:
	$(PYTHON) -m autoflake -r --in-place --remove-all-unused-imports datapipe_ml/ tests/
	$(PYTHON) -m black --verbose --config black.toml tests/ datapipe_ml/
	$(PYTHON) -m ruff check --fix datapipe_ml/ tests/
	$(PYTHON) -m isort --profile black datapipe_ml/ tests/

mypy:
	$(PYTHON) -m mypy --config-file mypy-config.toml -p datapipe_ml --ignore-missing-imports --follow-imports=silent --check-untyped-defs

PYTHON ?= $(if $(wildcard ../.venv/bin/python),../.venv/bin/python,python)

.PHONY: black mypy

black:
	$(PYTHON) -m autoflake -r --in-place --remove-all-unused-imports datapipe_ml/ tests/
	$(PYTHON) -m black --verbose --config black.toml tests/ datapipe_ml/
	$(PYTHON) -m ruff check --fix datapipe_ml/ tests/
	$(PYTHON) -m isort --profile black datapipe_ml/ tests/

mypy:
	$(PYTHON) -m mypy --config-file mypy-config.toml -p datapipe_ml --ignore-missing-imports --follow-imports=silent --check-untyped-defs
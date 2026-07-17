black:
	autoflake -r --in-place --remove-all-unused-imports datapipe_label_studio/ tests/
	isort --profile black toml datapipe_label_studio/ tests/
	black --verbose --config black.toml datapipe_label_studio/ tests/

mypy:
	mypy -p datapipe_label_studio --ignore-missing-imports --follow-imports=silent --check-untyped-defs
black:
	autoflake -r --in-place --remove-all-unused-imports datapipe_cvat/
	isort --profile black toml datapipe_cvat/
	black --verbose --config black.toml datapipe_cvat/

mypy:
	mypy -p datapipe_cvat --ignore-missing-imports --follow-imports=silent --check-untyped-defs
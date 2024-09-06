black:
	autoflake -r --in-place --remove-all-unused-imports steps/ *.py brandlink_utils/
	black --verbose --config black.toml steps/ alembic *.py brandlink_utils/

mypy:
	mypy -p datapipe --ignore-missing-imports --follow-imports=silent --namespace-packages
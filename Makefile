black:
	autoflake -r --in-place --remove-all-unused-imports steps/ *.py brandlink_utils/
	black --verbose --config black.toml steps/ alembic *.py brandlink_utils/

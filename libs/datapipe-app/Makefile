build-example:
	docker build -f examples/datapipe_app/Dockerfile .

build: 
	uv build --package datapipe-app

lint:
	black --line-length=120 datapipe_app
	flake8 --max-line-length=120 datapipe_app
	mypy -p datapipe_app --ignore-missing-imports --follow-imports=silent --namespace-packages --check-untyped-defs

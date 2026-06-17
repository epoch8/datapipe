build-example:
	docker build -f examples/datapipe_app/Dockerfile .

build-frontend:
	docker run --rm -v ${CURDIR}/frontend:/app -v ${CURDIR}/datapipe_app/frontend:/app/build -w /app node:18.7.0-slim yarn
	docker run --rm -v ${CURDIR}/frontend:/app -v ${CURDIR}/datapipe_app/frontend:/app/build -w /app node:18.7.0-slim yarn build

build: build-frontend
	uv build --package datapipe-app

lint:
	black --line-length=120 datapipe_app
	flake8 --max-line-length=120 datapipe_app
	mypy -p datapipe_app --ignore-missing-imports --follow-imports=silent --namespace-packages --check-untyped-defs

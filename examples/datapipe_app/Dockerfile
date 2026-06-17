FROM node:18.7.0-slim AS frontend-build

WORKDIR /srv
COPY libs/datapipe-app/frontend/ /srv/

RUN yarn && yarn build

FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY libs/datapipe-core ./libs/datapipe-core
COPY libs/datapipe-app ./libs/datapipe-app
COPY examples/datapipe_app ./examples/datapipe_app
COPY --from=frontend-build /srv/build ./libs/datapipe-app/datapipe_app/frontend

RUN pip install uv && uv pip install --system -e "libs/datapipe-core[sqlite]" -e "libs/datapipe-app"

WORKDIR /app/examples/datapipe_app

CMD uvicorn app:app --host=0.0.0.0 --port=${PORT:-8000}

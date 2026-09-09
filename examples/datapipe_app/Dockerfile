FROM node:18.7.0-slim AS frontend-build

WORKDIR /monorepo
COPY package.json yarn.lock ./
COPY libs/datapipe-ui/package.json libs/datapipe-ui/
RUN yarn install --frozen-lockfile
COPY libs/datapipe-ui libs/datapipe-ui
RUN yarn workspace @datapipe/ui build:package

FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml ./
COPY libs/datapipe-core ./libs/datapipe-core
COPY libs/datapipe-app ./libs/datapipe-app
COPY --from=frontend-build /monorepo/libs/datapipe-ui ./libs/datapipe-ui
COPY examples/datapipe_app ./examples/datapipe_app

RUN pip install uv && uv pip install --system -e "libs/datapipe-core[sqlite]" -e "libs/datapipe-ui" -e "libs/datapipe-app[ui]"

WORKDIR /app/examples/datapipe_app

CMD uvicorn app:app --host=0.0.0.0 --port=${PORT:-8000}

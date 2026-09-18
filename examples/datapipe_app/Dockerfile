FROM node:20-bookworm-slim AS frontend-build

ENV COREPACK_ENABLE_DOWNLOAD_PROMPT=0
RUN corepack enable

WORKDIR /monorepo
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml .npmrc ./
COPY packages packages
COPY libs/datapipe-ui libs/datapipe-ui
RUN pnpm install --frozen-lockfile --filter @datapipe/ui...
RUN pnpm --filter @datapipe/ui build:package

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

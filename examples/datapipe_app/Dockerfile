FROM node:18.7.0-slim AS frontend-build

WORKDIR /srv
COPY package.json yarn.lock ./
COPY libs/datapipe-ui/package.json libs/datapipe-ui/
COPY libs/datapipe-ui-ml/package.json libs/datapipe-ui-ml/
COPY libs/datapipe-ui ./libs/datapipe-ui
COPY libs/datapipe-ui-ml ./libs/datapipe-ui-ml

RUN yarn install --frozen-lockfile
RUN yarn workspace @datapipe/ui build

FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1
ENV DB_CONN_URI=sqlite+pysqlite3:///store.sqlite

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

COPY libs/datapipe-core ./libs/datapipe-core
COPY libs/datapipe-ui ./libs/datapipe-ui
COPY libs/datapipe-app ./libs/datapipe-app
COPY examples/datapipe_app ./examples/datapipe_app
COPY --from=frontend-build /srv/libs/datapipe-ui/build ./libs/datapipe-ui/datapipe_ui/static

WORKDIR /app/examples/datapipe_app

RUN uv sync --frozen --no-dev && uv run alembic upgrade head

EXPOSE 8000

CMD ["uv", "run", "datapipe", "api", "--host", "0.0.0.0", "--port", "8000"]

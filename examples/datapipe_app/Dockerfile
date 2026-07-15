FROM node:18.7.0-slim AS frontend-build

WORKDIR /srv
COPY libs/datapipe-app/frontend/ /srv/

RUN yarn && yarn build

FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1
ENV DB_CONN_URI=sqlite+pysqlite3:///store.sqlite

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

COPY libs/datapipe-core ./libs/datapipe-core
COPY libs/datapipe-app ./libs/datapipe-app
COPY examples/datapipe_app ./examples/datapipe_app
COPY --from=frontend-build /srv/build ./libs/datapipe-app/datapipe_app/frontend

WORKDIR /app/examples/datapipe_app

RUN uv sync --frozen --no-dev && uv run alembic upgrade head

EXPOSE 8000

CMD ["uv", "run", "datapipe", "api", "--host", "0.0.0.0", "--port", "8000"]

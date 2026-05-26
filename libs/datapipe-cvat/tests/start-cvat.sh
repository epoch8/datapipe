#!/usr/bin/env bash
set -euo pipefail

CVAT_VERSION="${CVAT_VERSION:-v2.65.0}"
CVAT_REPO_DIR="${CVAT_REPO_DIR:-/tmp/cvat}"
COMPOSE_FILE="${CVAT_REPO_DIR}/docker-compose.yml"

if [ ! -d "${CVAT_REPO_DIR}/.git" ]; then
  git clone --depth 1 --branch "${CVAT_VERSION}" https://github.com/cvat-ai/cvat.git "${CVAT_REPO_DIR}"
fi

cleanup() {
  docker compose --project-directory "${CVAT_REPO_DIR}" -f "${COMPOSE_FILE}" down
}

trap cleanup INT TERM EXIT

CVAT_VERSION="${CVAT_VERSION}" docker compose --project-directory "${CVAT_REPO_DIR}" -f "${COMPOSE_FILE}" up


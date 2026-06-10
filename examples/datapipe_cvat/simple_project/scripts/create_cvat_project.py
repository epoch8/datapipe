#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys

CVAT_LABELS = ["cat", "dog"]
DEFAULT_PROJECT_NAME = "datapipe-cvat-example"


def create_cvat_project(*, name: str = DEFAULT_PROJECT_NAME) -> int:
    from cvat_sdk import Client as CVATClient
    from cvat_sdk.models import PatchedLabelRequest, ProjectWriteRequest

    url = os.environ.get("CVAT_URL", "http://localhost:8080")
    username = os.environ.get("CVAT_USERNAME", "admin")
    password = os.environ.get("CVAT_PASSWORD", "admin")
    organization = os.environ.get("CVAT_ORGANIZATION", "")

    client = CVATClient(url)
    client.organization_slug = organization
    client.login(credentials=(username, password))

    for project in client.projects.list():
        if project.name == name:
            print(f"CVAT project already exists: id={project.id} name={project.name!r}", file=sys.stderr)
            return int(project.id)

    project = client.projects.create(
        ProjectWriteRequest(
            name=name,
            labels=[PatchedLabelRequest(name=label) for label in CVAT_LABELS],
        )
    )
    print(f"Created CVAT project: id={project.id} name={project.name!r}", file=sys.stderr)
    return int(project.id)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create a CVAT project for the simple_project example (or reuse an existing one)."
    )
    parser.add_argument("--project-name", default=DEFAULT_PROJECT_NAME, help="CVAT project display name")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    project_id = create_cvat_project(name=args.project_name)
    print(project_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

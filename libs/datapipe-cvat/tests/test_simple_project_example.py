from __future__ import annotations

import importlib.util
import os
import sys
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

import pandas as pd
import pytest
from cvat_sdk.models import LabeledDataRequest, LabeledShapeRequest
from datapipe.compute import run_steps
from datapipe_cvat.cvat_step import create_cvat_client
from PIL import Image

from test_cvat_integration import _require_cvat


pytestmark = pytest.mark.cvat


@contextmanager
def _cwd(path: Path):
    old_cwd = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(old_cwd)


def _load_module(module_path: Path, module_name: str):
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot import {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _write_sample_input(project_dir: Path, count: int = 5) -> None:
    input_dir = project_dir / "input"
    annotations_dir = input_dir / "annotations"
    annotations_dir.mkdir(parents=True)

    for idx in range(count):
        image_id = f"image_{idx}"
        Image.new("RGB", (100 + idx, 80 + idx), color=(255, 255, 255)).save(input_dir / f"{image_id}.jpg")
        (annotations_dir / f"{image_id}.xml").write_text("<image></image>", encoding="utf-8")


def _create_example_project(repo_root: Path, project_name: str) -> int:
    script_path = repo_root / "examples" / "datapipe_cvat" / "simple_project" / "scripts" / "create_cvat_project.py"
    module = _load_module(script_path, f"create_cvat_project_{uuid4().hex}")
    return int(module.create_cvat_project(name=project_name))


def test_simple_project_roundtrips_completed_cvat_annotations(tmp_dir, cvat_url, cvat_credentials, monkeypatch):
    _require_cvat(cvat_url)

    repo_root = Path(__file__).resolve().parents[3]
    example_dir = repo_root / "examples" / "datapipe_cvat" / "simple_project"
    project_name = f"datapipe-cvat-simple-project-test-{uuid4().hex}"

    monkeypatch.setenv("CVAT_URL", cvat_url)
    monkeypatch.setenv("CVAT_USERNAME", cvat_credentials[0])
    monkeypatch.setenv("CVAT_PASSWORD", cvat_credentials[1])
    monkeypatch.setenv("CVAT_ORGANIZATION", "")

    project_id = _create_example_project(repo_root, project_name)
    monkeypatch.setenv("CVAT_PROJECT_ID", str(project_id))

    client = create_cvat_client(cvat_url, "", cvat_credentials)
    try:
        _write_sample_input(tmp_dir)

        with _cwd(tmp_dir):
            app_module = _load_module(example_dir / "app.py", f"simple_cvat_app_{uuid4().hex}")
            app = app_module.app
            app.ds.meta_dbconn.sqla_metadata.create_all(app.ds.meta_dbconn.con)

            run_steps(app.ds, app.steps)

            project = client.projects.retrieve(project_id)
            tasks = project.get_tasks()
            assert len(tasks) == 1
            task = tasks[0]
            assert task.name.endswith("TaskQueue:queue1 batch:0")
            assert len(app.ds.get_table("cvat_images").get_data()) == 5

            labels_by_name = {label.name: label.id for label in task.get_labels()}
            cvat_files = app.ds.get_table("cvat_images").get_data().sort_values("inner_frame_id")
            shapes = [
                LabeledShapeRequest(
                    type="rectangle",
                    label_id=labels_by_name["cat" if idx % 2 == 0 else "dog"],
                    frame=int(row["inner_frame_id"]),
                    points=[10 + idx, 5 + idx, 40 + idx, 35 + idx],
                    occluded=False,
                    outside=False,
                    z_order=0,
                )
                for idx, (_, row) in enumerate(cvat_files.iterrows())
            ]
            task.set_annotations(LabeledDataRequest(version=0, tags=[], shapes=shapes, tracks=[]))
            task.get_jobs()[0].update({"state": "completed"})

            run_steps(app.ds, app.steps)

            annotations = app.ds.get_table("cvat_annotation").get_data().sort_values("image_id")
            assert len(annotations) == 5
            assert set(annotations["image_id"]) == {f"image_{idx}" for idx in range(5)}
            assert annotations["annotations"].str.contains("<box ").all()

            sync = app.ds.get_table("cvat_task_sync_table").get_data()
            assert sync["cvat_job__status"].tolist() == ["completed"]
    finally:
        project = client.projects.retrieve(project_id)
        for task in project.get_tasks():
            client.tasks.remove_by_ids([task.id])
        client.projects.remove_by_ids([project_id])

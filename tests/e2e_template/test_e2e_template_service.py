from __future__ import annotations

import os
import subprocess
import sys
import logging
from pathlib import Path

import pytest
from label_studio_sdk import LabelStudio

from datapipe_label_studio.sdk_utils import get_project_by_title, get_tasks_iter

pytestmark = [
    pytest.mark.e2e_examples,
    pytest.mark.e2e_template,
    pytest.mark.service_e2e,
]


def _ensure_label_studio_token() -> None:
    e2e_dir = Path(__file__).resolve().parents[2] / "examples" / "e2e_template"
    token = subprocess.check_output(
        [sys.executable, str(e2e_dir / "scripts/label_studio_token.py")],
        cwd=e2e_dir,
        env=os.environ.copy(),
        text=True,
    ).strip()
    os.environ["LABEL_STUDIO_API_KEY"] = token


def _require_service_env() -> None:
    missing = [
        name
        for name in [
            "DB_URL",
            "LABEL_STUDIO_URL",
            "LABEL_STUDIO_API_KEY",
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "DATAPIPE_E2E_DIR",
            "S3_ENDPOINT_URL",
        ]
        if not os.environ.get(name)
    ]
    if missing:
        pytest.skip(f"service e2e env is not configured: {', '.join(missing)}")


def _ensure_sample_images_seeded() -> None:
    e2e_dir = Path(__file__).resolve().parents[2] / "examples/e2e_template"
    seed_script = e2e_dir / "scripts/seed_sample_data.py"
    sample_dir = e2e_dir / "sample_data" / "images"
    cmd = [
        sys.executable,
        str(seed_script),
        "--detection-limit",
        "2",
        "--keypoints-limit",
        "2",
    ]
    if sample_dir.exists() and any(sample_dir.glob("*.jpg")):
        cmd.append("--skip-download")
    subprocess.run(cmd, check=True, cwd=e2e_dir, env=os.environ.copy())


def _ls_client() -> LabelStudio:
    return LabelStudio(base_url=os.environ["LABEL_STUDIO_URL"], api_key=os.environ["LABEL_STUDIO_API_KEY"])


def _annotation_result(pipeline_name: str, idx: int) -> list[dict]:
    if pipeline_name == "detection":
        return [
            {
                "id": f"bbox-{idx}",
                "from_name": "label",
                "to_name": "image",
                "type": "rectanglelabels",
                "original_width": 100,
                "original_height": 100,
                "value": {
                    "x": 10 + idx,
                    "y": 12 + idx,
                    "width": 20,
                    "height": 24,
                    "rotation": 0,
                    "rectanglelabels": ["Cat" if idx % 2 == 0 else "Dog"],
                },
            }
        ]

    bbox_id = f"bbox-{idx}"
    return [
        {
            "id": bbox_id,
            "from_name": "bbox",
            "to_name": "image",
            "type": "rectanglelabels",
            "original_width": 100,
            "original_height": 100,
            "value": {
                "x": 10 + idx,
                "y": 12 + idx,
                "width": 20,
                "height": 24,
                "rotation": 0,
                "rectanglelabels": ["person"],
            },
        },
        {
            "id": f"kp-{idx}",
            "from_name": "kp",
            "to_name": "image",
            "type": "keypointlabels",
            "parentID": bbox_id,
            "original_width": 100,
            "original_height": 100,
            "value": {
                "x": 15 + idx,
                "y": 18 + idx,
                "rotation": 0,
                "keypointlabels": ["nose"],
            },
        },
    ]


def _annotate_uploaded_tasks(pipeline_name: str, app) -> None:
    from helpers import load_template_module

    config = load_template_module(pipeline_name, "config")
    client = _ls_client()
    project = get_project_by_title(client, config.PROJECT_NAME)
    assert project is not None
    project_id = int(project["id"])

    ls_task = app.ds.get_table("ls_task").get_data().sort_values("image_name").reset_index(drop=True)
    assert not ls_task.empty
    expected_task_ids = set(ls_task["task_id"].astype(int))
    tasks_by_id = {}
    for tasks_page in get_tasks_iter(client, project_id, page_size=200):
        for task in tasks_page:
            if task["id"] in expected_task_ids:
                tasks_by_id[task["id"]] = task
    assert set(tasks_by_id) == expected_task_ids

    for idx, row in ls_task.iterrows():
        task_id = int(row["task_id"])
        if tasks_by_id[task_id].get("annotations"):
            continue
        client.annotations.create(
            id=task_id,
            result=_annotation_result(pipeline_name, idx),
            was_cancelled=False,
            task=task_id,
        )

    # Sanity check against the service: all current pipeline tasks now have annotations.
    annotated_task_ids = set()
    for tasks_page in get_tasks_iter(client, project_id, page_size=200):
        for task in tasks_page:
            if task["id"] in expected_task_ids:
                assert task.get("annotations")
                annotated_task_ids.add(task["id"])
    assert annotated_task_ids == expected_task_ids


def _assert_roundtripped_annotations(pipeline_name: str, app) -> None:
    ls_task = app.ds.get_table("ls_task").get_data()
    ls_annotations = app.ds.get_table("ls_annotations").get_data()
    ground_truth = app.ds.get_table("image__ground_truth").get_data()

    assert len(ls_annotations) == len(ls_task)
    assert len(ground_truth) == len(ls_task)
    assert ls_annotations["annotations"].apply(lambda annotations: len(annotations) == 1).all()
    assert ground_truth["bboxes"].apply(bool).all()
    assert ground_truth["labels"].apply(bool).all()
    if pipeline_name == "keypoints":
        assert ground_truth["keypoints"].apply(bool).all()

    assert set(ls_annotations["image_name"]) == set(ls_task["image_name"])
    assert set(ground_truth["image_name"]) == set(ls_task["image_name"])


def _frozen_dataset_table_name(pipeline_name: str) -> str:
    return f"{pipeline_name}_frozen_dataset"


def _trained_model_table_name(pipeline_name: str) -> str:
    return f"{pipeline_name}_model_train"


def _freeze_min_delta(pipeline_name: str, app) -> int:
    frozen_dataset_table = _frozen_dataset_table_name(pipeline_name)
    for step in app.pipeline.steps:
        if getattr(step, f"output__{frozen_dataset_table}", None) == frozen_dataset_table:
            return int(step.min_delta)
    raise AssertionError(f"Cannot find freeze step for {pipeline_name}")


def test_detection_template_annotation_stage():
    from helpers import run_template_stage

    _require_service_env()
    _ensure_label_studio_token()
    _ensure_sample_images_seeded()

    app = run_template_stage("detection", "annotation")

    assert not app.ds.get_table("s3_images").get_data().empty
    assert not app.ds.get_table("detection_model").get_data().empty
    assert not app.ds.get_table("detection_predictions").get_data().empty
    assert not app.ds.get_table("images_with_predictions").get_data().empty
    assert not app.ds.get_table("ls_task").get_data().empty


@pytest.mark.parametrize("pipeline_name", ["detection", "keypoints"])
def test_template_annotation_roundtrip_from_label_studio(pipeline_name: str):
    from helpers import run_template_stage

    _require_service_env()
    _ensure_label_studio_token()
    _ensure_sample_images_seeded()

    app = run_template_stage(pipeline_name, "annotation")
    assert not app.ds.get_table("ls_task").get_data().empty

    _annotate_uploaded_tasks(pipeline_name, app)

    app = run_template_stage(pipeline_name, "annotation")
    _assert_roundtripped_annotations(pipeline_name, app)


@pytest.mark.parametrize("pipeline_name", ["detection", "keypoints"])
def test_template_training_waits_for_freeze_min_delta_annotations(pipeline_name: str, caplog):
    from helpers import run_template_stage

    caplog.set_level(logging.ERROR)
    _require_service_env()
    _ensure_label_studio_token()
    _ensure_sample_images_seeded()

    app = run_template_stage(pipeline_name, "annotation")
    frozen_dataset_table = _frozen_dataset_table_name(pipeline_name)
    trained_model_table = _trained_model_table_name(pipeline_name)
    freeze_min_delta = _freeze_min_delta(pipeline_name, app)
    initial_annotations_count = len(app.ds.get_table("image__ground_truth").get_data())

    caplog.clear()
    run_template_stage(pipeline_name, "train-prepare")
    assert f"Not enough changed idx for freezing ({initial_annotations_count} < {freeze_min_delta})" in caplog.text
    assert app.ds.get_table(frozen_dataset_table).get_data().empty
    assert app.ds.get_table(trained_model_table).get_data().empty

    _annotate_uploaded_tasks(pipeline_name, app)
    app = run_template_stage(pipeline_name, "annotation")
    _assert_roundtripped_annotations(pipeline_name, app)
    synced_annotations_count = len(app.ds.get_table("image__ground_truth").get_data())
    assert synced_annotations_count < freeze_min_delta

    # CI seeds fewer annotated images than the freeze step requires, so training must stay blocked.
    caplog.clear()
    run_template_stage(pipeline_name, "train-prepare")
    assert f"Not enough changed idx for freezing ({synced_annotations_count} < {freeze_min_delta})" in caplog.text
    assert app.ds.get_table(frozen_dataset_table).get_data().empty
    assert app.ds.get_table(trained_model_table).get_data().empty


def test_keypoints_template_annotation_stage():
    from helpers import run_template_stage

    _require_service_env()
    _ensure_label_studio_token()
    _ensure_sample_images_seeded()

    app = run_template_stage("keypoints", "annotation")

    assert not app.ds.get_table("s3_images").get_data().empty
    assert not app.ds.get_table("keypoints_model").get_data().empty
    assert not app.ds.get_table("keypoints_predictions").get_data().empty
    assert not app.ds.get_table("images_with_predictions").get_data().empty
    assert not app.ds.get_table("ls_task").get_data().empty

from __future__ import annotations

import os
import subprocess
import sys
import logging
from pathlib import Path

import pytest
from label_studio_sdk import LabelStudio

from datapipe_label_studio.sdk_utils import get_project_by_title, get_tasks_iter, project_to_dict

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
                "rectanglelabels": ["Person"],
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


def _expected_model_version(pipeline_name: str) -> str:
    from helpers import load_template_module

    config = load_template_module(pipeline_name, "config")
    if pipeline_name == "detection":
        return config.DETECTION_MODEL_CONFIG["detection_model_id"]
    return config.KEYPOINTS_MODEL_CONFIG["keypoints_model_id"]


def _assert_prelabeling_enabled(pipeline_name: str, app) -> None:
    from helpers import load_template_module

    expected_model_version = _expected_model_version(pipeline_name)

    current_model_version = app.ds.get_table("ls_current_model_version").get_data()
    assert not current_model_version.empty
    assert set(current_model_version["model_version"]) == {expected_model_version}

    config = load_template_module(pipeline_name, "config")
    client = _ls_client()
    project = get_project_by_title(client, config.PROJECT_NAME)
    assert project is not None
    assert set(current_model_version["project_id"]) == {int(project["id"])}

    project_settings = project_to_dict(client.projects.get(id=int(project["id"])))
    assert project_settings.get("show_collab_predictions") is True
    assert project_settings.get("model_version") == expected_model_version


def _training_blocked_tables(pipeline_name: str) -> tuple[str, str, str]:
    if pipeline_name == "detection":
        return (
            "detection_frozen_dataset",
            "detection_model_is_trained_on_detection_frozen_dataset",
            "detection_training_status",
        )
    return (
        "keypoints_frozen_dataset",
        "keypoints_model_is_trained_on_keypoints_frozen_dataset",
        "keypoints_training_status",
    )


def _assert_training_still_blocked(pipeline_name: str, app) -> None:
    frozen_dataset_table, trained_on_table, training_status_table = _training_blocked_tables(pipeline_name)
    assert app.ds.get_table(frozen_dataset_table).get_data().empty
    assert app.ds.get_table(trained_on_table).get_data().empty
    assert app.ds.get_table(training_status_table).get_data().empty


def _freeze_min_delta(pipeline_name: str, app) -> int:
    from datapipe_ml.tasks.detection.freeze import DetectionFreezeDataset
    from datapipe_ml.tasks.keypoints.freeze import KeypointsFreezeDataset

    freeze_step_types = {
        "detection": DetectionFreezeDataset,
        "keypoints": KeypointsFreezeDataset,
    }
    step_type = freeze_step_types[pipeline_name]
    for step in app.pipeline.steps:
        if isinstance(step, step_type):
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
    assert not app.ds.get_table("ls_detection_prediction").get_data().empty
    assert not app.ds.get_table("images_with_predictions").get_data().empty
    assert not app.ds.get_table("ls_task").get_data().empty
    _assert_prelabeling_enabled("detection", app)


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
def test_template_training_waits_for_freeze_min_delta_annotations(pipeline_name: str):
    from helpers import run_template_stage

    _require_service_env()
    _ensure_label_studio_token()
    _ensure_sample_images_seeded()

    app = run_template_stage(pipeline_name, "annotation")
    freeze_min_delta = _freeze_min_delta(pipeline_name, app)

    # Freeze step logs and swallows ValueError when min_delta is not met.
    app = run_template_stage(pipeline_name, "train-prepare")
    _assert_training_still_blocked(pipeline_name, app)

    _annotate_uploaded_tasks(pipeline_name, app)
    app = run_template_stage(pipeline_name, "annotation")
    _assert_roundtripped_annotations(pipeline_name, app)
    synced_annotations_count = len(app.ds.get_table("image__ground_truth").get_data())
    assert synced_annotations_count < freeze_min_delta

    app = run_template_stage(pipeline_name, "train-prepare")
    _assert_training_still_blocked(pipeline_name, app)


def test_keypoints_template_annotation_stage():
    from helpers import run_template_stage

    _require_service_env()
    _ensure_label_studio_token()
    _ensure_sample_images_seeded()

    app = run_template_stage("keypoints", "annotation")

    assert not app.ds.get_table("s3_images").get_data().empty
    assert not app.ds.get_table("keypoints_model").get_data().empty
    assert not app.ds.get_table("ls_keypoints_prediction").get_data().empty
    assert not app.ds.get_table("images_with_predictions").get_data().empty
    assert not app.ds.get_table("ls_task").get_data().empty
    _assert_prelabeling_enabled("keypoints", app)

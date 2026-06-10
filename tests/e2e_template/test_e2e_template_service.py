from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

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
            "S3_BUCKET",
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

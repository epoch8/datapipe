from __future__ import annotations

import pytest

from datapipe_ml.training.resume import select_resume_checkpoint
from datapipe_ml.training.specs import TrainingResumeConfig
from datapipe_ml.training.sync import write_checkpoint_manifest
from tests.helpers.cloud_smoke import (
    CLOUD_SMOKE_CASE_PARAMS,
    cloud_smoke_artifact,
    cloud_smoke_runtime_kwargs,
    cloud_smoke_status_table,
    cloud_smoke_steps,
)
from tests.helpers.cloud_storage import (
    assert_model_path_under_working_dir,
    assert_url_exists,
    cloud_working_dir,
    join_cloud_path,
    s3_base_url,
    s3_storage_options,
    upload_local_file,
)
from tests.helpers.failure_injection import (
    FAIL_AFTER_EPOCH_ENV,
    FAIL_MODE_ENV,
    training_failure_hooks,
)
from tests.helpers.training_recovery import (
    REAL_RECOVERY_CASE_PARAMS,
    assert_status_manifest,
    make_recovery_runtime,
    recovery_case_by_id,
)
from tests.helpers.training_smoke import (
    assert_completed_training_status_with_manifest,
    assert_model_artifact,
    assert_training_uses_architecture_label,
    copy_ultralytics_preset_checkpoint,
    detection_freeze_step,
    detection_train_step_with_local_checkpoint,
    make_cloud_runtime,
    run_pipeline,
)

import fsspec

pytestmark = [pytest.mark.cloud_storage, pytest.mark.service_e2e]


def test_s3_minio_manifest_resume_checkpoint_roundtrip() -> None:
    base = s3_base_url()
    storage_options = s3_storage_options()
    run_pathy = base / "model-a"
    checkpoint_pathy = run_pathy / "weights" / "epoch1.pt"
    fs, checkpoint_key = fsspec.core.url_to_fs(str(checkpoint_pathy), **storage_options)
    fs.makedirs(str(checkpoint_pathy.parent), exist_ok=True)
    with fs.open(checkpoint_key, "wb") as out:
        out.write(b"checkpoint")

    manifest_path = write_checkpoint_manifest(
        run_dir=str(run_pathy),
        model_id="model-a",
        checkpoint_paths=[str(checkpoint_pathy)],
    )

    selected = select_resume_checkpoint(
        manifest_path=manifest_path,
        config=TrainingResumeConfig(continue_train_failed_models=True, min_completed_epochs=1),
    )

    assert selected is not None
    assert selected.path == str(checkpoint_pathy)


@pytest.mark.parametrize("case_id", CLOUD_SMOKE_CASE_PARAMS)
def test_cloud_working_dir_task_smoke(tmp_path, case_id: str) -> None:
    if case_id == "tensorflow_classification":
        pytest.importorskip("tensorflow")
    runtime, workdir = make_cloud_runtime(tmp_path, case_id, **cloud_smoke_runtime_kwargs(case_id))
    steps = cloud_smoke_steps(case_id, workdir, tmp_path)
    run_pipeline(runtime, steps)

    table_name, type_column, path_column, expected_type = cloud_smoke_artifact(case_id)
    assert_model_artifact(runtime, table_name, type_column, path_column, expected_type)
    assert_completed_training_status_with_manifest(runtime, cloud_smoke_status_table(case_id))

    df_model = runtime.ds.get_table(table_name).get_data()
    assert_model_path_under_working_dir(str(df_model[path_column].iloc[0]), workdir)


@pytest.mark.parametrize("case_id", REAL_RECOVERY_CASE_PARAMS)
def test_cloud_working_dir_recovery_writes_status_and_manifest(tmp_path, case_id: str) -> None:
    if case_id == "tensorflow_classification":
        pytest.importorskip("tensorflow")
    case = recovery_case_by_id(case_id)
    workdir = str(cloud_working_dir("recovery", case_id))
    runtime, steps = make_recovery_runtime(tmp_path, case, working_dir=workdir)
    run_pipeline(runtime, steps)

    assert_status_manifest(runtime, case, status="completed")
    df_model = runtime.ds.get_table(case.model_table).get_data()
    assert_model_path_under_working_dir(str(df_model[f"{case.model_table}__model_path"].iloc[0]), workdir)


@pytest.mark.torch
@pytest.mark.training
@pytest.mark.slow
def test_cloud_working_dir_training_with_cloud_preset_checkpoint(tmp_path) -> None:
    runtime, workdir = make_cloud_runtime(tmp_path, "yolov8-detection-cloud-preset")
    run_pipeline(
        runtime,
        [
            detection_freeze_step(workdir),
            detection_train_step_with_local_checkpoint(
                workdir,
                "yolo11n.pt",
                local_scratch=tmp_path,
            ),
        ],
    )

    assert_model_artifact(
        runtime,
        "detection_model",
        "detection_model__type",
        "detection_model__model_path",
        "yolov8",
    )
    assert_training_uses_architecture_label(
        runtime,
        table_name="detection_model",
        model_id_column="detection_model_id",
        model_path_column="detection_model__model_path",
        architecture="yolo11n",
    )
    df_model = runtime.ds.get_table("detection_model").get_data()
    assert_model_path_under_working_dir(str(df_model["detection_model__model_path"].iloc[0]), workdir)
    assert_url_exists(str(join_cloud_path(workdir, "custom_pretrained.pt")))


@pytest.mark.torch
@pytest.mark.training
@pytest.mark.slow
@pytest.mark.usefixtures("training_failure_hooks")
def test_cloud_working_dir_resume_after_failure_from_cloud_checkpoint(tmp_path, monkeypatch) -> None:
    case = recovery_case_by_id("yolov8_detection")
    workdir = str(cloud_working_dir("resume", "yolov8_detection"))
    runtime, steps = make_recovery_runtime(tmp_path, case, working_dir=workdir)
    monkeypatch.setenv(FAIL_AFTER_EPOCH_ENV, "1")
    monkeypatch.setenv(FAIL_MODE_ENV, "error")

    try:
        run_pipeline(runtime, steps)
    except Exception:
        pass

    failed = assert_status_manifest(runtime, case, status="failed")
    failed_model_id = failed[case.model_id_column]
    monkeypatch.delenv(FAIL_AFTER_EPOCH_ENV)
    monkeypatch.delenv(FAIL_MODE_ENV)

    run_pipeline(runtime, steps[1:])

    completed = assert_status_manifest(runtime, case, status="completed")
    assert completed[case.model_id_column] == failed_model_id
    df_model = runtime.ds.get_table(case.model_table).get_data()
    assert_model_path_under_working_dir(str(df_model["detection_model__model_path"].iloc[0]), workdir)


def test_cloud_preset_upload_roundtrip(tmp_path) -> None:
    workdir = str(cloud_working_dir("preset-roundtrip"))
    preset_path = copy_ultralytics_preset_checkpoint(workdir, "yolo11n.pt", local_scratch=tmp_path)
    assert str(preset_path).startswith(workdir)
    assert_url_exists(str(preset_path))

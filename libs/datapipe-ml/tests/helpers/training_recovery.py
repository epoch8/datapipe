from __future__ import annotations

import os
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Iterable, Optional

import pandas as pd
import pytest
from datapipe.compute import Pipeline, PipelineStep, build_compute
from datapipe.types import IndexDF

from datapipe_ml.training.runs import active_lease
from datapipe_ml.training.specs import TrainingResumeConfig, TrainingSyncConfig
from datapipe_ml.training.sync import manifest_path_for_run, read_checkpoint_manifest, verify_manifest_checkpoint
from tests.helpers.training_smoke import (
    SmokeRuntime,
    Workdir,
    detection_freeze_step,
    detection_train_step,
    detection_yolov5_train_step,
    keypoints_freeze_step,
    keypoints_train_step,
    make_runtime,
    run_pipeline,
    segmentation_freeze_step,
    segmentation_train_step,
)

if TYPE_CHECKING:
    from datapipe_ml.tasks.detection.train.yolov5 import Train_YoloV5_DetectionModel
    from datapipe_ml.tasks.detection.train.yolov8 import Train_YoloV8_DetectionModel
    from datapipe_ml.tasks.keypoints.train.yolov8 import Train_YoloV8_KeypointsModel
    from datapipe_ml.tasks.segmentation.train.yolov8 import Train_YoloV8_SegmentationModel

    RecoveryTrainStep = (
        Train_YoloV8_DetectionModel
        | Train_YoloV5_DetectionModel
        | Train_YoloV8_SegmentationModel
        | Train_YoloV8_KeypointsModel
    )
else:
    RecoveryTrainStep = Any

TORCH_RECOVERY_CASE_IDS = (
    "yolov8_detection",
    "yolov5_detection",
    "yolov8_segmentation",
    "yolov8_keypoints",
)

TENSORFLOW_RECOVERY_CASE_IDS = ("tensorflow_classification",)

REAL_RECOVERY_CASE_PARAMS = [
    *[pytest.param(case_id, marks=pytest.mark.torch) for case_id in TORCH_RECOVERY_CASE_IDS],
    pytest.param("tensorflow_classification", marks=pytest.mark.tensorflow),
]

_LINK_TABLE_BY_MODE = dict(
    detection="detection_model_link",
    segmentation="segmentation_model_link",
    keypoints="keypoints_model_link",
)


@dataclass(frozen=True)
class RealRecoveryCase:
    id: str
    mode: str
    status_table: str
    model_table: str
    model_id_column: str
    models_subdir: str
    make_runtime_kwargs: dict
    steps_factory: Callable[[Workdir, Path], list]
    train_fn: Callable
    input_tables: tuple[str, ...]


StepConfigureFn = Callable[[RecoveryTrainStep, int], None]
TrainKwargsFn = Callable[[SmokeRuntime, "RealRecoveryCase", RecoveryTrainStep], dict[str, object]]


def _torch_case(
    case_id: str,
    *,
    mode: str,
    status_table: str,
    model_table: str,
    model_id_column: str,
    models_subdir: str,
    steps_factory: Callable[[Workdir, Path], list],
    train_fn: Callable,
    input_tables: tuple[str, ...],
    make_runtime_kwargs: dict | None = None,
) -> pytest.ParameterSet:
    return pytest.param(
        RealRecoveryCase(
            id=case_id,
            mode=mode,
            status_table=status_table,
            model_table=model_table,
            model_id_column=model_id_column,
            models_subdir=models_subdir,
            make_runtime_kwargs=make_runtime_kwargs or dict(),
            steps_factory=steps_factory,
            train_fn=train_fn,
            input_tables=input_tables,
        ),
        id=case_id,
        marks=(pytest.mark.torch,),
    )


def real_recovery_torch_cases() -> list:
    from datapipe_ml.tasks.detection.train.yolov5 import train_yolov5
    from datapipe_ml.tasks.detection.train.yolov8 import train_yolov8
    from datapipe_ml.tasks.keypoints.train.yolov8 import train_yolov8_keypoints
    from datapipe_ml.tasks.segmentation.train.yolov8 import train_yolov8_segmentation

    return [
        _torch_case(
            "yolov8_detection",
            mode="detection",
            status_table="detection_training_status",
            model_table="detection_model",
            model_id_column="detection_model_id",
            models_subdir="models",
            steps_factory=lambda workdir, scratch: [
                detection_freeze_step(workdir),
                detection_train_step(
                    workdir,
                    local_scratch=scratch,
                ),
            ],
            train_fn=train_yolov8,
            input_tables=(
                "detection_frozen_dataset",
                "yolov8_train_config",
                "yolov8_detection_class_names",
                "yolov8_detection_resized_image_file",
                "yolov8_detection_yolo_txt",
            ),
        ),
        _torch_case(
            "yolov5_detection",
            mode="detection",
            status_table="detection_training_status",
            model_table="detection_model",
            model_id_column="detection_model_id",
            models_subdir="models",
            steps_factory=lambda workdir, scratch: [
                detection_freeze_step(workdir),
                detection_yolov5_train_step(
                    workdir,
                    local_scratch=scratch,
                ),
            ],
            train_fn=train_yolov5,
            input_tables=(
                "detection_frozen_dataset",
                "yolov5_train_config",
                "yolov5_detection_class_names",
                "yolov5_detection_resized_image_file",
                "yolov5_detection_yolo_txt",
            ),
        ),
        _torch_case(
            "yolov8_segmentation",
            mode="segmentation",
            status_table="segmentation_training_status",
            model_table="segmentation_model",
            model_id_column="segmentation_model_id",
            models_subdir="segmentation_models",
            steps_factory=lambda workdir, scratch: [
                segmentation_freeze_step(workdir),
                segmentation_train_step(
                    workdir,
                    local_scratch=scratch,
                ),
            ],
            train_fn=train_yolov8_segmentation,
            input_tables=(
                "segmentation_frozen_dataset",
                "segmentation_yolov8_train_config",
                "segmentation_class_names",
                "segmentation_resized_image_file",
                "segmentation_yolo_txt",
            ),
        ),
        _torch_case(
            "yolov8_keypoints",
            mode="keypoints",
            status_table="keypoints_training_status",
            model_table="keypoints_model",
            model_id_column="keypoints_model_id",
            models_subdir="keypoints_models",
            steps_factory=lambda workdir, scratch: [
                keypoints_freeze_step(workdir),
                keypoints_train_step(
                    workdir,
                    local_scratch=scratch,
                ),
            ],
            train_fn=train_yolov8_keypoints,
            input_tables=(
                "keypoints_frozen_dataset",
                "keypoints_yolov8_train_config",
                "keypoints_class_names",
                "keypoints_resized_image_file",
                "keypoints_yolo_txt",
            ),
            make_runtime_kwargs=dict(include_keypoints_gt=True),
        ),
    ]


def real_recovery_cases() -> list:
    return real_recovery_torch_cases()


def recovery_case_by_id(case_id: str) -> RealRecoveryCase:
    if case_id in TENSORFLOW_RECOVERY_CASE_IDS:
        from tests.helpers.training_recovery_tensorflow import recovery_tensorflow_case_by_id

        return recovery_tensorflow_case_by_id(case_id)
    for param in real_recovery_torch_cases():
        case = param.values[0]
        if case.id == case_id:
            return case
    raise KeyError(case_id)


def recovery_extra_step_configure(case: RealRecoveryCase) -> dict[type, StepConfigureFn] | None:
    if case.id in TENSORFLOW_RECOVERY_CASE_IDS:
        from tests.helpers.training_recovery_tensorflow import tensorflow_step_configure

        return tensorflow_step_configure()
    return None


def yolov8_detection_recovery_case() -> RealRecoveryCase:
    return recovery_case_by_id("yolov8_detection")


def _set_epoch_training_configs(configs: Iterable[object], epochs: int) -> None:
    for config in configs:
        config.epochs = epochs
        config.save_period = 1
        config.patience = epochs


def _configure_yolov8_step(step: RecoveryTrainStep, epochs: int) -> None:
    _set_epoch_training_configs(step.yolov8_train_configs, epochs)


def _configure_yolov5_step(step: RecoveryTrainStep, epochs: int) -> None:
    _set_epoch_training_configs(step.yolov5_train_configs, epochs)


def _recovery_step_configure() -> dict[type, StepConfigureFn]:
    from datapipe_ml.tasks.detection.train.yolov5 import Train_YoloV5_DetectionModel
    from datapipe_ml.tasks.detection.train.yolov8 import Train_YoloV8_DetectionModel
    from datapipe_ml.tasks.keypoints.train.yolov8 import Train_YoloV8_KeypointsModel
    from datapipe_ml.tasks.segmentation.train.yolov8 import Train_YoloV8_SegmentationModel

    return {
        Train_YoloV8_DetectionModel: _configure_yolov8_step,
        Train_YoloV8_SegmentationModel: _configure_yolov8_step,
        Train_YoloV8_KeypointsModel: _configure_yolov8_step,
        Train_YoloV5_DetectionModel: _configure_yolov5_step,
    }


def _set_recovery_sync_and_resume(
    step: RecoveryTrainStep,
    *,
    resume_enabled: bool,
    sync_interval_s: Optional[int],
    reset_attempts_after: str = "1s",
) -> None:
    step.sync_config = TrainingSyncConfig(enabled=True, interval_s=sync_interval_s, retries=1, retry_sleep_s=0)
    step.resume_config = TrainingResumeConfig(
        continue_train_failed_models=resume_enabled,
        min_completed_epochs=1,
        max_attempts=3,
        reset_attempts_after=reset_attempts_after,
    )


def configure_recovery_steps(
    steps: Iterable[PipelineStep],
    *,
    epochs: int = 10,
    resume_enabled: bool = True,
    sync_interval_s: Optional[int] = 1,
    reset_attempts_after: str = "1s",
    extra_configure: dict[type, StepConfigureFn] | None = None,
    include_torch_configure: bool = True,
) -> list[PipelineStep]:
    configure_by_type = {
        **(_recovery_step_configure() if include_torch_configure else dict()),
        **(extra_configure or dict()),
    }
    steps = list(steps)
    for step in steps:
        configure = configure_by_type.get(type(step))
        if configure is None:
            continue
        configure(step, epochs)
        _set_recovery_sync_and_resume(
            step,
            resume_enabled=resume_enabled,
            sync_interval_s=sync_interval_s,
            reset_attempts_after=reset_attempts_after,
        )
    return steps


def make_recovery_runtime(
    tmp_path: Path, case: RealRecoveryCase, *, working_dir: Workdir | None = None
) -> tuple[SmokeRuntime, list]:
    if case.id in TENSORFLOW_RECOVERY_CASE_IDS:
        from tests.helpers.training_recovery_tensorflow import make_recovery_runtime as make_tf_recovery_runtime

        return make_tf_recovery_runtime(tmp_path, case, working_dir=working_dir)
    workdir = working_dir if working_dir is not None else tmp_path
    runtime = make_runtime(tmp_path, working_dir=workdir, **case.make_runtime_kwargs)
    steps = configure_recovery_steps(case.steps_factory(workdir, tmp_path))
    return runtime, steps


def reopen_recovery_runtime(
    tmp_path: Path, case: RealRecoveryCase, *, working_dir: Workdir | None = None
) -> tuple[SmokeRuntime, list]:
    """Reattach to an existing workdir and register pipeline tables for status reads."""
    runtime, steps = make_recovery_runtime(tmp_path, case, working_dir=working_dir)
    build_compute(runtime.ds, runtime.catalog, Pipeline(steps))
    return runtime, steps


def status_rows(runtime: SmokeRuntime, case: RealRecoveryCase) -> pd.DataFrame:
    try:
        return runtime.ds.get_table(case.status_table).get_data()
    except KeyError:
        return pd.DataFrame()


def model_rows(runtime: SmokeRuntime, case: RealRecoveryCase) -> pd.DataFrame:
    try:
        return runtime.ds.get_table(case.model_table).get_data()
    except KeyError:
        return pd.DataFrame()


def assert_status_manifest(runtime: SmokeRuntime, case: RealRecoveryCase, *, status: str = "completed") -> pd.Series:
    rows = status_rows(runtime, case)
    selected = rows[rows["training_status__status"] == status]
    assert len(selected) >= 1
    row = selected.iloc[-1]
    manifest_path = row.get("training_status__manifest_path")
    assert isinstance(manifest_path, str) and manifest_path
    manifest = read_checkpoint_manifest(manifest_path)
    assert manifest is not None
    assert manifest.checkpoints
    assert all(verify_manifest_checkpoint(item) for item in manifest.checkpoints)
    return row


def wait_for_running_status(runtime: SmokeRuntime, case: RealRecoveryCase, *, timeout_s: int = 60) -> pd.Series:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        rows = status_rows(runtime, case)
        running = rows[rows.get("training_status__status", pd.Series(dtype=object)) == "running"]
        if len(running):
            return running.iloc[-1]
        time.sleep(0.5)
    raise AssertionError(f"No running status for {case.id}")


def wait_until_stale_running_lease(
    runtime: SmokeRuntime,
    case: RealRecoveryCase,
    *,
    timeout_s: int = 30,
) -> pd.Series:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        rows = status_rows(runtime, case)
        running = rows[rows.get("training_status__status", pd.Series(dtype=object)) == "running"]
        if len(running):
            row = running.iloc[-1]
            if not active_lease(row):
                return row
        time.sleep(0.5)
    raise AssertionError(f"Stale running lease not observed for {case.id}")


def assert_stale_running_after_pipe_death(
    runtime: SmokeRuntime,
    case: RealRecoveryCase,
    *,
    min_epoch: int = 6,
    timeout_s: int = 30,
) -> pd.Series:
    row = wait_until_stale_running_lease(runtime, case, timeout_s=timeout_s)
    assert row["training_status__status"] == "running"
    run_dir = row.get("training_status__run_dir")
    assert isinstance(run_dir, str) and run_dir
    manifest = read_checkpoint_manifest(manifest_path_for_run(run_dir))
    assert manifest is not None
    epoch_candidates = [item for item in manifest.checkpoints if item.epoch is not None]
    assert epoch_candidates
    assert max(int(item.epoch) for item in epoch_candidates) >= min_epoch
    return row


def run_recovery_in_subprocess(
    workdir: Path,
    case: RealRecoveryCase,
    phase: str,
    *,
    extra_env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    runner = Path(__file__).with_name("recovery_pipe_runner.py")
    package_root = Path(__file__).parents[2]
    cmd = [
        sys.executable,
        str(runner),
        "--workdir",
        str(workdir),
        "--case-id",
        case.id,
        "--phase",
        phase,
    ]
    pythonpath = str(package_root)
    if existing := os.environ.get("PYTHONPATH"):
        pythonpath = f"{pythonpath}{os.pathsep}{existing}"
    env = {**os.environ, "PYTHONPATH": pythonpath, **(extra_env or dict())}
    return subprocess.run(
        cmd,
        cwd=str(package_root),
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


def wait_for_manifest(runtime: SmokeRuntime, case: RealRecoveryCase, *, timeout_s: int = 120):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        rows = status_rows(runtime, case)
        for _, row in rows.iterrows():
            manifest_path = row.get("training_status__manifest_path")
            if isinstance(manifest_path, str) and read_checkpoint_manifest(manifest_path) is not None:
                return read_checkpoint_manifest(manifest_path)
            run_dir = row.get("training_status__run_dir")
            if isinstance(run_dir, str) and run_dir:
                manifest = read_checkpoint_manifest(manifest_path_for_run(run_dir))
                if manifest is not None:
                    return manifest
        time.sleep(1)
    raise AssertionError(f"No manifest for {case.id}")


def run_pipeline_in_thread(runtime: SmokeRuntime, steps: list) -> tuple[threading.Thread, list[BaseException]]:
    errors: list[BaseException] = []

    def target() -> None:
        try:
            run_pipeline(runtime, steps)
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    return thread, errors


def invoke_real_train_callable_for_backfill(
    runtime: SmokeRuntime,
    case: RealRecoveryCase,
    step: RecoveryTrainStep,
) -> None:
    if case.id in TENSORFLOW_RECOVERY_CASE_IDS:
        from tests.helpers.training_recovery_tensorflow import (
            invoke_real_train_callable_for_backfill as invoke_tf_backfill,
        )

        invoke_tf_backfill(runtime, case, step)
        return
    idx = IndexDF(pd.DataFrame([{}]))
    input_dts = [runtime.ds.get_table(name) for name in case.input_tables]
    case.train_fn(runtime.ds, idx, input_dts, kwargs=direct_train_kwargs(runtime, case, step))


def _shared_train_runtime_kwargs(
    runtime: SmokeRuntime,
    case: RealRecoveryCase,
    step: RecoveryTrainStep,
) -> dict[str, object]:
    from pathy import Pathy

    return dict(
        models_dir=str(Pathy.fluid(runtime.workdir) / case.models_subdir),
        max_within_time=step.max_within_time,
        dt__training_status=runtime.ds.get_table(case.status_table),
        tmp_folder=step.tmp_folder,
        model_suffix=step.model_suffix,
        training_launcher_config=step.training_launcher_config,
        sync_config=step.sync_config,
        resume_config=step.resume_config,
    )


def _frozen_dataset_link_table(runtime: SmokeRuntime, case: RealRecoveryCase, task_mode: str):
    link_table = _LINK_TABLE_BY_MODE.get(task_mode)
    if case.mode == task_mode and link_table is not None:
        return runtime.ds.get_table(link_table)
    return runtime.ds.get_table(case.status_table)


def _yolo_train_kwargs(
    runtime: SmokeRuntime,
    case: RealRecoveryCase,
    step: RecoveryTrainStep,
) -> dict[str, object]:
    kwargs = dict(_shared_train_runtime_kwargs(runtime, case, step))
    kwargs.update(
        dict(
            dt__detection_model=runtime.ds.get_table(case.model_table),
            dt__segmentation_model=runtime.ds.get_table(case.model_table),
            dt__keypoints_model=runtime.ds.get_table(case.model_table),
            dt__detection_model_is_trained_on_detection_frozen_dataset=_frozen_dataset_link_table(
                runtime, case, "detection"
            ),
            dt__segm_model_is_trained_on_segm_frozen_dataset=_frozen_dataset_link_table(runtime, case, "segmentation"),
            dt__keypoints_model_is_trained_on_keypoints_frozen_dataset=_frozen_dataset_link_table(
                runtime, case, "keypoints"
            ),
            detection_model_other_primary_keys=list(),
            segmentation_model_other_primary_keys=list(),
            keypoints_model_other_primary_keys=list(),
            detection_model_id__name="detection_model_id",
            segmentation_model_id__name="segmentation_model_id",
            keypoints_model_id__name="keypoints_model_id",
            detection_frozen_dataset_id__name="detection_frozen_dataset_id",
            segmentation_frozen_dataset_id__name="segmentation_frozen_dataset_id",
            keypoints_frozen_dataset_id__name="keypoints_frozen_dataset_id",
            extra_class_names_to_yaml_fields=dict(),
            ignore_errors_sample_sizes=getattr(step, "ignore_errors_sample_sizes", False),
        )
    )
    return kwargs


def _yolov5_train_kwargs(runtime: SmokeRuntime, case: RealRecoveryCase, step: RecoveryTrainStep) -> dict[str, object]:
    return _yolo_train_kwargs(runtime, case, step)


def _train_kwargs_builders(
    extra_builders: dict[type, TrainKwargsFn] | None = None,
    *,
    include_torch: bool = True,
) -> dict[type, TrainKwargsFn]:
    builders: dict[type, TrainKwargsFn] = dict(extra_builders or dict())
    if not include_torch:
        return builders

    from datapipe_ml.tasks.detection.train.yolov5 import Train_YoloV5_DetectionModel
    from datapipe_ml.tasks.detection.train.yolov8 import Train_YoloV8_DetectionModel
    from datapipe_ml.tasks.keypoints.train.yolov8 import Train_YoloV8_KeypointsModel
    from datapipe_ml.tasks.segmentation.train.yolov8 import Train_YoloV8_SegmentationModel

    builders.update(
        {
            Train_YoloV8_DetectionModel: _yolo_train_kwargs,
            Train_YoloV8_SegmentationModel: _yolo_train_kwargs,
            Train_YoloV8_KeypointsModel: _yolo_train_kwargs,
            Train_YoloV5_DetectionModel: _yolov5_train_kwargs,
        }
    )
    return builders


def direct_train_kwargs(
    runtime: SmokeRuntime,
    case: RealRecoveryCase,
    step: RecoveryTrainStep,
    *,
    extra_builders: dict[type, TrainKwargsFn] | None = None,
    include_torch_builders: bool = True,
) -> dict[str, object]:
    builder = _train_kwargs_builders(extra_builders, include_torch=include_torch_builders).get(type(step))
    if builder is None:
        raise TypeError(f"Unsupported recovery train step: {type(step)!r}")
    return builder(runtime, case, step)

from __future__ import annotations

import os
from dataclasses import replace

import pytest

from tests.helpers.failure_injection import (
    FAIL_AFTER_EPOCH_ENV,
    FAIL_MODE_ENV,
    patch_sky_vast_worker_entrypoint_for_failure_hooks,
)
from tests.helpers.training_recovery import (
    assert_status_manifest,
    configure_recovery_steps,
    make_recovery_runtime,
    recovery_case_by_id,
)
from tests.helpers.training_smoke import run_pipeline
from tests.test_training_sky_vast import _run_with_accelerator_candidates, _sky_vast_config, sky_vast_environment

pytestmark = [pytest.mark.slow, pytest.mark.training, pytest.mark.torch, pytest.mark.sky_vast]


def _require_sky_vast_recovery() -> None:
    if os.getenv("DATAPIPE_ML_RUN_SKY_VAST_RECOVERY") != "1":
        pytest.skip("Set DATAPIPE_ML_RUN_SKY_VAST_RECOVERY=1 to run live Sky/Vast recovery tests.")


def _yolov8_detection_case():
    return recovery_case_by_id("yolov8_detection")


def _run_recovery_on_sky_vast(
    tmp_path,
    monkeypatch,
    *,
    infra: str,
    accelerator: str,
    instance_type: str | None,
    resume_enabled: bool,
    fail_mode: str | None = None,
) -> None:  # noqa: ANN001
    case = _yolov8_detection_case()
    runtime, steps = make_recovery_runtime(tmp_path, case)
    steps = configure_recovery_steps(steps, resume_enabled=resume_enabled, sync_interval_s=5)
    train_step = steps[1]
    launcher_envs: dict[str, str] = {}
    if fail_mode is not None:
        patch_sky_vast_worker_entrypoint_for_failure_hooks(monkeypatch)
        launcher_envs = {
            FAIL_AFTER_EPOCH_ENV: "6",
            FAIL_MODE_ENV: fail_mode,
        }
    train_step.training_launcher_config = replace(
        _sky_vast_config(
            cluster_name=f"datapipe-ml-recovery-{os.getpid()}",
            infra=infra,
            accelerators=accelerator,
            instance_type=instance_type,
            run_timeout_s=int(os.getenv("DATAPIPE_ML_SKY_VAST_RECOVERY_TIMEOUT_S", "7200")),
        ),
        envs=launcher_envs,
    )
    try:
        run_pipeline(runtime, steps)
    except Exception:
        pass
    if fail_mode is not None:
        assert_status_manifest(runtime, case, status="failed")
        run_pipeline(runtime, steps[1:])
    assert_status_manifest(runtime, case, status="completed")


def test_real_yolov8_detection_sky_vast_failed_run_syncs_partial_outputs_best_effort(
    tmp_path, monkeypatch, sky_vast_environment
):
    _require_sky_vast_recovery()
    _run_with_accelerator_candidates(
        tmp_path,
        lambda attempt_path, infra, accelerator, instance_type: _run_recovery_on_sky_vast(
            attempt_path,
            monkeypatch,
            infra=infra,
            accelerator=accelerator,
            instance_type=instance_type,
            resume_enabled=True,
            fail_mode="error",
        ),
    )


def test_real_yolov8_detection_sky_vast_reattaches_live_training_and_resumes_output_sync(
    tmp_path, monkeypatch, sky_vast_environment
):
    _require_sky_vast_recovery()
    _run_with_accelerator_candidates(
        tmp_path,
        lambda attempt_path, infra, accelerator, instance_type: _run_recovery_on_sky_vast(
            attempt_path,
            monkeypatch,
            infra=infra,
            accelerator=accelerator,
            instance_type=instance_type,
            resume_enabled=True,
            fail_mode="kill9",
        ),
    )


def test_real_yolov8_detection_sky_vast_trains_new_run_when_resume_disabled_after_remote_failure(
    tmp_path, monkeypatch, sky_vast_environment
):
    _require_sky_vast_recovery()
    _run_with_accelerator_candidates(
        tmp_path,
        lambda attempt_path, infra, accelerator, instance_type: _run_recovery_on_sky_vast(
            attempt_path,
            monkeypatch,
            infra=infra,
            accelerator=accelerator,
            instance_type=instance_type,
            resume_enabled=False,
            fail_mode="error",
        ),
    )

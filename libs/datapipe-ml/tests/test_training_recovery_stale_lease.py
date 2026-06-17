from __future__ import annotations

import pytest

from tests.helpers.failure_injection import FAIL_AFTER_EPOCH_ENV, FAIL_MODE_ENV
from tests.helpers.training_recovery import (
    assert_stale_running_after_pipe_death,
    assert_status_manifest,
    model_rows,
    reopen_recovery_runtime,
    run_recovery_in_subprocess,
    yolov8_detection_recovery_case,
)

pytestmark = [pytest.mark.slow, pytest.mark.training, pytest.mark.torch]


def test_real_architecture_recovers_after_orchestrator_pipe_death_with_stale_lease(tmp_path):
    """Orchestrator/pipe dies without mark_failed; retry takes over stale running lease."""
    case = yolov8_detection_recovery_case()
    workdir = tmp_path

    train_result = run_recovery_in_subprocess(
        workdir,
        case,
        "train",
        extra_env={
            FAIL_AFTER_EPOCH_ENV: "6",
            FAIL_MODE_ENV: "kill_pipe",
        },
    )
    assert train_result.returncode == 137, train_result.stdout + train_result.stderr

    runtime, _ = reopen_recovery_runtime(workdir, case)
    stale = assert_stale_running_after_pipe_death(runtime, case, min_epoch=6)
    stale_model_id = stale[case.model_id_column]

    resume_result = run_recovery_in_subprocess(workdir, case, "resume")
    assert resume_result.returncode == 0, resume_result.stdout + resume_result.stderr

    runtime, _ = reopen_recovery_runtime(workdir, case)
    completed = assert_status_manifest(runtime, case, status="completed")
    assert completed[case.model_id_column] == stale_model_id
    assert int(completed["training_status__attempt"]) == 2
    assert len(model_rows(runtime, case)) == 1

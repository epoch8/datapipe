from __future__ import annotations

import pytest

from tests.helpers.failure_injection import (
    FAIL_AFTER_EPOCH_ENV,
    FAIL_MODE_ENV,
    training_failure_hooks,
)
from tests.helpers.training_recovery import (
    assert_status_manifest,
    configure_recovery_steps,
    invoke_real_train_callable_for_backfill,
    make_recovery_runtime,
    model_rows,
    real_recovery_torch_cases,
    run_pipeline_in_thread,
    status_rows,
    wait_for_manifest,
    wait_for_running_status,
)
from tests.helpers.training_smoke import run_pipeline

pytestmark = [pytest.mark.slow, pytest.mark.training, pytest.mark.torch]


def _run_pipeline_allowing_failure(runtime, steps) -> None:  # noqa: ANN001
    try:
        run_pipeline(runtime, steps)
    except Exception:
        pass


@pytest.mark.parametrize("case", real_recovery_torch_cases())
def test_real_architecture_training_writes_status_and_manifest_during_run(tmp_path, case):
    runtime, steps = make_recovery_runtime(tmp_path, case)

    thread, errors = run_pipeline_in_thread(runtime, steps)
    running_row = wait_for_running_status(runtime, case)
    manifest = wait_for_manifest(runtime, case)
    thread.join(timeout=300)

    assert not thread.is_alive()
    assert errors == []
    assert running_row["training_status__status"] == "running"
    assert manifest.checkpoints
    assert_status_manifest(runtime, case, status="completed")


@pytest.mark.parametrize("case", real_recovery_torch_cases())
def test_real_architecture_backfills_completed_status_from_existing_link(tmp_path, case):
    runtime, steps = make_recovery_runtime(tmp_path, case)
    run_pipeline(runtime, steps)

    existing_status = status_rows(runtime, case)
    runtime.ds.get_table(case.status_table).delete_by_idx(existing_status[["training_status_id"]])
    assert status_rows(runtime, case).empty

    invoke_real_train_callable_for_backfill(runtime, case, steps[1])

    rows = status_rows(runtime, case)
    assert len(rows) == 1
    row = rows.iloc[0]
    assert row["training_status__status"] == "completed"
    assert int(row["training_status__attempt"]) == 0


@pytest.mark.usefixtures("training_failure_hooks")
@pytest.mark.parametrize("case", real_recovery_torch_cases())
def test_real_architecture_resume_after_epoch6_script_error_with_sync_enabled(tmp_path, monkeypatch, case):
    runtime, steps = make_recovery_runtime(tmp_path, case)
    monkeypatch.setenv(FAIL_AFTER_EPOCH_ENV, "6")
    monkeypatch.setenv(FAIL_MODE_ENV, "error")

    _run_pipeline_allowing_failure(runtime, steps)

    failed = assert_status_manifest(runtime, case, status="failed")
    failed_model_id = failed[case.model_id_column]
    monkeypatch.delenv(FAIL_AFTER_EPOCH_ENV)
    monkeypatch.delenv(FAIL_MODE_ENV)

    run_pipeline(runtime, steps[1:])

    completed = assert_status_manifest(runtime, case, status="completed")
    assert completed[case.model_id_column] == failed_model_id


@pytest.mark.usefixtures("training_failure_hooks")
@pytest.mark.parametrize("case", real_recovery_torch_cases())
def test_real_architecture_trains_new_model_when_resume_disabled_after_epoch6_error(tmp_path, monkeypatch, case):
    runtime, steps = make_recovery_runtime(tmp_path, case)
    steps = configure_recovery_steps(steps, resume_enabled=False)
    monkeypatch.setenv(FAIL_AFTER_EPOCH_ENV, "6")
    monkeypatch.setenv(FAIL_MODE_ENV, "error")

    _run_pipeline_allowing_failure(runtime, steps)

    failed = assert_status_manifest(runtime, case, status="failed")
    failed_model_id = failed[case.model_id_column]
    monkeypatch.delenv(FAIL_AFTER_EPOCH_ENV)
    monkeypatch.delenv(FAIL_MODE_ENV)

    run_pipeline(runtime, steps[1:])

    completed = assert_status_manifest(runtime, case, status="completed")
    assert completed[case.model_id_column] != failed_model_id


@pytest.mark.usefixtures("training_failure_hooks")
@pytest.mark.parametrize("case", real_recovery_torch_cases())
def test_real_architecture_recovers_after_epoch6_hard_training_subprocess_death(tmp_path, monkeypatch, case):
    """Training child dies via kill9; orchestrator marks failed and resume reuses model_id."""
    runtime, steps = make_recovery_runtime(tmp_path, case)
    monkeypatch.setenv(FAIL_AFTER_EPOCH_ENV, "6")
    monkeypatch.setenv(FAIL_MODE_ENV, "kill9")

    _run_pipeline_allowing_failure(runtime, steps)

    failed = assert_status_manifest(runtime, case, status="failed")
    failed_model_id = failed[case.model_id_column]
    monkeypatch.delenv(FAIL_AFTER_EPOCH_ENV)
    monkeypatch.delenv(FAIL_MODE_ENV)

    run_pipeline(runtime, steps[1:])

    completed = assert_status_manifest(runtime, case, status="completed")
    assert completed[case.model_id_column] == failed_model_id
    assert len(model_rows(runtime, case)) == 1

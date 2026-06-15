from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


def _install_short_lease_status_manager() -> None:
    import datapipe_ml.training.orchestrator as orchestrator_module
    import datapipe_ml.training.runs as runs_module

    def short_training_lease_settings(resume_config):  # noqa: ANN001
        return 1, 2

    runs_module.training_lease_settings = short_training_lease_settings
    orchestrator_module.training_lease_settings = short_training_lease_settings


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run recovery pipeline phase in an isolated subprocess.")
    parser.add_argument("--workdir", required=True)
    parser.add_argument("--case-id", required=True)
    parser.add_argument("--phase", choices=["train", "resume"], required=True)
    args = parser.parse_args(argv)

    workdir = Path(args.workdir).resolve()
    if args.phase == "train":
        _install_short_lease_status_manager()

    from tests.helpers.failure_injection import install_pipe_death_hooks_direct
    from tests.helpers.failure_injection_bootstrap import WORK_DIR_ENV
    from tests.helpers.training_recovery import (
        configure_recovery_steps,
        make_recovery_runtime,
        recovery_case_by_id,
    )
    from datapipe.compute import Pipeline, build_compute, run_steps
    from tests.helpers.training_smoke import run_pipeline

    os.environ[WORK_DIR_ENV] = str(workdir)

    case = recovery_case_by_id(args.case_id)
    runtime, steps = make_recovery_runtime(workdir, case)
    steps = configure_recovery_steps(steps, sync_interval_s=1, reset_attempts_after="1d")

    if args.phase == "train":
        install_pipe_death_hooks_direct()
        run_pipeline(runtime, steps)
        return 0

    compute_steps = build_compute(runtime.ds, runtime.catalog, Pipeline(steps))
    run_steps(runtime.ds, compute_steps[1:])
    return 0


if __name__ == "__main__":
    sys.exit(main())

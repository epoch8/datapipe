from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from datapipe_ml.training.runs import (
    RESUMABLE_TRAINING_STATUSES,
    TrainingStatus,
    TrainingStatusManager,
    base_status_row,
    get_active_status_row,
    get_status_row,
    status_id_for_attempt,
    subprocess_exitcode_from_runtime_error,
    training_run_key,
    utc_iso,
    utc_now,
)
from datapipe_ml.training.resume import select_resume_checkpoint
from datapipe_ml.training.specs import Algo, TrainingResumeConfig, TrainingSyncConfig
from datapipe_ml.training.sync import (
    LOCAL_TRAIN_OUTPUT_SUBDIR,
    PeriodicTrainingSync,
    copy_tree_best_effort,
    discover_checkpoint_paths,
    dst_sync_lock,
    manifest_path_for_run,
    orchestrator_owns_output_sync,
    plan_orchestrator_output_sync,
    read_checkpoint_manifest,
    sync_training_tree_and_manifest,
    write_checkpoint_manifest,
)
from datapipe_ml.training.specs import TrainContext


class _MemoryStatusTable:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def get_data(self, idx=None):  # noqa: ANN001
        if idx is None:
            return self.df.copy()
        ids = set(idx["training_status_id"].tolist())
        return self.df[self.df["training_status_id"].isin(ids)].copy()

    def store_chunk(self, data_df, processed_idx=None, now=None, run_config=None):  # noqa: ANN001
        if data_df is None or data_df.empty:
            return pd.DataFrame()
        data_df = data_df.reset_index(drop=True).copy()
        if self.df.empty:
            self.df = data_df
            return data_df
        keys = ["training_status_id"]
        keep = self.df.copy()
        for _, row in data_df.iterrows():
            mask = pd.Series([True] * len(keep))
            for key in keys:
                if key in keep.columns:
                    mask &= keep[key] == row[key]
            keep = keep[~mask]
        self.df = pd.concat([keep, data_df], ignore_index=True)
        return data_df


def test_resumable_training_statuses_include_interrupted() -> None:
    assert TrainingStatus.INTERRUPTED in RESUMABLE_TRAINING_STATUSES
    assert TrainingStatus.FAILED in RESUMABLE_TRAINING_STATUSES
    assert TrainingStatus.COMPLETED not in RESUMABLE_TRAINING_STATUSES


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        ("Training subprocess exited before returning a result. exitcode=-2", -2),
        ("Training subprocess exited before returning a result. exitcode=130", 130),
        ("Training subprocess exited before returning a result. exitcode=2", 2),
        ("no exit code here", None),
    ],
)
def test_subprocess_exitcode_from_runtime_error_parses_sigint_codes(message: str, expected: int | None) -> None:
    assert subprocess_exitcode_from_runtime_error(RuntimeError(message)) == expected


def test_get_active_status_row_ignores_interrupted_status() -> None:
    dt = _MemoryStatusTable(
        pd.DataFrame(
            [
                {
                    "training_status_id": "run__attempt_1",
                    "training_status__run_key": "run",
                    "training_status__status": TrainingStatus.INTERRUPTED.value,
                    "training_status__lease_expires_at": utc_iso(utc_now().replace(year=utc_now().year + 1)),
                }
            ]
        )
    )

    assert get_active_status_row(dt, "run") is None  # type: ignore[arg-type]


def test_select_resume_checkpoint_works_for_interrupted_run_manifest(tmp_path: Path) -> None:
    run_dir = tmp_path / "models" / "model-a"
    weights_dir = run_dir / "weights"
    weights_dir.mkdir(parents=True)
    epoch1 = weights_dir / "epoch1.pt"
    epoch2 = weights_dir / "epoch2.pt"
    epoch1.write_bytes(b"epoch-1")
    epoch2.write_bytes(b"epoch-2")

    manifest_path = write_checkpoint_manifest(
        run_dir=str(run_dir),
        model_id="model-a",
        checkpoint_paths=[str(epoch1), str(epoch2)],
    )

    selected = select_resume_checkpoint(
        manifest_path=manifest_path,
        config=TrainingResumeConfig(continue_train_failed_models=True, min_completed_epochs=1),
    )

    assert selected is not None
    assert selected.path == str(epoch2)
    assert selected.epoch == 2


def test_training_status_manager_mark_interrupted_clears_lease() -> None:
    table = _MemoryStatusTable(pd.DataFrame())
    row = base_status_row(
        run_key="run",
        idx=pd.DataFrame([{}]),
        model_other_primary_keys=[],
        frozen_dataset_id_col="frozen_dataset_id",
        frozen_dataset_id="fd",
        train_config_id_col="train_config_id",
        train_config_id="cfg",
        model_id_col="model_id",
        model_id="model-a",
        models_dir="/tmp/models",
        run_dir="/tmp/models/model-a",
        launcher_config=None,
        attempt=1,
        status=TrainingStatus.RUNNING,
    )
    manager = TrainingStatusManager(dt=table, row=row)  # type: ignore[arg-type]
    manager.start()
    manager.mark_interrupted(manifest_path="/tmp/models/model-a/datapipe_ml_training_sync.json")

    stored = table.df.iloc[0]
    assert stored["training_status__status"] == TrainingStatus.INTERRUPTED.value
    assert stored["training_status__lease_expires_at"] is None
    assert stored["training_status__error"] == "Training interrupted by user."
    assert stored["training_status__manifest_path"] == "/tmp/models/model-a/datapipe_ml_training_sync.json"


def test_write_manifest_and_select_latest_epoch_checkpoint(tmp_path: Path) -> None:
    run_dir = tmp_path / "models" / "model-a"
    weights_dir = run_dir / "weights"
    weights_dir.mkdir(parents=True)
    epoch1 = weights_dir / "epoch1.pt"
    epoch2 = weights_dir / "epoch2.pt"
    epoch1.write_bytes(b"epoch-1")
    epoch2.write_bytes(b"epoch-2")

    manifest_path = write_checkpoint_manifest(
        run_dir=str(run_dir),
        model_id="model-a",
        checkpoint_paths=[str(epoch1), str(epoch2)],
    )

    selected = select_resume_checkpoint(
        manifest_path=manifest_path,
        config=TrainingResumeConfig(continue_train_failed_models=True, min_completed_epochs=2),
    )

    assert selected is not None
    assert selected.path == str(epoch2)
    assert selected.epoch == 2


def test_resume_ignores_unmanifested_checkpoint(tmp_path: Path) -> None:
    run_dir = tmp_path / "models" / "model-a"
    weights_dir = run_dir / "weights"
    weights_dir.mkdir(parents=True)
    unmanifested = weights_dir / "epoch3.pt"
    unmanifested.write_bytes(b"epoch-3")

    manifest_path = write_checkpoint_manifest(
        run_dir=str(run_dir),
        model_id="model-a",
        checkpoint_paths=[],
    )

    selected = select_resume_checkpoint(
        manifest_path=manifest_path,
        config=TrainingResumeConfig(continue_train_failed_models=True),
    )

    assert selected is None


def test_resume_ignores_checkpoint_with_mismatched_size(tmp_path: Path) -> None:
    run_dir = tmp_path / "models" / "model-a"
    weights_dir = run_dir / "weights"
    weights_dir.mkdir(parents=True)
    checkpoint = weights_dir / "epoch2.pt"
    checkpoint.write_bytes(b"epoch-2")
    manifest_path = write_checkpoint_manifest(
        run_dir=str(run_dir),
        model_id="model-a",
        checkpoint_paths=[str(checkpoint)],
    )
    checkpoint.write_bytes(b"changed-size")

    selected = select_resume_checkpoint(
        manifest_path=manifest_path,
        config=TrainingResumeConfig(continue_train_failed_models=True),
    )

    assert selected is None


def test_resume_ignores_epochless_alias_when_min_epoch_required(tmp_path: Path) -> None:
    run_dir = tmp_path / "models" / "model-a"
    weights_dir = run_dir / "weights"
    weights_dir.mkdir(parents=True)
    best = weights_dir / "best.pt"
    best.write_bytes(b"best")
    manifest_path = write_checkpoint_manifest(
        run_dir=str(run_dir),
        model_id="model-a",
        checkpoint_paths=[str(best)],
    )

    selected = select_resume_checkpoint(
        manifest_path=manifest_path,
        config=TrainingResumeConfig(continue_train_failed_models=True, min_completed_epochs=1),
    )

    assert selected is None


def test_resume_allows_epochless_alias_when_min_epoch_is_zero(tmp_path: Path) -> None:
    run_dir = tmp_path / "models" / "model-a"
    weights_dir = run_dir / "weights"
    weights_dir.mkdir(parents=True)
    best = weights_dir / "best.pt"
    best.write_bytes(b"best")
    manifest_path = write_checkpoint_manifest(
        run_dir=str(run_dir),
        model_id="model-a",
        checkpoint_paths=[str(best)],
    )

    selected = select_resume_checkpoint(
        manifest_path=manifest_path,
        config=TrainingResumeConfig(continue_train_failed_models=True, min_completed_epochs=0, checkpoint="best"),
    )

    assert selected is not None
    assert selected.path == str(best)


def test_copy_tree_best_effort_retries_after_transient_failure(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    src = tmp_path / "src"
    dst = tmp_path / "dst"
    src.mkdir()
    (src / "weights.pt").write_bytes(b"weights")
    attempts = {"count": 0}

    from datapipe_ml.training import sync as sync_module

    original_copy = sync_module.copy_tree_snapshot

    def flaky_copy_tree_snapshot(src_url: str, dst_url: str) -> None:
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise RuntimeError("network down")
        original_copy(src_url, dst_url)

    monkeypatch.setattr(sync_module, "copy_tree_snapshot", flaky_copy_tree_snapshot)

    copy_tree_best_effort(str(src), str(dst), retries=2, retry_sleep_s=0)

    assert attempts["count"] == 2
    assert (dst / "weights.pt").read_bytes() == b"weights"


def test_copy_tree_snapshot_can_force_copy_mutable_files(tmp_path: Path) -> None:
    from datapipe_ml.training.sync import copy_tree_snapshot

    src = tmp_path / "src"
    dst = tmp_path / "dst"
    src.mkdir()
    results = src / "results.csv"
    results.write_text("epoch,loss\n0,1.0\n")

    copy_tree_snapshot(str(src), str(dst), require_stable=False)

    assert (dst / "results.csv").read_text() == results.read_text()


def test_manifest_path_for_run_is_stable(tmp_path: Path) -> None:
    assert manifest_path_for_run(str(tmp_path / "run")).endswith("/datapipe_ml_training_sync.json")


def test_discover_checkpoint_paths_finds_yolo_and_tf_checkpoints(tmp_path: Path) -> None:
    run_dir = tmp_path / "model-a"
    weights = run_dir / "weights"
    weights.mkdir(parents=True)
    (weights / "epoch1.pt").write_bytes(b"1")
    (weights / "last.pt").write_bytes(b"last")
    (run_dir / "02__model.keras").write_bytes(b"keras")
    (run_dir / "args.yaml").write_text("not a checkpoint")

    paths = {Path(path).name for path in discover_checkpoint_paths(str(run_dir))}

    assert paths == {"epoch1.pt", "last.pt", "02__model.keras"}


def test_plan_orchestrator_output_sync_uses_local_staging_for_remote_models_dir(tmp_path: Path) -> None:
    plan = plan_orchestrator_output_sync(
        models_dir="s3://bucket/models",
        tmp_folder=str(tmp_path),
        sync_config=TrainingSyncConfig(enabled=True, interval_s=1),
        owns_output_sync=True,
    )

    assert plan is not None
    assert plan.remote_dst.endswith("/models")
    assert plan.local_src.endswith(LOCAL_TRAIN_OUTPUT_SUBDIR)


def test_plan_orchestrator_output_sync_skips_local_models_dir(tmp_path: Path) -> None:
    plan = plan_orchestrator_output_sync(
        models_dir=str(tmp_path / "models"),
        tmp_folder=str(tmp_path),
        sync_config=TrainingSyncConfig(enabled=True, interval_s=1),
        owns_output_sync=True,
    )

    assert plan is None


def test_orchestrator_owns_output_sync_only_for_local_launcher() -> None:
    ctx = TrainContext(
        models_dir="/tmp/models",
        max_within_time="1d",
        tmp_folder="/tmp",
        model_suffix="_smoke",
        dt__model=None,  # type: ignore[arg-type]
        dt__link=None,  # type: ignore[arg-type]
        dt__training_status=None,  # type: ignore[arg-type]
        dt__frozen_dataset=None,  # type: ignore[arg-type]
        dt__frozen_dataset__has__image_gt=None,
        dt__train_config=None,  # type: ignore[arg-type]
        model_other_primary_keys=[],
        model_id__name="model_id",
        frozen_dataset_id__name="frozen_dataset_id",
    )
    assert orchestrator_owns_output_sync(ctx) is True


def test_dst_sync_lock_serializes_concurrent_tree_copies(tmp_path: Path) -> None:
    import threading
    import time

    dst = tmp_path / "dst"
    dst.mkdir()
    order: list[str] = []
    start = threading.Barrier(2)

    def worker(name: str) -> None:
        src = tmp_path / f"src-{name}"
        src.mkdir()
        (src / "weights.pt").write_bytes(name.encode())
        start.wait()
        with dst_sync_lock(str(dst)):
            order.append(f"{name}-enter")
            time.sleep(0.05)
            copy_tree_best_effort(str(src), str(dst / name), retries=1, retry_sleep_s=0)
            order.append(f"{name}-exit")

    threads = [threading.Thread(target=worker, args=(name,)) for name in ("a", "b")]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert order in (
        ["a-enter", "a-exit", "b-enter", "b-exit"],
        ["b-enter", "b-exit", "a-enter", "a-exit"],
    )


def test_sync_training_tree_and_manifest_includes_post_training_files(tmp_path: Path) -> None:
    src = tmp_path / "src"
    dst = tmp_path / "dst"
    run_dir = src / "model-a"
    weights = run_dir / "weights"
    weights.mkdir(parents=True)
    (weights / "epoch1.pt").write_bytes(b"epoch")
    (run_dir / "class_names.json").write_text('["cat"]')

    sync_training_tree_and_manifest(
        src=str(src),
        dst=str(dst),
        config=TrainingSyncConfig(enabled=True, interval_s=None, retries=1, retry_sleep_s=0),
        model_id="model-a",
    )

    assert (dst / "model-a" / "class_names.json").read_text() == '["cat"]'
    manifest = read_checkpoint_manifest(str(dst / "model-a" / "datapipe_ml_training_sync.json"))
    assert manifest is not None
    assert [Path(item.path).name for item in manifest.checkpoints] == ["epoch1.pt"]


def test_periodic_training_sync_publishes_manifest_after_copy(tmp_path: Path) -> None:
    src = tmp_path / "src"
    dst = tmp_path / "dst"
    weights = src / "model-a" / "weights"
    weights.mkdir(parents=True)
    (weights / "epoch1.pt").write_bytes(b"epoch")
    sync = PeriodicTrainingSync(
        src=str(src),
        dst=str(dst),
        config=TrainingSyncConfig(enabled=True, interval_s=None, retries=1, retry_sleep_s=0),
        model_id="model-a",
    )

    sync.stop(final_sync=True)

    manifest = read_checkpoint_manifest(str(dst / "model-a" / "datapipe_ml_training_sync.json"))
    assert manifest is not None
    assert [Path(item.path).name for item in manifest.checkpoints] == ["epoch1.pt"]


def test_manifest_resume_roundtrip_on_storage_matrix(storage_workdir: str) -> None:
    run_dir = f"{storage_workdir.rstrip('/')}/models/model-a"
    checkpoint = f"{run_dir}/weights/epoch1.pt"
    import fsspec

    fs, stripped = fsspec.core.url_to_fs(checkpoint)
    fs.makedirs(str(Path(stripped).parent), exist_ok=True)
    with fs.open(stripped, "wb") as out:
        out.write(b"checkpoint")

    manifest_path = write_checkpoint_manifest(
        run_dir=run_dir,
        model_id="model-a",
        checkpoint_paths=[checkpoint],
    )
    selected = select_resume_checkpoint(
        manifest_path=manifest_path,
        config=TrainingResumeConfig(continue_train_failed_models=True, min_completed_epochs=1),
    )

    assert selected is not None
    assert selected.path == checkpoint


def test_resume_config_does_not_enable_sync() -> None:
    sync_config = TrainingSyncConfig()
    resume_config = TrainingResumeConfig(continue_train_failed_models=True)

    assert sync_config.enabled is False
    assert resume_config.continue_train_failed_models is True


def test_base_algo_resume_hook_does_not_mutate_train_params() -> None:
    class DummyAlgo(Algo):
        train_config_id_col = "train_config_id"
        train_params_col = "train_config__params"
        frozen_created_at_col = None
        images_count_col = None
        model_row_prefix = "model"

        def check_accelerator(self, train_params):
            return None

        def prepare_data(self, ctx, idx):
            raise NotImplementedError

        def build_model_id(self, ctx, idx, train_params):
            return "model"

        def launch_training(self, ctx, idx, model_id, train_params, data, resume_checkpoint=None):
            raise NotImplementedError

        def select_best(self, raw_result, idx):
            raise NotImplementedError

        def build_model_row(self, ctx, idx, model_id, best, train_params):
            raise NotImplementedError

    params = {"epochs": 2}
    updated = DummyAlgo().apply_resume_checkpoint(ctx=None, train_params=params, checkpoint_path="checkpoint.pt")  # type: ignore[arg-type]

    assert updated == params
    assert updated is not params


def test_yolo_resume_hook_sets_typed_training_params() -> None:
    from datapipe_ml.frameworks.yolo.training import YoloBaseAlgo

    class DummyYoloAlgo(YoloBaseAlgo):
        pass

    params = {"epochs": 2, "save_period": -1}

    updated = DummyYoloAlgo().apply_resume_checkpoint(None, params, "checkpoint.pt")  # type: ignore[arg-type]

    assert updated["initial_weights_path"] == "checkpoint.pt"
    assert updated["resume"] is True
    assert updated["exist_ok"] is True
    assert updated["save_period"] == 1
    assert "initial_weights_path" not in params


def test_status_attempt_ids_are_distinct_for_same_run_key() -> None:
    assert status_id_for_attempt("run", 1) == "run__attempt_1"
    assert status_id_for_attempt("run", 2) == "run__attempt_2"


def test_training_run_key_is_distinct_for_model_subset_keys() -> None:
    first = training_run_key(
        idx=pd.DataFrame([{"source_id": "first"}]),
        model_other_primary_keys=["source_id"],
        frozen_dataset_id_col="detection_frozen_dataset_id",
        frozen_dataset_id="fd",
        train_config_id_col="detection_train_config_id",
        train_config_id="cfg",
        model_suffix="_store",
    )
    second = training_run_key(
        idx=pd.DataFrame([{"source_id": "second"}]),
        model_other_primary_keys=["source_id"],
        frozen_dataset_id_col="detection_frozen_dataset_id",
        frozen_dataset_id="fd",
        train_config_id_col="detection_train_config_id",
        train_config_id="cfg",
        model_suffix="_store",
    )

    assert first != second


def test_get_status_row_returns_latest_attempt_for_run_key() -> None:
    dt = _MemoryStatusTable(
        pd.DataFrame(
            [
                {
                    "training_status_id": "run__attempt_1",
                    "training_status__run_key": "run",
                    "training_status__attempt": 1,
                    "training_status__started_at": "2024-01-01T00:00:00+00:00",
                },
                {
                    "training_status_id": "run__attempt_2",
                    "training_status__run_key": "run",
                    "training_status__attempt": 2,
                    "training_status__started_at": "2024-01-02T00:00:00+00:00",
                },
            ]
        )
    )

    row = get_status_row(dt, "run")  # type: ignore[arg-type]

    assert row is not None
    assert row["training_status_id"] == "run__attempt_2"


def test_get_active_status_row_requires_running_unexpired_lease() -> None:
    dt = _MemoryStatusTable(
        pd.DataFrame(
            [
                {
                    "training_status_id": "run__attempt_1",
                    "training_status__run_key": "run",
                    "training_status__status": "failed",
                    "training_status__lease_expires_at": utc_iso(utc_now()),
                },
                {
                    "training_status_id": "run__attempt_2",
                    "training_status__run_key": "run",
                    "training_status__status": "running",
                    "training_status__lease_expires_at": utc_iso(utc_now().replace(year=utc_now().year + 1)),
                },
            ]
        )
    )

    row = get_active_status_row(dt, "run")  # type: ignore[arg-type]

    assert row is not None
    assert row["training_status_id"] == "run__attempt_2"

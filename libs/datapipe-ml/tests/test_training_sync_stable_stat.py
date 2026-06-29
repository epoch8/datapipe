from __future__ import annotations

from pathlib import Path

import pytest

from datapipe_ml.training import sync as sync_module
from datapipe_ml.training.sync import verify_manifest_checkpoint, write_checkpoint_manifest
from datapipe_ml.training.sync import TrainingCheckpointEntry


def test_stable_stat_rejects_changing_file(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"count": 0}

    def fake_stat(_url: str):
        calls["count"] += 1
        if calls["count"] == 1:
            return (100, 1.0)
        if calls["count"] == 2:
            return (200, 2.0)
        return (200, 2.0)

    monkeypatch.setattr(sync_module, "_stat_url", fake_stat)
    monkeypatch.setattr(sync_module.time, "sleep", lambda *_args, **_kwargs: None)

    with pytest.raises(RuntimeError, match="Checkpoint changed while being inspected"):
        sync_module._stable_stat("s3://bucket/checkpoint.pt", max_attempts=1)


def test_write_manifest_skips_unstable_checkpoint_during_sigint_flush(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Trainer still flushing weights on Ctrl+C must not be published to the manifest."""
    run_dir = tmp_path / "models" / "model-a"
    weights_dir = run_dir / "weights"
    weights_dir.mkdir(parents=True)
    stable = weights_dir / "epoch1.pt"
    unstable = weights_dir / "epoch2.pt"
    stable.write_bytes(b"stable")
    unstable.write_bytes(b"partial")

    original_stable_stat = sync_module._stable_stat

    def selective_stable_stat(url: str, *, max_attempts: int = sync_module.STABLE_STAT_MAX_ATTEMPTS):
        if str(unstable) in url:
            raise RuntimeError(f"Checkpoint changed while being inspected: {url}")
        return original_stable_stat(url, max_attempts=max_attempts)

    monkeypatch.setattr(sync_module, "_stable_stat", selective_stable_stat)

    manifest_path = write_checkpoint_manifest(
        run_dir=str(run_dir),
        model_id="model-a",
        checkpoint_paths=[str(stable), str(unstable)],
    )

    manifest = sync_module.read_checkpoint_manifest(manifest_path)
    assert manifest is not None
    assert [item.path for item in manifest.checkpoints] == [str(stable)]


def test_write_checkpoint_manifest_preserves_previous_content_on_failed_replace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from datapipe_ml.core import atomic_io
    from datapipe_ml.training.sync import read_checkpoint_manifest, write_checkpoint_manifest

    run_dir = tmp_path / "models" / "model-a"
    weights_dir = run_dir / "weights"
    weights_dir.mkdir(parents=True)
    checkpoint = weights_dir / "epoch1.pt"
    checkpoint.write_bytes(b"stable")

    manifest_path = write_checkpoint_manifest(
        run_dir=str(run_dir),
        model_id="model-a",
        checkpoint_paths=[str(checkpoint)],
    )
    first = read_checkpoint_manifest(manifest_path)
    assert first is not None
    assert len(first.checkpoints) == 1

    def fail_replace(_dst_fs, _tmp_path: str, _dst_path: str) -> None:
        raise OSError("simulated interrupt before manifest replace")

    monkeypatch.setattr(atomic_io, "_replace_url_atomically", fail_replace)

    with pytest.raises(OSError, match="simulated interrupt"):
        write_checkpoint_manifest(
            run_dir=str(run_dir),
            model_id="model-a",
            checkpoint_paths=[str(checkpoint)],
        )

    second = read_checkpoint_manifest(manifest_path)
    assert second is not None
    assert len(second.checkpoints) == 1
    assert second.checkpoints[0].path == first.checkpoints[0].path


def test_storage_urls_equal_normalizes_trailing_slashes() -> None:
    from datapipe_ml.training.sync import storage_urls_equal

    assert storage_urls_equal("/tmp/exp", "/tmp/exp/")
    assert storage_urls_equal("/tmp/exp", "/tmp/exp")
    assert not storage_urls_equal("/tmp/exp", "/tmp/exp2")


def test_remap_path_under_root_rewrites_under_write_prefix() -> None:
    from datapipe_ml.training.sync import remap_path_under_root

    assert (
        remap_path_under_root(
            "/local/exp/weights/best.pt",
            "/local/exp",
            "s3://bucket/models/exp",
        )
        == "s3://bucket/models/exp/weights/best.pt"
    )


def test_remap_path_under_root_leaves_unrelated_paths() -> None:
    from datapipe_ml.training.sync import remap_path_under_root

    assert (
        remap_path_under_root(
            "/local/exp2/weights/best.pt",
            "/local/exp",
            "/persisted/exp",
        )
        == "/local/exp2/weights/best.pt"
    )


def test_copy_tree_snapshot_without_stable_gate_copies_changing_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    src_dir = tmp_path / "src" / "model-a" / "weights"
    dst_dir = tmp_path / "dst" / "model-a" / "weights"
    src_dir.mkdir(parents=True)
    checkpoint = src_dir / "best.pt"
    checkpoint.write_bytes(b"v1")

    calls = {"count": 0}

    def fake_stat(_url: str):
        calls["count"] += 1
        return (len(checkpoint.read_bytes()), float(calls["count"]))

    monkeypatch.setattr(sync_module, "_stat_url", fake_stat)
    monkeypatch.setattr(sync_module.time, "sleep", lambda *_args, **_kwargs: None)

    sync_module.copy_tree_snapshot(str(src_dir.parent.parent), str(dst_dir.parent.parent), require_stable=False)

    assert (dst_dir / "best.pt").read_bytes() == b"v1"


def test_copy_tree_snapshot_stable_gate_skips_changing_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    # Default require_stable=True must skip a file whose stat keeps changing.
    src_dir = tmp_path / "src" / "model-a" / "weights"
    dst_dir = tmp_path / "dst" / "model-a" / "weights"
    src_dir.mkdir(parents=True)
    (src_dir / "best.pt").write_bytes(b"v1")

    calls = {"count": 0}

    def fake_stat(_url: str):
        calls["count"] += 1
        return (calls["count"], float(calls["count"]))  # never stable

    monkeypatch.setattr(sync_module, "_stat_url", fake_stat)
    monkeypatch.setattr(sync_module.time, "sleep", lambda *_a, **_k: None)

    sync_module.copy_tree_snapshot(str(src_dir.parent.parent), str(dst_dir.parent.parent), require_stable=True)

    assert not (dst_dir / "best.pt").exists()


def test_sync_training_tree_and_manifest_final_copy_skips_stable_gate(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from datapipe_ml.training.specs import TrainingSyncConfig

    src_root = tmp_path / "src"
    dst_root = tmp_path / "dst"
    run_dir = src_root / "model-a"
    weights_dir = run_dir / "weights"
    weights_dir.mkdir(parents=True)
    (weights_dir / "best.pt").write_bytes(b"best")

    captured: dict[str, bool] = {}

    def fake_copy_tree_best_effort(*_args, **kwargs):
        captured["require_stable"] = kwargs.get("require_stable", True)

    monkeypatch.setattr(sync_module, "copy_tree_best_effort", fake_copy_tree_best_effort)
    monkeypatch.setattr(sync_module, "write_checkpoint_manifest", lambda **_kwargs: "manifest.json")

    sync_module.sync_training_tree_and_manifest(
        src=str(src_root),
        dst=str(dst_root),
        config=TrainingSyncConfig(enabled=True, interval_s=30, retries=1, retry_sleep_s=0),
        model_id="model-a",
        discover_checkpoints=lambda _run_dir: [str(weights_dir / "best.pt")],
        require_stable=False,
    )

    assert captured["require_stable"] is False


def test_sync_training_tree_and_manifest_defaults_to_stable_gate(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from datapipe_ml.training.specs import TrainingSyncConfig

    src_root = tmp_path / "src"
    dst_root = tmp_path / "dst"
    weights_dir = src_root / "model-a" / "weights"
    weights_dir.mkdir(parents=True)
    (weights_dir / "best.pt").write_bytes(b"best")

    captured: dict[str, bool] = {}

    def fake_copy_tree_best_effort(*_args, **kwargs):
        captured["require_stable"] = kwargs.get("require_stable", True)

    monkeypatch.setattr(sync_module, "copy_tree_best_effort", fake_copy_tree_best_effort)
    monkeypatch.setattr(sync_module, "write_checkpoint_manifest", lambda **_kwargs: "manifest.json")

    sync_module.sync_training_tree_and_manifest(
        src=str(src_root),
        dst=str(dst_root),
        config=TrainingSyncConfig(enabled=True, interval_s=30, retries=1, retry_sleep_s=0),
        model_id="model-a",
        discover_checkpoints=lambda _run_dir: [str(weights_dir / "best.pt")],
    )

    assert captured["require_stable"] is True


# ---------------------------------------------------------------------------
# PeriodicTrainingSync: which sync uses the stable gate and which skips it
# ---------------------------------------------------------------------------


def _make_periodic_sync(monkeypatch: pytest.MonkeyPatch) -> tuple[object, list[bool]]:
    from datapipe_ml.training.specs import TrainingSyncConfig
    from datapipe_ml.training.sync import PeriodicTrainingSync

    captured: list[bool] = []

    def fake_sync_tree(*, require_stable: bool = True, **_kwargs):
        captured.append(require_stable)

    monkeypatch.setattr(sync_module, "sync_training_tree_and_manifest", fake_sync_tree)

    sync = PeriodicTrainingSync(
        src="/src",
        dst="/dst",
        config=TrainingSyncConfig(enabled=True, interval_s=30),
        model_id="model-a",
    )
    return sync, captured


def test_periodic_sync_once_post_training_skips_stable_gate(monkeypatch: pytest.MonkeyPatch) -> None:
    sync, captured = _make_periodic_sync(monkeypatch)
    sync.sync_once(label="post-training")
    assert captured == [False]


def test_periodic_sync_final_sync_skips_stable_gate(monkeypatch: pytest.MonkeyPatch) -> None:
    sync, captured = _make_periodic_sync(monkeypatch)
    sync.stop(final_sync=True, wait_for_thread=False)
    assert captured == [False]


def test_periodic_sync_stop_without_final_sync_does_not_sync(monkeypatch: pytest.MonkeyPatch) -> None:
    sync, captured = _make_periodic_sync(monkeypatch)
    sync.stop(final_sync=False, wait_for_thread=False)
    assert captured == []


def test_periodic_sync_loop_iteration_uses_stable_gate(monkeypatch: pytest.MonkeyPatch) -> None:
    sync, captured = _make_periodic_sync(monkeypatch)
    # Directly exercise one periodic iteration (the stable gate must stay on here).
    sync._sync_once_non_fatal("periodic sync", require_stable=True)
    assert captured == [True]


def test_periodic_sync_disabled_config_does_not_sync(monkeypatch: pytest.MonkeyPatch) -> None:
    from datapipe_ml.training.specs import TrainingSyncConfig
    from datapipe_ml.training.sync import PeriodicTrainingSync

    captured: list[bool] = []
    monkeypatch.setattr(
        sync_module,
        "sync_training_tree_and_manifest",
        lambda *, require_stable=True, **_kwargs: captured.append(require_stable),
    )

    sync = PeriodicTrainingSync(
        src="/src",
        dst="/dst",
        config=TrainingSyncConfig(enabled=False),
        model_id="model-a",
    )
    sync.sync_once(label="post-training")
    assert captured == []


def test_copy_tree_best_effort_propagates_require_stable(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: list[bool] = []
    monkeypatch.setattr(
        sync_module,
        "copy_tree_snapshot",
        lambda _src, _dst, *, require_stable=True: captured.append(require_stable),
    )
    sync_module.copy_tree_best_effort("/src", "/dst", retries=1, retry_sleep_s=0, require_stable=False)
    assert captured == [False]


def test_copy_tree_best_effort_retries_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    attempts = {"count": 0}

    def flaky(_src, _dst, *, require_stable=True):
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise RuntimeError("transient")

    monkeypatch.setattr(sync_module, "copy_tree_snapshot", flaky)
    monkeypatch.setattr(sync_module.time, "sleep", lambda *_a, **_k: None)

    sync_module.copy_tree_best_effort("/src", "/dst", retries=3, retry_sleep_s=0, require_stable=False)
    assert attempts["count"] == 3


def test_copy_tree_best_effort_raises_after_exhausting_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    def always_fail(_src, _dst, *, require_stable=True):
        raise RuntimeError("permanent")

    monkeypatch.setattr(sync_module, "copy_tree_snapshot", always_fail)
    monkeypatch.setattr(sync_module.time, "sleep", lambda *_a, **_k: None)

    with pytest.raises(RuntimeError, match="permanent"):
        sync_module.copy_tree_best_effort("/src", "/dst", retries=2, retry_sleep_s=0)


def test_verify_manifest_checkpoint_rejects_truncated_checkpoint_after_interrupt(tmp_path: Path) -> None:
    checkpoint = tmp_path / "weights" / "epoch1.pt"
    checkpoint.parent.mkdir(parents=True)
    checkpoint.write_bytes(b"full-checkpoint")

    entry = TrainingCheckpointEntry(
        path=str(checkpoint),
        epoch=1,
        size=len(b"full-checkpoint"),
        mtime=1.0,
    )

    assert verify_manifest_checkpoint(entry) is True

    checkpoint.write_bytes(b"truncated")

    assert verify_manifest_checkpoint(entry) is False

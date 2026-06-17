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

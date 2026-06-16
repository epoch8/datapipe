from __future__ import annotations

import io
import zipfile
from pathlib import Path

import pytest

from datapipe_ml.core.atomic_io import atomic_write_local
from datapipe_ml.training.checkpoint_verify import is_zip_checkpoint_loadable
from tests.helpers.checkpoint_fixtures import write_corrupt_zip_checkpoint, write_valid_zip_checkpoint
from tests.helpers.checkpoint_kill9 import (
    kill9_during_atomic_checkpoint_save,
    kill9_during_non_atomic_checkpoint_save,
)


def test_atomic_write_local_publishes_complete_file(tmp_path: Path) -> None:
    target = tmp_path / "weights" / "last.pt"
    with atomic_write_local(target) as tmp_path:
        write_valid_zip_checkpoint(tmp_path, label="fresh")

    assert is_zip_checkpoint_loadable(str(target))


def test_non_atomic_write_leaves_corrupt_checkpoint_on_interrupt(tmp_path: Path) -> None:
    target = tmp_path / "weights" / "last.pt"
    target.parent.mkdir(parents=True)
    write_valid_zip_checkpoint(target, label="previous")

    with open(target, "wb") as handle:
        handle.write(b"PK\x03\x04")
        handle.flush()

    assert not is_zip_checkpoint_loadable(str(target))


def test_kill9_during_atomic_save_preserves_previous_checkpoint(tmp_path: Path) -> None:
    checkpoint = tmp_path / "weights" / "last.pt"
    checkpoint.parent.mkdir(parents=True)

    kill9_during_atomic_checkpoint_save(checkpoint)

    assert is_zip_checkpoint_loadable(str(checkpoint))
    with zipfile.ZipFile(checkpoint) as archive:
        assert archive.read("checkpoint.txt") == b"previous"


def test_kill9_during_non_atomic_save_leaves_corrupt_checkpoint(tmp_path: Path) -> None:
    checkpoint = tmp_path / "weights" / "last.pt"
    checkpoint.parent.mkdir(parents=True)

    kill9_during_non_atomic_checkpoint_save(checkpoint)

    assert not is_zip_checkpoint_loadable(str(checkpoint))


def test_yolo_resume_falls_back_when_last_checkpoint_is_corrupt(tmp_path: Path) -> None:
    from datapipe_ml.frameworks.yolo.checkpoint_selection import select_yolo_resume_checkpoint
    from datapipe_ml.frameworks.yolo.checkpoint_sync import infer_epoch_from_checkpoint_path as yolo_epoch_for_path
    from datapipe_ml.training.specs import TrainingResumeConfig
    from datapipe_ml.training.sync import write_checkpoint_manifest

    run_dir = tmp_path / "models" / "model-a"
    weights_dir = run_dir / "weights"
    weights_dir.mkdir(parents=True)
    last = weights_dir / "last.pt"
    epoch2 = weights_dir / "epoch2.pt"
    write_corrupt_zip_checkpoint(last)
    write_valid_zip_checkpoint(epoch2, label="epoch-2")

    manifest_path = write_checkpoint_manifest(
        run_dir=str(run_dir),
        model_id="model-a",
        checkpoint_paths=[str(last), str(epoch2)],
        epoch_for_path=yolo_epoch_for_path,
    )

    selected = select_yolo_resume_checkpoint(
        manifest_path=manifest_path,
        config=TrainingResumeConfig(continue_train_failed_models=True, min_completed_epochs=1, checkpoint="last"),
    )

    assert selected is not None
    assert selected.path == str(epoch2)
    assert selected.epoch == 2


def test_tf_resume_falls_back_when_last_checkpoint_is_corrupt(tmp_path: Path) -> None:
    from datapipe_ml.frameworks.tensorflow.checkpoint_selection import select_tf_resume_checkpoint
    from datapipe_ml.frameworks.tensorflow.checkpoint_sync import infer_epoch_from_checkpoint_path as tf_epoch_for_path
    from datapipe_ml.training.specs import TrainingResumeConfig
    from datapipe_ml.training.sync import write_checkpoint_manifest

    run_dir = tmp_path / "model-a"
    run_dir.mkdir()
    last = run_dir / "002__last.keras"
    metric = run_dir / "001__train_precision_0.10_train_recall_0.20__val_precision_0.30_val_recall_0.40_val_f1_score_0.50.keras"
    write_corrupt_zip_checkpoint(last)
    write_valid_zip_checkpoint(metric, label="metric")

    manifest_path = write_checkpoint_manifest(
        run_dir=str(run_dir),
        model_id="model-a",
        checkpoint_paths=[str(last), str(metric)],
        epoch_for_path=tf_epoch_for_path,
    )

    selected = select_tf_resume_checkpoint(
        manifest_path=manifest_path,
        config=TrainingResumeConfig(continue_train_failed_models=True, min_completed_epochs=1, checkpoint="last"),
    )

    assert selected is not None
    assert selected.path == str(metric)
    assert selected.epoch == 1


def test_tf_style_checkpoint_survives_kill9_before_replace(tmp_path: Path) -> None:
    checkpoint = tmp_path / "remote" / "002__last.keras"
    checkpoint.parent.mkdir(parents=True)
    write_valid_zip_checkpoint(checkpoint, label="previous")

    kill9_during_atomic_checkpoint_save(checkpoint)

    assert is_zip_checkpoint_loadable(str(checkpoint))
    with zipfile.ZipFile(checkpoint) as archive:
        assert archive.read("checkpoint.txt") == b"previous"


def test_torch_save_uses_atomic_checkpoint_io(tmp_path: Path) -> None:
    pytest.importorskip("torch")
    from datapipe_ml.frameworks.yolo.checkpoint_io import atomic_yolo_checkpoint_io
    import torch

    target = tmp_path / "weights" / "epoch1.pt"
    target.parent.mkdir(parents=True)
    with atomic_yolo_checkpoint_io():
        torch.save({"epoch": 0}, target)

    assert is_zip_checkpoint_loadable(str(target))


def test_ultralytics_write_bytes_uses_atomic_checkpoint_io(tmp_path: Path) -> None:
    from datapipe_ml.frameworks.yolo.checkpoint_io import atomic_yolo_checkpoint_io

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("checkpoint.txt", b"ultralytics")
    payload = buffer.getvalue()

    target = tmp_path / "weights" / "last.pt"
    target.parent.mkdir(parents=True)
    with atomic_yolo_checkpoint_io():
        target.write_bytes(payload)

    assert is_zip_checkpoint_loadable(str(target))


def test_atomic_yolo_checkpoint_io_restores_patches(tmp_path: Path) -> None:
    pytest.importorskip("torch")
    from datapipe_ml.frameworks.yolo.checkpoint_io import atomic_yolo_checkpoint_io
    import torch
    from pathlib import Path as StdPath

    original_torch_save = torch.save
    original_write_bytes = StdPath.write_bytes
    with atomic_yolo_checkpoint_io():
        assert torch.save is not original_torch_save
        assert StdPath.write_bytes is not original_write_bytes
    assert torch.save is original_torch_save
    assert StdPath.write_bytes is original_write_bytes


def test_yolov5_style_checkpoint_names_use_atomic_torch_save(tmp_path: Path) -> None:
    pytest.importorskip("torch")
    from datapipe_ml.frameworks.yolo.checkpoint_io import atomic_yolo_checkpoint_io
    import torch

    with atomic_yolo_checkpoint_io():
        for name in ("last.pt", "best.pt", "epoch3.pt"):
            target = tmp_path / "weights" / name
            target.parent.mkdir(parents=True, exist_ok=True)
            torch.save({"epoch": 2}, target)
            assert is_zip_checkpoint_loadable(str(target))

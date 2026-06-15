from __future__ import annotations

from datapipe_ml.frameworks.tensorflow.checkpoint_selection import (
    select_best_classification_checkpoint,
    select_tf_resume_checkpoint,
)
from datapipe_ml.training.specs import TrainingResumeConfig
from datapipe_ml.training.sync import write_checkpoint_manifest


def test_select_best_classification_checkpoint_uses_highest_val_f1_score(tmp_path) -> None:
    logdir = tmp_path / "exp"
    logdir.mkdir()
    lower = logdir / "001__train_precision_0.10_train_recall_0.20__val_precision_0.30_val_recall_0.40_val_f1_score_0.50.keras"
    higher = logdir / "002__train_precision_0.20_train_recall_0.30__val_precision_0.40_val_recall_0.50_val_f1_score_0.90.keras"
    lower.write_bytes(b"lower")
    higher.write_bytes(b"higher")

    selected = select_best_classification_checkpoint([str(lower), str(higher)])

    assert selected == str(higher)


def test_select_best_classification_checkpoint_falls_back_to_lexicographic_last(tmp_path) -> None:
    logdir = tmp_path / "exp"
    logdir.mkdir()
    first = logdir / "001.keras"
    second = logdir / "002.keras"
    first.write_bytes(b"first")
    second.write_bytes(b"second")

    selected = select_best_classification_checkpoint([str(first), str(second)])

    assert selected == str(second)


def test_select_tf_resume_checkpoint_uses_highest_epoch_for_last(tmp_path) -> None:
    run_dir = tmp_path / "model-a"
    run_dir.mkdir()
    epoch1 = run_dir / "01__model.keras"
    epoch3 = run_dir / "03__model.keras"
    epoch1.write_bytes(b"1")
    epoch3.write_bytes(b"3")

    manifest_path = write_checkpoint_manifest(
        run_dir=str(run_dir),
        model_id="model-a",
        checkpoint_paths=[str(epoch1), str(epoch3)],
    )

    selected = select_tf_resume_checkpoint(
        manifest_path=manifest_path,
        config=TrainingResumeConfig(continue_train_failed_models=True, min_completed_epochs=1, checkpoint="last"),
    )

    assert selected is not None
    assert selected.path == str(epoch3)


def test_select_tf_resume_checkpoint_uses_best_val_f1_for_best(tmp_path) -> None:
    run_dir = tmp_path / "model-a"
    run_dir.mkdir()
    lower = run_dir / "001__val_f1_score_0.50.keras"
    higher = run_dir / "002__val_f1_score_0.90.keras"
    lower.write_bytes(b"lower")
    higher.write_bytes(b"higher")

    manifest_path = write_checkpoint_manifest(
        run_dir=str(run_dir),
        model_id="model-a",
        checkpoint_paths=[str(lower), str(higher)],
    )

    selected = select_tf_resume_checkpoint(
        manifest_path=manifest_path,
        config=TrainingResumeConfig(continue_train_failed_models=True, min_completed_epochs=1, checkpoint="best"),
    )

    assert selected is not None
    assert selected.path == str(higher)

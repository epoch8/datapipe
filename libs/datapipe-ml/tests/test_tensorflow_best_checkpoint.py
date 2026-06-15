from __future__ import annotations

from datapipe_ml.frameworks.tensorflow.checkpoint_selection import select_best_classification_checkpoint


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

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import pytest

from datapipe_ml.frameworks.yolo.artifacts import (
    yolo_collect_results_generic,
    yolo_finalize_training_output,
    yolo_persisted_exp_folder,
    yolo_remap_collected_model_paths,
)
from datapipe_ml.training.staging import LocalStagingDir


@dataclass
class _CollectResultStub:
    detection_model_id: str
    class_names: List[str]
    epoch: int
    model_path: Optional[str]
    metrics_mAP_0_5: float
    metrics_mAP_0_5_to_0_95: float
    f1_curve_image: Optional[object] = None
    best_threshold: Optional[float] = None
    objects_count: Optional[int] = None


@dataclass
class _ModelPathStub:
    epoch: int
    model_path: Optional[str]


def _write_results_csv(exp_dir: Path, rows: List[str]) -> None:
    exp_dir.mkdir(parents=True, exist_ok=True)
    (exp_dir / "results.csv").write_text("\n".join(["epoch,metrics/mAP50(B),metrics/mAP50-95(B)", *rows]))


_RENAME_MAP = {
    "metrics/mAP50(B)": "metrics_mAP_0_5",
    "metrics/mAP50-95(B)": "metrics_mAP_0_5_to_0_95",
}


def _collect(exp_dir: Path) -> List[_CollectResultStub]:
    return yolo_collect_results_generic(
        exp_folder=str(exp_dir),
        result_cls=_CollectResultStub,
        id_field_name="detection_model_id",
        id_field_value="run-1",
        class_names=["cat"],
        objects_count=1,
        f1_image_field_name="f1_curve_image",
        f1_image=None,
        best_threshold=0.45,
        rename_map=_RENAME_MAP,
        best_metric_col="metrics_mAP_0_5_to_0_95",
        weights_subdir="weights",
    )


def test_yolo_collect_results_resolves_best_and_last_without_epoch_files(tmp_path) -> None:
    exp_dir = tmp_path / "exp"
    weights_dir = exp_dir / "weights"
    weights_dir.mkdir(parents=True)
    (weights_dir / "best.pt").write_bytes(b"best")
    (weights_dir / "last.pt").write_bytes(b"last")

    (exp_dir / "results.csv").write_text(
        "\n".join(
            [
                "epoch,metrics/mAP50(B),metrics/mAP50-95(B)",
                "1,0.10,0.05",
                "2,0.80,0.70",
                "3,0.60,0.50",
            ]
        )
    )

    results = yolo_collect_results_generic(
        exp_folder=str(exp_dir),
        result_cls=_CollectResultStub,
        id_field_name="detection_model_id",
        id_field_value="run-1",
        class_names=["cat"],
        objects_count=1,
        f1_image_field_name="f1_curve_image",
        f1_image=None,
        best_threshold=0.45,
        rename_map={
            "metrics/mAP50(B)": "metrics_mAP_0_5",
            "metrics/mAP50-95(B)": "metrics_mAP_0_5_to_0_95",
        },
        best_metric_col="metrics_mAP_0_5_to_0_95",
        weights_subdir="weights",
    )

    by_epoch = {result.epoch: result for result in results}
    assert set(by_epoch) == {2, 3}
    assert by_epoch[2].model_path.endswith("/weights/best.pt")
    assert by_epoch[3].model_path.endswith("/weights/last.pt")


def test_yolo_collect_results_raises_when_best_epoch_has_no_weight(tmp_path) -> None:
    exp_dir = tmp_path / "exp"
    weights_dir = exp_dir / "weights"
    weights_dir.mkdir(parents=True)
    (weights_dir / "last.pt").write_bytes(b"last")

    (exp_dir / "results.csv").write_text(
        "\n".join(
            [
                "epoch,metrics/mAP50(B),metrics/mAP50-95(B)",
                "1,0.10,0.05",
                "2,0.80,0.70",
                "3,0.60,0.50",
            ]
        )
    )

    with pytest.raises(FileNotFoundError, match="best epoch 2"):
        yolo_collect_results_generic(
            exp_folder=str(exp_dir),
            result_cls=_CollectResultStub,
            id_field_name="detection_model_id",
            id_field_value="run-1",
            class_names=["cat"],
            objects_count=1,
            f1_image_field_name="f1_curve_image",
            f1_image=None,
            best_threshold=0.45,
            rename_map={
                "metrics/mAP50(B)": "metrics_mAP_0_5",
                "metrics/mAP50-95(B)": "metrics_mAP_0_5_to_0_95",
            },
            best_metric_col="metrics_mAP_0_5_to_0_95",
            weights_subdir="weights",
        )


def test_yolo_collect_results_reads_local_when_persisted_mirror_is_incomplete(tmp_path) -> None:
    local_exp = tmp_path / "local" / "exp"
    persisted_exp = tmp_path / "persisted" / "exp"
    local_weights = local_exp / "weights"
    persisted_weights = persisted_exp / "weights"
    local_weights.mkdir(parents=True)
    persisted_weights.mkdir(parents=True)
    (local_weights / "best.pt").write_bytes(b"best")
    (local_weights / "last.pt").write_bytes(b"last")
    (persisted_weights / "last.pt").write_bytes(b"last")

    (local_exp / "results.csv").write_text(
        "\n".join(
            [
                "epoch,metrics/mAP50(B),metrics/mAP50-95(B)",
                "1,0.10,0.05",
                "2,0.80,0.70",
            ]
        )
    )

    results = yolo_collect_results_generic(
        exp_folder=str(local_exp),
        result_cls=_CollectResultStub,
        id_field_name="detection_model_id",
        id_field_value="run-1",
        class_names=["cat"],
        objects_count=1,
        f1_image_field_name="f1_curve_image",
        f1_image=None,
        best_threshold=0.45,
        rename_map={
            "metrics/mAP50(B)": "metrics_mAP_0_5",
            "metrics/mAP50-95(B)": "metrics_mAP_0_5_to_0_95",
        },
        best_metric_col="metrics_mAP_0_5_to_0_95",
        weights_subdir="weights",
    )
    yolo_remap_collected_model_paths(
        results,
        local_exp_root=str(local_exp),
        persisted_exp_root=str(persisted_exp),
    )

    by_epoch = {result.epoch: result for result in results}
    assert by_epoch[2].model_path.endswith("/persisted/exp/weights/best.pt")


def test_yolo_finalize_training_output_copies_local_exp_to_persisted_root(tmp_path) -> None:
    local_exp = tmp_path / "local" / "exp"
    weights_dir = local_exp / "weights"
    weights_dir.mkdir(parents=True)
    (weights_dir / "best.pt").write_bytes(b"best")
    (local_exp / "results.csv").write_text("epoch,loss\n1,0.1\n")

    persisted_root = tmp_path / "persisted"
    yolo_finalize_training_output(
        local_exp,
        persisted_project_dir=str(persisted_root),
        tmp_dir_images_cls=None,
        tmp_dir_project_cls=None,
        tmp_dir_model_cls=None,
    )

    assert (persisted_root / "exp" / "weights" / "best.pt").read_bytes() == b"best"


def test_yolo_persisted_exp_folder_uses_local_exp_name(tmp_path) -> None:
    local_exp = tmp_path / "runs" / "model-abc"
    local_exp.mkdir(parents=True)
    assert yolo_persisted_exp_folder(str(tmp_path / "models"), local_exp).endswith("/models/model-abc")


def test_yolo_persisted_exp_folder_accepts_str_local_exp(tmp_path) -> None:
    result = yolo_persisted_exp_folder(str(tmp_path / "models"), str(tmp_path / "runs" / "exp42"))
    assert result.endswith("/models/exp42")


def test_yolo_persisted_exp_folder_supports_remote_root() -> None:
    result = yolo_persisted_exp_folder("s3://bucket/models", "/local/runs/exp7")
    assert result == "s3://bucket/models/exp7"


# ---------------------------------------------------------------------------
# yolo_remap_collected_model_paths — detailed branch coverage
# ---------------------------------------------------------------------------


def test_remap_rewrites_paths_under_local_prefix() -> None:
    results = [
        _ModelPathStub(epoch=1, model_path="/local/exp/weights/epoch1.pt"),
        _ModelPathStub(epoch=2, model_path="/local/exp/weights/best.pt"),
    ]
    yolo_remap_collected_model_paths(
        results,
        local_exp_root="/local/exp",
        persisted_exp_root="s3://bucket/models/exp",
    )
    assert results[0].model_path == "s3://bucket/models/exp/weights/epoch1.pt"
    assert results[1].model_path == "s3://bucket/models/exp/weights/best.pt"


def test_remap_is_noop_when_local_equals_persisted() -> None:
    results = [_ModelPathStub(epoch=1, model_path="/local/exp/weights/best.pt")]
    yolo_remap_collected_model_paths(
        results,
        local_exp_root="/local/exp",
        persisted_exp_root="/local/exp",
    )
    assert results[0].model_path == "/local/exp/weights/best.pt"


def test_remap_skips_none_and_empty_model_paths() -> None:
    results = [
        _ModelPathStub(epoch=1, model_path=None),
        _ModelPathStub(epoch=2, model_path=""),
    ]
    yolo_remap_collected_model_paths(
        results,
        local_exp_root="/local/exp",
        persisted_exp_root="/persisted/exp",
    )
    assert results[0].model_path is None
    assert results[1].model_path == ""


def test_remap_leaves_paths_outside_local_prefix_untouched() -> None:
    results = [_ModelPathStub(epoch=1, model_path="/other/place/weights/best.pt")]
    yolo_remap_collected_model_paths(
        results,
        local_exp_root="/local/exp",
        persisted_exp_root="/persisted/exp",
    )
    assert results[0].model_path == "/other/place/weights/best.pt"


def test_remap_does_not_rewrite_sibling_prefix_false_positive() -> None:
    # "/local/exp2" must not be treated as being under "/local/exp".
    results = [_ModelPathStub(epoch=1, model_path="/local/exp2/weights/best.pt")]
    yolo_remap_collected_model_paths(
        results,
        local_exp_root="/local/exp",
        persisted_exp_root="/persisted/exp",
    )
    assert results[0].model_path == "/local/exp2/weights/best.pt"


def test_remap_rewrites_exact_prefix_match() -> None:
    results = [_ModelPathStub(epoch=1, model_path="/local/exp")]
    yolo_remap_collected_model_paths(
        results,
        local_exp_root="/local/exp",
        persisted_exp_root="/persisted/exp",
    )
    assert results[0].model_path == "/persisted/exp"


def test_remap_handles_trailing_slash_in_roots() -> None:
    results = [_ModelPathStub(epoch=1, model_path="/local/exp/weights/best.pt")]
    yolo_remap_collected_model_paths(
        results,
        local_exp_root="/local/exp/",
        persisted_exp_root="/persisted/exp/",
    )
    assert results[0].model_path == "/persisted/exp/weights/best.pt"


def test_remap_returns_same_list_object() -> None:
    results = [_ModelPathStub(epoch=1, model_path="/local/exp/weights/best.pt")]
    out = yolo_remap_collected_model_paths(
        results, local_exp_root="/local/exp", persisted_exp_root="/persisted/exp"
    )
    assert out is results


# ---------------------------------------------------------------------------
# yolo_finalize_training_output — detailed branch coverage
# ---------------------------------------------------------------------------


def test_finalize_no_persisted_dir_returns_local_path(tmp_path) -> None:
    local_exp = tmp_path / "exp"
    (local_exp / "weights").mkdir(parents=True)
    (local_exp / "weights" / "best.pt").write_bytes(b"best")

    final = yolo_finalize_training_output(
        local_exp,
        persisted_project_dir=None,
        tmp_dir_images_cls=None,
        tmp_dir_project_cls=None,
        tmp_dir_model_cls=None,
    )
    assert str(final) == str(local_exp)


def test_finalize_cloud_staged_branch_copies_tree(tmp_path) -> None:
    # tmp_dir_project_cls is not None -> project was cloud-staged.
    local_exp = tmp_path / "staged" / "exp"
    (local_exp / "weights").mkdir(parents=True)
    (local_exp / "weights" / "best.pt").write_bytes(b"best")

    persisted_root = tmp_path / "persisted"
    staging = LocalStagingDir(path=tmp_path / "staged")

    final = yolo_finalize_training_output(
        local_exp,
        persisted_project_dir=str(persisted_root),
        tmp_dir_images_cls=None,
        tmp_dir_project_cls=staging,
        tmp_dir_model_cls=None,
    )
    assert (persisted_root / "exp" / "weights" / "best.pt").read_bytes() == b"best"
    assert str(final) == str(persisted_root / "exp")


def test_finalize_local_write_branch_copies_when_dst_differs(tmp_path) -> None:
    local_exp = tmp_path / "local" / "exp"
    (local_exp / "weights").mkdir(parents=True)
    (local_exp / "weights" / "best.pt").write_bytes(b"best")
    (local_exp / "results.csv").write_text("epoch,loss\n1,0.1\n")

    persisted_root = tmp_path / "persisted"
    final = yolo_finalize_training_output(
        local_exp,
        persisted_project_dir=str(persisted_root),
        tmp_dir_images_cls=None,
        tmp_dir_project_cls=None,
        tmp_dir_model_cls=None,
    )
    assert (persisted_root / "exp" / "weights" / "best.pt").read_bytes() == b"best"
    assert (persisted_root / "exp" / "results.csv").read_text() == "epoch,loss\n1,0.1\n"
    assert str(final) == str(persisted_root / "exp")


def test_finalize_local_write_branch_noop_when_local_equals_dst(tmp_path) -> None:
    # When persisted_project_dir already equals the local exp parent, no self-copy.
    persisted_root = tmp_path / "project"
    local_exp = persisted_root / "exp"
    (local_exp / "weights").mkdir(parents=True)
    (local_exp / "weights" / "best.pt").write_bytes(b"best")

    final = yolo_finalize_training_output(
        local_exp,
        persisted_project_dir=str(persisted_root),
        tmp_dir_images_cls=None,
        tmp_dir_project_cls=None,
        tmp_dir_model_cls=None,
    )
    assert (local_exp / "weights" / "best.pt").read_bytes() == b"best"
    assert str(final) == str(local_exp)


def test_finalize_local_write_branch_tolerates_missing_local_dir(tmp_path) -> None:
    local_exp = tmp_path / "missing" / "exp"
    persisted_root = tmp_path / "persisted"

    final = yolo_finalize_training_output(
        local_exp,
        persisted_project_dir=str(persisted_root),
        tmp_dir_images_cls=None,
        tmp_dir_project_cls=None,
        tmp_dir_model_cls=None,
    )
    assert str(final) == str(persisted_root / "exp")
    assert not (persisted_root / "exp").exists()


def test_finalize_cleans_up_all_staging_dirs(tmp_path) -> None:
    local_exp = tmp_path / "exp"
    (local_exp / "weights").mkdir(parents=True)
    (local_exp / "weights" / "best.pt").write_bytes(b"best")

    cleaned: List[str] = []

    class _RecordingStaging(LocalStagingDir):
        def __init__(self, name: str) -> None:
            super().__init__(path=tmp_path / name)
            self._name = name

        def cleanup(self) -> None:
            cleaned.append(self._name)

    yolo_finalize_training_output(
        local_exp,
        persisted_project_dir=None,
        tmp_dir_images_cls=_RecordingStaging("images"),
        tmp_dir_project_cls=_RecordingStaging("project"),
        tmp_dir_model_cls=_RecordingStaging("model"),
    )
    assert set(cleaned) == {"images", "project", "model"}


# ---------------------------------------------------------------------------
# end-to-end: collect locally then remap to persisted weights
# ---------------------------------------------------------------------------


def test_collect_then_remap_detection_save_period_minus_one(tmp_path) -> None:
    # detection scenario: only best.pt/last.pt exist locally; S3 mirror missing best.pt.
    local_exp = tmp_path / "local" / "exp"
    (local_exp / "weights").mkdir(parents=True)
    (local_exp / "weights" / "best.pt").write_bytes(b"best")
    (local_exp / "weights" / "last.pt").write_bytes(b"last")
    _write_results_csv(local_exp, ["1,0.10,0.05", "2,0.80,0.70", "3,0.60,0.50"])

    persisted_exp = tmp_path / "persisted" / "exp"
    results = _collect(local_exp)
    yolo_remap_collected_model_paths(
        results,
        local_exp_root=str(local_exp),
        persisted_exp_root=str(persisted_exp),
    )

    by_epoch = {result.epoch: result for result in results}
    assert by_epoch[2].model_path.endswith("/persisted/exp/weights/best.pt")
    assert by_epoch[3].model_path.endswith("/persisted/exp/weights/last.pt")


def test_collect_keypoints_save_period_one_uses_epoch_files(tmp_path) -> None:
    # keypoints scenario: per-epoch weights exist -> every epoch resolves.
    local_exp = tmp_path / "local" / "exp"
    (local_exp / "weights").mkdir(parents=True)
    for epoch in (1, 2, 3):
        (local_exp / "weights" / f"epoch{epoch}.pt").write_bytes(b"w")
    (local_exp / "weights" / "best.pt").write_bytes(b"best")
    (local_exp / "weights" / "last.pt").write_bytes(b"last")
    _write_results_csv(local_exp, ["1,0.10,0.05", "2,0.80,0.70", "3,0.60,0.50"])

    results = _collect(local_exp)
    by_epoch = {result.epoch: result for result in results}
    assert set(by_epoch) == {1, 2, 3}
    assert by_epoch[1].model_path.endswith("/weights/epoch1.pt")
    assert by_epoch[2].model_path.endswith("/weights/epoch2.pt")

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from datapipe_ml.training.orchestrator import orchestrate
from datapipe_ml.training.runs import status_id_for_attempt, training_run_key, utc_iso, utc_now
from datapipe_ml.training.specs import Algo, PreparedData, TrainContext, TrainingResumeConfig
from datapipe_ml.training.sync import write_checkpoint_manifest


class FakeTable:
    def __init__(self, rows: list[dict[str, Any]] | None = None, primary_keys: list[str] | None = None):
        self.df = pd.DataFrame(rows or [])
        self.primary_keys = primary_keys or []

    def get_data(self, idx=None):  # noqa: ANN001
        if idx is None or self.df.empty or idx.empty:
            return self.df.copy()
        result = self.df
        for column in idx.columns:
            if column in result.columns:
                result = result[result[column].isin(idx[column].tolist())]
        return result.reset_index(drop=True)

    def store_chunk(self, data_df, processed_idx=None, now=None, run_config=None):  # noqa: ANN001
        if data_df is None or data_df.empty:
            return pd.DataFrame()
        data_df = data_df.reset_index(drop=True).copy()
        if self.df.empty:
            self.df = data_df
            return data_df[self.primary_keys] if self.primary_keys else data_df
        keys = [key for key in self.primary_keys if key in data_df.columns and key in self.df.columns]
        if keys:
            keep = self.df.copy()
            for _, row in data_df.iterrows():
                mask = pd.Series([True] * len(keep))
                for key in keys:
                    mask &= keep[key] == row[key]
                keep = keep[~mask]
            self.df = pd.concat([keep, data_df], ignore_index=True)
        else:
            self.df = pd.concat([self.df, data_df], ignore_index=True)
        return data_df[self.primary_keys] if self.primary_keys else data_df

    def delete_by_idx(self, idx, now=None, run_config=None):  # noqa: ANN001
        if self.df.empty or idx is None or idx.empty:
            return
        keep = self.df.copy()
        for _, row in idx.iterrows():
            mask = pd.Series([True] * len(keep))
            for key in idx.columns:
                if key in keep.columns:
                    mask &= keep[key] == row[key]
            keep = keep[~mask]
        self.df = keep.reset_index(drop=True)


@dataclass
class FakeRawResult:
    model_path: str | None = None


class FakeAlgo(Algo):
    train_config_id_col = "train_config_id"
    train_params_col = "train_config__params"
    frozen_created_at_col = None
    images_count_col = None
    model_row_prefix = "model"

    def __init__(self):
        self.launch_calls: list[dict[str, Any]] = []

    def check_accelerator(self, train_params: dict[str, Any]) -> None:
        return None

    def prepare_data(self, ctx: TrainContext, idx) -> PreparedData:
        return PreparedData()

    def build_model_id(self, ctx: TrainContext, idx, train_params: dict[str, Any]) -> str:
        return "new-model"

    def launch_training(self, ctx: TrainContext, idx, model_id: str, train_params: dict[str, Any], data: PreparedData):
        self.launch_calls.append({"model_id": model_id, "train_params": dict(train_params)})
        return FakeRawResult(model_path=str(Path(ctx.models_dir) / model_id / "weights" / "epoch1.pt"))

    def select_best(self, raw_result: FakeRawResult, idx) -> dict[str, Any]:
        assert raw_result.model_path is not None
        return {
            "model_path": raw_result.model_path,
            "class_names": ["class"],
            "score_threshold": 0.5,
            "type_name": "fake",
        }

    def build_model_row(
        self,
        ctx: TrainContext,
        idx,
        model_id: str,
        best: dict[str, Any],
        train_params: dict[str, Any],
    ) -> pd.DataFrame:
        row = {
            **{k: idx.loc[0][k] for k in ctx.model_other_primary_keys},
            ctx.model_id__name: model_id,
            f"{self.model_row_prefix}__model_path": best["model_path"],
            f"{self.model_row_prefix}__class_names": best["class_names"],
        }
        return pd.DataFrame([row])

    def collect_checkpoint_paths(self, raw_result: FakeRawResult) -> list[str]:
        return [raw_result.model_path] if raw_result.model_path is not None else []

    def apply_resume_checkpoint(
        self,
        ctx: TrainContext,
        train_params: dict[str, Any],
        checkpoint_path: str | None,
    ) -> dict[str, Any]:
        updated = dict(train_params)
        updated["resume_checkpoint"] = checkpoint_path
        return updated


def _ctx(
    tmp_path: Path,
    *,
    model_rows=None,
    link_rows=None,
    status_rows=None,
    resume_config=None,
    model_other_primary_keys: list[str] | None = None,
    frozen_rows: list[dict[str, Any]] | None = None,
) -> TrainContext:
    model_other_primary_keys = model_other_primary_keys or []
    return TrainContext(
        models_dir=str(tmp_path / "models"),
        max_within_time="1d",
        tmp_folder=str(tmp_path / "tmp"),
        model_suffix="_suffix",
        dt__model=FakeTable(model_rows, [*model_other_primary_keys, "model_id"]),  # type: ignore[arg-type]
        dt__link=FakeTable(
            link_rows,
            [*model_other_primary_keys, "model_id", "frozen_dataset_id", "train_config_id"],
        ),  # type: ignore[arg-type]
        dt__training_status=FakeTable(status_rows, ["training_status_id"]),  # type: ignore[arg-type]
        dt__frozen_dataset=FakeTable(frozen_rows or [{"frozen_dataset_id": "fd"}], [*model_other_primary_keys, "frozen_dataset_id"]),  # type: ignore[arg-type]
        dt__frozen_dataset__has__image_gt=None,
        dt__train_config=FakeTable(
            [{"train_config_id": "cfg", "train_config__params": {"epochs": 2, "imgsz": 16}}],
            ["train_config_id"],
        ),  # type: ignore[arg-type]
        model_other_primary_keys=model_other_primary_keys,
        model_id__name="model_id",
        frozen_dataset_id__name="frozen_dataset_id",
        resume_config=resume_config,
    )


def _idx() -> pd.DataFrame:
    return pd.DataFrame([{}])


def _run_key() -> str:
    return training_run_key(
        idx=_idx(),
        model_other_primary_keys=[],
        frozen_dataset_id_col="frozen_dataset_id",
        frozen_dataset_id="fd",
        train_config_id_col="train_config_id",
        train_config_id="cfg",
        model_suffix="_suffix",
    )


def _source_idx(source_id: str) -> pd.DataFrame:
    return pd.DataFrame([{"source_id": source_id}])


def test_orchestrator_backfills_completed_status_when_link_exists(tmp_path: Path) -> None:
    ctx = _ctx(
        tmp_path,
        model_rows=[
            {
                "model_id": "trained-model",
                "model__model_path": str(tmp_path / "models" / "trained-model" / "weights" / "best.pt"),
            }
        ],
        link_rows=[{"model_id": "trained-model", "frozen_dataset_id": "fd", "train_config_id": "cfg"}],
    )

    outputs = orchestrate(_idx(), ctx, FakeAlgo())

    assert len(outputs.df__training_status) == 1
    row = outputs.df__training_status.iloc[0]
    assert row["training_status__status"] == "completed"
    assert row["model_id"] == "trained-model"
    assert row["training_status__attempt"] == 0


def test_orchestrator_blocks_active_running_lease(tmp_path: Path) -> None:
    run_key = _run_key()
    ctx = _ctx(
        tmp_path,
        status_rows=[
            {
                "training_status_id": status_id_for_attempt(run_key, 1),
                "training_status__run_key": run_key,
                "training_status__status": "running",
                "training_status__attempt": 1,
                "training_status__lease_expires_at": utc_iso(utc_now().replace(year=utc_now().year + 1)),
            }
        ],
    )

    with pytest.raises(RuntimeError, match="already leased"):
        orchestrate(_idx(), ctx, FakeAlgo())


def test_orchestrator_resumes_stale_running_lease_from_run_dir_manifest(tmp_path: Path) -> None:
    run_key = _run_key()
    run_dir = tmp_path / "models" / "old-model"
    checkpoint = run_dir / "weights" / "epoch6.pt"
    checkpoint.parent.mkdir(parents=True)
    checkpoint.write_bytes(b"checkpoint")
    write_checkpoint_manifest(
        run_dir=str(run_dir),
        model_id="old-model",
        checkpoint_paths=[str(checkpoint)],
    )
    stale_lease = utc_now().replace(year=utc_now().year - 1)
    ctx = _ctx(
        tmp_path,
        status_rows=[
            {
                "training_status_id": status_id_for_attempt(run_key, 1),
                "training_status__run_key": run_key,
                "training_status__status": "running",
                "training_status__attempt": 1,
                "training_status__manifest_path": None,
                "training_status__run_dir": str(run_dir),
                "training_status__lease_expires_at": utc_iso(stale_lease),
                "model_id": "old-model",
            }
        ],
        resume_config=TrainingResumeConfig(continue_train_failed_models=True, max_attempts=3),
    )
    algo = FakeAlgo()

    orchestrate(_idx(), ctx, algo)

    assert algo.launch_calls[0]["model_id"] == "old-model"
    assert algo.launch_calls[0]["train_params"]["resume_checkpoint"] == str(checkpoint)
    assert set(ctx.dt__training_status.df["training_status__attempt"]) == {1, 2}


def test_orchestrator_resumes_failed_status_from_manifest(tmp_path: Path) -> None:
    run_key = _run_key()
    checkpoint = tmp_path / "models" / "old-model" / "weights" / "epoch1.pt"
    checkpoint.parent.mkdir(parents=True)
    checkpoint.write_bytes(b"checkpoint")
    manifest = write_checkpoint_manifest(
        run_dir=str(checkpoint.parent.parent),
        model_id="old-model",
        checkpoint_paths=[str(checkpoint)],
    )
    ctx = _ctx(
        tmp_path,
        status_rows=[
            {
                "training_status_id": status_id_for_attempt(run_key, 1),
                "training_status__run_key": run_key,
                "training_status__status": "failed",
                "training_status__attempt": 1,
                "training_status__manifest_path": manifest,
                "model_id": "old-model",
            }
        ],
        resume_config=TrainingResumeConfig(continue_train_failed_models=True, max_attempts=3),
    )
    algo = FakeAlgo()

    orchestrate(_idx(), ctx, algo)

    assert algo.launch_calls[0]["model_id"] == "old-model"
    assert algo.launch_calls[0]["train_params"]["resume_checkpoint"] == str(checkpoint)
    assert set(ctx.dt__training_status.df["training_status__attempt"]) == {1, 2}


def test_orchestrator_blocks_max_attempts_before_reset_window(tmp_path: Path) -> None:
    run_key = _run_key()
    ctx = _ctx(
        tmp_path,
        status_rows=[
            {
                "training_status_id": status_id_for_attempt(run_key, 3),
                "training_status__run_key": run_key,
                "training_status__status": "failed",
                "training_status__attempt": 3,
                "training_status__finished_at": utc_iso(),
                "model_id": "old-model",
            }
        ],
        resume_config=TrainingResumeConfig(continue_train_failed_models=True, max_attempts=3, reset_attempts_after="1d"),
    )

    with pytest.raises(RuntimeError, match="Max training attempts exceeded"):
        orchestrate(_idx(), ctx, FakeAlgo())


def test_orchestrator_reset_attempts_after_cooldown(tmp_path: Path) -> None:
    run_key = _run_key()
    old_time = utc_now().replace(year=utc_now().year - 1)
    ctx = _ctx(
        tmp_path,
        status_rows=[
            {
                "training_status_id": status_id_for_attempt(run_key, 3),
                "training_status__run_key": run_key,
                "training_status__status": "failed",
                "training_status__attempt": 3,
                "training_status__finished_at": utc_iso(old_time),
                "model_id": "old-model",
            }
        ],
        resume_config=TrainingResumeConfig(continue_train_failed_models=True, max_attempts=3, reset_attempts_after="1d"),
    )

    orchestrate(_idx(), ctx, FakeAlgo())

    assert 1 in set(ctx.dt__training_status.df["training_status__attempt"])


def test_orchestrator_creates_distinct_status_rows_for_source_id_groups(tmp_path: Path) -> None:
    ctx = _ctx(
        tmp_path,
        model_other_primary_keys=["source_id"],
        frozen_rows=[
            {"source_id": "first", "frozen_dataset_id": "fd-first"},
            {"source_id": "second", "frozen_dataset_id": "fd-second"},
        ],
    )

    orchestrate(_source_idx("first"), ctx, FakeAlgo())
    orchestrate(_source_idx("second"), ctx, FakeAlgo())

    statuses = ctx.dt__training_status.df
    assert set(statuses["source_id"]) == {"first", "second"}
    assert len(set(statuses["training_status__run_key"])) == 2
    assert set(statuses["training_status__status"]) == {"completed"}

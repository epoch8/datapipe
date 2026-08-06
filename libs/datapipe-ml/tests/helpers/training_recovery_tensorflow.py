from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from datapipe.types import IndexDF

from tests.helpers.training_recovery import (
    RealRecoveryCase,
    RecoveryTrainStep,
    SmokeRuntime,
    StepConfigureFn,
    TrainKwargsFn,
    _shared_train_runtime_kwargs,
    configure_recovery_steps,
    direct_train_kwargs,
    make_runtime,
)
from tests.helpers.training_smoke import Workdir, classification_freeze_step, classification_train_step

if TYPE_CHECKING:
    from datapipe_ml.frameworks.tensorflow.classification_runner import TF_ClassificationTrainingConfig


def _configure_tf_configs(configs: list[TF_ClassificationTrainingConfig], epochs: int) -> None:
    for config in configs:
        config.epochs = epochs
        config.early_stopping_patience = epochs


def _configure_tf_step(step: RecoveryTrainStep, epochs: int) -> None:
    from datapipe_ml.tasks.classification.train.tensorflow import Train_Tensorflow_ClassificationModel

    assert isinstance(step, Train_Tensorflow_ClassificationModel)
    _configure_tf_configs(step.tf_classification_train_configs, epochs)
    step.clean_checkpoints_after_train = False


def tensorflow_step_configure() -> dict[type, StepConfigureFn]:
    from datapipe_ml.tasks.classification.train.tensorflow import Train_Tensorflow_ClassificationModel

    return {Train_Tensorflow_ClassificationModel: _configure_tf_step}


def _tf_train_kwargs(runtime: SmokeRuntime, case: RealRecoveryCase, step: RecoveryTrainStep) -> dict[str, object]:
    from datapipe_ml.tasks.classification.train.tensorflow import Train_Tensorflow_ClassificationModel

    assert isinstance(step, Train_Tensorflow_ClassificationModel)
    kwargs = dict(_shared_train_runtime_kwargs(runtime, case, step))
    kwargs.update(
        dict(
            dt__classification_model=runtime.ds.get_table(case.model_table),
            dt__classification_model_is_trained_on_cls_frozen_dataset=runtime.ds.get_table("classification_model_link"),
            classification_model_other_primary_keys=list(),
            classification_model_id__name="classification_model_id",
            classification_frozen_dataset_id__name="classification_frozen_dataset_id",
            clean_checkpoints_after_train=step.clean_checkpoints_after_train,
        )
    )
    return kwargs


def tensorflow_train_kwargs_builders() -> dict[type, TrainKwargsFn]:
    from datapipe_ml.tasks.classification.train.tensorflow import Train_Tensorflow_ClassificationModel

    return {Train_Tensorflow_ClassificationModel: _tf_train_kwargs}


def real_recovery_tensorflow_cases() -> list:
    from datapipe_ml.tasks.classification.train.tensorflow import train_tf_classification_model

    return [
        pytest.param(
            RealRecoveryCase(
                id="tensorflow_classification",
                mode="classification",
                status_table="classification_training_status",
                model_table="classification_model",
                model_id_column="classification_model_id",
                models_subdir="models",
                make_runtime_kwargs=dict(include_classification_gt=True),
                steps_factory=lambda workdir, scratch: [
                    classification_freeze_step(workdir),
                    classification_train_step(
                        workdir,
                        local_scratch=scratch,
                    ),
                ],
                train_fn=train_tf_classification_model,
                input_tables=(
                    "classification_frozen_dataset",
                    "classification_training_request",
                    "classification_frozen_dataset__has__image_gt",
                ),
            ),
            id="tensorflow_classification",
            marks=(pytest.mark.tensorflow,),
        ),
    ]


def recovery_tensorflow_case_by_id(case_id: str) -> RealRecoveryCase:
    for param in real_recovery_tensorflow_cases():
        case = param.values[0]
        if case.id == case_id:
            return case
    raise KeyError(case_id)


def make_recovery_runtime(
    tmp_path: Path, case: RealRecoveryCase, *, working_dir: Workdir | None = None
) -> tuple[SmokeRuntime, list]:
    workdir = working_dir if working_dir is not None else tmp_path
    runtime = make_runtime(tmp_path, working_dir=workdir, **case.make_runtime_kwargs)
    steps = configure_recovery_steps(
        case.steps_factory(workdir, tmp_path),
        extra_configure=tensorflow_step_configure(),
        include_torch_configure=False,
    )
    return runtime, steps


def invoke_real_train_callable_for_backfill(
    runtime: SmokeRuntime,
    case: RealRecoveryCase,
    step: RecoveryTrainStep,
) -> None:
    request_table = runtime.ds.get_table(case.input_tables[1])
    requests = request_table.get_data()
    if requests.empty or "training_request_id" not in requests.columns:
        raise AssertionError(
            f"Expected training requests in {case.input_tables[1]!r} for backfill"
        )
    idx = IndexDF(requests[["training_request_id"]])
    input_dts = [runtime.ds.get_table(name) for name in case.input_tables]
    case.train_fn(
        runtime.ds,
        idx,
        input_dts,
        kwargs=direct_train_kwargs(
            runtime,
            case,
            step,
            extra_builders=tensorflow_train_kwargs_builders(),
            include_torch_builders=False,
        ),
    )

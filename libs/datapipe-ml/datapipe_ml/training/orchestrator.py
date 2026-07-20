import logging
from dataclasses import replace
from traceback import format_exc
from typing import Any, Dict, Optional, Tuple, cast

import pandas as pd
from datapipe.types import IndexDF

from datapipe_ml.core.datapipe import is_frozen_dataset_old
from datapipe_ml.training.runs import (
    TrainingStatus,
    TrainingStatusManager,
    attempts_reset_allowed,
    base_status_row,
    get_active_status_row,
    get_status_row,
    get_status_rows,
    is_training_user_interrupt,
    launcher_type,
    launcher_config_json,
    initial_launcher_state,
    status_id_for_attempt,
    store_status_row,
    training_lease_settings,
    training_run_key,
)
from datapipe_ml.training.sync import (
    PeriodicTrainingSync,
    manifest_path_for_run,
    orchestrator_owns_output_sync,
    plan_orchestrator_output_sync,
    read_checkpoint_manifest,
    remap_path_under_root,
    write_checkpoint_manifest,
)

from .specs import Algo, TrainContext, TrainOutputs

logger = logging.getLogger("datapipe.ml")


def _append_row(df: pd.DataFrame, row: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return row.reset_index(drop=True)
    if row.empty:
        return df.reset_index(drop=True)
    return pd.concat([df, row], ignore_index=True)


def _ensure_chunk_one(idx: IndexDF) -> None:
    if len(idx) not in [0, 1]:
        raise ValueError("Argument chunk_size must be 1 for this transformation")


def _already_trained(ctx: TrainContext, idx_like_df: Optional[IndexDF]) -> Tuple[bool, pd.DataFrame, pd.DataFrame]:
    df__is_trained = ctx.dt__link.get_data(idx=idx_like_df)
    # df__is_trained contains primary keys of the model table; treat it as IndexDF
    df__model = ctx.dt__model.get_data(idx=cast(IndexDF, df__is_trained))
    return (len(df__model) >= 1 and len(df__is_trained) >= 1), df__model, df__is_trained


def _dataset_is_old(ctx: TrainContext, algo: Algo, idx: IndexDF) -> bool:
    if not algo.frozen_created_at_col:
        return False

    df = ctx.dt__frozen_dataset.get_data(idx)
    frozen_dataset_ids = tuple(df.iloc[0][ctx.model_other_primary_keys + [ctx.frozen_dataset_id__name]])
    return is_frozen_dataset_old(
        dt__frozen_dataset=ctx.dt__frozen_dataset,
        frozen_dataset_id__names=ctx.model_other_primary_keys + [ctx.frozen_dataset_id__name],
        frozen_dataset__created_at__name=algo.frozen_created_at_col,
        frozen_dataset_ids=frozen_dataset_ids,
        max_within_time=ctx.max_within_time,
    )


def _backfill_completed_status_if_missing(
    *,
    ctx: TrainContext,
    algo: Algo,
    idx: IndexDF,
    run_key: str,
    frozen_dataset_id: str,
    train_config_id: str,
    df_model_existing: pd.DataFrame,
) -> None:
    if get_status_row(ctx.dt__training_status, run_key) is not None:
        return

    model_row = df_model_existing.iloc[0]
    model_id = str(model_row[ctx.model_id__name])
    model_path_col = f"{algo.model_row_prefix}__model_path"
    run_dir = ""
    if model_path_col in model_row and not pd.isna(model_row[model_path_col]):
        run_dir = algo.run_dir_from_model_path(str(model_row[model_path_col]))

    status_row: dict[str, Any] = dict(
        training_status_id=status_id_for_attempt(run_key, 0),
        training_status__run_key=run_key,
        training_status__launcher_type=launcher_type(ctx.training_launcher_config),
        training_status__launcher_config=launcher_config_json(ctx.training_launcher_config),
        training_status__launcher_state=initial_launcher_state(ctx.training_launcher_config),
        training_status__models_dir=ctx.models_dir,
        training_status__run_dir=run_dir,
        training_status__status=TrainingStatus.COMPLETED.value,
        training_status__started_at=None,
        training_status__finished_at=None,
        training_status__attempt=0,
        training_status__manifest_path=None,
        training_status__error=None,
        training_status__owner_id=None,
        training_status__heartbeat_at=None,
        training_status__lease_expires_at=None,
    )
    status_row[ctx.frozen_dataset_id__name] = frozen_dataset_id
    status_row[algo.train_config_id_col] = train_config_id
    status_row[ctx.model_id__name] = model_id
    for key in ctx.model_other_primary_keys:
        status_row[key] = idx.loc[0][key]
    store_status_row(ctx.dt__training_status, status_row)


def _discover_manifest_path(
    *,
    manifest_path: Optional[str],
    run_dir: str,
    model_id: str,
    models_dir: str,
    training_output_write_dir: Optional[str],
    ctx: TrainContext,
    algo: Algo,
) -> Optional[str]:
    if manifest_path is not None:
        return manifest_path
    discover_run_dir = run_dir
    if training_output_write_dir:
        discover_run_dir = remap_path_under_root(
            run_dir,
            models_dir,
            training_output_write_dir,
        )
    checkpoint_paths = algo.discover_checkpoint_paths_in_run_dir(discover_run_dir)
    if not checkpoint_paths:
        return None
    try:
        return write_checkpoint_manifest(
            run_dir=run_dir,
            model_id=model_id,
            checkpoint_paths=checkpoint_paths,
            local_write_root=training_output_write_dir,
            persisted_root=models_dir if training_output_write_dir else None,
            epoch_for_path=algo.infer_epoch_from_checkpoint_path,
        )
    except Exception:
        logger.exception("Failed to write checkpoint manifest while finalizing training run")
        return None


def _abort_training_run(
    *,
    exc: BaseException,
    status_manager: TrainingStatusManager,
    output_sync: Optional[PeriodicTrainingSync],
    manifest_path: Optional[str],
    run_dir: str,
    model_id: str,
    training_output_write_dir: Optional[str],
    ctx: TrainContext,
    algo: Algo,
) -> None:
    if output_sync is not None:
        interrupted = is_training_user_interrupt(exc)
        output_sync.stop(final_sync=not interrupted, wait_for_thread=not interrupted)
    manifest_path = _discover_manifest_path(
        manifest_path=manifest_path,
        run_dir=run_dir,
        model_id=model_id,
        models_dir=ctx.models_dir,
        training_output_write_dir=training_output_write_dir,
        ctx=ctx,
        algo=algo,
    )
    if is_training_user_interrupt(exc):
        status_manager.mark_interrupted(manifest_path=manifest_path)
    else:
        status_manager.mark_failed(error=format_exc(), manifest_path=manifest_path)


def orchestrate(
    idx: IndexDF,
    ctx: TrainContext,
    algo: Algo,
    *,
    df_train_config: Optional[pd.DataFrame] = None,
    force_training: bool = False,
) -> TrainOutputs:
    """
    Universal orchestrator that delegates model-specific logic to Algo.

    ``df_train_config`` may be supplied to bypass reading ``ctx.dt__train_config``
    (used by the request runner, which builds a compat config from the request
    snapshot). ``force_training=True`` skips the ``max_within_time`` short-circuit
    for manual training requests (spec §11.2).
    """
    _ensure_chunk_one(idx)

    # Fetch ids and configs
    df_fd = ctx.dt__frozen_dataset.get_data(idx)
    if df_train_config is not None:
        df_tc = df_train_config
    elif ctx.dt__train_config is not None:
        df_tc = ctx.dt__train_config.get_data(idx)
    else:
        raise ValueError("orchestrate requires df_train_config or ctx.dt__train_config")
    if len(df_fd) == 0 or len(df_tc) == 0:
        return TrainOutputs(
            ctx.dt__model.get_data(idx),
            ctx.dt__link.get_data(idx),
            pd.DataFrame(),
        )
    if len(df_fd) != 1:
        raise ValueError(
            "orchestrate expects exactly one frozen dataset row; "
            f"got {len(df_fd)} (idx columns={list(idx.columns)!r}). "
            "When training by request id, include frozen_dataset_id on idx."
        )

    train_config_id = df_tc.iloc[0][algo.train_config_id_col]
    train_params: Dict = df_tc.iloc[0][algo.train_params_col]
    if ctx.training_launcher_config is None:
        algo.check_accelerator(train_params)

    frozen_dataset_id = df_fd.iloc[0][ctx.frozen_dataset_id__name]
    run_key = training_run_key(
        idx=idx,
        model_other_primary_keys=ctx.model_other_primary_keys,
        frozen_dataset_id_col=ctx.frozen_dataset_id__name,
        frozen_dataset_id=frozen_dataset_id,
        train_config_id_col=algo.train_config_id_col,
        train_config_id=train_config_id,
        model_suffix=ctx.model_suffix,
    )

    # Already trained?
    idx_like = pd.DataFrame(
        [
            {
                **{k: idx.loc[0][k] for k in ctx.model_other_primary_keys},
                ctx.frozen_dataset_id__name: frozen_dataset_id,
                algo.train_config_id_col: train_config_id,
            }
        ]
    )
    trained, df_model_existing, df_link_existing = _already_trained(ctx, cast(IndexDF, idx_like))
    if trained:
        _backfill_completed_status_if_missing(
            ctx=ctx,
            algo=algo,
            idx=idx,
            run_key=run_key,
            frozen_dataset_id=frozen_dataset_id,
            train_config_id=train_config_id,
            df_model_existing=df_model_existing,
        )
        logger.info(
            f"This dataset and train config are already trained by models {list(df_model_existing[ctx.model_id__name])}. Skipping."
        )
        return TrainOutputs(
            ctx.dt__model.get_data(idx),
            ctx.dt__link.get_data(idx),
            get_status_rows(ctx.dt__training_status, run_key),
        )

    # Dataset age check (skipped when a manual request forces training)
    if not force_training and _dataset_is_old(ctx, algo, idx):
        logger.info(
            f"This dataset {df_fd.iloc[0][ctx.frozen_dataset_id__name]} is older than {ctx.max_within_time}. Skipping."
        )
        return TrainOutputs(
            ctx.dt__model.get_data(idx),
            ctx.dt__link.get_data(idx),
            get_status_rows(ctx.dt__training_status, run_key),
        )

    # Prepare data + build model_id + train + select best.
    # Filter resized images / YOLO labels by imgsz when present so a frozen
    # dataset that was prepared at multiple sizes does not yield multiple
    # labels directories for one training request.
    logger.info("Preparing data")
    data_idx = idx
    imgsz = train_params.get("imgsz") if isinstance(train_params, dict) else None
    if imgsz is not None:
        data_idx = idx.copy()
        size = int(imgsz)
        data_idx["width"] = size
        data_idx["height"] = size
    prep = algo.prepare_data(ctx=ctx, idx=data_idx)
    logger.info("Building model_id")
    existing_status = get_status_row(ctx.dt__training_status, run_key)
    active_status = get_active_status_row(ctx.dt__training_status, run_key)
    if active_status is not None:
        raise RuntimeError(f"Training run {run_key!r} is already leased by another worker")

    attempt = 1
    model_id = algo.build_model_id(ctx=ctx, idx=idx, train_params=train_params)
    resume_checkpoint = None
    resume_enabled = bool(ctx.resume_config and ctx.resume_config.continue_train_failed_models)
    if existing_status is not None:
        previous_attempt = int(existing_status.get("training_status__attempt") or 0)
        attempt = previous_attempt + 1
        if resume_enabled:
            assert ctx.resume_config is not None
            if previous_attempt >= ctx.resume_config.max_attempts and not attempts_reset_allowed(
                existing_status, ctx.resume_config
            ):
                error = f"Max training attempts exceeded for training run {run_key!r}"
                failed_row = dict(existing_status)
                failed_row["training_status__status"] = TrainingStatus.FAILED.value
                failed_row["training_status__error"] = error
                ctx.dt__training_status.store_chunk(pd.DataFrame([failed_row]))
                raise RuntimeError(error)
            if attempts_reset_allowed(existing_status, ctx.resume_config):
                attempt = 1
            previous_model_id = existing_status.get(ctx.model_id__name)
            if previous_model_id:
                model_id = str(previous_model_id)
            manifest_path = existing_status.get("training_status__manifest_path")
            if not manifest_path:
                run_dir_for_manifest = existing_status.get("training_status__run_dir")
                if isinstance(run_dir_for_manifest, str) and run_dir_for_manifest:
                    candidate = manifest_path_for_run(run_dir_for_manifest)
                    if read_checkpoint_manifest(candidate) is not None:
                        manifest_path = candidate
            resume_checkpoint = algo.select_resume_checkpoint(
                manifest_path=manifest_path,
                config=ctx.resume_config,
            )
        else:
            # Fresh retry without resume: keep status history but avoid model_id collisions.
            previous_model_id = existing_status.get(ctx.model_id__name)
            if previous_model_id and model_id == str(previous_model_id):
                model_id = f"{model_id}_attempt{attempt}"

    run_dir = algo.run_dir_from_model_id(ctx, model_id)
    heartbeat_interval_s, lease_ttl_s = training_lease_settings(ctx.resume_config)
    running_row = base_status_row(
        run_key=run_key,
        idx=idx,
        model_other_primary_keys=ctx.model_other_primary_keys,
        frozen_dataset_id_col=ctx.frozen_dataset_id__name,
        frozen_dataset_id=frozen_dataset_id,
        train_config_id_col=algo.train_config_id_col,
        train_config_id=train_config_id,
        model_id_col=ctx.model_id__name,
        model_id=model_id,
        models_dir=ctx.models_dir,
        run_dir=run_dir,
        launcher_config=ctx.training_launcher_config,
        attempt=attempt,
        status=TrainingStatus.RUNNING,
        lease_ttl_s=lease_ttl_s,
    )
    manifest_path = None
    output_sync = None
    output_sync_plan = plan_orchestrator_output_sync(
        models_dir=ctx.models_dir,
        tmp_folder=ctx.tmp_folder,
        sync_config=ctx.sync_config,
        owns_output_sync=orchestrator_owns_output_sync(ctx),
    )
    training_output_write_dir = output_sync_plan.local_src if output_sync_plan is not None else None
    train_ctx = replace(ctx, training_output_write_dir=training_output_write_dir) if training_output_write_dir else ctx
    status_manager = TrainingStatusManager(
        dt=ctx.dt__training_status,
        row=running_row,
        heartbeat_interval_s=heartbeat_interval_s,
        lease_ttl_s=lease_ttl_s,
    )
    with status_manager:
        try:
            logger.info("Launching training")
            if output_sync_plan is not None and ctx.sync_config is not None:
                output_sync = PeriodicTrainingSync(
                    src=output_sync_plan.local_src,
                    dst=output_sync_plan.remote_dst,
                    config=ctx.sync_config,
                    model_id=model_id,
                    discover_checkpoints=algo.discover_checkpoint_paths_in_run_dir,
                    epoch_for_path=algo.infer_epoch_from_checkpoint_path,
                    training_run_key=run_key,
                )
                output_sync.start()
            raw_result = algo.launch_training(
                ctx=train_ctx,
                idx=idx,
                model_id=model_id,
                train_params=train_params,
                data=prep,
                resume_checkpoint=resume_checkpoint,
            )
            if output_sync is not None:
                output_sync.sync_once(label="post-training")
            checkpoint_paths = algo.collect_checkpoint_paths(raw_result)
            if checkpoint_paths:
                run_dir = algo.run_dir_from_model_path(checkpoint_paths[0])
                manifest_path = write_checkpoint_manifest(
                    run_dir=run_dir,
                    model_id=model_id,
                    checkpoint_paths=checkpoint_paths,
                    epoch_for_path=algo.infer_epoch_from_checkpoint_path,
                )
            logger.info("Selecting best from training")
            best = algo.select_best(raw_result=raw_result, idx=idx)
        except (KeyboardInterrupt, Exception) as exc:
            _abort_training_run(
                exc=exc,
                status_manager=status_manager,
                output_sync=output_sync,
                manifest_path=manifest_path,
                run_dir=run_dir,
                model_id=model_id,
                training_output_write_dir=training_output_write_dir,
                ctx=ctx,
                algo=algo,
            )
            output_sync = None
            if not is_training_user_interrupt(exc):
                if resume_enabled:
                    assert ctx.resume_config is not None
                    logger.error(
                        "Training FAILED for run %r (attempt %s/%s, model_id=%r): no model was produced. "
                        "continue_train_failed_models=True, so this datapipe step still exits successfully "
                        "and the run is retried on the next pipeline execution; "
                        "RuntimeError is raised only once max_attempts=%s is exhausted.",
                        run_key,
                        attempt,
                        ctx.resume_config.max_attempts,
                        model_id,
                        ctx.resume_config.max_attempts,
                    )
                else:
                    logger.error(
                        "Training FAILED for run %r (attempt %s, model_id=%r): no model was produced.",
                        run_key,
                        attempt,
                        model_id,
                    )
            raise
        finally:
            if output_sync is not None:
                output_sync.stop(final_sync=True)

    logger.info("Building outputs")
    # Build outputs
    df_model_new = ctx.dt__model.get_data(idx)
    df_model_new = _append_row(
        df_model_new,
        algo.build_model_row(ctx=ctx, idx=idx, model_id=model_id, best=best, train_params=train_params),
    )

    df_link_new = ctx.dt__link.get_data(idx)
    df_link_new = _append_row(
        df_link_new,
        algo.build_link_row(
            ctx=ctx, idx=idx, model_id=model_id, train_config_id=train_config_id, train_params=train_params
        ),
    )

    status_manager.mark_completed(run_dir=run_dir, manifest_path=manifest_path)
    return TrainOutputs(df_model_new, df_link_new, get_status_rows(ctx.dt__training_status, run_key))

import logging
from typing import Dict, Optional, Tuple, cast

import pandas as pd
from datapipe.types import IndexDF

from datapipe_ml.core.datapipe import is_frozen_dataset_old

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


def _already_trained(
    ctx: TrainContext, idx_like_df: Optional[IndexDF], model_id__name: str
) -> Tuple[bool, pd.DataFrame, pd.DataFrame]:
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


def orchestrate(idx: IndexDF, ctx: TrainContext, algo: Algo) -> TrainOutputs:
    """
    Universal orchestrator that delegates model-specific logic to Algo.
    """
    _ensure_chunk_one(idx)

    # Fetch ids and configs
    df_fd = ctx.dt__frozen_dataset.get_data(idx)
    df_tc = ctx.dt__train_config.get_data(idx)
    if len(df_fd) == 0 or len(df_tc) == 0:
        return TrainOutputs(ctx.dt__model.get_data(idx), ctx.dt__link.get_data(idx))

    train_config_id = df_tc.iloc[0][algo.train_config_id_col]
    train_params: Dict = df_tc.iloc[0][algo.train_params_col]
    if ctx.training_launcher_config is None:
        algo.check_accelerator(train_params)

    # Already trained?
    idx_like = pd.DataFrame(
        [
            {
                **{k: idx.loc[0][k] for k in ctx.model_other_primary_keys},
                ctx.frozen_dataset_id__name: df_fd.iloc[0][ctx.frozen_dataset_id__name],
                algo.train_config_id_col: train_config_id,
            }
        ]
    )
    trained, df_model_existing, df_link_existing = _already_trained(ctx, cast(IndexDF, idx_like), ctx.model_id__name)
    if trained:
        logger.info(
            f"This dataset and train config are already trained by models {list(df_model_existing[ctx.model_id__name])}. Skipping."
        )
        return TrainOutputs(ctx.dt__model.get_data(idx), ctx.dt__link.get_data(idx))

    # Dataset age check
    if _dataset_is_old(ctx, algo, idx):
        logger.info(
            f"This dataset {df_fd.iloc[0][ctx.frozen_dataset_id__name]} is older than {ctx.max_within_time}. Skipping."
        )
        return TrainOutputs(ctx.dt__model.get_data(idx), ctx.dt__link.get_data(idx))

    # Prepare data + build model_id + train + select best
    logger.info("Preparing data")
    prep = algo.prepare_data(ctx=ctx, idx=idx)
    logger.info("Building model_id")
    model_id = algo.build_model_id(ctx=ctx, idx=idx, train_params=train_params)
    logger.info("Launching training")
    raw_result = algo.launch_training(ctx=ctx, idx=idx, model_id=model_id, train_params=train_params, data=prep)
    logger.info("Selecting best from training")
    best = algo.select_best(raw_result=raw_result, idx=idx)
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

    return TrainOutputs(df_model_new, df_link_new)

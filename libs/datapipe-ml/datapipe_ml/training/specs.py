from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Literal, Optional, Protocol, Tuple, Union

import pandas as pd
from datapipe.datatable import DataTable
from datapipe.types import IndexDF

# ---- Common lightweight DTOs ----


@dataclass
class PreparedData:
    pass


@dataclass(frozen=True)
class TrainingPathMap:
    """Declares one local/cloud path and its corresponding path on the remote worker."""

    local: str
    remote: str


@dataclass
class TrainingLaunchRequest:
    """A serializable request for running a training callable through a launcher.

    ``target`` and everything in ``args`` must be pickle-safe because remote launchers
    serialize this object and execute only ``target(*args)`` on the worker. Datapipe
    state, table objects, and credentials stay on the host.

    ``input_dirs`` are local or cloud paths copied by the host to the remote worker.
    ``output_dirs`` are ``(local_or_cloud_path, remote_path)`` pairs copied back by
    the host, including periodic syncs when supported by the launcher. ``path_rewrites``
    are applied recursively to ``args`` before remote execution and in reverse to
    returned results, so any local paths embedded in args/results should be covered.
    """

    target: Callable[..., Any]
    args: Tuple[Any, ...]
    cluster_suffix: str
    input_dirs: Tuple[str, ...] = ()
    output_dirs: Tuple[Tuple[str, str], ...] = ()
    path_rewrites: Tuple[Tuple[str, str], ...] = ()

    @classmethod
    def from_path_maps(
        cls,
        *,
        target: Callable[..., Any],
        args: Tuple[Any, ...],
        cluster_suffix: str,
        inputs: Tuple[TrainingPathMap, ...] = (),
        outputs: Tuple[TrainingPathMap, ...] = (),
        extra_path_rewrites: Tuple[Tuple[str, str], ...] = (),
    ) -> "TrainingLaunchRequest":
        return cls(
            target=target,
            args=args,
            cluster_suffix=cluster_suffix,
            input_dirs=tuple(item.local for item in inputs),
            output_dirs=tuple((item.local, item.remote) for item in outputs),
            path_rewrites=tuple((item.local, item.remote) for item in (*inputs, *outputs)) + extra_path_rewrites,
        )


@dataclass
class TrainingSyncConfig:
    enabled: bool = False
    interval_s: Optional[int] = 600
    retries: int = 3
    retry_sleep_s: int = 30
    max_consecutive_sync_failures: int = 10


@dataclass
class TrainingResumeConfig:
    continue_train_failed_models: bool = False
    min_completed_epochs: int = 1
    checkpoint: Literal["last", "best"] = "last"
    max_attempts: int = 3
    reset_attempts_after: Optional[str] = "1d"
    lease_ttl_s: int = 600
    heartbeat_interval_s: int = 60


@dataclass(frozen=True)
class TrainingResumeCheckpoint:
    path: str
    epoch: Optional[int] = None


class TrainingLauncher(Protocol):
    def launch(self, request: TrainingLaunchRequest) -> Any: ...


@dataclass
class LocalTrainingLauncher:
    def launch(self, request: TrainingLaunchRequest) -> Any:
        from datapipe_ml.core.multiprocessing import _spawn

        return _spawn(request.target, *request.args)


@dataclass
class SkyVastTrainingLauncherConfig:
    cluster_name: str = "datapipe-ml-train"
    infra: str = "vast"
    instance_type: Optional[str] = None
    accelerators: str = "RTX3060:1"
    idle_minutes: int = 240
    disk_size: str = "10GB"
    cpus: str = "1+"
    memory: str = "16+"
    image_id: str = "vastai/base-image:@vastai-automatic-tag"
    setup_commands: Tuple[str, ...] = ()
    source_install_extras: Tuple[str, ...] = ("torch", "tensorflow")
    source_install_backend: str = "uv"
    source_install_deps: bool = True
    envs: Dict[str, str] = field(default_factory=dict)
    max_reconnect: int = 10
    reconnect_sleep_s: int = 30
    sky_launch_timeout_s: int = 300
    sky_status_timeout_s: int = 120
    ssh_connect_timeout_s: int = 600
    poll_s: int = 10
    run_timeout_s: Optional[int] = None
    output_sync_interval_s: Optional[int] = 600
    transport_retries: int = 3
    transport_retry_sleep_s: int = 30
    transfer_concurrency: int = 8
    stream_logs: bool = False
    down_on_finish: bool = True


TrainingLauncherConfig = Union[LocalTrainingLauncher, SkyVastTrainingLauncherConfig]


def build_training_launcher(config: Optional[TrainingLauncherConfig]) -> TrainingLauncher:
    if config is None:
        return LocalTrainingLauncher()
    if isinstance(config, LocalTrainingLauncher):
        return config
    if isinstance(config, SkyVastTrainingLauncherConfig):
        from datapipe_ml.training.sky_vast.launcher import SkyVastTrainingLauncher

        return SkyVastTrainingLauncher(config)
    raise TypeError(f"Unsupported training launcher config: {type(config)!r}")


@dataclass
class TrainContext:
    # Things that every algo needs to know at runtime
    models_dir: str
    max_within_time: str
    tmp_folder: str
    model_suffix: str
    # data tables (polymorphic naming)
    dt__model: DataTable
    dt__link: DataTable  # "is_trained_on_*"
    dt__training_status: DataTable
    dt__frozen_dataset: DataTable
    dt__frozen_dataset__has__image_gt: Optional[DataTable]
    dt__train_config: Optional[DataTable]
    # column names (polymorphic)
    model_other_primary_keys: List[str]
    model_id__name: str
    frozen_dataset_id__name: str
    training_launcher_config: Optional[TrainingLauncherConfig] = field(default=None, kw_only=True)
    sync_config: Optional[TrainingSyncConfig] = field(default=None, kw_only=True)
    resume_config: Optional[TrainingResumeConfig] = field(default=None, kw_only=True)
    # Local write root for training artifacts; orchestrator mirrors this to models_dir.
    training_output_write_dir: Optional[str] = field(default=None, kw_only=True)


@dataclass
class TrainOutputs:
    df__model: pd.DataFrame
    df__link: pd.DataFrame
    df__training_status: pd.DataFrame


# ---- Algo interface ----


class Algo(ABC):
    """
    Strategy interface for training algorithms.
    """

    # Column/field names in the train_config table
    train_config_id_col: str
    train_params_col: str
    # Frozen dataset created_at & size column names (None if N/A)
    frozen_created_at_col: Optional[str]
    images_count_col: Optional[str]
    # For building final row names like detection_model__*, classification_model__*
    model_row_prefix: str  # e.g. 'detection_model' or 'classification_model' or 'segmentation_model'
    extra_model_metric_map: Dict[str, str] = {}

    @abstractmethod
    def check_accelerator(self, train_params: Dict[str, Any]) -> None:
        """Raise if suitable accelerator is not available."""
        ...

    @abstractmethod
    def prepare_data(self, ctx: TrainContext, idx: IndexDF) -> PreparedData:
        """Read tables, validate layout, compute paths and counts."""
        ...

    @abstractmethod
    def build_model_id(self, ctx: TrainContext, idx: IndexDF, train_params: Dict[str, Any]) -> str:
        """Compose deterministic model_id string."""
        ...

    @abstractmethod
    def launch_training(
        self,
        ctx: TrainContext,
        idx: IndexDF,
        model_id: str,
        train_params: Dict[str, Any],
        data: PreparedData,
        resume_checkpoint: Optional[TrainingResumeCheckpoint] = None,
    ) -> Any:
        """Start (spawn) training process and return raw TrainModelResult."""
        ...

    @abstractmethod
    def select_best(self, raw_result: Any, idx: IndexDF) -> Dict[str, Any]:
        """Return model artifact metadata produced by launch_training."""
        ...

    @abstractmethod
    def build_model_row(
        self, ctx: TrainContext, idx: IndexDF, model_id: str, best: Dict[str, Any], train_params: Dict[str, Any]
    ) -> pd.DataFrame:
        """Compose a single-row DataFrame for the model table."""
        ...

    def run_dir_from_model_id(self, ctx: TrainContext, model_id: str) -> str:
        from pathy import Pathy

        return str(Pathy.fluid(ctx.models_dir) / model_id)

    def run_dir_from_model_path(self, model_path: str) -> str:
        from pathy import Pathy

        return str(Pathy.fluid(model_path).parent)

    def collect_checkpoint_paths(self, raw_result: Any) -> List[str]:
        return []

    def discover_checkpoint_paths_in_run_dir(self, run_dir: str) -> List[str]:
        return []

    def infer_epoch_from_checkpoint_path(self, path: str) -> Optional[int]:
        return None

    def apply_resume_checkpoint(
        self,
        ctx: TrainContext,
        train_params: Dict[str, Any],
        checkpoint_path: Optional[str],
        checkpoint_epoch: Optional[int] = None,
    ) -> Dict[str, Any]:
        return dict(train_params)

    def select_resume_checkpoint(
        self,
        *,
        manifest_path: Optional[str],
        config: Optional[TrainingResumeConfig],
    ) -> Optional[TrainingResumeCheckpoint]:
        from datapipe_ml.training.resume import select_default_resume_checkpoint

        return select_default_resume_checkpoint(manifest_path=manifest_path, config=config)

    def build_link_row(
        self, ctx: TrainContext, idx: IndexDF, model_id: str, train_config_id: str, train_params: Dict[str, Any]
    ) -> pd.DataFrame:
        """Compose a single-row DataFrame for the linking table model_is_trained_on_*."""
        from pandas import DataFrame

        row = {
            **{k: idx.loc[0][k] for k in ctx.model_other_primary_keys},
            ctx.model_id__name: model_id,
            ctx.frozen_dataset_id__name: ctx.dt__frozen_dataset.get_data(idx).iloc[0][ctx.frozen_dataset_id__name],
            self.train_config_id_col: train_config_id,
            self.train_params_col: train_params,
        }
        return DataFrame([row])

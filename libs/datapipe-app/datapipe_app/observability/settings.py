from __future__ import annotations

import inspect
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING, Literal, Optional

from pydantic_settings import BaseSettings, SettingsConfigDict

from datapipe.datatable import DataStore

from datapipe_app.pipeline_binding import bind_pipeline_module, pipeline_module_for

if TYPE_CHECKING:
    from datapipe.compute import DatapipeApp

_SKIP_MODULE_PREFIXES = ("datapipe_app.", "datapipe.", "importlib.", "click.")


class OpsSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DATAPIPE_APP_")

    mode: Literal["agent", "central"] = "agent"
    pipeline_id: Optional[str] = None
    observability_db_url: Optional[str] = None
    show_step_status: bool = False


_active_ops: Optional[OpsSettings] = None


def pipeline_id_from_spec(pipeline_spec: str) -> Optional[str]:
    module, _, _ = pipeline_spec.partition(":")
    if not module:
        return None
    return module.replace(".", "_")


def pipeline_id_from_module(pipeline_module: ModuleType | None) -> Optional[str]:
    if pipeline_module is None:
        return None
    file = pipeline_module.__file__
    if not file:
        return None
    return Path(file).stem


def pipeline_module_from_caller() -> ModuleType | None:
    for frame_info in inspect.stack()[1:]:
        mod = inspect.getmodule(frame_info.frame)
        if not isinstance(mod, ModuleType):
            continue
        if any(mod.__name__.startswith(prefix) for prefix in _SKIP_MODULE_PREFIXES):
            continue
        if mod.__file__:
            return mod
    return None


def resolve_ops_settings(
    *,
    ds: Optional[DataStore] = None,
    pipeline_id: Optional[str] = None,
    pipeline_spec: Optional[str] = None,
    pipeline_module: ModuleType | None = None,
) -> OpsSettings:
    settings = OpsSettings()

    if not settings.pipeline_id:
        module = pipeline_module or pipeline_module_from_caller()
        settings.pipeline_id = (
            pipeline_id
            or pipeline_id_from_module(module)
            or (pipeline_id_from_spec(pipeline_spec) if pipeline_spec else None)
        )

    if not settings.observability_db_url and ds is not None:
        settings.observability_db_url = ds.meta_dbconn.connstr

    return settings


def configure_active_ops(settings: OpsSettings) -> None:
    global _active_ops
    _active_ops = settings


def get_ops_settings() -> OpsSettings:
    if _active_ops is not None:
        return _active_ops
    return OpsSettings()


def bind_pipeline_ops(
    app: DatapipeApp,
    *,
    pipeline_module: ModuleType,
    pipeline_spec: str,
) -> None:
    bind_pipeline_module(app, pipeline_module)

    from datapipe_app.datapipe_api import DatapipeAPI

    if isinstance(app, DatapipeAPI):
        app.refresh_ops_settings(pipeline_module=pipeline_module, pipeline_spec=pipeline_spec)


OPS_SETTINGS = OpsSettings()

from typing import Literal, Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class OpsSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DATAPIPE_APP_")

    mode: Literal["agent", "central"] = "agent"
    pipeline_id: Optional[str] = None
    observability_db_url: Optional[str] = None
    show_step_status: bool = False


def get_ops_settings() -> OpsSettings:
    return OpsSettings()


OPS_SETTINGS = OpsSettings()

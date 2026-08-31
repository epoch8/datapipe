from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class APISettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DATAPIPE_APP_")

    show_step_status: bool = False  # "DATAPIPE_APP_SHOW_STEP_STATUS" in .env
    pipeline_id: Optional[str] = None  # "DATAPIPE_APP_PIPELINE_ID" in .env


API_SETTINGS = APISettings()

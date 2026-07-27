from pydantic_settings import BaseSettings


class DatapipeSettings(BaseSettings):
    model_config = {"env_prefix": "DATAPIPE_"}

    fail_fast: bool = False


settings = DatapipeSettings()

__all__ = ["SkyVastTrainingLauncher", "SkyVastTrainingLauncherConfig"]


def __getattr__(name: str):
    if name == "SkyVastTrainingLauncher":
        from datapipe_ml.training.sky_vast.launcher import SkyVastTrainingLauncher

        return SkyVastTrainingLauncher
    if name == "SkyVastTrainingLauncherConfig":
        from datapipe_ml.training.specs import SkyVastTrainingLauncherConfig

        return SkyVastTrainingLauncherConfig
    raise AttributeError(name)

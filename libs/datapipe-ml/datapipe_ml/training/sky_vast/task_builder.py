from __future__ import annotations

import yaml

from datapipe_ml.training.sky_vast.constants import (
    REMOTE_INPUT,
    REMOTE_OUTPUT,
    REMOTE_SIGNALS,
    REMOTE_SOURCE,
    REMOTE_SOURCE_ARCHIVE,
    REMOTE_VENV,
    REMOTE_WORKER_ENTRYPOINT,
)
from datapipe_ml.training.specs import SkyVastTrainingLauncherConfig


DATAPIPE_CORE_SOURCE_INSTALL_EXTRAS = ("gcsfs", "s3fs", "ray")


def _source_install_commands(
    config: SkyVastTrainingLauncherConfig,
    datapipe_core_install_target: str,
    datapipe_ml_install_target: str,
) -> list[str]:
    dependency_flag = "" if config.source_install_deps else " --no-deps"
    if config.source_install_backend == "pip":
        return [
            f"python3 -m pip install --user{dependency_flag} -e '{datapipe_core_install_target}'",
            f"python3 -m pip install --user{dependency_flag} -e '{datapipe_ml_install_target}'",
        ]
    if config.source_install_backend == "uv":
        return [
            "python3 -m pip install --user uv",
            f"python3 -m uv venv {REMOTE_VENV} --seed",
            f"python3 -m uv pip install --python {REMOTE_VENV / 'bin/python'}{dependency_flag} -e '{datapipe_core_install_target}'",
            f"python3 -m uv pip install --python {REMOTE_VENV / 'bin/python'} --no-sources{dependency_flag} -e '{datapipe_ml_install_target}'",
        ]
    raise ValueError(f"Unsupported source install backend: {config.source_install_backend!r}")


def _worker_python(config: SkyVastTrainingLauncherConfig) -> str:
    if config.source_install_backend == "uv":
        return str(REMOTE_VENV / "bin/python")
    return "python3"


def _datapipe_core_install_target() -> str:
    return f"{REMOTE_SOURCE / 'datapipe-core'}[{','.join(DATAPIPE_CORE_SOURCE_INSTALL_EXTRAS)}]"


def _datapipe_ml_install_target(config: SkyVastTrainingLauncherConfig) -> str:
    source_install_target = str(REMOTE_SOURCE / "datapipe-ml")
    if config.source_install_extras:
        source_install_target = f"{source_install_target}[{','.join(config.source_install_extras)}]"
    return source_install_target


def build_task(config: SkyVastTrainingLauncherConfig):
    import sky.task

    datapipe_core_install_target = _datapipe_core_install_target()
    datapipe_ml_install_target = _datapipe_ml_install_target(config)
    resources = {
        "infra": config.infra,
        "cpus": config.cpus,
        "memory": config.memory,
        "disk_size": config.disk_size,
        "image_id": config.image_id,
        "autostop": {"idle_minutes": config.idle_minutes, "down": True},
    }
    if config.instance_type:
        resources["instance_type"] = config.instance_type
    elif config.accelerators:
        resources["accelerators"] = config.accelerators
    task_config = {
        "resources": resources,
        "envs": {"IDLE_MINUTES": str(config.idle_minutes), **config.envs},
        "setup": "\n".join(
            [
                "set -euo pipefail",
                f"mkdir -p {REMOTE_INPUT} {REMOTE_OUTPUT} {REMOTE_SIGNALS}",
                *config.setup_commands,
                f"touch {REMOTE_SIGNALS / 'VM_BOOT'}",
            ]
        ),
        "run": "\n".join(
            [
                "set -euo pipefail",
                f"while [ ! -f {REMOTE_WORKER_ENTRYPOINT} ]; do sleep 5; done",
                f"rm -rf {REMOTE_SOURCE}",
                f"mkdir -p {REMOTE_SOURCE}",
                f"tar -xzf {REMOTE_SOURCE_ARCHIVE} -C {REMOTE_SOURCE}",
                f"cp {REMOTE_SOURCE / 'README.md'} {REMOTE_SOURCE / '../README.md'}",
                *_source_install_commands(config, datapipe_core_install_target, datapipe_ml_install_target),
                f"{_worker_python(config)} {REMOTE_WORKER_ENTRYPOINT}",
                f"while [ ! -f {REMOTE_SIGNALS / 'SHUTDOWN'} ]; do sleep 5; done",
            ]
        ),
    }
    return sky.task.Task.from_yaml_config(yaml.safe_load(yaml.safe_dump(task_config)))

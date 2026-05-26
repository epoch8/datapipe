from __future__ import annotations

import yaml

from datapipe_ml.training.sky_vast.constants import (
    REMOTE_INPUT,
    REMOTE_OUTPUT,
    REMOTE_SIGNALS,
    REMOTE_SOURCE,
    REMOTE_SOURCE_ARCHIVE,
    REMOTE_WORKER_ENTRYPOINT,
)
from datapipe_ml.training.specs import SkyVastTrainingLauncherConfig


def build_task(config: SkyVastTrainingLauncherConfig):
    import sky.task

    source_install_target = str(REMOTE_SOURCE)
    if config.source_install_extras:
        source_install_target = f"{source_install_target}[{','.join(config.source_install_extras)}]"
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
                f"python3 -m pip install --user -e '{source_install_target}'",
                f"python3 {REMOTE_WORKER_ENTRYPOINT}",
                f"while [ ! -f {REMOTE_SIGNALS / 'SHUTDOWN'} ]; do sleep 5; done",
            ]
        ),
    }
    return sky.task.Task.from_yaml_config(yaml.safe_load(yaml.safe_dump(task_config)))

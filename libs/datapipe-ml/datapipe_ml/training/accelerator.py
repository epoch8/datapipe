from __future__ import annotations

import os
from typing import Any, Mapping


def gpu_training_explicitly_disabled() -> bool:
    """True when CUDA was intentionally hidden via empty visibility env vars."""
    for env_var in ("CUDA_VISIBLE_DEVICES", "NVIDIA_VISIBLE_DEVICES"):
        if env_var in os.environ and os.environ[env_var].strip() == "":
            return True
    return False


def training_requests_cpu(train_params: Mapping[str, Any]) -> bool:
    device = train_params.get("device")
    return isinstance(device, str) and device.strip().lower() == "cpu"


def cpu_training_allowed(train_params: Mapping[str, Any]) -> bool:
    return training_requests_cpu(train_params) or gpu_training_explicitly_disabled()

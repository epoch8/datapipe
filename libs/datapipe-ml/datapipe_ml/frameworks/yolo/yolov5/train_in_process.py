from __future__ import annotations

import importlib.util
import logging
import os
import sys
from pathlib import Path
from typing import List

logger = logging.getLogger(__name__)


def run_yolov5_train_in_process(yolov5_train_script: Path, arguments: List[str]) -> None:
    """Run YOLOv5 training in the current process (same contract as train.py CLI)."""
    os.environ.setdefault("TORCH_FORCE_NO_WEIGHTS_ONLY_LOAD", "1")
    os.environ.setdefault("WANDB_DISABLED", "true")

    module_name = "datapipe_ml_yolov5_train"
    spec = importlib.util.spec_from_file_location(module_name, yolov5_train_script)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load YOLOv5 train module from {yolov5_train_script}")

    train_mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = train_mod
    spec.loader.exec_module(train_mod)

    if not hasattr(train_mod, "parse_opt") or not hasattr(train_mod, "main"):
        raise AttributeError(f"{yolov5_train_script} must expose parse_opt() and main(opt)")

    old_argv = sys.argv
    try:
        sys.argv = [str(yolov5_train_script), *arguments]
        opt = train_mod.parse_opt()
        logger.info("Starting in-process YOLOv5 training with %s", arguments)
        train_mod.main(opt)
    finally:
        sys.argv = old_argv

from __future__ import annotations

import weakref
from types import ModuleType

from datapipe.compute import DatapipeApp

_bound_modules: weakref.WeakKeyDictionary[DatapipeApp, ModuleType] = weakref.WeakKeyDictionary()


def bind_pipeline_module(app: DatapipeApp, pipeline_module: ModuleType) -> None:
    _bound_modules[app] = pipeline_module


def pipeline_module_for(app: DatapipeApp | None) -> ModuleType | None:
    if app is None:
        return None
    return _bound_modules.get(app)

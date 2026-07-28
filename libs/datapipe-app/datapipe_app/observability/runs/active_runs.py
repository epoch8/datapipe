"""In-process registry of active pipeline runs for urgent stop."""

from __future__ import annotations

import threading
from typing import List, Optional

from datapipe.cancel import CancelToken


class ActiveRunRegistry:
    """Map of ``run_id`` → ``CancelToken`` for the Ops stop API."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._tokens: dict[str, CancelToken] = {}

    def register(self, run_id: str, token: CancelToken) -> None:
        with self._lock:
            self._tokens[run_id] = token

    def unregister(self, run_id: str) -> None:
        with self._lock:
            self._tokens.pop(run_id, None)

    def get(self, run_id: str) -> Optional[CancelToken]:
        with self._lock:
            return self._tokens.get(run_id)

    def request_stop(self, run_id: str) -> bool:
        token = self.get(run_id)
        if token is None:
            return False
        token.request_cancel()
        return True

    def active_run_ids(self) -> List[str]:
        with self._lock:
            return list(self._tokens.keys())


_default_registry = ActiveRunRegistry()


def get_active_run_registry() -> ActiveRunRegistry:
    return _default_registry

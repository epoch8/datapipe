from __future__ import annotations

import logging
import signal
import time
from types import FrameType
from typing import Any, Callable, Optional

logger = logging.getLogger("datapipe.ml.sky_vast")


class Timeout:
    def __init__(self, seconds: int, label: str):
        self.seconds = seconds
        self.label = label
        self._previous_handler: Callable[[int, FrameType | None], Any] | int | None = None

    def __enter__(self):
        if self.seconds > 0 and hasattr(signal, "SIGALRM"):
            self._previous_handler = signal.signal(signal.SIGALRM, self._raise_timeout)
            signal.alarm(self.seconds)
        return self

    def __exit__(self, exc_type, exc, tb):
        if self.seconds > 0 and hasattr(signal, "SIGALRM"):
            signal.alarm(0)
            if self._previous_handler is not None:
                signal.signal(signal.SIGALRM, self._previous_handler)
        return False

    def _raise_timeout(self, signum: int, frame: FrameType | None) -> None:
        raise TimeoutError(f"Timed out while {self.label} after {self.seconds}s")


def retry(fn: Callable[[], Any], *, attempts: int, sleep_s: int, label: str) -> Any:
    last_exc: Optional[BaseException] = None
    for attempt in range(attempts + 1):
        try:
            return fn()
        except BaseException as exc:
            last_exc = exc
            if attempt >= attempts:
                break
            logger.info("[%s] retry %s/%s: %s", label, attempt + 1, attempts, exc)
            time.sleep(sleep_s)
    assert last_exc is not None
    raise last_exc

"""Cooperative / urgent cancellation for in-process pipeline runs.

``CancelToken`` is set for the duration of a background run (via contextvar).
Training subprocess spawn loops poll the token and terminate child processes
when cancel is requested.
"""

from __future__ import annotations

import contextvars
import threading
from contextlib import contextmanager
from multiprocessing.process import BaseProcess
from typing import Generator, List, Optional

_current_token: contextvars.ContextVar[Optional["CancelToken"]] = contextvars.ContextVar(
    "datapipe_cancel_token", default=None
)


class RunCancelledError(KeyboardInterrupt):
    """Raised in the run thread after an urgent stop request.

    Subclasses ``KeyboardInterrupt`` so existing training interrupt handlers
    (``is_training_user_interrupt``) treat it as a user stop.
    """


class CancelToken:
    """Thread-safe cancel flag plus registered child processes to kill."""

    def __init__(self) -> None:
        self._event = threading.Event()
        self._lock = threading.Lock()
        self._processes: List[BaseProcess] = []

    def is_cancelled(self) -> bool:
        return self._event.is_set()

    def register_process(self, process: BaseProcess) -> None:
        with self._lock:
            if self._event.is_set():
                _terminate_process(process)
                return
            self._processes.append(process)

    def unregister_process(self, process: BaseProcess) -> None:
        with self._lock:
            self._processes = [p for p in self._processes if p is not process]

    def request_cancel(self) -> None:
        with self._lock:
            self._event.set()
            processes = list(self._processes)
        for process in processes:
            _terminate_process(process)

    def raise_if_cancelled(self) -> None:
        if self._event.is_set():
            raise RunCancelledError("Run stopped by user")


def get_cancel_token() -> Optional[CancelToken]:
    return _current_token.get()


@contextmanager
def cancel_token_scope(token: Optional[CancelToken]) -> Generator[Optional[CancelToken], None, None]:
    if token is None:
        yield None
        return
    reset = _current_token.set(token)
    try:
        yield token
    finally:
        _current_token.reset(reset)


def _terminate_process(process: BaseProcess) -> None:
    try:
        if not process.is_alive():
            return
        process.terminate()
        process.join(timeout=5)
        if process.is_alive():
            process.kill()
            process.join(timeout=2)
    except Exception:
        # Best-effort kill; the wait loop will observe the exit.
        return

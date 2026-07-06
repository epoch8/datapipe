from __future__ import annotations

import contextvars
import logging
import sys
import threading
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Generator, Optional, TextIO

_LOGGER_NAMES = ("datapipe", "datapipe_ml", "ultralytics")

_CAPTURE: contextvars.ContextVar[Optional[_CaptureState]] = contextvars.ContextVar(
    "_run_output_capture", default=None
)
_CAPTURE_STACK: list["_CaptureState"] = []
_CAPTURE_LOCK = threading.Lock()


@dataclass
class _CaptureState:
    buffer: object
    run_id: str
    level: str = "INFO"
    _linebuf: str = ""


def _append_stream_chunk(state: _CaptureState, data: str) -> None:
    if not data:
        return
    if "\r" in data and "\n" not in data:
        parts = [part for part in data.split("\r") if part]
        line = parts[-1] if parts else ""
        if line.strip():
            state.buffer.append(state.run_id, state.level, line)  # type: ignore[attr-defined]
        state._linebuf = ""
        return
    state._linebuf += data
    while "\n" in state._linebuf:
        line, state._linebuf = state._linebuf.split("\n", 1)
        line = line.rstrip("\r")
        if line.strip():
            state.buffer.append(state.run_id, state.level, line)  # type: ignore[attr-defined]


def _flush_stream_buffer(state: _CaptureState) -> None:
    if state._linebuf:
        line = state._linebuf.rstrip("\r")
        state._linebuf = ""
        if line.strip():
            state.buffer.append(state.run_id, state.level, line)  # type: ignore[attr-defined]


class _TeeStream:
    """Mirror writes to the original stream and append complete lines to the run log buffer."""

    def __init__(
        self,
        original: TextIO,
        buffer: object,
        run_id: str,
        *,
        level: str = "INFO",
    ):
        self._original = original
        self._state = _CaptureState(buffer=buffer, run_id=run_id, level=level)

    def write(self, data: str) -> int:
        self._original.write(data)
        _append_stream_chunk(self._state, data)
        return len(data) if data else 0

    def flush(self) -> None:
        self._original.flush()
        _flush_stream_buffer(self._state)

    def fileno(self) -> int:
        return self._original.fileno()

    def isatty(self) -> bool:
        return self._original.isatty()

    def __getattr__(self, name: str):
        return getattr(self._original, name)


class _ContextLogHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        state = _active_capture_state()
        if state is None:
            return
        state.buffer.append(state.run_id, record.levelname, record.getMessage())  # type: ignore[attr-defined]


def _active_capture_state() -> Optional[_CaptureState]:
    state = _CAPTURE.get()
    if state is not None:
        return state
    with _CAPTURE_LOCK:
        if len(_CAPTURE_STACK) == 1:
            return _CAPTURE_STACK[0]
    return None


class _MultiplexStream:
    """Process-global stdout/stderr wrapper that routes output via contextvars."""

    def __init__(self, original: TextIO, *, default_level: str = "INFO"):
        self._original = original
        self._default_level = default_level

    def write(self, data: str) -> int:
        self._original.write(data)
        state = _active_capture_state()
        if state is not None:
            _append_stream_chunk(state, data)
        return len(data) if data else 0

    def flush(self) -> None:
        self._original.flush()
        state = _active_capture_state()
        if state is not None:
            _flush_stream_buffer(state)

    def fileno(self) -> int:
        return self._original.fileno()

    def isatty(self) -> bool:
        return self._original.isatty()

    def __getattr__(self, name: str):
        return getattr(self._original, name)


_real_stdout = sys.stdout
_real_stderr = sys.stderr
_log_handler_installed = False


def _ensure_global_capture() -> None:
    global _log_handler_installed
    if not isinstance(sys.stdout, _MultiplexStream):
        sys.stdout = _MultiplexStream(_real_stdout)  # type: ignore[assignment]
    if not isinstance(sys.stderr, _MultiplexStream):
        sys.stderr = _MultiplexStream(_real_stderr, default_level="INFO")  # type: ignore[assignment]
    if not _log_handler_installed:
        handler = _ContextLogHandler()
        for logger_name in _LOGGER_NAMES:
            logger = logging.getLogger(logger_name)
            if logger_name in ("datapipe", "datapipe_ml"):
                logger.setLevel(logging.INFO)
            logger.addHandler(handler)
        _log_handler_installed = True


@contextmanager
def capture_run_output(
    buffer: object,
    run_id: str,
) -> Generator[None, None, None]:
    """Capture datapipe/datapipe_ml/ultralytics logging and stdout/stderr for a pipeline run.

    Uses contextvars so concurrent runs (e.g. Ray workers or background threads) do not
    clobber each other's stdout/stderr routing.
    """
    _ensure_global_capture()
    state = _CaptureState(buffer=buffer, run_id=run_id)
    token = _CAPTURE.set(state)
    with _CAPTURE_LOCK:
        _CAPTURE_STACK.append(state)
    try:
        yield
    finally:
        _flush_stream_buffer(state)
        with _CAPTURE_LOCK:
            _CAPTURE_STACK.remove(state)
        _CAPTURE.reset(token)


def active_capture_run_id() -> Optional[str]:
    state = _active_capture_state()
    return state.run_id if state is not None else None

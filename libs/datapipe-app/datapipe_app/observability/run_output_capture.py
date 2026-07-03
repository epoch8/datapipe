from __future__ import annotations

import logging
import sys
from contextlib import contextmanager
from typing import Generator, Optional, TextIO

from datapipe_app.observability.log_buffer import RunLogBuffer, RunLogHandler

_LOGGER_NAMES = ("datapipe", "ultralytics")


class _TeeStream:
    """Mirror writes to the original stream and append complete lines to the run log buffer."""

    def __init__(
        self,
        original: TextIO,
        buffer: RunLogBuffer,
        run_id: str,
        *,
        level: str = "INFO",
    ):
        self._original = original
        self._buffer = buffer
        self._run_id = run_id
        self._level = level
        self._linebuf = ""

    def write(self, data: str) -> int:
        self._original.write(data)
        if not data:
            return len(data)
        if "\r" in data and "\n" not in data:
            self._linebuf = data.split("\r")[-1]
            return len(data)
        self._linebuf += data
        while "\n" in self._linebuf:
            line, self._linebuf = self._linebuf.split("\n", 1)
            self._append_line(line)
        return len(data)

    def flush(self) -> None:
        self._original.flush()
        if self._linebuf:
            self._append_line(self._linebuf)
            self._linebuf = ""

    def fileno(self) -> int:
        return self._original.fileno()

    def isatty(self) -> bool:
        return self._original.isatty()

    def __getattr__(self, name: str):
        return getattr(self._original, name)

    def _append_line(self, raw: str) -> None:
        line = raw.rstrip("\r")
        if line.strip():
            self._buffer.append(self._run_id, self._level, line)


@contextmanager
def capture_run_output(
    buffer: RunLogBuffer,
    run_id: str,
) -> Generator[None, None, None]:
    """Capture datapipe/ultralytics logging and stdout/stderr for a pipeline run."""
    handler = RunLogHandler(buffer, run_id)
    attached: list[tuple[logging.Logger, RunLogHandler]] = []
    old_levels: dict[str, int] = {}

    for logger_name in _LOGGER_NAMES:
        logger = logging.getLogger(logger_name)
        old_levels[logger_name] = logger.level
        if logger_name == "datapipe":
            logger.setLevel(logging.INFO)
        logger.addHandler(handler)
        attached.append((logger, handler))

    old_stdout, old_stderr = sys.stdout, sys.stderr
    sys.stdout = _TeeStream(old_stdout, buffer, run_id)  # type: ignore[assignment]
    sys.stderr = _TeeStream(old_stderr, buffer, run_id, level="INFO")  # type: ignore[assignment]

    try:
        yield
    finally:
        sys.stdout.flush()
        sys.stderr.flush()
        sys.stdout, sys.stderr = old_stdout, old_stderr
        for logger, log_handler in attached:
            logger.removeHandler(log_handler)
            logger.setLevel(old_levels.get(logger.name, logging.NOTSET))

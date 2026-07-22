from __future__ import annotations

import logging
import threading
from collections import deque
from dataclasses import dataclass
from typing import Deque, Optional

from datapipe_app.observability.store.db import ObservabilityStore, utc_now

logger = logging.getLogger(__name__)

MAX_LINES_PER_RUN = 10_000
FLUSH_BATCH = 1


@dataclass
class LogLine:
    seq: int
    logged_at: str
    level: str
    message: str


class RunLogBuffer:
    """Ring buffer for run logs; persists to DB in batches."""

    def __init__(self, store: ObservabilityStore):
        self.store = store
        self._lock = threading.Lock()
        self._buffers: dict[str, Deque[LogLine]] = {}
        self._seq: dict[str, int] = {}
        self._pending_flush: dict[str, list[dict]] = {}

    def start_run(self, run_id: str) -> None:
        with self._lock:
            self._buffers[run_id] = deque(maxlen=MAX_LINES_PER_RUN)
            self._seq[run_id] = 0
            self._pending_flush[run_id] = []

    def append(self, run_id: str, level: str, message: str) -> LogLine:
        with self._lock:
            seq = self._seq.get(run_id, 0) + 1
            self._seq[run_id] = seq
            line = LogLine(
                seq=seq,
                logged_at=utc_now().isoformat(),
                level=level,
                message=message,
            )
            buf = self._buffers.setdefault(run_id, deque(maxlen=MAX_LINES_PER_RUN))
            buf.append(line)
            pending = self._pending_flush.setdefault(run_id, [])
            pending.append(
                {
                    "run_id": run_id,
                    "seq": seq,
                    "logged_at": utc_now(),
                    "level": level,
                    "message": message,
                }
            )
            if len(pending) >= FLUSH_BATCH:
                self._flush_locked(run_id)
            return line

    def finish_run(self, run_id: str) -> None:
        with self._lock:
            self._flush_locked(run_id)
            self._buffers.pop(run_id, None)
            self._pending_flush.pop(run_id, None)
            self._seq.pop(run_id, None)

    def flush_all(self) -> None:
        with self._lock:
            for run_id in list(self._pending_flush.keys()):
                self._flush_locked(run_id)

    def _flush_locked(self, run_id: str) -> None:
        pending = self._pending_flush.get(run_id, [])
        if not pending:
            return
        rows = list(pending)
        try:
            self.store.append_run_logs(rows)
        except Exception:
            logger.exception("Failed to persist run logs for %s", run_id)
            raise
        pending.clear()

    def get_lines(self, run_id: str, *, after: int = 0, limit: int = 500) -> list[LogLine]:
        # Pass after/limit through to the store so clients can page beyond the
        # in-memory ring (MAX_LINES_PER_RUN). Previously this always loaded
        # after=0..MAX_LINES_PER_RUN, so seq > 10_000 was unreachable via API.
        db_lines = [
            LogLine(
                seq=r.seq,
                logged_at=r.logged_at.isoformat() if r.logged_at else "",
                level=r.level,
                message=r.message,
            )
            for r in self.store.get_run_logs(run_id, after=after, limit=limit)
        ]
        with self._lock:
            buf = self._buffers.get(run_id)
            if buf is None:
                return db_lines
            by_seq = {ln.seq: ln for ln in db_lines}
            for ln in buf:
                if ln.seq > after:
                    by_seq[ln.seq] = ln
            merged = sorted(by_seq.values(), key=lambda item: item.seq)
            return [ln for ln in merged if ln.seq > after][:limit]

    def get_max_seq(self, run_id: str) -> int:
        db_max = self.store.get_last_log_seq(run_id)
        with self._lock:
            mem_max = self._seq.get(run_id, 0)
        return max(db_max, mem_max)


class RunLogHandler(logging.Handler):
    def __init__(self, buffer: RunLogBuffer, run_id: str):
        super().__init__()
        self.buffer = buffer
        self.run_id = run_id
        self.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            self.buffer.append(self.run_id, record.levelname, msg)
        except Exception:
            self.handleError(record)


_log_buffer: Optional[RunLogBuffer] = None


def get_log_buffer(store: ObservabilityStore) -> RunLogBuffer:
    global _log_buffer
    if _log_buffer is None:
        _log_buffer = RunLogBuffer(store)
    return _log_buffer

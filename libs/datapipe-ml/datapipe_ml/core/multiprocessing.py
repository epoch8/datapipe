import multiprocessing as mp
import os
import queue
import sys
import tempfile
import threading
import time
from typing import Any, Callable, Optional, TextIO


class TrainingSubprocessError(RuntimeError):
    """Raised when a training subprocess exits before returning a queue result."""

    def __init__(self, message: str, *, exitcode: Optional[int] = None):
        super().__init__(message)
        self.exitcode = exitcode


def finish_training_subprocess(queue: mp.Queue, result: Any, *, failed: bool) -> None:
    """Deliver training result to the parent and exit with a meaningful status code."""
    queue.put(result)
    raise SystemExit(1 if failed else 0)


def _redirect_stdio_to_files(stdout_path: str, stderr_path: str) -> None:
    stdout_f = open(stdout_path, "a", buffering=1, encoding="utf-8", errors="replace")
    stderr_f = open(stderr_path, "a", buffering=1, encoding="utf-8", errors="replace")
    os.dup2(stdout_f.fileno(), 1)
    os.dup2(stderr_f.fileno(), 2)
    stdout_f.close()
    stderr_f.close()


def _spawn_wrapper(
    target: Callable[..., Any],
    result_queue: mp.Queue,
    stdout_path: str,
    stderr_path: str,
    *args: Any,
) -> None:
    _redirect_stdio_to_files(stdout_path, stderr_path)
    target(result_queue, *args)


def _tail_log_file(path: str, stream: TextIO, stop: threading.Event) -> None:
    with open(path, "a+", encoding="utf-8", errors="replace") as log_file:
        log_file.seek(0)
        pending = ""
        while True:
            chunk = log_file.read(4096)
            if chunk:
                pending += chunk
                pending = _emit_forwarded_output(pending, stream)
                continue
            if stop.is_set():
                if pending:
                    _emit_forwarded_output(pending + "\n", stream)
                break
            time.sleep(0.05)


def _emit_forwarded_output(pending: str, stream: TextIO) -> str:
    while "\n" in pending:
        line, pending = pending.split("\n", 1)
        if line.strip():
            stream.write(line + "\n")
            stream.flush()
    if "\r" in pending:
        parts = [part for part in pending.split("\r") if part]
        line = parts[-1] if parts else ""
        if line.strip():
            stream.write(line + "\n")
            stream.flush()
        pending = ""
    return pending


def _spawn(target, *args):
    # https://github.com/pytorch/pytorch/issues/3492#issuecomment-522393847
    ctx = mp.get_context("spawn")
    q: mp.Queue = ctx.Queue()
    log_dir = tempfile.mkdtemp(prefix="datapipe-train-stdio-")
    stdout_path = os.path.join(log_dir, "stdout.log")
    stderr_path = os.path.join(log_dir, "stderr.log")
    open(stdout_path, "w", encoding="utf-8").close()
    open(stderr_path, "w", encoding="utf-8").close()

    stop = threading.Event()
    stdout_thread = threading.Thread(
        target=_tail_log_file,
        args=(stdout_path, sys.stdout, stop),
        daemon=True,
    )
    stderr_thread = threading.Thread(
        target=_tail_log_file,
        args=(stderr_path, sys.stderr, stop),
        daemon=True,
    )
    stdout_thread.start()
    stderr_thread.start()

    p = ctx.Process(
        target=_spawn_wrapper,
        args=(target, q, stdout_path, stderr_path, *args),
    )
    p.start()
    try:
        while True:
            try:
                res = q.get(timeout=1)
                break
            except queue.Empty:
                if not p.is_alive():
                    p.join()
                    raise TrainingSubprocessError(
                        "Training subprocess exited before returning a result.",
                        exitcode=p.exitcode,
                    )
        p.join()
        return res
    except KeyboardInterrupt:
        if p.is_alive():
            p.terminate()
            p.join(timeout=5)
            if p.is_alive():
                p.kill()
                p.join()
        raise
    finally:
        if p.is_alive():
            p.join()
        stop.set()
        stdout_thread.join(timeout=5)
        stderr_thread.join(timeout=5)
        for path in (stdout_path, stderr_path):
            try:
                os.remove(path)
            except OSError:
                pass
        try:
            os.rmdir(log_dir)
        except OSError:
            pass

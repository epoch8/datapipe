import multiprocessing as mp
import queue
from typing import Any, Optional


class TrainingSubprocessError(RuntimeError):
    """Raised when a training subprocess exits before returning a queue result."""

    def __init__(self, message: str, *, exitcode: Optional[int] = None):
        super().__init__(message)
        self.exitcode = exitcode


def finish_training_subprocess(queue: mp.Queue, result: Any, *, failed: bool) -> None:
    """Deliver training result to the parent and exit with a meaningful status code."""
    queue.put(result)
    raise SystemExit(1 if failed else 0)


def _spawn(target, *args):
    # https://github.com/pytorch/pytorch/issues/3492#issuecomment-522393847
    ctx = mp.get_context("spawn")
    q: mp.Queue = ctx.Queue()
    p = ctx.Process(target=target, args=(q, *args))
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

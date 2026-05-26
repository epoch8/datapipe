from __future__ import annotations

import base64
import logging
import multiprocessing as mp
import pickle
import queue as queue_module
import sys
from pathlib import Path

ROOT = Path("/workspace/datapipe_ml")
SIGNALS_DIR = ROOT / "signals"
PAYLOAD_PATH = ROOT / "payload.txt"
RESULT_PATH = ROOT / "result.txt"
TRACEBACK_PATH = ROOT / "traceback.txt"

logger = logging.getLogger("datapipe.ml.sky_vast.worker_entrypoint")


def dumps_to_text(value):
    return base64.b64encode(pickle.dumps(value)).decode("ascii")


def loads_from_text(value: str):
    return pickle.loads(base64.b64decode(value.encode("ascii")))


def main() -> int:
    SIGNALS_DIR.mkdir(parents=True, exist_ok=True)
    try:
        print("[sky-vast-worker] loading training payload", flush=True)
        request = loads_from_text(PAYLOAD_PATH.read_text())
        print(
            f"[sky-vast-worker] starting target={request.target!r} "
            f"args={len(request.args)} input_dirs={request.input_dirs} output_dirs={request.output_dirs}",
            flush=True,
        )
        ctx = mp.get_context("spawn")
        result_queue: mp.Queue = ctx.Queue()
        process = ctx.Process(target=request.target, args=(result_queue, *request.args))
        process.start()
        print(f"[sky-vast-worker] spawned training process pid={process.pid}", flush=True)
        while True:
            try:
                result = result_queue.get(timeout=1)
                break
            except queue_module.Empty:
                if process.exitcode is not None:
                    raise RuntimeError(
                        f"Remote training process exited with code {process.exitcode} before returning a result"
                    )
        print(f"[sky-vast-worker] received training result type={type(result).__name__}", flush=True)
        process.join()
        print(f"[sky-vast-worker] training process exitcode={process.exitcode}", flush=True)
        if process.exitcode not in (0, None):
            raise RuntimeError(f"Remote training process exited with code {process.exitcode}")
        RESULT_PATH.write_text(dumps_to_text(result))
        print(f"[sky-vast-worker] wrote result to {RESULT_PATH}", flush=True)
        (SIGNALS_DIR / "TRAIN_DONE").touch()
        print("[sky-vast-worker] signaled TRAIN_DONE", flush=True)
        return 0
    except Exception as exc:
        from traceback_with_variables import format_exc

        details = format_exc(exc)
        TRACEBACK_PATH.write_text(details)
        (SIGNALS_DIR / "SCRIPT_FAILED").touch()
        print(f"[sky-vast-worker] failed: {exc!r}", file=sys.stderr, flush=True)
        print(details, file=sys.stderr, flush=True)
        logger.exception("Remote training failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

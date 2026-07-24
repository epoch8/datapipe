from __future__ import annotations

import multiprocessing as mp

from datapipe_ml.core.multiprocessing import _spawn


def _child_print_and_return(queue: mp.Queue, message: str) -> None:
    print(message, flush=True)
    queue.put("ok")


def test_spawn_forwards_subprocess_stdout(capsys):
    result = _spawn(_child_print_and_return, "training epoch 1/30")
    captured = capsys.readouterr()

    assert result == "ok"
    assert "training epoch 1/30" in captured.out

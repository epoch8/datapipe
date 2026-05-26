from __future__ import annotations


def remote_smoke_process(queue) -> None:
    queue.put({"ok": True})

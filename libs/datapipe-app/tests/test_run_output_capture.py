from __future__ import annotations

import io
import logging
import sys
import threading

from datapipe_app.observability.db import ObservabilityStore
from datapipe_app.observability.log_buffer import RunLogBuffer
from datapipe_app.observability.run_output_capture import _TeeStream, capture_run_output


def test_tee_stream_appends_complete_lines(tmp_path):
    store = ObservabilityStore.from_url(f"sqlite:///{tmp_path / 'obs.db'}")
    buffer = RunLogBuffer(store)
    run_id = "run-1"
    buffer.start_run(run_id)

    original = io.StringIO()
    tee = _TeeStream(original, buffer, run_id)
    tee.write("line one\npartial")
    tee.write(" end\n")
    tee.flush()

    messages = [line.message for line in buffer.get_lines(run_id)]
    assert messages == ["line one", "partial end"]
    assert original.getvalue() == "line one\npartial end\n"


def test_tee_stream_appends_carriage_return_updates(tmp_path):
    store = ObservabilityStore.from_url(f"sqlite:///{tmp_path / 'obs2.db'}")
    buffer = RunLogBuffer(store)
    run_id = "run-1b"
    buffer.start_run(run_id)

    original = io.StringIO()
    tee = _TeeStream(original, buffer, run_id)
    tee.write("  0%|          | 0/7\r")
    tee.write(" 50%|█████     | 3/7\r")
    tee.write("100%|██████████| 7/7\n")

    messages = [line.message for line in buffer.get_lines(run_id)]
    assert "  0%|          | 0/7" in messages
    assert " 50%|█████     | 3/7" in messages
    assert "100%|██████████| 7/7" in messages


def test_capture_run_output_records_logging_and_stdout(tmp_path):
    store = ObservabilityStore.from_url(f"sqlite:///{tmp_path / 'obs.db'}")
    buffer = RunLogBuffer(store)
    run_id = "run-2"
    buffer.start_run(run_id)

    with capture_run_output(buffer, run_id):
        logging.getLogger("datapipe.test").info("from logger")
        logging.getLogger("datapipe_ml.core.files").info("Copying training sync file: a -> b")
        print("from stdout", flush=True)
        sys.stderr.write("from stderr\n")
        sys.stderr.flush()

    messages = [line.message for line in buffer.get_lines(run_id)]
    assert any("from logger" in msg for msg in messages)
    assert any("Copying training sync file" in msg for msg in messages)
    assert "from stdout" in messages
    assert "from stderr" in messages


def test_capture_run_output_isolates_concurrent_threads(tmp_path) -> None:
    store = ObservabilityStore.from_url(f"sqlite:///{tmp_path / 'obs.db'}")
    buffer = RunLogBuffer(store)
    run_a = "run-a"
    run_b = "run-b"
    buffer.start_run(run_a)
    buffer.start_run(run_b)
    barrier = threading.Barrier(2)
    errors: list[str] = []

    def worker(run_id: str, message: str) -> None:
        try:
            with capture_run_output(buffer, run_id):
                barrier.wait(timeout=5)
                print(message, flush=True)
        except Exception as exc:
            errors.append(str(exc))

    threads = [
        threading.Thread(target=worker, args=(run_a, "message-a")),
        threading.Thread(target=worker, args=(run_b, "message-b")),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)

    assert not errors
    messages_a = [line.message for line in buffer.get_lines(run_a)]
    messages_b = [line.message for line in buffer.get_lines(run_b)]
    assert "message-a" in messages_a
    assert "message-b" in messages_b
    assert "message-b" not in messages_a
    assert "message-a" not in messages_b


def test_capture_run_output_teardown_from_different_thread(tmp_path) -> None:
    """Simulates background pipeline runs: enter in request thread, exit in worker thread."""
    store = ObservabilityStore.from_url(f"sqlite:///{tmp_path / 'obs-cross.db'}")
    buffer = RunLogBuffer(store)
    run_id = "run-cross"
    buffer.start_run(run_id)

    cm = capture_run_output(buffer, run_id)
    cm.__enter__()
    errors: list[str] = []

    def worker() -> None:
        try:
            print("worker stdout", flush=True)
            cm.__exit__(None, None, None)
        except Exception as exc:
            errors.append(str(exc))

    thread = threading.Thread(target=worker)
    thread.start()
    thread.join(timeout=5)

    assert not errors
    messages = [line.message for line in buffer.get_lines(run_id)]
    assert "worker stdout" in messages

from __future__ import annotations

import io
import logging
import sys

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


def test_capture_run_output_records_logging_and_stdout(tmp_path):
    store = ObservabilityStore.from_url(f"sqlite:///{tmp_path / 'obs.db'}")
    buffer = RunLogBuffer(store)
    run_id = "run-2"
    buffer.start_run(run_id)

    with capture_run_output(buffer, run_id):
        logging.getLogger("datapipe.test").info("from logger")
        print("from stdout", flush=True)
        sys.stderr.write("from stderr\n")
        sys.stderr.flush()

    messages = [line.message for line in buffer.get_lines(run_id)]
    assert any("from logger" in msg for msg in messages)
    assert "from stdout" in messages
    assert "from stderr" in messages
